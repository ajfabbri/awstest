package com.cloudera.awstest;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 *  Attempt to reproduce CDH-51347 using AWS SDK directly (without Hadoop code).
 */
public class ParallelCopyRepro {

  private static final File CREDS_FILE = new File(System.getProperty("user.home") + "/.aws/awsTestAccount.properties.personal");
  private static final File TEST_FILE = new File("145mb-test-file.dat");
  private static final String BUCKET = "fabbri-dev";
  private static final String KEY_PREFIX = "145mb-test-file-";
  private static final int COPIES = 200;
  static final int MAX_CONNECTIONS = 500;
  static final int MAX_ERROR_RETRIES = 20;
  static final int SOCK_ESTABLISH_TIMEOUT = 50 * 1000;
  static final int SOCK_TIMEOUT = 200 * 1000;
  static final int SOCKET_SEND_BUFFER = 8 * 1024;
  static final int SOCKET_RCV_BUFFER = 8 * 1024;
  static final int NUMBER_ROUNDS = 100;

  private final AWSCredentials creds;
  private final AmazonS3 s3;
  private final TransferManager tm;
  private final ExecutorService
      executorService = Executors.newFixedThreadPool(200);

  public ParallelCopyRepro() throws IOException {
    creds = new PropertiesCredentials(CREDS_FILE);
    s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds))
        .withClientConfiguration(new ClientConfiguration()
          .withMaxConnections(MAX_CONNECTIONS)
            .withConnectionTimeout(SOCK_ESTABLISH_TIMEOUT)
            .withSocketTimeout(SOCK_TIMEOUT)
            .withMaxErrorRetry(MAX_ERROR_RETRIES)
            .withSocketBufferSizeHints(SOCKET_SEND_BUFFER, SOCKET_RCV_BUFFER))
        .build();
    tm = TransferManagerBuilder.standard()
        .withMultipartCopyThreshold(16L * Constants.MB)
        .withMinimumUploadPartSize(8L * Constants.MB)
        .withExecutorFactory(new ExecutorFactory() {
          public ExecutorService newExecutor() {
            return Executors.newFixedThreadPool(100);
          }
        })
        .withS3Client(s3).build();
  }

  public void setup() throws InterruptedException {
    System.out.println("Uploading first copy of file..");
    final PutObjectRequest putFirst =
        new PutObjectRequest(BUCKET, KEY_PREFIX + 0, TEST_FILE);
    s3.putObject(putFirst);
    final CountDownLatch latch = new CountDownLatch(COPIES - 1);
    ExecutorService exec = Executors.newFixedThreadPool(20);
    for (int i = 1; i < COPIES; i++) {
      final int _i = i;
      exec.submit(new Runnable() {
        public void run() {
          try {
            final String dst = KEY_PREFIX + _i;
            s3.copyObject(new CopyObjectRequest(BUCKET, putFirst.getKey(), BUCKET, dst));
            System.out.println("Copied to " + dst);
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            latch.countDown();
          }
        }
      });
    }
    latch.await();
    System.out.println("Objects copied to S3.");
    exec.shutdown();
  }

  public void emptyBucket() {
    ListObjectsV2Request listObjects = new ListObjectsV2Request().withBucketName(BUCKET);

    int deleted = 0;
    while (true) {
      ListObjectsV2Result result = s3.listObjectsV2(listObjects);
      for (S3ObjectSummary summary : result.getObjectSummaries()) {
        s3.deleteObject(new DeleteObjectRequest(BUCKET, summary.getKey()));
        deleted++;
      }
      if (result.getContinuationToken() != null) {
        listObjects.setContinuationToken(result.getContinuationToken());
      } else {
        break;
      }
    }
    System.out.println("Deleted " + deleted + " objects");
  }

  public void doParallelCopies() throws Throwable {
    for (int i = 0; i < NUMBER_ROUNDS; ++i) {
      long startTime = System.currentTimeMillis();
      System.out.println("Doing round " + i + " at time " + startTime);
      doRound(COPIES * i);
      System.out.println("Finished round " + i + " at time " + System.currentTimeMillis());
    }
  }

  public void doRound(int initial) throws Throwable {
//    InterruptedException,
//      IOException {
    final AtomicLong dummy = new AtomicLong();
    List<Future<Void>> futures = new ArrayList<Future<Void>>();
    for (int i = initial; i < initial + COPIES; ++i) {
      final int num = i;
        futures.add(executorService.submit(new Callable<Void>() {
          public Void call() {
            String sourceKey = KEY_PREFIX + num;
            String dstKey = KEY_PREFIX + (num + COPIES);
            System.out.printf("(%d) copy %s -> %s\n", num, sourceKey, dstKey);
            Copy copy = tm.copy(
                new CopyObjectRequest(BUCKET, sourceKey, BUCKET, dstKey));
            copy.addProgressListener(new ProgressListener() {
              public void progressChanged(ProgressEvent progressEvent) {
//                  System.out.printf("(%d) progress %s\n", num, progressEvent);
                dummy.incrementAndGet();
              }
            });
            try {
              copy.waitForCompletion();
            } catch (InterruptedException ie) {
              System.out.println("Interrupted.");
              Thread.currentThread().interrupt();
            } s3.deleteObject(new DeleteObjectRequest(BUCKET, sourceKey));

            System.out.printf("Thread %s finished.\n",
                Thread.currentThread().getId());
            return null;
          }
        }));
    }

    // just using 'dummy' value so compiler doesn't complain
    System.out.printf(
        "Waiting for threads to finish. Had %d progress " + "events so far\n",
        dummy.get());

    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (ExecutionException ee) {
        Throwable t = ee.getCause();
        System.out.println("Execution exception cause: ");
        t.printStackTrace();
        Throwable t2 = t;
        while (t2.getCause() != null) {
          t2 = t2.getCause();
          System.out.println(".. cause: ");
          t2.printStackTrace();
        }
        executorService.shutdownNow();
        throw t2;
      } catch (Exception e) {
        System.out.println(e.getMessage());
        executorService.shutdownNow();
        throw new IOException(e);
      }
    }
    System.out.println("Round completed");
  }

  public static void main(String[] args) throws Throwable {
    ParallelCopyRepro repro = new ParallelCopyRepro();

    repro.emptyBucket();
    repro.setup();
    repro.doParallelCopies();
  }
}
