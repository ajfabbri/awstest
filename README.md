# To Build

mvn clean compile assembly:single

# To Run

## Make a data file to upload
dd if=/dev/urandom of=145mb-test-file.dat bs=1M count=145

## Run the test
java -jar target/aws-test-1.0-SNAPSHOT-jar-with-dependencies.jar 
