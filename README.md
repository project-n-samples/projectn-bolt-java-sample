# AWS Lambda Function in Java for Bolt

Sample AWS Lambda Applications in Java that utilizes [Java SDK for Bolt](https://gitlab.com/projectn-oss/projectn-bolt-java)

These applications can be built using any of the standard Java IDEs 
(including IntelliJ IDEA and Eclipse IDE for Java Developers) using the included project files.

### Requirements

- Java 8.0 or later
- Apache Maven (3.0 or higher) / Gradle (5.0 or higher)
- [Java SDK for Bolt](https://gitlab.com/projectn-oss/projectn-bolt-java)

### Build From Source

#### Maven
* Maven is the recommended way to build and deploy the Java AWS Lambda applications for Bolt.

* Install the Java SDK for Bolt by following instructions given [here](https://gitlab.com/projectn-oss/projectn-bolt-java-sample#maven)

* Download the source and build the deployment package (jar):

```bash
git clone https://gitlab.com/projectn-oss/projectn-bolt-java-sample.git
cd projectn-bolt-java-sample
mvn clean package
```

#### Gradle
* Install the Java SDK for Bolt by following instructions given [here](https://gitlab.com/projectn-oss/projectn-bolt-java-sample#gradle)

* Download the source, in the same root directory containing Java SDK for Bolt, and build the deployment package (zip):

```bash
git clone https://gitlab.com/projectn-oss/projectn-bolt-java-sample.git
cd projectn-bolt-java-sample
gradle buildZip
```

### Deploy

* Deploy the function to AWS Lambda by uploading the deployment package 

* Depending on the build methodology used, replace the <path-to-deployment package> by the following:
  * Maven: target/bolt-java-lambda-demo.jar
  * Gradle: build/distributions/bolt-java-lambda-demo.zip

```bash
cd projectn-bolt-java-sample

aws lambda create-function \
    --function-name <function-name> \
    --runtime java11 \
    --zip-file fileb://<path-to-deployment package> \
    --handler com.projectn.bolt.BoltS3ListObjsV2Handler \
    --role <function-execution-role-ARN> \
    --environment "Variables={BOLT_URL=<Bolt-Service-Url>}" \
    --memory-size 512 \
    --timeout 30
```