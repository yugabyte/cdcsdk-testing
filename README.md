# Setup Credentials for cloud resources
## S3 Access


    mkdir -p ~/.aws
    touch ~/.aws/credentials

Copy credentials from [AWS Management Console](https://aws.amazon.com/blogs/security/aws-single-sign-on-now-enables-command-line-interface-access-for-aws-accounts-using-corporate-credentials/)


Follow instructions to create a profile in `~/.aws/credentials`. Change the
profile to default. The file should look like:


    [default]
    aws_access_key_id=...
    aws_secret_access_key=...
    aws_session_token=...


**Note that these credentials and temporary and you may have to regularly pick
up new credentials**

## Kinesis

Kinesis requires a credentials files similar to the instructions for S3 Access. It also requires the following parameters
set as environment variables

* USERID: UID of the user running tests. Required to mount the credentials file. `USERID=$(id -u)`
* AWS_SHARED_CREDENTIALS_FILE: Path to credentials file. For example `AWS_SHARED_CREDENTIALS_FILE=/home/centos/.aws/credentials`

The following env variables are optional:
* AWS_REGION: Default is `us-west-2`
* AWS_PROFILE: Default is `default`

## Pubsub

The environment variables that have to be set for Google Pub/Sub are:

* USERID: UID of the user running tests. Required to mount the credentials file. `USERID=$(id -u)`
* GOOGLE_APPLICATION_CREDENTIALS: File containing a service account key. Refer to
  [Google Cloud Docs](https://cloud.google.com/docs/authentication/application-default-credentials)
  to obtain the keys
* GCLOUD_PROJECT: Name of the project. For example, `yugabyte`
* SUBSCRIPTION_ID: Name of the subscription For example, `dbserver1.public.test_table-sub`.
  A subscription by this name should be created in the topic `dbserver1.public.test_table-sub`

# Run Release Tests

To run the tests, follow these steps:

    export AWS_ACCESS_KEY_ID='<your-access-key-id>'
    export AWS_SECRET_ACCESS_KEY='<your-secret-access-key>'

    # Start a YugabyteDB instance on you local IP
    ./yugabyted start --advertise_address $(hostname -i)`

    cd code/cdcsdk-testing
    mvn clean verify -PreleaseTests`

    # Run a specific integration test
    mvn verify -Dit.test=HttpIT -DfailIfNoTests=false

## Running tests with Specific Images for CDCSDK server and Kafka Connect

To run tests with specific images for CDCSDK server and Kafka Connect, you can set the following environment variables
```
    export KAFKA_CONNECT_IMAGE = "<image-for-kafka-connect>"
    export CDCSDK_SERVER_IMAGE = "<image-for-CDCSDK-server>"
```
The default image for Kafka connect is ```quay.io/yugabyte/debezium-connector:latest```
and the default image for CDCSDK Server is ```quay.io/yugabyte/cdcsdk-server:latest```

## Running tests with CDCSDK Server as well as Kafka Connect
Tests can be run for CDCSDK Server, Kafka Connect or both by annotating the test.
* To run with only CDCSDK Server ```@ValueSource(strings = UtilStrings.CDC_CLIENT_CDCSDK)```
* To run with only Kafka Connect ```@ValueSource(strings = UtilStrings.CDC_CLIENT_KAFKA_CONNECT)```
* To run with both, one at a time ```@ValueSource(strings = {UtilStrings.CDC_CLIENT_CDCSDK, UtilStrings.CDC_CLIENT_KAFKA_CONNECT })``` (This is the default behaviour)

For example, `PostgresSinkConsumerIT` is parameterized. Running this will first run the test using CDCSDK Server and
Kafka Connect.

## Setup Tests for Hosted Queues

Tests for Pubsub, Eventhub and Kinesis require credentials to be passed in using environment variables.


### Eventhub

    export EVENTHUB_CONNECTIONSTRING=<connection string>
    export EVENTHUB_HUBNAME=<hub name>