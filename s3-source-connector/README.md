# Amazon S3 Storage (S3) source connector for Apache Kafka®

This is a source Apache Kafka Connect connector that takes stored AWS S3 bucket objects and places them onto an Apache Kafka topic.

The connector class name is: `io.aiven.kafka.connect.s3.S3SourceConnector`.


**Table of Contents**

- [How it works](#how-it-works)
- [Data Format](#data-format)
- [Usage](#usage)
- [Configuration](#configuration)
- [Development](#development)


## How it works

The connector connects to Amazon S3 and periodically queries its data
sources. Each object from the s3 response is transformed into a record and
published into the corresponding Kafka topic.

### Requirements

The connector requires Java 11 or Java 17 for development and production.  These are the same versions supported for Kafka Connect development and production.

#### Authorization

The connector needs the following permissions to the specified bucket:
* ``s3:GetObject``
* ``s3:ListObjectsV2``

In case of ``Access Denied`` error, see the [AWS documentation](https://aws.amazon.com/premiumsupport/knowledge-center/S3-troubleshoot-403/).

#### Authentication

To make the connector work, a user has to specify AWS credentials that allow writing to S3.
There are several ways to specify AWS credentials in this connector:

1) Long term credentials.

   It requires both `aws.access.key.id` and `aws.secret.access.key` to be specified.

2) Short term credentials.

   The connector will request a temporary token from the AWS STS service and assume a role from another AWS account.
   It requires `aws.sts.role.arn`, `aws.sts.role.session.name` to be specified.

3) Use default provider chain or custom provider

   If you prefer to use AWS default provider chain, you can leave {`aws.access.key.id` and `aws.secret.access.key`} and
   {`aws.sts.role.arn`, `aws.sts.role.session.name`} blank. In case you prefer to build your own custom
   provider, pass the custom provider class as a parameter to `aws.credential.provider`

It is important not to use both 1 and 2 simultaneously.
Using option 2, it is recommended to specify the S3 bucket region in `aws.s3.region` and the
corresponding AWS STS endpoint in `aws.sts.config.endpoint`. It's better to specify both or none.
It is also important to specify `aws.sts.role.external.id` for the security reason.  The
[AWS Identity and Access Management](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html)
documentation provides details.

### S3 Object key name format

The connector uses the following format for input files (blobs):
`<prefix><filename>`.

`<prefix>`is the optional prefix that can be used, for example, for
subdirectories in the bucket.
`<filename>` is the file name. The connector has the configurable
template for file names.

Set the property `native.start.key` with an object key to begin processing objects from **AFTER** that key.
In the event of a restart the connector will restart from this object key.

The configuration property `file.name.template` is **mandatory**. If not set **no objects will be processed**.

The file name format supports placeholders with variable names of the form: `{{variable_name}}`.  The currently, supported variables are:

  | name | matches | notes |
  | ---- | ------- | ----- |
  | `topic` | a-z, A-Z, 0-9, '-', '_', and '.' one or more times | Ths pattern is the Kafka topic. Once the pattern starts matching it continues until a non-matchable character is encountered or the end of the name is reached. Particular care must be taken to ensure that the topic does not match another part of the file name. If specified, this pattern will specify the topic that the data should be written to. |
  | `partition` |  0-9 one or more times | This pattern is the Kafka partition. If specified, this pattern will specify the partition that the data should be written to. |
  | `start_offset` |  0-9 one or more times | This is the Kafka offset of the first record in the file. |
  | `timestamp` |  0-9 one or more times | This is the timestamp of when the Kafka record was processed by the connector. |

**NOTE:** The `file.name.template` may accidentally match unintended parts of the S3 object key as is noted in the table below.

#### Pattern match examples
 | pattern | matches                                                          | values                                                        |
 | ------- |------------------------------------------------------------------|---------------------------------------------------------------|
 | {{topic}}-{{partition}}-{{start_offset}} | `customer-topic-1-1734445664111.txt`                             | topic=customer-topic, partition=1, start_offset=1734445664111 |
 | {{topic}}-{{partition}}-{{start_offset}} | `22-10-12/customer-topic-1-1734445664111.txt`                    | topic=22, partition=10, start_offset=112                      |
 | {{topic}}/{{partition}}/{{start_offset}} | `customer-topic/1/1734445664111.txt`                             | topic=customer-topic, partition=1, start_offset=1734445664111 |
  | topic/{{topic}}/partition/{{partition}}/startOffset/{{start_offset}} | `topic/customer-topic/partition/1/startOffset/1734445664111.txt` | topic=customer-topic, partition=1, start_offset=1734445664111 |


## Data Format

### Kafka topic names

The Kafka topic name on which the extracted data will be written is specified by one of the following.  The options are checked in the order specified here and the first result is used.

1. The  `topic` configuration file entry.
2. The `{{topic}}` entry in the `file.name.template`.

### Kafka partitions

The Kafka partitions on which the extracted data will be written are specified by one of the following.  The options are checked in the order specified here and the first result is used.

1. The `{{partition}}` entry in the `file.name.template`.
2. A partition by the Kafka producer.  The [producer java documentation](https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html) states: ```If a valid partition number is specified that partition will be used when sending the record. If no partition is specified but a key is present a partition will be chosen using a hash of the key. If neither key nor partition is present a partition will be assigned in a round-robin fashion.```  This connector alwasy produces a key.

### Data File Format

S3 Object files are text files that contain one record per line (i.e.,
they're separated by `\n`) except `PARQUET` format.

There are four types of data format available:

- Complex structure, where file is in format of [JSON lines](https://jsonlines.org/).
  It contains one record per line and each line is a valid JSON object(`jsonl`)

  Configuration: ```input.format=jsonl```.

- Complex structure, where file is a valid avro file with multiple records.

  Configuration: ```input.format=avro```.

- Complex structure, where file is in Apache Parquet file format.

  Configuration: ```input.format=parquet```.
-
- Complex structure, where file is in bytes format.

  Configuration: ```input.format=bytes```.

The connector can output the following fields from records into the
output: the key, the value, the timestamp, the offset and headers. (The set and the order of
output: the key, the value, the timestamp, the offset and headers. The set of
these output fields is configurable.) The field values are separated by comma.

#### JSONL Format example

For example, if we output `key,value,offset,timestamp`, a record line might look like:

```json
 { "key": "k1", "value": "v0", "offset": 1232155, "timestamp":"2020-01-01T00:00:01Z" }
```

org.apache.kafka.connect.json.JsonConverter is used internally to convert this data and make output files human-readable.

**Note:** Value/Key schema will not be presented in output kafka event, even if `value.converter.schemas.enable` property is `true`,
  however, if this is set to true, it has no impact at the moment.

#### Parquet or Avro format example

For example, if we input `key,offset,timestamp,headers,value`, an input - Parquet schema in an S3 object might look like this:
```json
{
    "type": "record", "fields": [
      {"name": "key", "type": "RecordKeySchema"},
      {"name": "offset", "type": "long"},
      {"name": "timestamp", "type": "long"},
      {"name": "headers", "type": "map"},
      {"name": "value", "type": "RecordValueSchema"}
  ]
}
```
where `RecordKeySchema` - a key schema and `RecordValueSchema` - a record value schema.
This means that in case you have the record and key schema like:

Key schema:
```json
{
  "type": "string"
}
```

Record schema:
```json
{
    "type": "record", "fields": [
      {"name": "foo", "type": "string"},
      {"name": "bar", "type": "long"}
  ]
}
```
the final `Avro` schema for `Parquet` is:
```json
{
    "type": "record", "fields": [
      {"name": "key", "type": "string"},
      {"name": "offset", "type": "long"},
      {"name": "timestamp", "type": "long"},
      {"name": "headers", "type": "map", "values": "long"},
      { "name": "value",
        "type": "record",
        "fields": [
          {"name": "foo", "type": "string"},
          {"name": "bar", "type": "long"}
        ]
      }
  ]
}
```
**Note:** The connector works just fine with and without a schema registry.


### Acked Records

When records are delivered to Kafka the Kafka system will send an Ack for each record.  When debug logging is enabled, the acks are noted in the log.
The ack signifies that the record has been received by Kafka but may not yet be written to the `offset topic`.  If the connector stopped and restarted
fast enough it may attempt to read from the `offset topic` before Kafka has written the data.  This is an extremity rare occurrence but will result in
duplicated delivery of some records.

The `offset topic` is a topic that Kafka uses to track the offsets that the connector has sent.  This topic is used when restarting the connector to determine
where in the S3 object stream to start processing.  If an S3 object contains multiple records, for example in a parquet file, the `offset topic` will record which
record within the S3 object was the last one sent.

## Usage

### Connector Configuration

> **Important Note** Since this connector is developed aligning it with S3 sink connector,
> and since version `2.6`, all existing configuration in S3 sink
is deprecated and will be replaced with new one during a certain transition period (within 2-3 releases). Most of the
> configuration parameters remain same.

List of new configuration parameters:
- `aws.access.key.id` - AWS Access Key ID for accessing S3 bucket.
- `aws.secret.access.key` - AWS S3 Secret Access Key.
- `aws.s3.bucket.name` - - Name of an existing bucket for storing the records. Mandatory. See bucket name rules: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html>
- `aws.s3.endpoint` - The endpoint configuration (service endpoint & signing region) to be used for requests.
- `aws.s3.prefix` - The prefix that will be added to the file name in the bucket. Can be used for putting output files into a subdirectory.
- `aws.s3.region` - Name of the region for the bucket used for storing the records. Defaults to `us-east-1`.
- `aws.s3.fetch.buffer.size` - The Size of the buffer in processing S3 Object Keys to ensure slow to upload objects are not missed by Source Connector. Minimum value is 1.
- `aws.sts.role.arn` - AWS role ARN, for cross-account access role instead of `aws.access.key.id` and `aws.secret.access.key`
- `aws.sts.role.external.id` - AWS ExternalId for cross-account access role
- `aws.sts.role.session.name` - AWS session name for cross-account access role
- `aws.sts.role.session.duration` - Session duration for cross-account access role in Seconds. Minimum value - 900.
- `aws.sts.config.endpoint` - AWS STS endpoint for cross-account access role.
- `transformer.max.buffer.size` - [Optional] When using the ByteArrayTransformer you can alter the buffer size from 1 up to 2147483647 default is 4096
- `input.format` - Specify the format of the files being read from S3 supported values are avro, parquet, jsonl, and bytes, bytes is also the default
- `schema.registry.url` [Optional] The url of the schema registry you want to use
- ``

## Configuration

Apache Kafka has produced a users guide that describes [how to run Kafka Connect](https://kafka.apache.org/documentation/#connect_running).  This documentation describes the Connect workers configuration.  They have also produced a good description of [how Kafka Connect resumes after a failure](https://kafka.apache.org/documentation/#connect_resuming) or stoppage.


Below is an example connector configuration with descriptions:

```properties
### Standard connector configuration

## Fill in your values in these:

## These must have exactly these values:

# The Java class for the connector
connector.class=io.aiven.kafka.connect.s3.S3SourceConnector

# Number of worker tasks to run concurrently
tasks.max=1

# The key converter for this connector
key.converter=org.apache.kafka.connect.storage.StringConverter

# The type of data format used to write data to the kafka events.
# The supported values are: `jsonl`, 'avro', `parquet` and 'bytes'.
input.type=jsonl

# The topic to use as output for this connector
topic=topic1

### Connector-specific configuration
### Fill in you values
# AWS Access Key ID
aws.access.key.id=YOUR_AWS_KEY_ID

# AWS Access Secret Key
aws.secret.access.key=YOUR_AWS_SECRET_ACCESS_KEY

#AWS Region
aws.s3.region=us-east-1

#The name of the S3 bucket to use
#Required.
aws.s3.bucket.name=my-bucket

#The prefix of the S3 bucket to use
#Optional.
aws.s3.prefix=file-prefix

#Errors tolerance
# Possible values 'none' or 'all'. Default being 'none'
# Errors are logged and ignored for 'all'
errors.tolerance=none

#AWS S3 fetch Buffer
# Possible values are any value greater than 0. Default being '1000'
aws.s3.fetch.buffer.size=1000
```

We have produced a [complete S3Source configuration option list](S3SourceConfig.md).

### How the AWS S3 API works
The Aws S3 ListObjectsV2 api which the S3 Source Connector uses to list all objects in a bucket,
- For general purpose buckets, ListObjectsV2 returns objects in lexicographical order based on their key names
- Directory bucket - For directory buckets, ListObjectsV2 does not return objects in lexicographical order.
  - Directory buckets **are not supported** in the current release of the S3 Source Connector
The S3 Source connector uses the S3 ListObjectsV2 API with the 'startAfter' token allowing the connector to minimize the number of objects which are returned to the connector to those that it has yet to process.

This has a number of impacts which should be considered:

The first being that when adding Objects to S3 they should be added in lexicographical order,
for example objects should use a date format at the beginning of the key or use the offset value to identify the file when added to S3 from a sink connector.

The second consideration is that when using the source connector that if it is querying for new entries, if the connector process a file which is lexicographical larger and which uploaded to S3 quicker then another file due to
various reasons such as an API error with retries or the file being generally larger, it might be prudent to increase the `aws.s3.fetch.buffer.size` to ensure that the source connector
is always allowing for scenarios where a file is not uploaded in sequence. Any file which has already been processed will not be reprocessed as it is only added to the buffer after the file has completed processing.
A maximum 1,000 results are returned with each query, it may be of benefit to set the `aws.s3.fetch.buffer.size` to this maximum or slightly larger to ensure you do not miss any Objects.

If used in conjunction with the Aiven S3 Sink Connector, it should be possible to reduce this number to as low as '1' if you have one task per partition as the sink connector will only upload one file per partition at a time.



### Retry strategy configuration

#### Apache Kafka connect retry strategy configuration property

- `kafka.retry.backoff.ms` - The retry backoff in milliseconds. This config is used to notify Apache Kafka Connect to retry delivering a message batch or
  performing recovery in case of transient exceptions. Maximum value is `24` hours.

There are four configuration properties to configure retry strategy exists.

#### AWS S3 retry strategy configuration properties

 `aws.s3.backoff.delay.ms` - S3 default base sleep time
for non-throttled exceptions in milliseconds.
Default is `100` ms.
- `aws.s3.backoff.max.delay.ms` - S3 maximum back-off
time before retrying a request in milliseconds.
Default is `20 000` ms.
- `aws.s3.backoff.max.retries` - Maximum retry limit
(if the value is greater than 30, there can be
integer overflow issues during delay calculation).
Default is `3`.

### AWS S3 server side encryption properties

- `aws.s3.sse.algorithm` - The name of the Server-side encryption algorithm to use for uploads. If unset the default SSE-S3 is used.
- To use SSE-S3 set to `AES256` or leave empty
- To use SSE-KMS set to `aws:kms`
- To use DSSE-KMS set to `aws:kms:dsse`
### Connecting to Kafka
A source connector produces events to Kafka and when using SSL with certificates, you must ensure the SSL settings are prefixed with producer to send events to Kafka, but the connector also reads offsets, and configuration topics and so there should also be an entry prefixed with consumer for Connect to know what keystore and truststore to use.

producer.security.protocol=SSL
producer.ssl.truststore.location=\<path\>/client.truststore.jks
producer.ssl.truststore.password=\<your password\>
producer.ssl.keystore.location=\<path\>/client.keystore.p12
producer.ssl.keystore.password=\<your password\>
producer.ssl.key.password=\<your password\>

consumer.security.protocol=SSL
consumer.ssl.truststore.location=\<path\>/client.truststore.jks
consumer.ssl.truststore.password=\<your password\>
consumer.ssl.keystore.location=\<path\>/client.keystore.p12
consumer.ssl.keystore.password=\<your password\>
consumer.ssl.key.password=\<your password\>


## Development

### Developing together with Commons library

This project depends on [Common Module for Apache Kafka Connect](../commons/README.md).

### Integration testing

Integration tests are implemented using JUnit, Gradle and Docker.

To run them, you need:
- Docker installed.

Integration testing doesn't require valid AWS credentials.

To simulate AWS S3 behaviour, tests use [LocalStack](https://github.com/localstack/localstack-java-utils).

In order to run the integration tests, execute from the project root
directory:

```bash
./gradlew clean integrationTest
```

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).

## Trademarks

Apache Kafka, Apache Kafka Connect, Parquet, and Avro are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. AWS and Amazon S3 are a trademarks and property of  Amazon Web Services, Inc. All product and service names used in this website are for identification purposes only and do not imply endorsement.
