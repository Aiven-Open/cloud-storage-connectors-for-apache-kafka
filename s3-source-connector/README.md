# Aiven's S3 Source Connector for Apache Kafka

This is a source Apache Kafka Connect connector that stores AWS S3 bucket objects onto an Apache Kafka topic.

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

The connector requires Java 11 or newer for development and production.

#### Authorization

The connector needs the following permissions to the specified bucket:
* ``s3:GetObject``
* ``s3:ListObjectsV2``

In case of ``Access Denied`` error, see https://aws.amazon.com/premiumsupport/knowledge-center/s3-troubleshoot-403/

#### Authentication

To make the connector work, a user has to specify AWS credentials that allow writing to S3.
There are two ways to specify AWS credentials in this connector:

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
It is also important to specify `aws.sts.role.external.id` for the security reason.
(see some details [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html)).

### File name format

> File name format is tightly related to [Record Grouping](#record-grouping)

The connector uses the following format for input files (blobs):
`<prefix><filename>`.

`<prefix>`is the optional prefix that can be used, for example, for
subdirectories in the bucket.
`<filename>` is the file name. The connector has a fixed
template for file names.

    Fixed template for file : `{{topic}}-{{partition}}-{{start_offset}}`

Example object name : customertopic-00001-1734445664111.txt

## Data Format

Connector class name, in this case: `io.aiven.kafka.connect.s3.AivenKafkaConnectS3SourceConnector`.

### S3 Object Names

S3 connector reads series of files in the specified bucket.
Each object would be of pattern `[<aws.s3.prefix>]<topic>-<partition>-<start_offset>-<extension>`

### Kafka topic names
S3 object keys have format with topic names which would be the target kafka topics.

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

**NB!**

- Value/Key schema will not be presented in output kafka event, even if `value.converter.schemas.enable` property is `true`,
  however, if this is set to true, it has no impact at the moment.

#### Parquet or Avro format example

For example, if we input `key,offset,timestamp,headers,value`, an input - Parquet schema in an s3 object might look like this:
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
**NB!**

- Connector works just fine with and without Schema Registry

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
- `aws.sts.role.arn` - AWS role ARN, for cross-account access role instead of `aws.access.key.id` and `aws.secret.access.key`
- `aws.sts.role.external.id` - AWS ExternalId for cross-account access role
- `aws.sts.role.session.name` - AWS session name for cross-account access role
- `aws.sts.role.session.duration` - Session duration for cross-account access role in Seconds. Minimum value - 900.
- `aws.sts.config.endpoint` - AWS STS endpoint for cross-account access role.

## Configuration

[Here](https://kafka.apache.org/documentation/#connect_running) you can
read about the Connect workers configuration and
[here](https://kafka.apache.org/documentation/#connect_resuming), about
the connector Configuration.

Here is an example connector configuration with descriptions:

```properties
### Standard connector configuration

## Fill in your values in these:

## These must have exactly these values:

# The Java class for the connector
connector.class=io.aiven.kafka.connect.s3.AivenKafkaConnectS3SourceConnector

# Number of worker tasks to run concurrently
tasks.max=1

# The key converter for this connector
key.converter=org.apache.kafka.connect.storage.StringConverter

# The type of data format used to write data to the kafka events.
# The supported values are: `jsonl`, 'avro', `parquet` and 'bytes'.
input.type=jsonl

# A comma-separated list of topics to use as output for this connector
# Also a regular expression version `topics.regex` is supported.
# See https://kafka.apache.org/documentation/#connect_configuring
topics=topic1,topic2

# A comma-separated list of topic partitions where the connector's offset storage reader
# can read the stored offsets for those partitions. If not mentioned, s3 objects will be read again if
# available in the bucket
topic.partitions=1,2,3

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
```

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

Apache Kafka, Apache Kafka Connect are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. AWS S3 is a trademark and property of their respective owners. All product and service names used in this website are for identification purposes only and do not imply endorsement.
