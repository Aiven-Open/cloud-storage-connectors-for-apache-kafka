# Aiven's S3 Source Connector for Apache Kafka

This is a source Apache Kafka Connect connector that stores Apache Kafka messages in an AWS S3 bucket.

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

## TODO update documentation

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
