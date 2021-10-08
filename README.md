![Push Workflow](https://github.com/aiven/commons-for-apache-kafka-connect/workflows/Push%20Workflow/badge.svg)
![Release Workflow](https://github.com/aiven/commons-for-apache-kafka-connect/workflows/Release%20Workflow/badge.svg)
![Publish Workflow](https://github.com/aiven/commons-for-apache-kafka-connect/workflows/Publish%20Workflow/badge.svg)

# Avien's Common Module for Apache Kafka connectors

Shared common functionality among Aiven's connectors for Apache Kafka:
- [Aiven GCS Connector](https://github.com/aiven/gcs-connector-for-apache-kafka)
- [Aiven S3 Connector](https://github.com/aiven/s3-connector-for-apache-kafka)

# Development

To use this library for development, you need to build and publish it in your local Maven repository using command:
<br/>
`./gradlew clean build publishToMavenLocal`

# License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
