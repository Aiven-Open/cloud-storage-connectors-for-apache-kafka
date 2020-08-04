![Push Workflow](https://github.com/aiven/aiven-kafka-connect-commons/workflows/Push%20Workflow/badge.svg)
![Release Workflow](https://github.com/aiven/aiven-kafka-connect-commons/workflows/Release%20Workflow/badge.svg)
![Publish Workflow](https://github.com/aiven/aiven-kafka-connect-commons/workflows/Publish%20Workflow/badge.svg)

# Avien Kafka Connect Common Module

Shared common functionality among *Aiven Kafka Connectors*:
- [Aiven GCS Connector](https://github.com/aiven/aiven-kafka-connect-gcs)
- [Aiven S3 Connector](https://github.com/aiven/aiven-kafka-connect-s3)

# Development

To use this library for development *Aiven Kafka Connectors* you need to build and publish it in your local *maven* repository using command:
<br/>
`./gradlew clean build publishToMavenLocal`

# License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
