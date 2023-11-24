# Aiven's Common Module for Apache Kafka® connectors

Shared common functionality among Aiven's connectors for Apache Kafka:
- [Aiven GCS Connector](../gcs-connector/README.md)
- [Aiven S3 Connector](../s3-connector/README.md)

# Usage

When installing this library on Kafka Connect, use a specific plugin path, and **avoid placing them on the same path as the Kafka Connect binaries**, as some libraries may have conflicting versions.

# Development

     To use this library for development, you need to build and publish it in your local Maven repository using command:
<br/>
`./gradlew clean build publishToMavenLocal`

# License

This project is licensed under the [Apache License, Version 2.0](LICENSE).

# Trademarks

Apache Kafka, Apache Kafka Connect and Apache Maven are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.
