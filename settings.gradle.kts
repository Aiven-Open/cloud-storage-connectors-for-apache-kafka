rootProject.name = "cloud-storage-connectors-for-apache-kafka"

val assertJVersion by extra("3.26.0")
val avroVersion by extra("1.11.3")
val avroConverterVersion by extra("7.2.2")
val avroDataVersion by extra("7.2.2")
val awaitilityVersion by extra("4.2.1")
val amazonAwsSdkV1Version by extra("1.12.777")
val amazonAwsSdkV2Version by extra("2.29.34")
val commonsCollections4Version by extra("4.4")
val commonsIOVersion by extra("2.18.0")
val commonsLang3Version by extra("3.17.0")
val commonsTextVersion by extra("1.11.0")
val hadoopVersion by extra("3.4.0")
val hamcrestVersion by extra("2.2")
val jacksonVersion by extra("2.15.3")
val junitVersion by extra("5.11.4")
val jqwikVersion by extra("1.8.4")
val kafkaVersion by extra("3.3.1")
val kafkaAPIVersion by extra("1.1.0") // because we want the lowest possible API to function.
val logbackVersion by extra("1.5.6")
val localstackVersion by extra("0.2.23")
val mockitoVersion by extra("5.15.2")
val parquetVersion by extra("1.15.0")
val slf4jVersion by extra("1.7.36")
val snappyVersion by extra("1.1.10.5")
val spotbugsAnnotationsVersion by extra("4.8.1")
val stax2ApiVersion by extra("4.2.2")
val testcontainersVersion by extra("1.20.6")
val zstdVersion by extra("1.5.6-3")
val wireMockVersion by extra("2.35.0")
val azureVersion by extra("12.30.0")
val velocityCoreVersion by extra("2.4.1")
val velocityToolsVersion by extra("3.1")

dependencyResolutionManagement {
  versionCatalogs {
    create("apache") {
      library("avro", "org.apache.avro:avro:$avroVersion")
      library("commons-text", "org.apache.commons:commons-text:$commonsTextVersion")
      library(
          "commons-collection4",
          "org.apache.commons:commons-collections4:$commonsCollections4Version")
      library("commons-io", "commons-io:commons-io:$commonsIOVersion")
      library("commons-lang3", "org.apache.commons:commons-lang3:$commonsLang3Version")
      library("kafka-connect-api", "org.apache.kafka:connect-api:$kafkaAPIVersion")
      library("kafka-connect-json", "org.apache.kafka:connect-json:$kafkaVersion")
      library("kafka-connect-runtime", "org.apache.kafka:connect-runtime:$kafkaVersion")
      library("kafka-connect-transforms", "org.apache.kafka:connect-transforms:$kafkaVersion")
      library("kafka-clients", "org.apache.kafka:kafka-clients:${kafkaVersion}")
      library("hadoop-common", "org.apache.hadoop:hadoop-common:$hadoopVersion")
      library(
          "hadoop-mapreduce-client-core",
          "org.apache.hadoop:hadoop-mapreduce-client-core:$hadoopVersion")
      library("parquet-avro", "org.apache.parquet:parquet-avro:$parquetVersion")
      library(
          "velocity-engine-core", "org.apache.velocity:velocity-engine-core:${velocityCoreVersion}")
      library(
          "velocity-tools",
          "org.apache.velocity.tools:velocity-tools-generic:${velocityToolsVersion}")
    }
    create("amazonawssdk") {
      library("authentication", "software.amazon.awssdk:auth:$amazonAwsSdkV2Version")
      library("s3", "software.amazon.awssdk:s3:$amazonAwsSdkV2Version")
      library("sts", "software.amazon.awssdk:sts:$amazonAwsSdkV2Version")
    }

    create("amazonoldawssdk") {
      library("s3", "com.amazonaws:aws-java-sdk-s3:$amazonAwsSdkV1Version")
      library("sts", "com.amazonaws:aws-java-sdk-sts:$amazonAwsSdkV1Version")
    }

    create("azure") { library("storage-blob", "com.azure:azure-storage-blob:${azureVersion}") }
    create("compressionlibs") {
      library("snappy", "org.xerial.snappy:snappy-java:$snappyVersion")
      library("zstd-jni", "com.github.luben:zstd-jni:$zstdVersion")
    }
    create("confluent") {
      library(
          "kafka-connect-avro-converter",
          "io.confluent:kafka-connect-avro-converter:$avroConverterVersion")
      library("kafka-connect-avro-data", "io.confluent:kafka-connect-avro-data:$avroDataVersion")
    }
    create("jackson") {
      library("databind", "com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    }
    create("kafkalibs") {}

    create("logginglibs") {
      library("logback-classic", "ch.qos.logback:logback-classic:$logbackVersion")
      library("slf4j", "org.slf4j:slf4j-api:$slf4jVersion")
      library("slf4j-log4j12", "org.slf4j:slf4j-log4j12:$slf4jVersion")
      library("slf4j-simple", "org.slf4j:slf4j-simple:2.0.17")
    }
    create("tools") {
      library(
          "spotbugs-annotations",
          "com.github.spotbugs:spotbugs-annotations:$spotbugsAnnotationsVersion")
    }
    create("testinglibs") {
      library("assertj-core", "org.assertj:assertj-core:$assertJVersion")
      library("awaitility", "org.awaitility:awaitility:$awaitilityVersion")
      library("hamcrest", "org.hamcrest:hamcrest:$hamcrestVersion")
      library("localstack", "cloud.localstack:localstack-utils:$localstackVersion")
      library("junit-jupiter", "org.junit.jupiter:junit-jupiter:$junitVersion")
      library("junit-jupiter-api", "org.junit.jupiter:junit-jupiter-api:$junitVersion")
      library("junit-jupiter-engine", "org.junit.jupiter:junit-jupiter-engine:$junitVersion")
      library("junit-jupiter-params", "org.junit.jupiter:junit-jupiter-params:$junitVersion")
      library("jqwik", "net.jqwik:jqwik:$jqwikVersion")
      library("jqwik-engine", "net.jqwik:jqwik-engine:$jqwikVersion")
      library("mockito-core", "org.mockito:mockito-core:$mockitoVersion")
      library("mockito-junit-jupiter", "org.mockito:mockito-junit-jupiter:$mockitoVersion")
      library("wiremock", "com.github.tomakehurst:wiremock-jre8:$wireMockVersion")
      library("woodstox-stax2-api", "org.codehaus.woodstox:stax2-api:$stax2ApiVersion")
    }
    create("testcontainers") {
      library("junit-jupiter", "org.testcontainers:junit-jupiter:$testcontainersVersion")
      library(
          "kafka", "org.testcontainers:kafka:$testcontainersVersion") // this is not Kafka version
      library("localstack", "org.testcontainers:localstack:$testcontainersVersion")
      library("azure", "org.testcontainers:azure:$testcontainersVersion")
    }
  }
}

include("commons")

include("gcs-sink-connector")

include("azure-commons")

include("azure-sink-connector")

include("azure-source-connector")

include("s3-commons")

include("s3-sink-connector")

include("s3-source-connector")

include("site")
