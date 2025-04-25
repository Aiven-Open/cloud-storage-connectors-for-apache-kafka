import com.github.spotbugs.snom.SpotBugsTask

/*
 * Copyright 2024 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins { id("aiven-apache-kafka-connectors-all.java-conventions") }

val azureVersion by extra("12.29.0")

dependencies {
  testFixturesImplementation(apache.kafka.connect.api)

  implementation(project(":commons"))

  testFixturesImplementation("com.azure:azure-storage-blob:12.29.0")

  testFixturesImplementation(tools.spotbugs.annotations)
  testFixturesImplementation(logginglibs.slf4j)
  testFixturesImplementation(testinglibs.wiremock)
  testFixturesImplementation(testinglibs.junit.jupiter)
  testFixturesImplementation(testinglibs.hamcrest)
  testFixturesImplementation(testinglibs.assertj.core)
  testFixturesImplementation(testinglibs.mockito.core)
  testFixturesImplementation(testinglibs.mockito.junit.jupiter)
  testFixturesImplementation(testinglibs.jqwik)
  // is provided by "jqwik", but need this in testImplementation scope
  testFixturesImplementation(testinglibs.jqwik.engine)

  testFixturesImplementation(apache.kafka.connect.api)
  testFixturesImplementation(apache.kafka.connect.runtime)
  testFixturesImplementation(apache.kafka.connect.json)

  testFixturesImplementation(tools.spotbugs.annotations)
  testFixturesImplementation("com.azure:azure-storage-blob:${azureVersion}")
  testFixturesImplementation(testFixtures(project(":commons")))
  testFixturesImplementation(testinglibs.localstack) {
    exclude(group = "io.netty", module = "netty-transport-native-epoll")
  }
  testFixturesImplementation(testcontainers.junit.jupiter)
  testFixturesImplementation(testcontainers.localstack)
  testFixturesImplementation(testinglibs.junit.jupiter)
  testFixturesImplementation(testinglibs.assertj.core)
  testFixturesImplementation(compressionlibs.snappy)
  testFixturesImplementation(compressionlibs.zstd.jni)
  testFixturesImplementation(tools.spotbugs.annotations)
  testFixturesImplementation(apache.kafka.connect.api)
  testFixturesImplementation("org.testcontainers:azure:1.20.6")

//  testImplementation(compressionlibs.snappy)
//  testImplementation(compressionlibs.zstd.jni)
//  testImplementation(apache.hadoop.mapreduce.client.core) {
//    exclude(group = "org.apache.hadoop", module = "hadoop-yarn-client")
//    exclude(group = "org.apache.hadoop.thirdparty", module = "hadoop-shaded-protobuf_3_7")
//    exclude(group = "com.google.guava", module = "guava")
//    exclude(group = "commons-cli", module = "commons-cli")
//    exclude(group = "org.apache.commons", module = "commons-math3")
//    exclude(group = "org.apache.httpcomponents", module = "httpclient")
//    exclude(group = "commons-codec", module = "commons-codec")
//    exclude(group = "commons-io", module = "commons-io")
//    exclude(group = "commons-net", module = "commons-net")
//    exclude(group = "org.eclipse.jetty")
//    exclude(group = "org.eclipse.jetty.websocket")
//    exclude(group = "javax.servlet")
//    exclude(group = "javax.servlet.jsp")
//    exclude(group = "javax.activation")
//    exclude(group = "com.sun.jersey")
//    exclude(group = "log4j")
//    exclude(group = "org.apache.commons", module = "commons-text")
//    exclude(group = "org.slf4j", module = "slf4j-api")
//    exclude(group = "org.apache.hadoop", module = "hadoop-auth")
//    exclude(group = "org.apache.hadoop", module = "hadoop-yarn-api")
//    exclude(group = "com.google.re2j")
//    exclude(group = "com.google.protobuf")
//    exclude(group = "com.google.code.gson")
//    exclude(group = "com.jcraft")
//    exclude(group = "org.apache.curator")
//    exclude(group = "org.apache.zookeeper")
//    exclude(group = "org.apache.htrace")
//    exclude(group = "com.google.code.findbugs")
//    exclude(group = "org.apache.kerby")
//    exclude(group = "com.fasterxml.jackson.core")
//    exclude(group = "com.fasterxml.woodstox", module = "woodstox-core:5.0.3")
//    exclude(group = "org.apache.avro", module = "avro")
//    exclude(group = "org.apache.hadoop", module = "hadoop-yarn-common")
//    exclude(group = "com.google.inject.extensions", module = "guice-servlet")
//    exclude(group = "io.netty", module = "netty")
//  }
//
//  testRuntimeOnly(logginglibs.slf4j.log4j12)
}

tasks.processResources {
  filesMatching("azure-blob-sink-connector-for-apache-kafka-version.properties") {
    expand(mapOf("version" to version))
  }
}

publishing {
  publications {
    create<MavenPublication>("publishMavenJavaArtifact") {
      groupId = group.toString()
      artifactId = "azure-blob-sink-connector-for-apache-kafka"
      version = version.toString()

      from(components["java"])

      pom {
        name = "Aiven's Azure Blob Sink Connector for Apache Kafka"
        description = "Aiven's Azure Blob Sink Connector for Apache Kafka"
        url = "https://github.com/Aiven-Open/cloud-storage-connectors-for-apache-kafka"
        organization {
          name = "Aiven Oy"
          url = "https://aiven.io"
        }

        licenses {
          license {
            name = "Apache 2.0"
            url = "http://www.apache.org/licenses/LICENSE-2.0"
            distribution = "repo"
          }
        }

        developers {
          developer {
            id = "aiven"
            name = "Aiven Opensource"
            email = "opensource@aiven.io"
          }
        }

        scm {
          connection =
              "scm:git:git://github.com:Aiven-Open/cloud-storage-connectors-for-apache-kafka.git"
          developerConnection =
              "scm:git:ssh://github.com:Aiven-Open/cloud-storage-connectors-for-apache-kafka.git"
          url = "https://github.com/Aiven-Open/cloud-storage-connectors-for-apache-kafka"
        }
      }
    }
  }

  repositories {
    maven {
      name = "sonatype"

      val releasesRepoUrl = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2")
      val snapshotsRepoUrl = uri("https://oss.sonatype.org/content/repositories/snapshots")
      url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl

      credentials(PasswordCredentials::class)
    }
  }
}

signing {
  sign(publishing.publications["publishMavenJavaArtifact"])
  useGpgCmd()
  // Some issue in the plugin:
  // GPG outputs already armored signatures. The plugin also does armoring for `asc` files.
  // This results in double armored signatures, i.e. garbage.
  // Override the signature type provider to use unarmored output for `asc` files, which works well
  // with GPG.
  class ASCSignatureProvider : AbstractSignatureTypeProvider() {
    val binary =
        object : BinarySignatureType() {
          override fun getExtension(): String {
            return "asc"
          }
        }

    init {
      register(binary)
      setDefaultType(binary.extension)
    }
  }
  signatureTypes = ASCSignatureProvider()
}
