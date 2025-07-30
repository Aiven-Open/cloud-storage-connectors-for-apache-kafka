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

plugins {
  id("aiven-apache-kafka-connectors-all.java-conventions")
  id("aiven-apache-kafka-connectors-all.docs")
}

val integrationTest: SourceSet =
    sourceSets.create("integrationTest") {
      java { srcDir("src/integration-test/java") }
      resources { srcDir("src/integration-test/resources") }
      compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
      runtimeClasspath += output + compileClasspath
    }

val integrationTestImplementation: Configuration by
    configurations.getting { extendsFrom(configurations.implementation.get()) }

tasks.register<Test>("integrationTest") {
  description = "Runs the integration tests."
  group = "verification"
  testClassesDirs = integrationTest.output.classesDirs
  classpath = integrationTest.runtimeClasspath

  // defines testing order
  shouldRunAfter("test")
  // requires archive for connect runner
  dependsOn("distTar")
  useJUnitPlatform()

  // Run always.
  outputs.upToDateWhen { false }

  // Pass the Azure connection string to the tests.
  if (project.hasProperty("testAzureStorageString")) {
    systemProperty(
        "integration-test.azure.connection.string",
        project.findProperty("testAzureStorageString").toString())
  }
  // Pass the Azure container name to the tests.
  systemProperty(
      "integration-test.azure.container", project.findProperty("testAzureStorage").toString())
  // Pass the distribution file path to the tests.
  val distTarTask = tasks.get("distTar") as Tar
  val distributionFilePath = distTarTask.archiveFile.get().asFile.path
  systemProperty("integration-test.distribution.file.path", distributionFilePath)
}

idea {
  module {
    testSources.from(integrationTest.java.srcDirs)
    testSources.from(integrationTest.resources.srcDirs)
  }
}

dependencies {
  compileOnly(apache.kafka.connect.api)

  implementation(project(":commons"))

  implementation("com.azure:azure-storage-blob:12.30.0")

  implementation(tools.spotbugs.annotations)
  implementation(logginglibs.slf4j)

  testImplementation(testinglibs.junit.jupiter)
  testImplementation(testinglibs.hamcrest)
  testImplementation(testinglibs.assertj.core)
  testImplementation(testinglibs.mockito.core)
  testImplementation(testinglibs.mockito.junit.jupiter)
  testImplementation(testinglibs.jqwik)
  // is provided by "jqwik", but need this in testImplementation scope
  testImplementation(testinglibs.jqwik.engine)

  testImplementation(apache.kafka.connect.api)
  testImplementation(apache.kafka.connect.runtime)
  testImplementation(apache.kafka.connect.json)

  testImplementation(compressionlibs.snappy)
  testImplementation(compressionlibs.zstd.jni)
  testImplementation(apache.hadoop.mapreduce.client.core) {
    exclude(group = "org.apache.hadoop", module = "hadoop-yarn-client")
    exclude(group = "org.apache.hadoop.thirdparty", module = "hadoop-shaded-protobuf_3_7")
    exclude(group = "com.google.guava", module = "guava")
    exclude(group = "commons-cli", module = "commons-cli")
    exclude(group = "org.apache.commons", module = "commons-math3")
    exclude(group = "org.apache.httpcomponents", module = "httpclient")
    exclude(group = "commons-codec", module = "commons-codec")
    exclude(group = "commons-io", module = "commons-io")
    exclude(group = "commons-net", module = "commons-net")
    exclude(group = "org.eclipse.jetty")
    exclude(group = "org.eclipse.jetty.websocket")
    exclude(group = "javax.servlet")
    exclude(group = "javax.servlet.jsp")
    exclude(group = "javax.activation")
    exclude(group = "com.sun.jersey")
    exclude(group = "log4j")
    exclude(group = "org.apache.commons", module = "commons-text")
    exclude(group = "org.slf4j", module = "slf4j-api")
    exclude(group = "org.apache.hadoop", module = "hadoop-auth")
    exclude(group = "org.apache.hadoop", module = "hadoop-yarn-api")
    exclude(group = "com.google.re2j")
    exclude(group = "com.google.protobuf")
    exclude(group = "com.google.code.gson")
    exclude(group = "com.jcraft")
    exclude(group = "org.apache.curator")
    exclude(group = "org.apache.zookeeper")
    exclude(group = "org.apache.htrace")
    exclude(group = "com.google.code.findbugs")
    exclude(group = "org.apache.kerby")
    exclude(group = "com.fasterxml.jackson.core")
    exclude(group = "com.fasterxml.woodstox", module = "woodstox-core:5.0.3")
    exclude(group = "org.apache.avro", module = "avro")
    exclude(group = "org.apache.hadoop", module = "hadoop-yarn-common")
    exclude(group = "com.google.inject.extensions", module = "guice-servlet")
    exclude(group = "io.netty", module = "netty")
  }

  testRuntimeOnly(logginglibs.slf4j.log4j12)

  integrationTestImplementation(testinglibs.wiremock)
  integrationTestImplementation(testcontainers.junit.jupiter)
  integrationTestImplementation(testcontainers.kafka) // this is not Kafka version
  integrationTestImplementation(testinglibs.awaitility)
  integrationTestImplementation("org.testcontainers:kafka:1.21.3")

  integrationTestImplementation(apache.kafka.connect.transforms)
  // TODO: add avro-converter to ConnectRunner via plugin.path instead of on worker classpath
  integrationTestImplementation(confluent.kafka.connect.avro.converter) {
    exclude(group = "org.apache.kafka", module = "kafka-clients")
  }

  // Make test utils from "test" available in "integration-test"
  integrationTestImplementation(sourceSets["test"].output)
}

tasks.named<Pmd>("pmdIntegrationTest") {
  ruleSetFiles = files("${project.rootDir}/gradle-config/aiven-pmd-test-ruleset.xml")
  ruleSets = emptyList() // Clear the default rulesets
}

tasks.named<SpotBugsTask>("spotbugsIntegrationTest") {
  reports.create("html") { setStylesheet("fancy-hist.xsl") }
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
