import com.github.spotbugs.snom.SpotBugsTask

/*
 * Copyright 2020 Aiven Oy
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
  id("com.bmuschko.docker-remote-api") version "9.4.0"
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
  compileOnly(apache.kafka.connect.runtime)
  compileOnly(project(":site"))

  compileOnly("org.apache.velocity:velocity-engine-core:2.4.1")
  compileOnly("org.apache.velocity.tools:velocity-tools-generic:3.1")

  implementation(apache.commons.collection4)
  implementation(project(":commons"))
  implementation(project(":s3-commons"))
  implementation(amazonawssdk.s3)
  implementation(amazonawssdk.sts)

  implementation(tools.spotbugs.annotations)
  implementation(logginglibs.slf4j)
  implementation(apache.avro)
  implementation(confluent.kafka.connect.avro.converter) {
    exclude(group = "org.apache.kafka", module = "kafka-clients")
  }

  testImplementation(testFixtures(project(":commons")))
  testImplementation(testFixtures(project(":s3-commons")))
  testImplementation(compressionlibs.snappy)
  testImplementation(compressionlibs.zstd.jni)
  testImplementation(testinglibs.awaitility)

  testImplementation(apache.kafka.connect.api)
  testImplementation(apache.kafka.connect.runtime)
  testImplementation(apache.kafka.connect.json)

  testImplementation(testinglibs.localstack) {
    exclude(group = "io.netty", module = "netty-transport-native-epoll")
  }
  testImplementation(testcontainers.junit.jupiter)
  testImplementation(testcontainers.kafka) // this is not Kafka version
  testImplementation(testcontainers.junit.jupiter)
  testImplementation(testcontainers.localstack)

  testImplementation(testinglibs.junit.jupiter)
  testImplementation(testinglibs.assertj.core)

  testImplementation(testinglibs.mockito.core)

  testRuntimeOnly(testinglibs.junit.jupiter.engine)
  testImplementation(testinglibs.mockito.junit.jupiter)

  implementation(apache.hadoop.common) {
    exclude(group = "org.apache.hadoop", module = "hadoop-yarn-client")
    exclude(group = "org.apache.hadoop.thirdparty", module = "hadoop-shaded-protobuf_3_7")
    exclude(group = "com.google.guava", module = "guava")
    exclude(group = "commons-cli", module = "commons-cli")
    exclude(group = "org.apache.commons", module = "commons-math3")
    exclude(group = "org.apache.httpcomponents", module = "httpclient")
    exclude(group = "commons-codec", module = "commons-codec")
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

  testRuntimeOnly(logginglibs.logback.classic)

  integrationTestImplementation(testinglibs.localstack) {
    exclude(group = "io.netty", module = "netty-transport-native-epoll")
  }
  integrationTestImplementation(testcontainers.junit.jupiter)
  integrationTestImplementation(testcontainers.kafka) // this is not Kafka version
  integrationTestImplementation(testcontainers.localstack)
  integrationTestImplementation(testinglibs.wiremock)
  integrationTestImplementation(testFixtures(project(":s3-commons")))
  integrationTestImplementation(testcontainers.localstack)
  integrationTestImplementation(confluent.kafka.connect.avro.converter) {
    exclude(group = "org.apache.kafka", module = "kafka-clients")
  }
  integrationTestImplementation(apache.kafka.connect.api)

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

  integrationTestImplementation(apache.kafka.connect.runtime)
  integrationTestImplementation(testFixtures(project(":commons")))
  integrationTestImplementation(testFixtures(project(":s3-source-connector")))

  // Make test utils from 'test' available in 'integration-test'
  integrationTestImplementation(sourceSets["test"].output)
  integrationTestImplementation(testinglibs.awaitility)

  testFixturesImplementation(amazonawssdk.s3)
  testFixturesImplementation(amazonawssdk.sts)
  testFixturesImplementation(testFixtures(project(":commons")))
  testFixturesImplementation(project(":s3-commons"))
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
}

tasks.named<Pmd>("pmdIntegrationTest") {
  ruleSetFiles = files("${project.rootDir}/gradle-config/aiven-pmd-test-ruleset.xml")
  ruleSets = emptyList() // Clear the default rulesets
}

tasks.named<SpotBugsTask>("spotbugsIntegrationTest") {
  reports.create("html") { setStylesheet("fancy-hist.xsl") }
}

tasks.processResources {
  filesMatching("s3-source-connector-for-apache-kafka-version.properties") {
    expand(mapOf("version" to version))
  }
}

tasks.jar { manifest { attributes(mapOf("Version" to project.version)) } }

tasks.distTar { dependsOn(":commons:jar") }

tasks.distZip { dependsOn(":commons:jar") }

distributions {
  main {
    contents {
      from("jar")
      from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })

      into("/") {
        from("$projectDir")
        include("version.txt", "README*", "LICENSE*", "NOTICE*", "licenses/")
        include("config/")
      }
    }
  }
}

publishing {
  publications {
    create<MavenPublication>("publishMavenJavaArtifact") {
      groupId = group.toString()
      artifactId = "s3-source-connector-for-apache-kafka"
      version = version.toString()

      from(components["java"])

      pom {
        name = "Aiven's S3 Source Connector for Apache Kafka"
        description = "Aiven's S3 Source Connector for Apache Kafka"
        url = "https://github.com/aiven-open/s3-source-connector-for-apache-kafka"
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
          connection = "scm:git:git://github.com:aiven/s3-source-connector-for-apache-kafka.git"
          developerConnection =
              "scm:git:ssh://github.com:aiven/s3-source-connector-for-apache-kafka.git"
          url = "https://github.com/aiven-open/s3-source-connector-for-apache-kafka"
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
  class ASCSignatureProvider() : AbstractSignatureTypeProvider() {
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

/** ******************************* */
/* Documentation building section */
/** ******************************* */
tasks.register("buildDocs") {
  dependsOn("buildConfigMd")
  dependsOn("buildConfigYml")
}

tasks.register<JavaExec>("buildConfigMd") {
  mainClass = "io.aiven.kafka.connect.tools.ConfigDoc"
  classpath =
      sourceSets.main
          .get()
          .compileClasspath
          .plus(files(tasks.jar))
          .plus(sourceSets.main.get().runtimeClasspath)
  args =
      listOf(
          "io.aiven.kafka.connect.s3.source.config.S3SourceConfig",
          "configDef",
          "src/templates/configData.md.vm",
          "build/site/markdown/s3-source-connector/S3SourceConfig.md")
}

tasks.register<JavaExec>("buildConfigYml") {
  mainClass = "io.aiven.kafka.connect.tools.ConfigDoc"
  classpath =
      sourceSets.main
          .get()
          .compileClasspath
          .plus(files(tasks.jar))
          .plus(sourceSets.main.get().runtimeClasspath)
  args =
      listOf(
          "io.aiven.kafka.connect.s3.source.config.S3SourceConfig",
          "configDef",
          "src/templates/configData.yml.vm",
          "build/site/s3-source-connector/S3SourceConfig.yml")
}

/** ****************************** */
/*  End of documentation section */
/** ****************************** */
