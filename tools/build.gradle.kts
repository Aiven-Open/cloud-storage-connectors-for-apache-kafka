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

plugins { id("aiven-apache-kafka-connectors-all.java-conventions")
  java }

dependencies {
  compileOnly("org.apache.kafka:connect-api:1.1.0")
  // compileOnly(apache.kafka.connect.runtime)

  // implementation(project(":commons"))
  // https://mvnrepository.com/artifact/org.apache.velocity/velocity
  implementation("org.apache.velocity:velocity-engine-core:2.4.1")
  implementation("org.apache.velocity.tools:velocity-tools-generic:3.1")

  // implementation(tools.spotbugs.annotations)
  // implementation(logginglibs.slf4j)

  //  testImplementation(testinglibs.junit.jupiter)
  //  testImplementation(testinglibs.hamcrest)
  //  testImplementation(testinglibs.assertj.core)
  //  testImplementation(testinglibs.mockito.core)

  //  testImplementation(apache.kafka.connect.api)
  //  testImplementation(apache.kafka.connect.runtime)
  //  testImplementation(apache.kafka.connect.json)
}

repositories {
  maven("https://plugins.gradle.org/m2/")
  mavenCentral()
  maven("https://packages.confluent.io/maven/")
}

tasks.jar { manifest { attributes(mapOf("Version" to project.version)) } }
