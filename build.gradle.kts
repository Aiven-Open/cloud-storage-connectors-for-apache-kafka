import com.diffplug.gradle.spotless.SpotlessExtension
import java.net.URI

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
    id("com.diffplug.spotless") version "6.22.0"
    id("com.github.spotbugs") version "5.1.3"
}


group = "io.aiven"

tasks.wrapper {
    distributionType = Wrapper.DistributionType.ALL
    doLast {
        val sha256Uri = URI("$distributionUrl.sha256")
        val sha256Sum = String(sha256Uri.toURL().readBytes())
        propertiesFile.appendBytes("distributionSha256Sum=${sha256Sum}\n".toByteArray())
        println("Added checksum to wrapper properties")
    }
}


val kafkaVersion by extra ("1.1.0")
val parquetVersion by extra ("1.11.2")
val junitVersion by extra ("5.10.0")
val confluentPlatformVersion by extra ("7.2.2")
val hadoopVersion by extra ("3.3.6")


configure<SpotlessExtension> {
    format("misc") {
        target("*.gradle", "*.md", ".gitignore")
        targetExclude(".*/**", "**/build/**", "**/.gradle/**")
        trimTrailingWhitespace()
        indentWithSpaces()
        endWithNewline()
    }
}
