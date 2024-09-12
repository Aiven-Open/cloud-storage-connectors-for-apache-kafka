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

import com.diffplug.spotless.LineEnding
import java.net.URI

plugins {

    // https://docs.gradle.org/current/userguide/java_library_plugin.html
    id("java-library")

    // https://docs.gradle.org/current/userguide/idea_plugin.html
    id("idea")

    // https://docs.gradle.org/current/userguide/jacoco_plugin.html
    id("jacoco")

    // https://docs.gradle.org/current/userguide/distribution_plugin.html
    id("distribution")

    // https://docs.gradle.org/current/userguide/publishing_maven.html
    id("maven-publish")

    // https://docs.gradle.org/current/userguide/signing_plugin.html
    id("signing")

    // https://plugins.gradle.org/plugin/com.diffplug.spotless
    id( "com.diffplug.spotless")

    id("pmd")

    id("com.github.spotbugs")
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11

    withJavadocJar()
    withSourcesJar()
}

tasks.withType<JavaCompile> {
    val compilerArgs = options.compilerArgs
    // Issue with s3-connector integration tests with test containers and junit 5: cannot find annotation method since()
    //compilerArgs.add("-Werror")
    compilerArgs.add("-Xlint:all")
}

tasks.withType<Javadoc> {
    (options as StandardJavadocDocletOptions).addBooleanOption("html5", true)
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:all,-missing", "-quiet")
}

jacoco {
    toolVersion = "0.8.7"
}

repositories {
    mavenLocal()
    mavenCentral()
    maven("https://packages.confluent.io/maven")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<Wrapper>  {
    distributionType = Wrapper.DistributionType.ALL
    doLast {
        val sha256Uri = URI("$distributionUrl.sha256")
        val sha256Sum = String(sha256Uri.toURL().readBytes())
        propertiesFile.appendBytes("distributionSha256Sum=${sha256Sum}\n".toByteArray())
        println("Added checksum to wrapper properties")
    }
}

pmd {
    isConsoleOutput = true
    toolVersion = "6.55.0"
    rulesMinimumPriority = 5

    tasks.pmdMain {
        ruleSetFiles = files("${project.rootDir}/gradle-config/aiven-pmd-ruleset.xml")
        ruleSets = ArrayList() // Clear the default rulesets
    }
    tasks.pmdTest {
        ruleSetFiles = files("${project.rootDir}/gradle-config/aiven-pmd-test-ruleset.xml")
        ruleSets = ArrayList() // Clear the default rulesets
    }
}

spotbugs {
    toolVersion = "4.8.3"
    excludeFilter.set(project.file("${project.rootDir}/gradle-config/spotbugs-exclude.xml"))

    tasks.spotbugsMain {
        reports.create("html") {
            enabled = true
            setStylesheet("fancy-hist.xsl")
        }
    }
    tasks.spotbugsTest {
        reports.create("html") {
            enabled = true
            setStylesheet("fancy-hist.xsl")
        }
    }
}

spotless {
    format("misc") {
        // define the files to apply `misc` to
        target("*.gradle", "*.md", ".gitignore", "**/META-INF/services/**")
        targetExclude(".*/**", "**/build/**", "**/.gradle/**")

        // define the steps to apply to those files
        trimTrailingWhitespace()
        indentWithSpaces()
        endWithNewline()
        // lineEndings = LineEnding.UNIX -> if we want to unify line endings we should set this
    }

    kotlinGradle {
        target("*.gradle.kts")
        ktfmt()
    }

    java {
        licenseHeaderFile(file("${project.rootDir}/gradle-config/java.header"))
        importOrder("javax", "java", "org.apache.kafka", "io.aiven", "")
        removeUnusedImports()
        replaceRegex("No wildcard imports.", "import(?:\\s+static)?\\s+[^\\*\\s]+\\*;(\r\n|\r|\n)", "$1")
        eclipse().configFile("${project.rootDir}/gradle-config/aiven-eclipse-formatter.xml")
        indentWithSpaces()
        endWithNewline()
        trimTrailingWhitespace()
    }
}

distributions {
    main {
        contents {
            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
            from(tasks.jar)
            from(configurations.runtimeClasspath.get())

            into("/") {
                from("$projectDir")
                include("README*", "notices/", "licenses/")
                include("config/")
                from("$rootDir") {
                    include("LICENSE*")
                }
            }
        }
    }
}
