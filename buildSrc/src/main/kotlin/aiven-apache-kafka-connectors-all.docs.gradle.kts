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

group = "io.aiven"

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

tasks.withType<Jar> {
    archiveBaseName.set(project.name + "-for-apache-kafka")
    manifest { attributes(mapOf("Version" to project.version)) }
    from("${project.rootDir}/LICENSE") {
        into("META-INF")
    }
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



distributions {
    main {
        distributionBaseName.set(project.name + "-for-apache-kafka")
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

tasks.register<Exec>("execVale") {
    description = "Executes the Vale text linter"
    group = "Documentation"
    executable("/usr/bin/docker")
    args(
        "run",
        "--rm",
        "-v",
        "${project.rootDir}:/project:Z",
        "-v",
        "${project.rootDir}/.github/vale/styles:/styles:Z",
        "-v",
        "${project.projectDir}:/docs:Z",
        "-w",
        "/docs",
        "jdkato/vale",
        "--filter=warn.expr",
        "--config=/project/.vale.ini",
        "--glob=!**/build/**",
        "."
    )
}

tasks.register<Copy>("copySiteAssets") {
    group = "Documentation"
    description = "Copies "
    outputs.upToDateWhen { false }
    dependsOn("javadoc")
//    dependsOn("processSiteAssets_apt", "processSiteAssets_asciidoc", "processSiteAssets_confluence", "processSiteAssets_docbook",
//        "processSiteAssets_fml", "processSiteAssets_markdown", "processSiteAssets_twiki", "processSiteAssets_xdoc", "processSiteAssets_xhtml")

    println("Copying from ${project.layout.projectDirectory.asFile}/src/site")
    println("          to ${project.layout.buildDirectory.asFile.get()}/site/")
    from("${project.layout.projectDirectory.asFile}/src/site")
    into("${project.layout.buildDirectory.asFile.get()}/site/")
    println("DONE")
}

//tasks.register<Copy>("processSiteAssets_apt") {
//    description = "Copies "
//    outputs.upToDateWhen { false }
//
//    println("Copying from ${project.layout.projectDirectory}/src/site/apt")
//    println("          to ${project.layout.buildDirectory.asFile.get()}/site/apt/${project.name}")
//
//    from("${project.layout.projectDirectory}/src/site/apt")
//    into("${project.layout.buildDirectory.asFile.get()}/site/apt/${project.name}")
//}
//
//tasks.register<Copy>("processSiteAssets_asciidoc") {
//    description = "Copies "
//    outputs.upToDateWhen { false }
//
//    println("Copying from ${project.layout.projectDirectory}/src/site/asciidoc")
//    println("          to ${project.layout.buildDirectory.asFile.get()}/site/asciidoc/${project.name}")
//    copy {
//        from("${project.layout.projectDirectory}/src/site/asciidoc")
//        into("${project.layout.buildDirectory.asFile.get()}/site/asciidoc/${project.name}")
//    }
//}
//
//tasks.register<Copy>("processSiteAssets_confluence") {
//    description = "Copies "
//    outputs.upToDateWhen { false }
//
//    println("Copying from ${project.layout.projectDirectory}/src/site/confluence")
//    println("          to ${project.layout.buildDirectory.asFile.get()}/site/confluence/${project.name}")
//    copy {
//        from("${project.layout.projectDirectory}/src/site/confluence")
//        into("${project.layout.buildDirectory.asFile.get()}/site/confluence/${project.name}")
//    }
//}
//
//tasks.register<Copy>("processSiteAssets_docbook") {
//    description = "Copies "
//    outputs.upToDateWhen { false }
//
//    println("Copying from ${project.layout.projectDirectory}/src/site/docbook")
//    println("          to ${project.layout.buildDirectory.asFile.get()}/site/docbook/${project.name}")
//    copy {
//        from("${project.layout.projectDirectory}/src/site/docbook")
//        into("${project.layout.buildDirectory.asFile.get()}/site/docbook/${project.name}")
//    }
//}
//
//tasks.register<Copy>("processSiteAssets_fml") {
//    description = "Copies "
//    outputs.upToDateWhen { false }
//
//
//    println("Copying from ${project.layout.projectDirectory}/src/site/fml")
//    println("          to ${project.layout.buildDirectory.asFile.get()}/site/fml/${project.name}")
//    copy {
//        from("${project.layout.projectDirectory}/src/site/fml")
//        into("${project.layout.buildDirectory.asFile.get()}/site/fml/${project.name}")
//    }
//}
//
//tasks.register<Copy>("processSiteAssets_markdown") {
//    description = "Copies "
//    outputs.upToDateWhen { false }
//
//
//    println("Copying from ${project.layout.projectDirectory}/src/site/markdown")
//    println("          to ${project.layout.buildDirectory.asFile.get()}/site/markdown/${project.name}")
//
//        from("${project.layout.projectDirectory}/src/site/markdown")
//        into("${project.layout.buildDirectory.asFile.get()}/site/markdown/${project.name}")
//}
//
//tasks.register<Copy>("processSiteAssets_twiki") {
//    description = "Copies "
//    outputs.upToDateWhen { false }
//
//
//    println("Copying from ${project.layout.projectDirectory}/src/site/twiki")
//    println("          to ${project.layout.buildDirectory.asFile.get()}/site/twiki/${project.name}")
//    copy {
//        from("${project.layout.projectDirectory}/src/site/twiki")
//        into("${project.layout.buildDirectory.asFile.get()}/site/twiki/${project.name}")
//    }
//}
//
//tasks.register<Copy>("processSiteAssets_xdoc") {
//    description = "Copies "
//    outputs.upToDateWhen { false }
//
//
//    println("Copying from ${project.layout.projectDirectory}/src/site/xdoc")
//    println("          to ${project.layout.buildDirectory.asFile.get()}/site/xdoc/${project.name}")
//    copy {
//        from("${project.layout.projectDirectory}/src/site/xdoc")
//        into("${project.layout.buildDirectory.asFile.get()}/site/xdoc/${project.name}")
//    }
//}
//
//tasks.register<Copy>("processSiteAssets_xhtml") {
//    description = "Copies "
//    outputs.upToDateWhen { false }
//
//
//    println("Copying from ${project.layout.projectDirectory}/src/site/xhtml")
//    println("          to ${project.layout.buildDirectory.asFile.get()}/site/xhtml/${project.name}")
//
//    from("${project.layout.projectDirectory}/src/site/xhtml")
//    into("${project.layout.buildDirectory.asFile.get()}/site/xhtml/${project.name}")
//}


//    println("Copying .md files from ./${project.name}/src")
//    println("          to ${project.layout.buildDirectory.asFile.get()}/site/markdown/${project.name}")
//    from("${project.layout.projectDirectory}/src")  {
//        include("**.md")
//        exclude("**/site/")
//    }
//    into("${project.layout.buildDirectory.asFile.get()}/site/markdown/{$version}/${project.name}")
//
//    println("Copying from ${project.layout.projectDirectory.asFile}/src/site")
//    println("          to ${project.layout.buildDirectory.asFile.get()}/site/${version}")
//    from("${project.layout.projectDirectory.asFile}/src/site")
//    into("${project.layout.buildDirectory.asFile.get()}/site/${version}")


//configurations {
//    create("siteAssets") {
//        dependencies {
//            tasks.getByName("processSiteAssets_apt").outputs.files
//            tasks.getByName("processSiteAssets_asciidoc").outputs.files
//            tasks.getByName("processSiteAssets_confluence").outputs.files
//            tasks.getByName("processSiteAssets_docbook").outputs.files
//            tasks.getByName("processSiteAssets_fml").outputs.files
//            tasks.getByName("processSiteAssets_markdown").outputs.files
//            tasks.getByName("processSiteAssets_twiki").outputs.files
//            tasks.getByName("processSiteAssets_xdoc").outputs.files
//            tasks.getByName("processSiteAssets_xhtml").outputs.files
//        }
//    }
//}



