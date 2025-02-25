import org.apache.tools.ant.taskdefs.Mkdir

plugins {
    `kotlin-dsl`
}

repositories {
    maven("https://plugins.gradle.org/m2/")
    mavenCentral()
}

val spotbugsVersion by extra ("6.0.4")
val spotlessVersion by extra ("6.23.2")

dependencies {
    implementation("com.diffplug.spotless:spotless-plugin-gradle:${spotlessVersion}")
    implementation("com.github.spotbugs.snom:spotbugs-gradle-plugin:${spotbugsVersion}")
    implementation(kotlin("stdlib-jdk8"))
}

tasks {
    register<Exec>("execVale") {
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
}



