
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
