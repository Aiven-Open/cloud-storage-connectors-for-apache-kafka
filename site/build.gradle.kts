plugins {
    id("io.freefair.aggregate-javadoc") version "8.12.1"
}

repositories {
    maven("https://plugins.gradle.org/m2/")
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

tasks.register<Exec>("execVale") {
    description = "Executes the Vale text linter"
    group = "Documentation"
    executable("/usr/bin/docker")
    args("run", "--rm", "-v", "${project.rootDir}:/project:Z", "-v", "${project.rootDir}/.github/vale/styles:/styles:Z", "-v", "${project.projectDir}:/site:Z", "-w", "/docs", "jdkato/vale", "--filter=warn.expr", "--config=/project/.vale.ini", "--glob=!**/build/**", ".")
}

tasks.register<Copy>("processSiteAssets") {
    group = "Documentation"
    description = "Copies "
    outputs.upToDateWhen { false }
    rootProject.subprojects.filter { s -> s.name != "site" }.forEach { s ->
        println("Copying from ${s.layout.projectDirectory}/src/site")
        println("          to ${project.layout.buildDirectory.asFile.get()}/site")
        rootProject.copy {
            from("${s.layout.projectDirectory}/src/site")
            into("${project.layout.buildDirectory.asFile.get()}/site")
        }

        println("Copying .md files from ${s.layout.projectDirectory}/src")
        println("          to ${project.layout.buildDirectory.asFile.get()}/site/markdown/${s.name}")
        rootProject.copy {
            from("${s.layout.projectDirectory}/src")  {
                include("**.md")
            }
            into("${project.layout.buildDirectory.asFile.get()}/site/markdown/${s.name}")
        }
        println("")
    }
    println("Copying from ${project.layout.projectDirectory.asFile}/src/site")
    println("          to ${project.layout.buildDirectory.asFile.get()}/build/site")
    rootProject.copy {
        from("${project.layout.projectDirectory.asFile}/src/site")
        into("${project.layout.buildDirectory.asFile.get()}/site")
    }
    println("DONE")
}

tasks.register<Exec>("createSite") {
    group = "Documentation"
    description = "Build site"
    outputs.upToDateWhen { false }
    dependsOn("processSiteAssets")
    println("Executing ${project.projectDir}/mvnw")
    executable("${project.projectDir}/mvnw")
    args("clean", "site:site")
}

tasks.register<Copy>("copySiteYamlFiles") {
    group = "Documentation"
    description = "Copies documentation yaml files."
    outputs.upToDateWhen { false }
    from("${project.layout.projectDirectory.asFile}/build/site")
    into("${project.layout.projectDirectory.asFile}/target/site")
    include("**/*.yml")
}

tasks.register<Copy>("copyJavadocs") {
    group = "Documentation"
    description = "Copies javadocs"
    outputs.upToDateWhen { false }
    println("Copying javadoc from subprojects to site")
    rootProject.subprojects.filter { s -> s.name != "site" }.forEach { s ->
        println("Copying from ${s.layout.projectDirectory}/build/docs/javadoc")
        println("          to ${project.layout.projectDirectory.asFile}/target/site/${s.name}/javadoc")
        rootProject.copy {
            from("${s.layout.projectDirectory}/build/docs/javadoc")
            into("${project.layout.projectDirectory.asFile}/target/site/${s.name}/javadoc")
        }
        println("")
    }
}

tasks.register<Copy>("deploySite") {
    group = "Documentation"
    description = "Copies javadocs"
    outputs.upToDateWhen { false }
    println("Copying site to docs directory")
    from("${project.layout.projectDirectory.asFile}/target/site")
    into("${rootProject.projectDir}/docs/")
}










