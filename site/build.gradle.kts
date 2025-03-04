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

tasks.register<Copy>("copySiteAssets") {
    outputs.upToDateWhen { false }
    println("Copying projects")
    rootProject.subprojects
        .filter { s -> s.name != "site" }
        .filter { s -> s.tasks.findByName("copySiteAssets") != null }
        .forEach { s ->
            println("Project: "+ s.name)
            dependsOn(s.tasks.getByName("copySiteAssets"))
                from(s.tasks.getByName("copySiteAssets").outputs.files)
            }
    from("${project.layout.projectDirectory}/src/site")
    into("${project.layout.buildDirectory.asFile.get()}/site")
}

tasks.register<Exec>("createSite") {
    outputs.upToDateWhen { false }
    mustRunAfter("copySiteAssets")
    dependsOn("copySiteAssets")
    println("Executing ${project.projectDir}/mvnw")
    executable("${project.projectDir}/mvnw")
    args("clean", "site:site")
}

tasks.register<Copy>("buildSite") {
    group = "Documentation"
    description = "Build site"
    outputs.upToDateWhen { false }
    //dependsOn("createSite")
    //mustRunAfter("createSite")
    rootProject.subprojects
        .filter { s -> s.name != "site" }
        .filter { s -> s.tasks.findByName("copySiteAssets") != null }
        .forEach { s ->
            dependsOn(s.tasks.getByName("javadoc"))
            project.copy {
                println("Project: " + s.name)
                println("to: ${project.layout.projectDirectory.asFile}/target/site/${s.name}/javadoc")
                from("${s.layout.buildDirectory.asFile.get()}/docs/javadoc")
                into("${project.layout.projectDirectory.asFile}/target/site/${s.name}/javadoc")
            }
            project.copy {
                println("to: ${project.layout.projectDirectory.asFile}/target/site/${s.name}")
                from("${project.layout.buildDirectory.asFile.get()}/site/${s.name}")
                into("${project.layout.projectDirectory.asFile}/target/site/${s.name}")
            }
        }
}











