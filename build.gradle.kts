plugins { id("aiven-apache-kafka-connectors-all.java-conventions") }

copy {
  println("Copying from ${rootProject.layout.projectDirectory.asFile}/*.md")
  println("          to ${project.layout.buildDirectory.asFile.get()}/site/")
  from("${rootProject.layout.projectDirectory}") {
    include("*.md")
    rename { fileName ->
      // a simple way is to remove the "-$version" from the jar filename
      // but you can customize the filename replacement rule as you wish.
      fileName.replace("README", "index")
    }
  }
  into("${project.layout.buildDirectory.asFile.get()}/site/markdown/")
}

tasks.register<Copy>("copySiteAssets") {
  outputs.upToDateWhen { false }
  println("Copying from ${rootProject.layout.projectDirectory.asFile}/*.md")
  println("          to ${project.layout.projectDirectory.asFile}/site/build/site/markdown")
  from("${rootProject.layout.projectDirectory}") { include("*.md") }
  from("${rootProject.layout.projectDirectory}") {
    include("README*.md")
    rename { fileName ->
      // a simple way is to remove the "-$version" from the jar filename
      // but you can customize the filename replacement rule as you wish.
      fileName.replace("README", "index")
    }
  }
  into("${project.layout.projectDirectory.asFile}/site/build/site/markdown")
}
