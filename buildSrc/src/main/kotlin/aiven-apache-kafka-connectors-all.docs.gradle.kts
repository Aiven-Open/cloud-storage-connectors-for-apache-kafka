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

group = "io.aiven"

tasks.register<Copy>("copyMarkdown") {
    group = "Documentation"
    description = "Copies m*.md to build/site/markdown/projectnName/"
    println("Copying from ${project.layout.projectDirectory.asFile}/*.md")
    println("          to ${project.layout.buildDirectory.asFile.get()}/site/markdown")
    from("${project.layout.projectDirectory}") {
        include("README.md")
        rename { fileName ->
            // a simple way is to remove the "-$version" from the jar filename
            // but you can customize the filename replacement rule as you wish.
            fileName.replace("README", "index")
        }
    }
    from("${project.layout.projectDirectory}") {
        include("*.md")
    }
    into("${project.layout.buildDirectory.asFile.get()}/site/markdown/${project.name}")
    println("")
}

tasks.register<Copy>("copySiteAssets") {
    group = "Documentation"
    description = "Copies "
    description = "Copies src/site to build/site"
    dependsOn("copyMarkdown")
    println("Copying from ${project.layout.projectDirectory.asFile}/src/site")
    println("          to ${project.layout.buildDirectory.asFile.get()}/site/")
    from("${project.layout.projectDirectory.asFile}/src/site")
    into("${project.layout.buildDirectory.asFile.get()}/site/")
    println("")
}
