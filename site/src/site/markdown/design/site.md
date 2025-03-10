# Site Documentation

## Overview

The documentation for this site is generated from javadocs, source code extracts, and a site publishing framework.  Each component is responsible for its own documentation.  Most of the documentation is in a `src/site` directory for the component.  The documentation is written in one of several document formats and converted to HTML during the site build process.  In addition, documentation can be extracted from the source code directly.  For example the information from the `ConfigDef` for a connector can be extracted into a file and inserted into a document generated for the site.

This implementation uses the [maven-site-plugin](https://maven.apache.org/plugins/maven-site-plugin/) so [examples](https://maven.apache.org/plugins/maven-site-plugin/examples/creating-content.html#Documentation_formats) from there may be useful.

## src/site Layout

The `src/site` directory contains the static information that will be posted to the final site.  It may contain one or more directories for specific file formats.  For example the `src/site/markdown` directory will contain markdown files that will be transformed into HTML.

### Submodules

Submodules should specify subdirectories with their names.  For example the `s3-source-connector` has directories `src/site/markdown/s3-source-connector` and `src/site/s3-source-connector`.  The `src/site/markdown/s3-source-connector` directory contains markdown based documents for the top level of the s3-source-connector documentation tree, while `src/site/markdown/s3-source-connector` contains files that are included in the top level of the s3-source-connector documentation tree but are not transformed.

When the directory is processed it will be placed into the final site in the `s3-source-connector` directory.

Submodules must include the following information in their `build.gradle.kts` file for the site generation to work.

```gradle
plugins {
  id("aiven-apache-kafka-connectors-all.docs")
}
```

This will enable the `copySiteAssets` task to copy the site files for the final site creation.

### Top Level Site

The top level site directory contains the information necessary to generate the entire site.  For example this document is found at `site/src/site/markdown/site.md` and is converted to `site.html` on the main site.

In addition, there is a `site/src/site/site.xml` document that describes the left hand menu found on every page.  This file should be edited to add new top level menu items for the submodules or for the site as a whole.


### File Formats

The system understands the following file formats and will convert them to HTML.

 | Format                                                                                     | Short description | /src/site Directory | File Extensions |
 |--------------------------------------------------------------------------------------------| ----------------- |---------------------|-----------------|
  | [Apt](https://maven.apache.org/doxia/references/apt-format.html)                           | Almost Plain Text | 	apt                | apt             |
 | [AsciiDoc](https://asciidoctor.org/)                                                       |	Asciidoctor  | asciidoc	           | adoc, asciidoc	 |
 | [Confluence](https://maven.apache.org/doxia/modules/index.html#Confluence)                 |	Confluence Enterprise Wiki | confluence          | confluence |
  | [Simplified DocBook](https://maven.apache.org/doxia/modules/index.html#Simplified_DocBook) | Simplified DocBook XML Standard | docbook             |xml |
 | [FML](https://maven.apache.org/doxia/references/fml-format.html)                           | FAQ Markup Language | fml                 | fml|
 | [Markdown](https://maven.apache.org/doxia/modules/index.html#Markdown)                     | Markdown markup language | markdown            | md, markdown*** |
 | [TWiki](https://maven.apache.org/doxia/modules/index.html#TWiki)                           | TWiki Structured Wiki | twiki               | twiki |
 | [Xdoc](https://maven.apache.org/doxia/references/xdoc-format.html)                         | XML Documentation Format | xdoc                | xml |
 | [XHTML](https://maven.apache.org/doxia/modules/index.html#XHTML)                           | Extensible Hypertext Markup Language	| xhtml               | xhtml |


#### Velocity Plugin

In addition to the file formats above the system will process the documents with [Velocity](https://velocity.apache.org/) using [VTL](https://velocity.apache.org/engine/2.4.1/vtl-reference.html) scripting.  This is most commonly used to merge additional information into a document using the [#include](https://velocity.apache.org/engine/2.4.1/user-guide.html#include) statement.  To use the Velocity engine create a file in a `src/site` directory that uses the `.vm` extension.  For example `/src/site/markdown/test.md.vm` would execute the Velocity engine against the file `test.md.vm` and produce the `test.md` file which would then be processed by the normal site processing. 

## Gradle Tasks

There are two `site` subproject gradle tasks:

 * `:site:createSite` - creates the complete HTML based site in the `/site/target/site` directory.
 * `:site:populateSite` - populates the complete HTML based site with files that were not included in the initial build.  This includes the javadoc for the submodules.

## GitHub Action

There is a `build_site.yml` GitHub workflow that executes the above-mentioned Gradle tasks and then publishes the resulting `./site/target/site` to the GitHub pages for the project. 
