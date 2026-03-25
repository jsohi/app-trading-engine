plugins {
    java
}

val sbeVersion: String by project

val sbeTool by configurations.creating

dependencies {
    implementation("uk.co.real-logic:sbe-all:$sbeVersion")
    sbeTool("uk.co.real-logic:sbe-all:$sbeVersion")
}

val schemaFile = layout.projectDirectory.file("src/main/resources/trading-schema.xml")
val codecOutputDir = layout.buildDirectory.dir("generated/src/main/java")

val generateCodecs = tasks.register<JavaExec>("generateCodecs") {
    group = "code generation"
    description = "Generate SBE codecs from trading-schema.xml"

    mainClass.set("uk.co.real_logic.sbe.SbeTool")
    classpath = sbeTool

    inputs.file(schemaFile).optional()
    outputs.dir(codecOutputDir)

    systemProperty("sbe.output.dir", codecOutputDir.get().asFile.absolutePath)
    systemProperty("sbe.target.language", "Java")
    systemProperty("sbe.validation.stop.on.error", "true")
    systemProperty("sbe.validation.xsd", "fpl/sbe.xsd")

    args(schemaFile.asFile.absolutePath)

    onlyIf { schemaFile.asFile.exists() }
}

sourceSets {
    main {
        java {
            srcDir(codecOutputDir)
        }
    }
}

tasks.compileJava {
    dependsOn(generateCodecs)
}
