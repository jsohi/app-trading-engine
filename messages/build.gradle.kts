val sbeTool by configurations.creating

dependencies {
    implementation(libs.agrona)
    sbeTool(libs.sbe.all)
    // jqwik for property-based codec round-trip tests on Phase 3 templates 54-57.
    // Test-only — never bundled into the runtime. Pinned via gradle/libs.versions.toml.
    testImplementation(libs.jqwik)
}

// jqwik registers itself as a JUnit Jupiter test engine via ServiceLoader, so no
// additional engine configuration is required — useJUnitPlatform() in the root
// build.gradle.kts subprojects block picks it up automatically.

val schemaFile = layout.projectDirectory.file("src/main/resources/trading-schema.xml")
val codecOutputDir = layout.buildDirectory.dir("generated/src/main/java")

val generateCodecs =
    tasks.register<JavaExec>("generateCodecs") {
        group = "code generation"
        description = "Generate SBE codecs from trading-schema.xml"

        mainClass.set("uk.co.real_logic.sbe.SbeTool")
        classpath = sbeTool

        inputs.file(schemaFile).optional()
        outputs.dir(codecOutputDir).optional()

        systemProperty("sbe.output.dir", codecOutputDir.get().asFile.absolutePath)
        systemProperty("sbe.target.language", "Java")
        systemProperty("sbe.validation.stop.on.error", "true")
        systemProperty("sbe.validation.warnings.fatal", "true")

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
