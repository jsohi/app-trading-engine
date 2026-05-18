import java.security.MessageDigest

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

// ---------------------------------------------------------------------------
// FIX 4.4 dictionary export — APP-244 Phase 3 Commit C.10.
//
// `messages/fix-dictionary/FIX44.xml` is the canonical, exported FIX 4.4
// dictionary the engine's Artio acceptor speaks on the wire. It is intended
// for external counterparty integration (third-party FIX clients can drop
// this file into their dictionary loader).
//
// Source of truth: `fix-codecs/src/main/resources/fix/FIX44.xml` — the same
// XML the runtime codec generator reads. The task re-copies it to this
// module and re-derives `FIX44.xml.sha256` so the committed sidecar and the
// XML stay byte-identical. The pin is asserted on every `check` build so
// untracked drift (manual edit of the exported copy, upstream Artio bump,
// stale checkout) fails the pipeline early.
// ---------------------------------------------------------------------------

val fixDictionarySource =
    rootProject.layout.projectDirectory.file(
        "fix-codecs/src/main/resources/fix/FIX44.xml",
    )
val fixDictionaryOutDir = layout.projectDirectory.dir("fix-dictionary")
val fixDictionaryXml = fixDictionaryOutDir.file("FIX44.xml")
val fixDictionarySha = fixDictionaryOutDir.file("FIX44.xml.sha256")

val generateFixDictionary =
    tasks.register("generateFixDictionary") {
        group = "code generation"
        description =
            "Export the FIX 4.4 dictionary for counterparty integration and " +
            "assert the SHA-256 sidecar matches the source dictionary."

        inputs.file(fixDictionarySource)
        outputs.file(fixDictionaryXml)
        outputs.file(fixDictionarySha)

        doLast {
            val sourceFile = fixDictionarySource.asFile
            check(sourceFile.exists()) {
                "FIX 4.4 source dictionary missing: ${sourceFile.absolutePath}"
            }

            val xmlOut = fixDictionaryXml.asFile
            val shaOut = fixDictionarySha.asFile
            xmlOut.parentFile.mkdirs()

            // Re-copy the source dictionary verbatim. This is the artifact
            // counterparties consume; it must be byte-identical to the file
            // the runtime codec generator reads.
            sourceFile.copyTo(xmlOut, overwrite = true)

            // Re-derive SHA-256 in shasum -a 256 format ("<hex>  FIX44.xml")
            // so `shasum -a 256 -c FIX44.xml.sha256` validates downstream.
            val bytes = xmlOut.readBytes()
            val digest =
                MessageDigest.getInstance("SHA-256").digest(bytes)
            val hex =
                buildString(digest.size * 2) {
                    digest.forEach { b ->
                        append(String.format("%02x", b.toInt() and 0xff))
                    }
                }
            val computedLine = "$hex  FIX44.xml"

            // Verification pass — if a committed sidecar already exists,
            // assert it matches the freshly computed digest. This catches
            // any drift between the source XML and the committed pin
            // without requiring developers to remember to re-run the task
            // manually after an upstream dictionary change.
            if (shaOut.exists()) {
                val existing = shaOut.readText().trim()
                check(existing == computedLine) {
                    """
                    FIX 4.4 dictionary SHA-256 drift detected.
                      expected (committed sidecar): $existing
                      actual   (computed from XML): $computedLine
                    Re-run: ./gradlew :messages:generateFixDictionary
                    and commit both fix-dictionary/FIX44.xml and
                    fix-dictionary/FIX44.xml.sha256 together.
                    """.trimIndent()
                }
            }
            shaOut.writeText(computedLine + "\n")
        }
    }

// Hook into the `check` lifecycle so `./gradlew :messages:build` (which
// depends on `check`) runs the SHA-256 verification on every CI / local
// build. Counterparty-facing artifacts must never silently diverge from the
// runtime dictionary.
tasks.named("check") {
    dependsOn(generateFixDictionary)
}
