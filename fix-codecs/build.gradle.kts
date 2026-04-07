plugins {
    `java-library`
}

// Generates Artio FIX 4.4 encoder/decoder sources from src/main/resources/fix/FIX44.xml.
// Produces classes under com.trading.engine.fix.builder and com.trading.engine.fix.decoder
// that the gateway and fix-client-bridge consume.
//
// Trading engine custom FIX tags (Tenor=10001, ProductType=10013, swap-points 10003,
// leg-level 10010-10012) are NOT in the stock QuickFIX/J FIX44.xml. They will be added
// in APP-45 (Wave 8 — FX Multi-Leg Translators Update). Until then, FX custom-tag
// fields on the SBE side have no FIX wire counterpart and will not round-trip.

val fixCodecTool by configurations.creating

dependencies {
    // Runtime helpers (DecimalFloat, AsciiBuffer, codec base classes) need to be on
    // the compile classpath of consumers, so we expose them via api().
    api(libs.artio.codecs)
    api(libs.agrona)

    // CodecGenerationTool main class lives in the same artio-codecs jar; we put it
    // in its own configuration so the tool isn't a runtime dep of consumers.
    fixCodecTool(libs.artio.codecs)
}

val fixDictionary = layout.projectDirectory.file("src/main/resources/fix/FIX44.xml")
val fixCodecDir = layout.buildDirectory.dir("generated/src/main/java")

val generateFixCodecs =
    tasks.register<JavaExec>("generateFixCodecs") {
        group = "code generation"
        description = "Generate Artio FIX 4.4 codecs from FIX44.xml"

        mainClass.set("uk.co.real_logic.artio.dictionary.CodecGenerationTool")
        classpath = fixCodecTool

        inputs.file(fixDictionary)
        outputs.dir(fixCodecDir)

        // CodecGenerationTool args: <output dir> <semicolon-separated dictionary paths>
        args(fixCodecDir.get().asFile.absolutePath, fixDictionary.asFile.absolutePath)

        // Enable flyweight (zero-allocation) decoder mode and place generated classes
        // under our package.
        systemProperty("fix.codecs.parent_package", "com.trading.engine.fix")
        systemProperty("fix.codecs.flyweight", "true")

        // Agrona's UnsafeApi needs jdk.internal.misc on JDK 17+ (matches root
        // build.gradle.kts's test JVM args).
        jvmArgs(
            "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        )

        onlyIf { fixDictionary.asFile.exists() }
    }

sourceSets {
    main {
        java {
            srcDir(fixCodecDir)
        }
    }
}

tasks.compileJava {
    dependsOn(generateFixCodecs)
}

// Note: the root build.gradle.kts already configures Spotless with `targetExclude("build/**")`
// for every subproject, which keeps the generated codec sources under build/generated/... out
// of the formatter. No per-module Spotless config needed here.
