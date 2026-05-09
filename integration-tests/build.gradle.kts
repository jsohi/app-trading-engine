plugins {
    application
}

application {
    mainClass.set("com.trading.engine.e2e.E2EFixTestClient")
    applicationDefaultJvmArgs =
        listOf(
            "-Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
            "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        )
}

dependencies {
    // E2EFixTestClient (standalone main — Artio initiator)
    implementation(project(":fix-codecs"))
    implementation(project(":messages"))
    implementation(libs.artio.core)
    implementation(libs.aeron.driver)
    implementation(libs.aeron.archive)
    implementation(libs.agrona)
    implementation(libs.log4j.api)
    implementation(libs.snakeyaml)

    // Existing test dependencies
    testImplementation(project(":cluster"))
    testImplementation(project(":launcher"))
    testImplementation(project(":query-service"))
    testImplementation(project(":test-support"))
    testImplementation(libs.artio.core)
    testImplementation(libs.aeron.test.support)
    // HdrHistogram for RfqLatencyRegressionIT P50/P99/P999 budgeting (APP-232 §11.3).
    testImplementation(libs.hdr.histogram)
}

// ─── perfTest task (APP-232 §7.9) ───────────────────────────────────────────
// Filters JUnit 6 tests tagged @Tag("perf"). Gated behind `-PperfTest` so PR pipelines run
// only the regular IT suite; nightly CI runs perf on a dedicated `perf-runner-v1` host.
tasks.register<Test>("perfTest") {
    description = "Runs latency / throughput regression tests tagged @Tag(\"perf\"). " +
        "Gated on -PperfTest so it never runs in the PR pipeline."
    group = "verification"
    useJUnitPlatform {
        includeTags("perf")
    }
    onlyIf { project.hasProperty("perfTest") }
    shouldRunAfter("test")
    // Pinned to the same JVM args as :test for determinism across runs.
    jvmArgs =
        listOf(
            "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        )
}

// Default :test excludes the perf tag so regular CI never picks them up.
tasks.named<Test>("test") {
    useJUnitPlatform {
        excludeTags("perf")
    }
}
