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

    // APP-244 Phase 3 C.8 — MultiIssuerLauncherRebootArtioTest: needs the websocket-server
    // module (JwtValidator, OidcDiscoveryClient, WebSocketServerConfig), the Nimbus JOSE/JWT
    // library (RSA key generation + JWT signing on the test side), and Jetty 11 (in-process
    // JWKS stub servers for issuers A and B). The Artio "reboot" is simulated by cycling the
    // FIX-side Artio FixEngine in-test; full launcher boot remains owned by
    // scripts/full-stack-e2e.sh §14.
    testImplementation(project(":websocket-server"))
    testImplementation(libs.nimbus.jose.jwt)
    testImplementation(libs.jetty.server)
    testImplementation(libs.jetty.servlet)
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
    // Pinned to the same JVM args as :test for determinism across runs. CodeRabbit PR #81 R2
    // flagged the drift after :test gained the G1GC + Aeron tuning flags (APP-225 §D10) — keep
    // these in sync, since a different GC profile on the perf path would invalidate the
    // baseline comparisons.
    jvmArgs =
        listOf(
            "-XX:+UseG1GC",
            "-XX:MaxGCPauseMillis=50",
            "-Daeron.term.buffer.length=1m",
            "-Daeron.dir.warn.if.exists=false",
            "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        )
}

// Default :test excludes the perf tag so regular CI never picks them up.
//
// JVM-flag pinning (APP-225 §D10) — pin the GC + Aeron tuning that the cluster needs
// for deterministic integration runs:
//   -XX:+UseG1GC + MaxGCPauseMillis=50  →  eliminates G1-pause flake on cold CI hosts
//   -Daeron.term.buffer.length=1m       →  bounds Aeron log segment size so /dev/shm
//                                          doesn't OOM on tight-loop tests
//   -Daeron.dir.warn.if.exists=false    →  silences the verbose first-time warning
//                                          that pollutes test output (Aeron-project
//                                          convention; the dir IS expected to exist
//                                          across test classes that share an MD)
// The `--add-opens` flags are inherited from `applicationDefaultJvmArgs` only on
// `application`-task JVM forks; `tasks.test` runs in its own forked JVM, so the
// reflective-access opens must be repeated here.
tasks.named<Test>("test") {
    useJUnitPlatform {
        excludeTags("perf")
    }
    jvmArgs(
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=50",
        "-Daeron.term.buffer.length=1m",
        "-Daeron.dir.warn.if.exists=false",
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    )
}
