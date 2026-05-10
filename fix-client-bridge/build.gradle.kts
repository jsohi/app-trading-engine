// =============================================================================
// FIX Client Bridge — Artio FIX 4.4 initiator + embedded Netty JSON WebSocket
// server (browser ↔ trading engine). See docs/fix-client-bridge.md.
//
// Logging: Log4j2 infra (parent-plan deviation, locked §1). Per-message
// zero-alloc invariants are enforced by *AllocTest regression tests on the
// dispatch pipeline (BrowserMessageReader.parse → JsonToFixTranslator →
// Session#trySend) rather than GFLog. Netty framing layers allocate via
// pooled buffers as :websocket-server already does.
// =============================================================================

plugins {
    application
    jacoco
}

// --- Source sets: separate integrationTest from unit test ---
// integrationTest spins up real Aeron MediaDriver + real FixGateway + real
// Artio acceptor + real dev-jwks-server.sh. Unit `test` must NOT touch any
// of those. Both run on the same Gradle worker (forkEvery=0) to avoid Artio
// FixEngine port-leak under fast worker recycling (residual risk R1).
sourceSets {
    create("integrationTest") {
        compileClasspath += sourceSets["main"].output + sourceSets["test"].output
        runtimeClasspath += sourceSets["main"].output + sourceSets["test"].output
        java.srcDir("src/integrationTest/java")
        resources.srcDir("src/integrationTest/resources")
    }
}

val integrationTestImplementation: Configuration by configurations.getting {
    extendsFrom(configurations["testImplementation"])
}
configurations["integrationTestRuntimeOnly"].extendsFrom(configurations["testRuntimeOnly"])

dependencies {
    implementation(platform(libs.netty.bom))

    implementation(project(":messages"))
    implementation(project(":fix-codecs"))
    // :gateway provides ClusterClient.forTesting and FixedPoint reused by both translators.
    implementation(project(":gateway"))
    // :websocket-server provides JwtValidator (public ctor + preflightOrThrow). Only auth
    // bits are reused — no SBE binary-frame plumbing comes across.
    implementation(project(":websocket-server"))

    implementation(libs.aeron.client)
    implementation(libs.agrona)

    implementation(libs.netty.transport)
    implementation(libs.netty.handler)
    implementation(libs.netty.codec.http)
    runtimeOnly(libs.netty.transport.native.epoll) { artifact { classifier = "linux-x86_64" } }
    runtimeOnly(libs.netty.transport.native.epoll) { artifact { classifier = "linux-aarch_64" } }
    runtimeOnly(libs.netty.transport.native.kqueue) { artifact { classifier = "osx-x86_64" } }
    runtimeOnly(libs.netty.transport.native.kqueue) { artifact { classifier = "osx-aarch_64" } }

    implementation(libs.artio.core)
    implementation(libs.nimbus.jose.jwt)
    implementation(libs.snakeyaml)
    // Metrics: Prometheus registry + core API. The bridge exposes per-session counters via the
    // standard Micrometer abstraction so the launcher can swap the registry impl (Prometheus
    // for prod, SimpleMeterRegistry for tests) without changing call sites.
    implementation(libs.micrometer.registry.prometheus)

    testImplementation(project(":test-support"))

    integrationTestImplementation(project(":test-support"))
    integrationTestImplementation(libs.aeron.driver)
    integrationTestImplementation(libs.aeron.archive)
}

application {
    // The bridge is launched as a component of :launcher (TradingEngineLauncher Step 10c).
    // The default mainClass here points at the smoke client used by scripts/e2e.sh — it ships
    // alongside the bridge classes on the runtime classpath via `installDist`.
    mainClass.set("com.trading.engine.fixbridge.smoke.BridgeSmokeClient")
    applicationDefaultJvmArgs =
        listOf(
            "-Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
        )
}

// --- Unit tests ---
tasks.named<Test>("test") {
    useJUnitPlatform()
    jvmArgs("-Dio.netty.leakDetection.level=PARANOID")
}

// --- Integration tests ---
val integrationTest =
    tasks.register<Test>("integrationTest") {
        description = "Runs integration tests that boot a real MediaDriver + FixGateway + Artio acceptor + dev JWKS server."
        group = "verification"
        testClassesDirs = sourceSets["integrationTest"].output.classesDirs
        classpath = sourceSets["integrationTest"].runtimeClasspath
        useJUnitPlatform()
        // forkEvery=0 keeps a single JVM per Gradle worker to mitigate Artio FixEngine port-leak
        // under fast worker recycling (residual risk R1).
        forkEvery = 0
        jvmArgs("-Dio.netty.leakDetection.level=PARANOID")
        // Enforce ordering — integrationTest must wait for unit tests to finish so a parallel
        // worker doesn't race the unit-test JVM on Aeron temp dirs / shared resources.
        // shouldRunAfter is advisory; mustRunAfter is enforcing.
        mustRunAfter(tasks.named("test"))
    }

// --- Allocation tripwire (separate task — not coverage-counted, opt-in) ---
val allocTest =
    tasks.register<Test>("allocTest") {
        description = "Runs *AllocTest regression suites under -DrunAllocTests=true. Not coverage-counted."
        group = "verification"
        testClassesDirs = sourceSets["test"].output.classesDirs
        classpath = sourceSets["test"].runtimeClasspath
        useJUnitPlatform {
            // The *AllocTest classes are tagged via @EnabledIfSystemProperty(named = "runAllocTests").
            // Setting the system property here gates them in. The regular `test` task does NOT set
            // this property, so alloc tests are skipped in the default coverage gate.
        }
        systemProperty("runAllocTests", "true")
        // Prevent JIT noise: a single fork keeps the same warmed code cache across tests.
        forkEvery = 0
    }

// --- Coverage merge: combine unit + integration JaCoCo for the bridge gate (locked §23) ---
val jacocoMergedReport =
    tasks.register<JacocoReport>("jacocoMergedReport") {
        description = "Merged unit + integration JaCoCo coverage report (excludes *AllocTest*)."
        group = "verification"
        dependsOn(tasks.named("test"), integrationTest)

        executionData(
            fileTree(layout.buildDirectory).include(
                "jacoco/test.exec",
                "jacoco/integrationTest.exec",
            ),
        )
        sourceSets(sourceSets["main"])

        // Per locked §23: alloc tests are not coverage-counted (they exercise warmed loops only).
        classDirectories.setFrom(
            sourceSets["main"].output.asFileTree.matching {
                exclude("**/*AllocTest*")
            },
        )

        reports {
            xml.required.set(true)
            html.required.set(true)
        }
    }

tasks.named("check") {
    dependsOn(integrationTest, jacocoMergedReport)
}
