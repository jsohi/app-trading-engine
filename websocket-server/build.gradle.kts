plugins {
    application
}

dependencies {
    implementation(platform(libs.netty.bom))

    implementation(project(":messages"))
    implementation(project(":projections"))
    implementation(project(":query-service"))

    implementation(libs.aeron.client)
    implementation(libs.aeron.cluster)
    implementation(libs.agrona)

    implementation(libs.netty.transport)
    implementation(libs.netty.handler)
    implementation(libs.netty.codec.http)
    implementation(libs.netty.tcnative.boringssl)
    // tcnative-boringssl-static native classifiers — modern Netty requires explicit
    // per-platform runtime deps; the classifier-less artifact only contains the Java
    // glue. Without these, OpenSslContext throws UnsatisfiedLinkError at runtime.
    runtimeOnly(libs.netty.tcnative.boringssl) { artifact { classifier = "linux-x86_64" } }
    runtimeOnly(libs.netty.tcnative.boringssl) { artifact { classifier = "linux-aarch_64" } }
    runtimeOnly(libs.netty.tcnative.boringssl) { artifact { classifier = "osx-x86_64" } }
    runtimeOnly(libs.netty.tcnative.boringssl) { artifact { classifier = "osx-aarch_64" } }
    runtimeOnly(libs.netty.tcnative.boringssl) { artifact { classifier = "windows-x86_64" } }
    runtimeOnly(libs.netty.transport.native.epoll) { artifact { classifier = "linux-x86_64" } }
    runtimeOnly(libs.netty.transport.native.epoll) { artifact { classifier = "linux-aarch_64" } }
    runtimeOnly(libs.netty.transport.native.kqueue) { artifact { classifier = "osx-x86_64" } }
    runtimeOnly(libs.netty.transport.native.kqueue) { artifact { classifier = "osx-aarch_64" } }

    implementation(libs.micrometer.registry.prometheus)
    implementation(libs.nimbus.jose.jwt)
    implementation(libs.snakeyaml)
    // Jackson for OIDC discovery doc parsing (RFC 8414 — see OidcDiscoveryClient).
    // Pinned to the project-wide jackson version to keep CVE patching uniform.
    implementation(libs.jackson.databind)

    testImplementation(project(":test-support"))
    // Jetty 11 stub server for WebSocketServerConfigOidcDiscoveryTest. JUnit-scoped only —
    // the production server uses Netty, not Jetty. Centralised in libs.versions.toml so OWASP
    // dependencyCheckAnalyze covers the version pin.
    testImplementation(libs.jetty.server)
    testImplementation(libs.jetty.servlet)
}

application {
    mainClass.set("com.trading.engine.websocket.WebSocketServerMain")
    applicationDefaultJvmArgs =
        listOf(
            "-Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
        )
}

tasks.withType<Test> {
    jvmArgs("-Dio.netty.leakDetection.level=PARANOID")
    // Allow per-PR stress phase to be opted in via -Pstress=true (see ReliableStreamTrackerReconnectTest).
    // Forwarded as a system property so JUnit5 @Tag exclusion can be aligned with the runtime
    // gate inside the test method (defence-in-depth).
    systemProperty("stress", findProperty("stress")?.toString() ?: "false")
    val runStress = (findProperty("stress")?.toString() ?: "false") == "true"
    useJUnitPlatform {
        if (!runStress) {
            excludeTags("stress")
        }
    }
}

// ----------------------------------------------------------------------------------------------
// JCStress: nanosecond-resolution concurrency harness for ReliableStreamTracker
// (plan §14). Source set lives at src/jcstress/java/. Annotation processor generates the
// harness classes at compile time. The runtime is invoked via java -jar with the JCStress
// fat jar; we use a JavaExec task that points the JVM at the production websocket-server
// classes + the compiled jcstress sources.
// ----------------------------------------------------------------------------------------------
sourceSets {
    create("jcstress") {
        java.srcDir("src/jcstress/java")
        compileClasspath += sourceSets["main"].output + configurations.runtimeClasspath.get()
        runtimeClasspath += output + compileClasspath
    }
}

val jcstressImplementation: Configuration by configurations.getting {
    extendsFrom(configurations.implementation.get())
}
val jcstressAnnotationProcessor: Configuration by configurations.getting

dependencies {
    jcstressImplementation(libs.jcstress.core)
    jcstressAnnotationProcessor(libs.jcstress.core)
}

// Fat-jar the jcstress source set + dependencies so the JCStress runner can find every
// @JCStressTest class via classpath scanning. Keeps the runtime invocation simple
// (no manifest fiddling, no Module-Path).
val jcstressJar =
    tasks.register<Jar>("jcstressJar") {
        group = "verification"
        description = "Builds a fat JAR of the JCStress source set + runtime deps."
        archiveClassifier.set("jcstress")
        from(sourceSets["jcstress"].output)
        from(sourceSets["main"].output)
        from(
            provider {
                configurations["jcstressRuntimeClasspath"].map { if (it.isDirectory) it else zipTree(it) }
            },
        )
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        manifest {
            attributes("Main-Class" to "org.openjdk.jcstress.Main")
        }
    }

tasks.register<JavaExec>("jcstress") {
    group = "verification"
    description = "Runs the JCStress concurrency-stress harness for ReliableStreamTracker."
    dependsOn(jcstressJar)
    classpath = files(jcstressJar.get().archiveFile)
    mainClass.set("org.openjdk.jcstress.Main")
    // Pinned run budget per plan §14: mode=quick + 20s/test + 1 fork + 5 iters. Keeps wall-clock
    // ~3-5 min for the two test classes; well within the 40-min :fullStackE2e ceiling. The
    // Playwright phase runs in parallel under scripts/full-stack-e2e.sh — they share no JVM.
    args(
        "-m",
        "quick",
        "-t",
        "ReliableStreamTracker.*JCStress",
        "-time",
        "20000",
        "-f",
        "1",
        "-iters",
        "5",
        "-r",
        "${project.layout.buildDirectory.get().asFile}/reports/jcstress",
    )
}
