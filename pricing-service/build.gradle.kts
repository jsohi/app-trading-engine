plugins {
    application
}

dependencies {
    implementation(project(":messages"))
    implementation(libs.aeron.client)
    implementation(libs.agrona)
    // Zero-alloc logging for hot path — no SLF4J, no Log4j2
    implementation(libs.gflog.api)
    runtimeOnly(libs.gflog.core)
    // YAML config loading (cold path, startup only)
    implementation(libs.snakeyaml)

    testImplementation(project(":test-support"))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

// Ensure no logging frameworks leak in via transitive dependencies
configurations.runtimeClasspath {
    exclude(group = "org.apache.logging.log4j")
    exclude(group = "org.slf4j")
    exclude(group = "com.lmax", module = "disruptor")
}

application {
    mainClass.set("com.trading.engine.pricing.PricingServiceMain")
}

// ----------------------------------------------------------------------------------------------
// JCStress concurrency-stress source set — Phase 3 Commit 4 / MarketDataPublisherSingleWriterJCStress.
// Mirrors the websocket-server module's JCStress setup. Source set lives at src/jcstress/java/.
// The annotation processor generates harness classes at compile time. Runtime is a JavaExec
// task pointing the JVM at the production pricing-service classes + the compiled jcstress
// sources. The MarketDataPublisher single-writer invariant guard is contract-tested here: the
// test asserts the runtime guard fires when two threads attempt onTick concurrently.
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
    description = "Runs the JCStress concurrency-stress harness for MarketDataPublisher."
    dependsOn(jcstressJar)
    classpath = files(jcstressJar.get().archiveFile)
    mainClass.set("org.openjdk.jcstress.Main")
    // Pinned run budget per Phase 3 plan §Commit 4: mode=quick + 20s/test + 1 fork + 5 iters.
    args(
        "-m",
        "quick",
        "-t",
        "MarketDataPublisher.*JCStress",
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
