plugins {
    application
}

dependencies {
    implementation(project(":messages"))
    implementation(project(":fix-codecs"))
    implementation(libs.aeron.cluster)
    implementation(libs.aeron.archive)
    implementation(libs.artio.core)
    implementation(libs.agrona)
    // Zero-alloc logging for hot path — no SLF4J, no Log4j2
    implementation(libs.gflog.api)
    runtimeOnly(libs.gflog.core)

    testImplementation(project(":test-support"))
}

// Ensure no logging frameworks leak in via transitive dependencies
configurations.runtimeClasspath {
    exclude(group = "org.apache.logging.log4j")
    exclude(group = "org.slf4j")
    exclude(group = "com.lmax", module = "disruptor")
}

application {
    mainClass.set("com.trading.engine.gateway.GatewayMain")
}

// Forward the opt-in -DrunAllocTests=true system property to the test JVM so the
// NoAllocationTest tripwire can read it. The test is gated behind @EnabledIfSystemProperty
// because GC counts are flaky on shared CI; local runs need the explicit opt-in.
tasks.test {
    systemProperty("runAllocTests", System.getProperty("runAllocTests", "false"))
}
