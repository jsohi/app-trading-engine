dependencies {
    implementation(project(":messages"))
    implementation(libs.aeron.cluster) // AeronCluster client for cluster ingress/egress (APP-31)
    implementation(libs.aeron.client) // Aeron IPC subscriptions/publications
    implementation(libs.agrona)
    // Zero-alloc logging for hot path — no SLF4J, no Log4j2
    implementation(libs.gflog.api)
    runtimeOnly(libs.gflog.core)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

// Ensure no logging frameworks leak in via transitive dependencies
configurations.runtimeClasspath {
    exclude(group = "org.apache.logging.log4j")
    exclude(group = "org.slf4j")
    exclude(group = "com.lmax", module = "disruptor")
}
