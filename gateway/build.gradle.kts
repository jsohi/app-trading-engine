plugins {
    application
}

dependencies {
    implementation(project(":messages"))
    implementation(project(":fix-codecs"))
    implementation(libs.aeron.cluster)
    implementation(libs.artio.core)
    implementation(libs.agrona)
    // Zero-alloc logging for hot path — no SLF4J, no Log4j2
    implementation(libs.gflog.api)
    runtimeOnly(libs.gflog.core)
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
