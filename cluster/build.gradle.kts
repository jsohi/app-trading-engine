plugins {
    `java-test-fixtures`
}

dependencies {
    implementation(project(":messages"))
    implementation(libs.aeron.cluster)
    implementation(libs.agrona)
    // Zero-alloc logging for hot path — no SLF4J, no Log4j2
    implementation(libs.gflog.api)
    runtimeOnly(libs.gflog.core)

    testImplementation(project(":test-support"))

    testFixturesImplementation(project(":messages"))
    testFixturesImplementation(libs.agrona)
}

// Ensure no logging frameworks leak in via transitive dependencies
configurations.runtimeClasspath {
    exclude(group = "org.apache.logging.log4j")
    exclude(group = "org.slf4j")
    exclude(group = "com.lmax", module = "disruptor")
}
