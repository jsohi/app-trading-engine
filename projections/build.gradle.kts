dependencies {
    implementation(project(":messages"))
    implementation(libs.aeron.client)
    implementation(libs.agrona)
    implementation(libs.gflog.api)
    runtimeOnly(libs.gflog.core)
    testImplementation(project(":test-support"))
}

// Per CLAUDE.md "Logging" section: zero-alloc hot path uses GFLog only. Exclude infra logging
// frameworks so transitive dependencies cannot smuggle them onto the projections runtime classpath.
configurations.runtimeClasspath {
    exclude(group = "org.apache.logging.log4j")
    exclude(group = "org.slf4j")
    exclude(group = "com.lmax", module = "disruptor")
}
