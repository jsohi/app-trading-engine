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

    // Existing test dependencies
    testImplementation(project(":launcher"))
    testImplementation(project(":query-service"))
    testImplementation(project(":test-support"))
    testImplementation(libs.artio.core)
    testImplementation(libs.aeron.test.support)
}
