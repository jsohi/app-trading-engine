plugins {
    application
}

dependencies {
    implementation(project(":cluster"))
    implementation(project(":gateway"))
    implementation(project(":projections"))
    implementation(project(":media-driver"))
    implementation(project(":orchestrator"))
    implementation(project(":pricing-service"))
    implementation(project(":websocket-server"))
    implementation(project(":reference-data"))
    implementation(project(":messages"))
    // Declared explicitly — launcher orchestrates driver/archive/cluster lifecycle directly
    implementation(libs.aeron.driver)
    implementation(libs.aeron.archive)
    implementation(libs.aeron.cluster)
    implementation(libs.agrona)
    implementation(libs.log4j.api)
    implementation(libs.log4j.core)
    implementation(libs.disruptor)

    testImplementation(project(":test-support"))
}

application {
    mainClass.set("com.trading.engine.launcher.TradingEngineLauncher")
    applicationDefaultJvmArgs =
        listOf(
            "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
            "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        )
}
