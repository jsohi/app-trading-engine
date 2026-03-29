plugins {
    application
}

dependencies {
    implementation(libs.aeron.driver)
    implementation(libs.aeron.archive)
    implementation(libs.agrona)
    implementation(libs.log4j.api)
    implementation(libs.log4j.core)
    implementation(libs.disruptor)
}

application {
    mainClass.set("com.trading.engine.media.driver.MediaDriverLauncher")
    applicationDefaultJvmArgs =
        listOf(
            "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
        )
}
