plugins {
    application
}

dependencies {
    implementation(libs.aeron.driver)
    implementation(libs.aeron.archive)
    implementation(libs.agrona)
}

application {
    mainClass.set("com.trading.engine.media.driver.MediaDriverLauncher")
    applicationDefaultJvmArgs =
        listOf(
            "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
        )
}
