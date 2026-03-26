plugins {
    application
}

dependencies {
    implementation(project(":messages"))
    implementation(libs.aeron.client)
    implementation(libs.agrona)
}

application {
    mainClass.set("com.trading.engine.websocket.WebSocketServerMain")
    applicationDefaultJvmArgs =
        listOf(
            "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
        )
}
