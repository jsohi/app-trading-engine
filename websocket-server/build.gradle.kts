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
            "-Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
        )
}
