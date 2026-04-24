plugins {
    application
}

dependencies {
    implementation(platform(libs.netty.bom))

    implementation(project(":messages"))
    implementation(project(":query-service"))

    implementation(libs.aeron.client)
    implementation(libs.aeron.cluster)
    implementation(libs.agrona)

    implementation(libs.netty.transport)
    implementation(libs.netty.handler)
    implementation(libs.netty.codec.http)
    implementation(libs.netty.tcnative.boringssl)
    runtimeOnly(libs.netty.transport.native.epoll) { artifact { classifier = "linux-x86_64" } }
    runtimeOnly(libs.netty.transport.native.kqueue) { artifact { classifier = "osx-x86_64" } }
    runtimeOnly(libs.netty.transport.native.kqueue) { artifact { classifier = "osx-aarch_64" } }

    implementation(libs.micrometer.registry.prometheus)
    implementation(libs.nimbus.jose.jwt)
    implementation(libs.snakeyaml)

    testImplementation(project(":test-support"))
}

application {
    mainClass.set("com.trading.engine.websocket.WebSocketServerMain")
    applicationDefaultJvmArgs =
        listOf(
            "-Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
        )
}

tasks.withType<Test> {
    jvmArgs("-Dio.netty.leakDetection.level=PARANOID")
}
