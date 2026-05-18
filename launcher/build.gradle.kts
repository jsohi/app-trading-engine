plugins {
    application
}

dependencies {
    implementation(project(":cluster"))
    implementation(project(":gateway"))
    // Required at runtime — gateway's FixDictionaryImpl is loaded by Artio reflectively
    runtimeOnly(project(":fix-codecs"))
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
    // Required by WebSocketLauncher to plumb the Micrometer registry from WebSocketMetrics
    // into Log4j2DiskFullErrorHandler.installAll(...). The transitive log4j-core dependency
    // alone is not enough — Micrometer's MeterRegistry must be on launcher's classpath.
    implementation(libs.micrometer.core)

    testImplementation(project(":test-support"))
}

application {
    mainClass.set("com.trading.engine.launcher.TradingEngineLauncher")
    applicationDefaultJvmArgs =
        listOf(
            "-Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
            "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        )
}

// Forward known system properties to the forked launcher JVM. The Gradle application plugin's
// run task (JavaExec) does NOT automatically propagate Gradle JVM system properties to the
// child process. Without this, -Dfix.port=19880 or -Daeron.dir.prefix=e2e from `./gradlew
// :launcher:run -Dfix.port=19880` would be silently ignored. We filter to known prefixes to
// avoid leaking unrelated Gradle internals.
val knownPrefixes =
    listOf("fix.", "cluster.", "log.", "aeron.", "accounts.", "currencies.", "risk-limits.", "driver.", "websocket.", "bridge.")
tasks.named<JavaExec>("run") {
    systemProperties(
        System
            .getProperties()
            .mapNotNull { (k, v) ->
                val key = k.toString()
                if (knownPrefixes.any { prefix -> key.startsWith(prefix) }) key to v.toString() else null
            }.toMap(),
    )
}
