plugins {
    base
    alias(libs.plugins.spotless)
    alias(libs.plugins.owasp)
}

dependencyCheck {
    failBuildOnCVSS = 7.0f
    formats = listOf("HTML", "JSON")
    suppressionFile = "$rootDir/owasp-suppressions.xml"
}

spotless {
    kotlinGradle {
        target("**/*.gradle.kts")
        ktlint()
    }
    format("misc") {
        target(".gitignore", ".editorconfig")
        trimTrailingWhitespace()
        endWithNewline()
    }
}

allprojects {
    group = "com.trading.engine"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    // web-ui is a Node project, skip Java plugin
    if (name == "web-ui") return@subprojects

    apply(plugin = "java")
    apply(plugin = "jacoco")
    apply(plugin = "com.diffplug.spotless")

    configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        java {
            googleJavaFormat("1.35.0")
            targetExclude("build/**")
        }
    }

    configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(25))
        }
    }

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        // Agrona's UnsafeApi requires access to jdk.internal.misc on JDK 17+
        jvmArgs(
            "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        )
    }

    tasks.withType<JacocoReport> {
        reports {
            xml.required.set(true)
            html.required.set(true)
        }
    }

    dependencies {
        "testImplementation"(rootProject.libs.junit.jupiter)
        "testRuntimeOnly"(rootProject.libs.junit.platform.launcher)
    }

    // Infra logging: Log4j2 API + Log4j2 Async + Disruptor
    // Hot-path modules use GFLog instead — no Log4j2
    val hotPathModules =
        setOf(
            "cluster",
            "gateway",
            "test-support",
            "pricing-service",
            "orchestrator",
            "projections",
            "messages",
            "fix-codecs",
            "event-logger",
        )
    if (name !in hotPathModules) {
        dependencies {
            "implementation"(rootProject.libs.log4j.api)
            "runtimeOnly"(rootProject.libs.log4j.core)
            "runtimeOnly"(rootProject.libs.disruptor)
        }
    }
}

// =============================================================================
// web-ui aggregation — root `build` runs typecheck + test + bundle + storybook
//                       + SBOM via the Node-plugin tasks in :web-ui. E2E is
//                       opt-in (long-running) and tied to ./gradlew :web-ui:webUiE2e.
// =============================================================================

tasks.named("build") {
    dependsOn(
        ":web-ui:webUiTypecheck",
        ":web-ui:webUiTest",
        ":web-ui:webUiBuild",
        ":web-ui:webUiStorybook",
        ":web-ui:webUiSbom",
    )
}

// =============================================================================
// E2E integration test — boots real 3-node cluster, sends FIX NOS, validates ER
// =============================================================================

tasks.register<Exec>("e2e") {
    group = "verification"
    description = "Run full e2e test — real 3-node cluster, FIX NOS, ExecutionReport validation"
    dependsOn("build", ":integration-tests:installDist")
    commandLine("bash", "scripts/e2e.sh")
    timeout.set(java.time.Duration.ofMinutes(3))
}

tasks.register<Delete>("e2eClean") {
    group = "verification"
    description = "Remove e2e test artifacts (logs, cluster data, aeron dirs)"
    delete("e2e/logs", "e2e/cluster-data")
    doLast {
        ProcessBuilder("bash", "-c", "rm -rf /tmp/aeron-e2e-*").start().waitFor()
        ProcessBuilder("bash", "-c", "pkill -9 -f -- '-Daeron.dir.prefix=e2e' 2>/dev/null || true")
            .start()
            .waitFor()
        // Also kill stale media drivers matched by aeron dir path (mirrors scripts/e2e.sh cleanup)
        ProcessBuilder("bash", "-c", "pkill -9 -f 'aeron-e2e-' 2>/dev/null || true")
            .start()
            .waitFor()
    }
}
