plugins {
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

    // Infra logging: SLF4J API + Log4j2 Async + Disruptor
    // Hot-path modules (cluster, gateway) use GFLog instead — no SLF4J
    val hotPathModules = setOf("cluster", "gateway", "test-support")
    if (name !in hotPathModules) {
        dependencies {
            "implementation"(rootProject.libs.slf4j.api)
            "runtimeOnly"(rootProject.libs.log4j.core)
            "runtimeOnly"(rootProject.libs.log4j.slf4j2)
            "runtimeOnly"(rootProject.libs.disruptor)
        }
    }
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
        Runtime.getRuntime().exec(arrayOf("bash", "-c", "rm -rf /tmp/aeron-e2e-*")).waitFor()
        Runtime
            .getRuntime()
            .exec(arrayOf("bash", "-c", "pkill -9 -f -- '-Daeron.dir.prefix=e2e' 2>/dev/null || true"))
            .waitFor()
        // Also kill stale media drivers matched by aeron dir path (mirrors scripts/e2e.sh cleanup)
        Runtime
            .getRuntime()
            .exec(arrayOf("bash", "-c", "pkill -9 -f 'aeron-e2e-' 2>/dev/null || true"))
            .waitFor()
    }
}
