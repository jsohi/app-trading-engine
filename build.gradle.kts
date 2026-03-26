plugins {
    alias(libs.plugins.spotless)
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
    }

    dependencies {
        "testImplementation"(rootProject.libs.junit.jupiter)
        "testRuntimeOnly"(rootProject.libs.junit.platform.launcher)
    }
}
