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
        "testImplementation"("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }
}
