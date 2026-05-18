// =============================================================================
// :monitoring — Grafana dashboards + Prometheus alert rules + build-time
//               schema validation (APP-244 Phase 3 Commit C.7).
//
// Why a dedicated Gradle subproject:
//   - Ship dashboards as version-controlled artefacts, not free-floating files.
//   - Validate them at build time so a hand-edit can never silently break import.
//   - Wire into `check` so CI guards the contract on every push.
//
// validateGrafana is a JavaExec task running the GrafanaValidator main class
// from src/main/java. The validator uses com.networknt:json-schema-validator
// (draft 2020-12 capable, actively maintained) and snakeyaml for the alerts
// YAML. Both stay on the monitoring runtime classpath only; they never leak
// into any other module.
// =============================================================================

plugins {
    `java-library`
    application
}

dependencies {
    implementation(libs.json.schema.validator)
    implementation(libs.snakeyaml)
    implementation(libs.jackson.databind)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

application {
    mainClass.set("com.trading.engine.monitoring.GrafanaValidator")
}

// =============================================================================
// validateGrafana — JSON-schema-validate every dashboard + the alerts file.
// Implemented as JavaExec so the validator runs in a forked JVM with the
// monitoring module's own runtime classpath. Fails the build on any violation,
// on duplicate dashboard uids, or on an empty dashboard set.
// =============================================================================
val validateGrafana =
    tasks.register<JavaExec>("validateGrafana") {
        group = "verification"
        description =
            "JSON-schema-validates Grafana 11 dashboards under monitoring/dashboards/ and " +
            "the Prometheus alerting rules at monitoring/alerts.yaml. Fails on any violation."
        classpath = sourceSets["main"].runtimeClasspath
        mainClass.set("com.trading.engine.monitoring.GrafanaValidator")
        // Positional args: <dashboardsDir> <alertsFile> <dashboardSchema> <alertsSchema> <markerFile>
        args(
            layout.projectDirectory
                .dir("dashboards")
                .asFile.absolutePath,
            layout.projectDirectory
                .file("alerts.yaml")
                .asFile.absolutePath,
            layout.projectDirectory
                .file("src/main/resources/schema/grafana-dashboard-v11.schema.json")
                .asFile.absolutePath,
            layout.projectDirectory
                .file("src/main/resources/schema/prometheus-alerts.schema.json")
                .asFile.absolutePath,
            layout.buildDirectory
                .file("validateGrafana/marker.txt")
                .get()
                .asFile.absolutePath,
        )
        // Inputs / outputs so Gradle up-to-date checks short-circuit when nothing changed.
        inputs.dir(layout.projectDirectory.dir("dashboards"))
        inputs.file(layout.projectDirectory.file("alerts.yaml"))
        inputs.file(
            layout.projectDirectory.file("src/main/resources/schema/grafana-dashboard-v11.schema.json"),
        )
        inputs.file(
            layout.projectDirectory.file("src/main/resources/schema/prometheus-alerts.schema.json"),
        )
        outputs.file(layout.buildDirectory.file("validateGrafana/marker.txt"))
    }

tasks.named("check") {
    dependsOn(validateGrafana)
}
