/*
 * SBE TypeScript code generator (APP-34 / Phase 1B).
 *
 * Reads `messages/src/main/resources/trading-schema.xml` via SBE 1.37.1's
 * standard `SbeTool` entry point and emits per-message TypeScript decoders
 * into `build/generated-ts/`. The npm workspace `@trading/sbe-codecs` (see
 * `package.json`) exposes the output to consumers (web-ui), with `main`
 * pointing at `build/generated-ts/index.ts`.
 *
 * Design — extension point chosen
 *   `uk.co.real_logic.sbe.generation.TargetCodeGeneratorLoader` is a closed
 *   enum (`JAVA, C, CPP, GOLANG, RUST`) in SBE 1.37.x; it is NOT a
 *   `ServiceLoader`-based SPI. However, `TargetCodeGeneratorLoader.get(name)`
 *   falls back to `Class.forName(name).getConstructor().newInstance()` for
 *   any FQCN passed via the `-Dsbe.target.language=...` system property. We
 *   exploit that fallback: our `TypeScriptTargetCodeGenerator` (public
 *   no-arg constructor, implements `TargetCodeGenerator`) is loaded by
 *   FQCN. SBE parses the XML schema into an `Ir` for free; our generator
 *   only emits TypeScript.
 *
 * Build wiring
 *   `:sbe-typescript-generator:generateTsCodecs` runs `SbeTool` as a
 *   `JavaExec` task with the generator's compiled classes + `sbe-all` on
 *   the runtime classpath. Inputs include the schema XML AND the compiled
 *   generator classes — without the latter, edits to `MessageGenerator.java`
 *   would not invalidate Gradle's UP-TO-DATE cache, leading to silently
 *   stale codecs.
 *
 * Stable contract across the 1A→1B handoff
 *   - Task name `generateTsCodecs` preserved
 *   - Output directory `build/generated-ts/` preserved
 *   - npm workspace name `@trading/sbe-codecs` preserved
 *   Consumers (`web-ui`) see only the contract change (more exports), not
 *   a path or task-name change.
 */

plugins {
    application
}

dependencies {
    // sbe-all provides:
    //   uk.co.real_logic.sbe.SbeTool                       (entry point)
    //   uk.co.real_logic.sbe.generation.TargetCodeGenerator (SPI we implement)
    //   uk.co.real_logic.sbe.generation.CodeGenerator       (returned by our SPI)
    //   uk.co.real_logic.sbe.ir.Ir                          (parsed schema we walk)
    implementation(libs.sbe.all)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    // chunk 6 lights up the first JVM tests in this module — needed for fixture-based
    // assertions on the emitted decoder source. Chunk 13 will add the ProcessBuilder/tsx
    // wiring and a `dependsOn(":web-ui:webUiInstall")` on the test task.
    testImplementation(project(":messages"))
    // chunk 12 — Gradle TestKit drives SbeGeneratorIncrementalTest's @TempDir fixture
    // builds. Tests construct a synthetic project that mirrors :generateTsCodecs's
    // input/output declarations and asserts UP_TO_DATE / cache-miss behaviour through
    // the same Gradle caching machinery the real task uses.
    testImplementation(gradleTestKit())
    // chunk 13 — Jackson parses the JSON the tsx round-trip driver writes to stdout.
    // Test-only; not on any production hot path. bigints arrive as decimal strings
    // (preserves int64 max precision) and Java parses them via new BigInteger(node.asText()).
    testImplementation(libs.jackson.databind)
}

tasks.test {
    // JUnit 6 (Jupiter) — without useJUnitPlatform() the legacy vintage runner discovers
    // zero tests and the task reports green silently. Chunk 6's MessageGeneratorChunk6Test
    // is the first JVM test in this module; chunk-1+2's deps were declared in anticipation
    // but never exercised.
    useJUnitPlatform()
    // chunk 12 — pass module's projectDir so SbeGeneratorIncrementalTest can resolve the
    // real `build.gradle.kts` (test #4 asserts the load-bearing input declarations are
    // present) without depending on the test JVM's cwd, which Gradle does not formally
    // guarantee. Also used by chunk 13's driverSource_* + barrelExportsHelpersAndRouter
    // string-assertion tests.
    systemProperty("moduleProjectDir", projectDir.absolutePath)

    // chunk 13 — RoundTripTest's @Nested TsxRoundTripTests spawn `tsx` and `tsc` from the
    // npm-workspace-hoisted bins at <rootProject>/node_modules/.bin/. dependsOn
    // applies test-task-wide (even SbeGeneratorIncrementalTest pays the cost on a cold
    // cache); acceptable because Gradle caches webUiInstall as UP-TO-DATE after the first
    // run, and the strict-default path avoids the failure mode where a developer runs the
    // round-trip tests against a checkout without `npm ci` having been performed. The
    // dependency also transitively triggers :sbe-typescript-generator:generateTsCodecs
    // (webUiInstall depends on it), which guarantees the test's tsdoc-propagation +
    // string-assertion tests see fresh emitted output. Do NOT optimise away.
    dependsOn(":web-ui:webUiInstall")
    // chunk 13 — pass workspace root so RoundTripTest can resolve node_modules/.bin/tsx,
    // node_modules/.bin/tsc, and the build/generated-ts/ tree. Same NPE-vs-skip rationale
    // as moduleProjectDir above.
    systemProperty("rootProjectDir", rootProject.projectDir.absolutePath)
}

application {
    // Not used by `generateTsCodecs` (which invokes `SbeTool` directly), but
    // satisfies the `application` plugin's contract and lets contributors
    // sanity-check the generator manually via `./gradlew :sbe-typescript-generator:run`.
    mainClass.set("uk.co.real_logic.sbe.SbeTool")
}

val schemaXml =
    rootProject.layout.projectDirectory.file(
        "messages/src/main/resources/trading-schema.xml",
    )
val generatedTsDir = layout.buildDirectory.dir("generated-ts")

tasks.register<JavaExec>("generateTsCodecs") {
    group = "code generation"
    description =
        "Generate TypeScript decoders from trading-schema.xml " +
        "via SBE Ir + the TypeScriptTargetCodeGenerator SPI implementation."

    mainClass.set("uk.co.real_logic.sbe.SbeTool")
    // Runtime classpath includes the generator's own compiled classes
    // (so `Class.forName(\"com.trading.engine.sbe.ts.TypeScriptTargetCodeGenerator\")`
    // resolves) plus sbe-all (transitively pulled by `implementation`).
    // Implicitly depends on `compileJava` and `processResources` because
    // `runtimeClasspath` references `main` sourceSet output — Gradle adds
    // those task dependencies automatically. This is intentional: emitter
    // edits MUST invalidate the cached output, and the implicit task
    // ordering also ensures the generator is compiled before invocation.
    classpath = sourceSets["main"].runtimeClasspath

    // Inputs/outputs declared so Gradle's UP-TO-DATE cache is correct.
    // The compiled generator classes are an explicit input — without
    // this, editing an emitter would not invalidate the cache and the
    // task would serve stale codecs.
    inputs.file(schemaXml)
    inputs.files(sourceSets["main"].output)
    outputs.dir(generatedTsDir)

    systemProperty(
        "sbe.target.language",
        "com.trading.engine.sbe.ts.TypeScriptTargetCodeGenerator",
    )
    systemProperty(
        "sbe.output.dir",
        generatedTsDir.get().asFile.absolutePath,
    )
    systemProperty("sbe.validation.stop.on.error", "true")
    systemProperty("sbe.validation.warnings.fatal", "true")

    args(schemaXml.asFile.absolutePath)

    // Defensive: skip cleanly if the schema is missing instead of
    // exploding mid-build (matches `:messages:generateCodecs` idiom).
    onlyIf { schemaXml.asFile.exists() }
}

// `compileJava` does NOT depend on `generateTsCodecs` — TS output is consumed
// by the npm workspace, not by Java compilation in this module.
