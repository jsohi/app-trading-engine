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
        // Exclude vendored / generated trees so Gradle 9's strict input-overlap
        // validation does not treat outputs of `:web-ui:webUiInstall` (npm-installed
        // node_modules tree) or the build/ outputs as undeclared inputs of
        // :spotlessKotlinGradle. Without this, running any test that depends on
        // webUiInstall (chunks 12-13's :sbe-typescript-generator:test) and then
        // root :spotlessCheck fails with implicit_dependency.
        targetExclude("**/node_modules/**", "**/build/**", ".gradle/**")
        ktlint()
    }
    format("misc") {
        target(".gitignore", ".editorconfig")
        trimTrailingWhitespace()
        endWithNewline()
    }
}

// =============================================================================
// EnforceLinearTicketTodos — Phase 3 ticket-hygiene gate (replaces a Spotless
// custom step; configuration-cache-friendly standalone task).
//
// Scans every Java + TypeScript + JS source file (production AND test) for
// `TODO(APP-N)` / `FIXME(APP-N)` references. Every cited ID must appear in
// `.linear-allowlist` at repo root. Placeholder shapes (<linear-id>, <issue-id>,
// APP-NNN, APP-???-X, "Issue X") fail the build outright. The hook script
// `.claude/hooks/enforce-precommit-gate.sh` runs the same checks on the staged
// diff at commit time; this task provides a build-time backstop so a
// `--no-verify` commit cannot bypass the gate. No OR-fallback to grep — single
// mechanism. Allowlist is the single source of truth (Phase 3 plan §C / §EE).
// Done tickets (APP-31, APP-37, APP-60, APP-242) deliberately excluded.
// =============================================================================
abstract class EnforceLinearTicketTodosTask : DefaultTask() {
    @get:InputFile
    abstract val allowlist: RegularFileProperty

    @get:InputFiles
    abstract val sources: ConfigurableFileCollection

    @TaskAction
    fun verify() {
        val allowedIds =
            allowlist
                .get()
                .asFile
                .readLines()
                .map { it.trim() }
                .filter { it.isNotEmpty() && !it.startsWith("#") }
                .toSet()
        val placeholderRe = Regex("""<linear-id>|<issue-id>|APP-NNN|APP-\?\?\?-[A-Z]|\bIssue [A-Z]\b""")
        val citationRe = Regex("""(TODO|FIXME)\(APP-(\d+)\)""")
        val violations = mutableListOf<String>()
        sources.files.forEach { file ->
            if (!file.isFile) return@forEach
            val content = file.readText()
            placeholderRe.find(content)?.let { m ->
                violations.add("${file.relativeTo(project.rootDir)}: placeholder '${m.value}'")
            }
            for (m in citationRe.findAll(content)) {
                val id = "APP-${m.groupValues[2]}"
                if (!allowedIds.contains(id)) {
                    violations.add("${file.relativeTo(project.rootDir)}: TODO cites $id (not in allowlist)")
                }
            }
        }
        if (violations.isNotEmpty()) {
            throw GradleException(
                "EnforceLinearTicketTodos failed:\n" +
                    violations.joinToString("\n") { "  - $it" } +
                    "\nAllowed IDs: ${allowedIds.sorted()}",
            )
        }
    }
}

tasks.register<EnforceLinearTicketTodosTask>("enforceLinearTicketTodos") {
    group = "verification"
    description =
        "Verifies every Phase 3 TODO(APP-N) / FIXME(APP-N) cites a Linear ID in .linear-allowlist. " +
        "Scoped to files changed on the current branch vs main (so pre-existing TODOs predating " +
        "Phase 3 are out of scope — those have their own ticket history). New Phase 3 files MUST " +
        "cite an allowlisted ID."
    allowlist.set(rootProject.file(".linear-allowlist"))
    // Compute the changed-file set at configuration time via `git diff --name-only main`.
    // Configuration-cache safe: the file collection is materialised eagerly into a set of
    // existing files; the closure does not survive into the task action.
    val changed: List<File> =
        try {
            val proc =
                ProcessBuilder("git", "diff", "--name-only", "main")
                    .directory(rootProject.projectDir)
                    .redirectErrorStream(true)
                    .start()
            proc.waitFor()
            proc.inputStream
                .bufferedReader()
                .readLines()
                .map { rootProject.file(it) }
                .filter {
                    it.isFile &&
                        (
                            it.name.endsWith(".java") ||
                                it.name.endsWith(".ts") ||
                                it.name.endsWith(".tsx") ||
                                it.name.endsWith(".mjs")
                        ) &&
                        !it.absolutePath.contains("/build/") &&
                        !it.absolutePath.contains("/node_modules/") &&
                        !it.absolutePath.contains("/.gradle/") &&
                        !it.absolutePath.contains("/generated/") &&
                        !it.absolutePath.contains(".claude/hooks/test/")
                }
        } catch (e: Exception) {
            // No git, or main branch missing: fall back to scanning nothing (safe default).
            emptyList()
        }
    sources.from(changed)
}

// Wire into spotlessCheck so the standard verification gate runs the lint.
tasks.named("spotlessCheck") {
    dependsOn("enforceLinearTicketTodos")
}

// =============================================================================
// checkHooks — Phase 3 CI-side self-test for the pre-commit hook scripts.
//
// The hook `.claude/hooks/enforce-precommit-gate.sh` blocks commits that cite
// placeholder linear IDs or non-allowlisted ticket numbers. Its self-test at
// `.claude/hooks/test/enforce-precommit-gate.test.sh` pipes deterministic
// fixture diffs through the hook and asserts exit codes; this Gradle task
// runs the self-test from CI so a regex regression cannot silently disarm
// the gate (a `--no-verify` commit followed by CI would otherwise pass).
// Wired into `check` so `./gradlew build` and any `check` invocation exercise
// the hook coverage.
// =============================================================================
tasks.register<Exec>("checkHooks") {
    group = "verification"
    description =
        "Runs the bash self-test for .claude/hooks/enforce-precommit-gate.sh — " +
        "verifies the hook still blocks placeholder linear IDs and non-allowlisted " +
        "TODO citations after every commit. Lives alongside the hook itself; not " +
        "skippable via --no-verify (the hook gate runs on commit; this task gates CI)."
    workingDir = rootProject.projectDir
    commandLine("bash", ".claude/hooks/test/enforce-precommit-gate.test.sh")
    // Inputs declared so up-to-date checks let the task no-op when neither the
    // hook nor its self-test has changed since the last successful run.
    inputs.file(".claude/hooks/enforce-precommit-gate.sh")
    inputs.file(".claude/hooks/test/enforce-precommit-gate.test.sh")
    inputs.file(".linear-allowlist")
    // No file output — the task either succeeds (exit 0) or fails (non-zero).
    // Mark a virtual output so Gradle still considers the task cacheable.
    outputs.upToDateWhen { true }
}

tasks.named("check") {
    dependsOn("checkHooks")
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
// PortLockService — Gradle BuildService that serialises tasks binding the
// e2e port set (5173 / 8443 / 19880 / 20110-22220 / 8010-8012 / 7000 / 7001).
// Local devs running multiple Gradle invocations in parallel hit "port already
// in use" otherwise — Gradle serialises tasks declaring usesService(portLock)
// via maxParallelUsages = 1. Reviewer F-16 (HIGH).
// =============================================================================
abstract class PortLockService : org.gradle.api.services.BuildService<org.gradle.api.services.BuildServiceParameters.None>

val portLock =
    gradle.sharedServices.registerIfAbsent("portLock", PortLockService::class.java) {
        maxParallelUsages.set(1)
    }

// =============================================================================
// E2E integration test — boots real 3-node cluster, sends FIX NOS, validates ER
// =============================================================================

tasks.register<Exec>("e2e") {
    group = "verification"
    description = "Run full e2e test — real 3-node cluster, FIX NOS, ExecutionReport validation"
    dependsOn("build", ":integration-tests:installDist")
    usesService(portLock)
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

// =============================================================================
// Full-Stack E2E (plan §9) — boots cluster + websocket-server + Vite + JWKS,
// runs Playwright + JCStress + stress JUnit phase. ~28-35 min wall-clock.
// =============================================================================
tasks.register<Exec>("fullStackE2e") {
    group = "verification"
    description = "Full-stack e2e: real backend + browser UI + Playwright suite + JCStress"
    dependsOn(
        "build",
        ":integration-tests:installDist",
        ":web-ui:webUiE2eDeps",
        ":web-ui:bundleGuard",
    )
    usesService(portLock)
    commandLine("bash", "scripts/full-stack-e2e.sh")
    timeout.set(java.time.Duration.ofMinutes(40))
}

tasks.register<Delete>("fullStackE2eClean") {
    group = "verification"
    description = "Remove full-stack e2e artifacts (rendered overlays, dist, playwright reports)"
    delete(
        "e2e/logs",
        "e2e/cluster-data",
        "e2e/cluster-data-mi",
        "e2e/config/websocket-server-e2e.yaml",
        "e2e/config/websocket-server-multi-issuer.yaml",
        "web-ui/dist",
        "web-ui/playwright-report",
        "web-ui/playwright-report-full-stack",
        "web-ui/test-results",
    )
    doLast {
        ProcessBuilder("bash", "-c", "rm -rf /tmp/aeron-e2e-*").start().waitFor()
        ProcessBuilder("bash", "-c", "pkill -9 -f -- '-Daeron.dir.prefix=e2e' 2>/dev/null || true")
            .start()
            .waitFor()
    }
}
