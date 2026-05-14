/*
 * Gradle wiring for the web-ui Node project. All Node tasks run via
 * the `com.github.node-gradle.node` plugin with a hermetic Node
 * download (no system PATH usage), and consistently resolve
 * `package-lock.json` at the repo root because npm workspaces lives
 * there.
 *
 * Tasks:
 *   webUiInstall     — `npm ci` at repo root (also generates SBE
 *                      codecs via the dependency on
 *                      :sbe-typescript-generator:generateTsCodecs).
 *   webUiTypecheck   — `tsc --noEmit` for the web-ui workspace.
 *   webUiTest        — Vitest unit suite.
 *   webUiBuild       — `vite build` production bundle.
 *   webUiE2e         — Playwright smoke (long-running; opt-in).
 *   webUiStorybook   — `storybook build` → storybook-static/.
 *   webUiSbom        — CycloneDX SBOM covering the workspace tree.
 *
 * The root `build` task aggregates webUiTypecheck + webUiTest +
 * webUiBuild + webUiStorybook + webUiSbom (see root build.gradle.kts).
 */
import com.github.gradle.node.npm.task.NpmTask

plugins {
    id("com.github.node-gradle.node") version "7.1.0"
}

node {
    version.set("22.22.2") // matches .nvmrc; pin the LTS minor floor
    download.set(true) // hermetic Node — never use system PATH
    nodeProjectDir.set(rootProject.layout.projectDirectory) // workspaces root
}

val webUiInstall by tasks.registering(NpmTask::class) {
    group = "build"
    description = "Run `npm ci` at the workspace root and generate SBE codecs."
    dependsOn(":sbe-typescript-generator:generateTsCodecs")
    workingDir.set(rootProject.layout.projectDirectory.asFile)
    args.set(listOf("ci"))

    // Treat package-lock.json + the workspace package.json files as
    // inputs so unrelated edits don't trigger reinstall.
    inputs.file(rootProject.layout.projectDirectory.file("package-lock.json"))
    inputs.file(rootProject.layout.projectDirectory.file("package.json"))
    inputs.file(layout.projectDirectory.file("package.json"))
    inputs.file(rootProject.layout.projectDirectory.file("sbe-typescript-generator/package.json"))
    outputs.dir(rootProject.layout.projectDirectory.dir("node_modules"))
}

val webUiTypecheck by tasks.registering(NpmTask::class) {
    group = "verification"
    description = "TypeScript --noEmit typecheck for the web-ui workspace."
    dependsOn(webUiInstall)
    workingDir.set(rootProject.layout.projectDirectory.asFile)
    args.set(listOf("run", "-w", "web-ui", "typecheck"))
}

val webUiTest by tasks.registering(NpmTask::class) {
    group = "verification"
    description = "Vitest unit suite for the web-ui workspace."
    dependsOn(webUiInstall)
    workingDir.set(rootProject.layout.projectDirectory.asFile)
    args.set(listOf("run", "-w", "web-ui", "test"))
}

val webUiBrowserTest by tasks.registering(NpmTask::class) {
    group = "verification"
    description = "Vitest @vitest/browser suite (Playwright/Chromium) for the web-ui workspace."
    dependsOn(webUiInstall)
    workingDir.set(rootProject.layout.projectDirectory.asFile)
    args.set(listOf("run", "-w", "web-ui", "test:browser"))
}

val webUiBuild by tasks.registering(NpmTask::class) {
    group = "build"
    description = "Vite production build."
    dependsOn(webUiInstall)
    workingDir.set(rootProject.layout.projectDirectory.asFile)
    args.set(listOf("run", "-w", "web-ui", "build"))
    outputs.dir(layout.projectDirectory.dir("dist"))
}

val webUiE2e by tasks.registering(NpmTask::class) {
    group = "verification"
    description = "Playwright e2e smoke (long-running; opt-in)."
    dependsOn(webUiInstall)
    workingDir.set(rootProject.layout.projectDirectory.asFile)
    args.set(listOf("run", "-w", "web-ui", "e2e:smoke"))
}

val webUiStorybook by tasks.registering(NpmTask::class) {
    group = "build"
    description = "Storybook static build → storybook-static/."
    dependsOn(webUiInstall)
    workingDir.set(rootProject.layout.projectDirectory.asFile)
    args.set(listOf("run", "-w", "web-ui", "build-storybook"))
    outputs.dir(layout.projectDirectory.dir("storybook-static"))
}

val webUiSbom by tasks.registering(NpmTask::class) {
    group = "build"
    description = "CycloneDX SBOM covering the npm workspace tree."
    dependsOn(webUiInstall, webUiBuild)
    workingDir.set(rootProject.layout.projectDirectory.asFile)
    args.set(
        listOf(
            "exec",
            "--",
            "cyclonedx-npm",
            "--output-file",
            "web-ui/build/sbom.cdx.json",
        ),
    )
    outputs.file(layout.buildDirectory.file("sbom.cdx.json"))
}
