/*
 * Copyright 2026 Jasandeep Singh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.trading.engine.sbe.ts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.TaskOutcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

/**
 * Locks the Gradle UP-TO-DATE / cache-miss contract for {@code :generateTsCodecs}. A future edit
 * that drops {@code inputs.file(schemaXml)} or {@code inputs.files(sourceSets["main"].output)} from
 * the real task in {@code build.gradle.kts} fails one of the four tests below, surfacing the
 * regression at module-test time rather than at consumer-build time.
 *
 * <h2>What's under test</h2>
 *
 * <p>Three TestKit invalidation paths plus one Java-string assertion:
 *
 * <ol>
 *   <li>Second run with no change → {@code UP_TO_DATE}.
 *   <li>Schema-content change → cache miss (schema is declared as {@code inputs.file}).
 *   <li>Classpath bytecode change → cache miss (compiled generator output is declared as {@code
 *       inputs.files}, locking the wiring that prevents stale codecs after an emitter edit).
 *   <li>Real {@code build.gradle.kts} declares both load-bearing input lines.
 * </ol>
 *
 * <h2>Why a synthetic task in the fixture, not the real {@code :generateTsCodecs}</h2>
 *
 * <p>The real task pulls {@code sbe-all} + Agrona transitives that do not belong in a TestKit
 * {@link TempDir} fixture. Gradle's caching machinery is task-implementation-agnostic — the cache
 * key is computed purely from declared inputs/outputs — so synthetic-task UP_TO_DATE behaviour is
 * byte-equivalent to the real task's. Test #4 (the Java-string assertion) closes the
 * synthetic-vs-real divergence by asserting the real task declares the same {@code inputs} shape.
 *
 * <h2>Stub class compilation</h2>
 *
 * <p>Two trivial Java sources ({@code Stub_v1} with field {@code int v1 = 1;}, {@code Stub_v2} with
 * field {@code int v2 = 2;}) are compiled in-process via {@link
 * ToolProvider#getSystemJavaCompiler()} so their resulting {@code .class} bytes differ in content
 * hash. Path 3 swaps v1 → v2 to verify Gradle's {@code inputs.files} cache key changes when the
 * compiled-classpath input changes — exactly the regression the real task's {@code
 * inputs.files(sourceSets["main"].output)} declaration prevents. JDK 25 ships the compiler in the
 * standard library; on JRE-only runtimes {@code getSystemJavaCompiler()} returns null and the test
 * is skipped with {@link org.junit.jupiter.api.Assumptions#assumeTrue}.
 *
 * <h2>Threading</h2>
 *
 * <p>Single-threaded JUnit 6 Jupiter. Each test gets its own {@link TempDir} fixture and its own
 * {@link GradleRunner} invocation; no shared state.
 *
 * <h2>Allocation</h2>
 *
 * <p>Build-time only — allocates freely while constructing the fixture project, compiling stubs,
 * and reading task output. Not part of any production hot path.
 *
 * <h2>Lifecycle</h2>
 *
 * <p>{@code @BeforeEach} initialises the fixture (writes {@code settings.gradle.kts}, {@code
 * build.gradle.kts}, {@code schema.txt}, compiles {@code Stub_v1.class}). Each test then performs
 * the path-specific mutation and re-runs the synthetic task. {@link TempDir} cleans up.
 *
 * <h2>Cross-references</h2>
 *
 * @see com.trading.engine.sbe.ts.TypeScriptCodeGenerator
 * @see com.trading.engine.sbe.ts.MessageGenerator
 */
@DisabledIfEnvironmentVariable(
    named = "SKIP_INCREMENTAL_TESTS",
    matches = "1",
    disabledReason =
        "Opt-out for environments where Gradle TestKit is unavailable; default-on so regressions surface locally.")
final class SbeGeneratorIncrementalTest {

  /**
   * Locked Kotlin DSL for the fixture project's {@code build.gradle.kts}. Mirrors {@code
   * :generateTsCodecs}'s input/output declarations exactly: schema as {@code inputs.file},
   * classpath as {@code inputs.files}, output dir as {@code outputs.dir}. The {@link
   * org.gradle.api.DefaultTask} body writes a fixed marker file so the task action itself never
   * varies — only Gradle's cache decisions are under test.
   */
  private static final String FIXTURE_BUILD_SCRIPT =
      """
      import org.gradle.api.DefaultTask
      import org.gradle.api.tasks.InputFile
      import org.gradle.api.tasks.InputFiles
      import org.gradle.api.tasks.OutputDirectory
      import org.gradle.api.tasks.TaskAction
      import org.gradle.api.file.RegularFileProperty
      import org.gradle.api.file.ConfigurableFileCollection
      import org.gradle.api.file.DirectoryProperty

      abstract class GenerateTsCodecsStub : DefaultTask() {
        @get:InputFile abstract val schema: RegularFileProperty
        @get:InputFiles abstract val classpath: ConfigurableFileCollection
        @get:OutputDirectory abstract val outputDir: DirectoryProperty
        @TaskAction fun run() {
          val out = outputDir.get().asFile
          out.mkdirs()
          java.io.File(out, "marker.ts").writeText("// stub\\n")
        }
      }

      tasks.register<GenerateTsCodecsStub>("generateTsCodecs") {
        schema.set(layout.projectDirectory.file("schema.txt"))
        classpath.from(layout.projectDirectory.dir("compiledClasses"))
        outputDir.set(layout.buildDirectory.dir("generated-ts"))
      }
      """;

  /**
   * Locked initial schema content. Path 2 mutates this file's bytes to drive cache invalidation.
   */
  private static final String INITIAL_SCHEMA_CONTENT = "version=1\nname=fixture\n# placeholder\n";

  /**
   * Two single-class Java sources whose {@code .class} bytes differ in content hash because their
   * field names differ. Path 3 swaps v1 → v2 to drive {@code inputs.files} cache invalidation.
   */
  private static final String STUB_V1_SOURCE = "public class Stub { public int v1 = 1; }";

  private static final String STUB_V2_SOURCE = "public class Stub { public int v2 = 2; }";

  @TempDir private Path fixtureDir;

  /**
   * The in-process Java compiler. Resolved in {@link #setup()} (with an {@code assumeTrue} guard
   * for JRE-only runtimes) and reused across tests so path #3 can recompile {@code Stub_v2} without
   * re-resolving (and without re-checking the precondition the {@code @BeforeEach} already
   * enforced).
   */
  private JavaCompiler compiler;

  @BeforeEach
  void setup() throws IOException {
    compiler = ToolProvider.getSystemJavaCompiler();
    assumeTrue(
        compiler != null,
        "JDK toolchain (not JRE) required — javax.tools.JavaCompiler is null on JRE-only runtimes.");

    Files.writeString(
        fixtureDir.resolve("settings.gradle.kts"), "rootProject.name = \"fixture\"\n");
    Files.writeString(fixtureDir.resolve("build.gradle.kts"), FIXTURE_BUILD_SCRIPT);
    Files.writeString(fixtureDir.resolve("schema.txt"), INITIAL_SCHEMA_CONTENT);

    final var classesDir = Files.createDirectories(fixtureDir.resolve("compiledClasses"));
    compileStubInto(compiler, STUB_V1_SOURCE, classesDir);
  }

  // -----------------------------------------------------------------------------------------
  // Path 1: no-change second run → UP_TO_DATE
  // -----------------------------------------------------------------------------------------

  @Test
  void generateTsCodecs_secondRunWithNoChange_isUpToDate() {
    final var first = runFixtureTask();
    assertEquals(
        TaskOutcome.SUCCESS,
        outcome(first),
        "first run should populate the cache (SUCCESS, not FROM_CACHE / UP_TO_DATE)");

    final var second = runFixtureTask();
    assertEquals(
        TaskOutcome.UP_TO_DATE,
        outcome(second),
        "second run with no input change must hit Gradle's UP-TO-DATE cache; "
            + "regression here means an `inputs`/`outputs` declaration was lost");
  }

  // -----------------------------------------------------------------------------------------
  // Path 2: schema content change → cache miss
  // -----------------------------------------------------------------------------------------

  @Test
  void generateTsCodecs_schemaContentChange_invalidatesCache() throws IOException {
    final var first = runFixtureTask();
    assertEquals(TaskOutcome.SUCCESS, outcome(first), "first run populates the cache");

    Files.writeString(fixtureDir.resolve("schema.txt"), INITIAL_SCHEMA_CONTENT + "extra-byte\n");

    final var second = runFixtureTask();
    assertEquals(
        TaskOutcome.SUCCESS,
        outcome(second),
        "schema-content change must invalidate the cache (SUCCESS, not UP_TO_DATE); "
            + "regression here means `inputs.file(schemaXml)` was lost from the real task");
  }

  // -----------------------------------------------------------------------------------------
  // Path 3: classpath bytecode change → cache miss
  // -----------------------------------------------------------------------------------------

  @Test
  void generateTsCodecs_classpathBytecodeChange_invalidatesCache() throws IOException {
    final var first = runFixtureTask();
    assertEquals(TaskOutcome.SUCCESS, outcome(first), "first run populates the cache");

    final var classesDir = fixtureDir.resolve("compiledClasses");
    Files.deleteIfExists(classesDir.resolve("Stub.class"));
    compileStubInto(compiler, STUB_V2_SOURCE, classesDir);

    final var second = runFixtureTask();
    assertEquals(
        TaskOutcome.SUCCESS,
        outcome(second),
        "compiled-classpath bytecode change must invalidate the cache (SUCCESS, not UP_TO_DATE); "
            + "regression here means `inputs.files(sourceSets[\"main\"].output)` was lost from "
            + "the real task — emitter edits would silently serve stale codecs");
  }

  // -----------------------------------------------------------------------------------------
  // Path 4: real build.gradle.kts declares the two load-bearing inputs (Java string assertion,
  // closes the synthetic-vs-real fixture-divergence gap)
  // -----------------------------------------------------------------------------------------

  @Test
  void realGenerateTsCodecsTask_declaresSourceSetOutputAndSchemaAsInputs() throws IOException {
    final var moduleDirProp = System.getProperty("moduleProjectDir");
    assumeTrue(
        moduleDirProp != null,
        "moduleProjectDir system property unset — run via `./gradlew :sbe-typescript-generator:test`, "
            + "not the IDE direct test runner. Configure your IDE to delegate test execution to Gradle.");
    final var moduleDir = Path.of(moduleDirProp);
    final var script = Files.readString(moduleDir.resolve("build.gradle.kts"));

    assertTrue(
        script.contains("inputs.files(sourceSets[\"main\"].output)"),
        "real :generateTsCodecs MUST declare sourceSets.main.output as input — load-bearing for "
            + "cache invalidation on emitter edits. The fixture's path-3 test verifies the caching "
            + "machinery works; this test verifies the real task is wired to use it.");
    assertTrue(
        script.contains("inputs.file(schemaXml)"),
        "real :generateTsCodecs MUST declare the schema as input — without it, edits to "
            + "trading-schema.xml would not invalidate cached codecs.");
    assertTrue(
        script.contains("outputs.dir(generatedTsDir)"),
        "real :generateTsCodecs MUST declare the generated-ts directory as output — required for "
            + "Gradle to track the task's product for cache + cleanup.");
  }

  // -----------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------

  /**
   * Runs the fixture's {@code :generateTsCodecs} task in a fresh Gradle daemon-mode build. TestKit
   * defaults to the version of Gradle running the suite (i.e. the wrapper version), so CI and local
   * runs match the project's pinned wrapper. Not calling {@code .withGradleVersion(...)} is
   * intentional: pinning to a hard-coded version would silently mask a project-wide wrapper upgrade
   * regression.
   */
  private BuildResult runFixtureTask() {
    return GradleRunner.create()
        .withProjectDir(fixtureDir.toFile())
        .withArguments(":generateTsCodecs")
        .withDebug(false)
        .build();
  }

  /** Returns the outcome of {@code :generateTsCodecs} from a TestKit build result. */
  private static TaskOutcome outcome(final BuildResult result) {
    final var task = result.task(":generateTsCodecs");
    assertNotNull(task, "TestKit BuildResult missing :generateTsCodecs task entry");
    return task.getOutcome();
  }

  /**
   * Compiles {@code source} into {@code outputDir} using the in-process Java compiler. Throws an
   * {@link AssertionError} (not a checked exception) if compilation fails — the source strings are
   * literals controlled by this test class, so a failure means the test itself is broken.
   */
  private static void compileStubInto(
      final JavaCompiler compiler, final String source, final Path outputDir) throws IOException {
    final var diagnostics = new StringWriter();

    try (final var fileManager =
        new InMemoryClassFileManager(
            compiler.getStandardFileManager(null, null, null), outputDir)) {
      final var sourceFile = new InMemorySource("Stub", source);
      final var task =
          compiler.getTask(
              new PrintWriter(diagnostics, true),
              fileManager,
              null,
              List.of("-proc:none"),
              null,
              List.of(sourceFile));
      final var success = task.call();
      if (!Boolean.TRUE.equals(success)) {
        throw new AssertionError("Stub compilation failed; diagnostics:\n" + diagnostics);
      }
    }
  }

  /** In-memory {@link JavaFileObject} that the compiler reads source bytes from. */
  private static final class InMemorySource extends SimpleJavaFileObject {
    private final String content;

    InMemorySource(final String className, final String content) {
      super(URI.create("string:///" + className + Kind.SOURCE.extension), Kind.SOURCE);
      this.content = content;
    }

    @Override
    public CharSequence getCharContent(final boolean ignoreEncodingErrors) {
      return content;
    }
  }

  /**
   * Forwards class-file output to a real directory under the fixture (rather than the compiler's
   * default in-memory storage), so Gradle's {@code inputs.files} declaration sees the bytes.
   */
  private static final class InMemoryClassFileManager
      extends ForwardingJavaFileManager<StandardJavaFileManager> {
    private final Path outputDir;

    InMemoryClassFileManager(final StandardJavaFileManager delegate, final Path outputDir) {
      super(delegate);
      this.outputDir = outputDir;
    }

    @Override
    public JavaFileObject getJavaFileForOutput(
        final Location location,
        final String className,
        final JavaFileObject.Kind kind,
        final FileObject sibling)
        throws IOException {
      // Disk-backed output — Gradle hashes bytes via inputs.files; in-memory class storage would
      // be invisible to the synthetic task's classpath input declaration.
      final var classFile = outputDir.resolve(className + kind.extension);
      Files.createDirectories(classFile.getParent() != null ? classFile.getParent() : outputDir);
      return new SimpleJavaFileObject(classFile.toUri(), kind) {
        @Override
        public OutputStream openOutputStream() throws IOException {
          return Files.newOutputStream(classFile);
        }
      };
    }
  }
}
