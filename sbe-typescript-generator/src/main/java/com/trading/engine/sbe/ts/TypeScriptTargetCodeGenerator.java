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

import java.util.Objects;
import uk.co.real_logic.sbe.generation.CodeGenerator;
import uk.co.real_logic.sbe.generation.TargetCodeGenerator;
import uk.co.real_logic.sbe.ir.Ir;

/**
 * SBE-tool entry point for TypeScript codec generation.
 *
 * <p>SBE 1.37.x's {@code TargetCodeGeneratorLoader} is a closed enum (Java/C/Cpp/Golang/Rust); it
 * is NOT a {@link java.util.ServiceLoader}-based SPI. However,
 * {@code TargetCodeGeneratorLoader.get(name)} falls back to
 * {@link Class#forName(String)}{@code .getConstructor().newInstance()} for any fully-qualified
 * class name passed via {@code -Dsbe.target.language=...}. This class is the FQCN that the Gradle
 * task {@code :sbe-typescript-generator:generateTsCodecs} hands to {@code SbeTool}; the public
 * no-arg constructor is required by that reflective load path.
 *
 * <h3>Rejected alternatives (design rationale)</h3>
 *
 * <ul>
 *   <li><b>Fork {@code sbe-tool} to add a TS enum value to {@code TargetCodeGeneratorLoader}</b>
 *       — rejected because it forks an upstream library we want to track at version 1.37.1, and
 *       the FQCN-fallback path is the documented escape hatch for exactly this case.
 *   <li><b>Post-process the C/Java generator's output</b> — rejected because SBE's generated C
 *       and Java codecs are imperative class structures; deriving a {@code DataView}-flyweight
 *       TypeScript module from them would be lossy and brittle. Walking the {@link Ir} directly
 *       maps cleanly to the TS emission target.
 *   <li><b>Build-out-of-band tool that reads {@code trading-schema.xml} via a third-party SBE
 *       parser</b> — rejected because SBE's own {@code SbeTool} already validates and parses the
 *       schema; reusing it eliminates an entire class of schema-drift bugs.
 * </ul>
 *
 * <h2>Threading model</h2>
 *
 * Not thread-safe. Build-time only. {@code SbeTool} invokes {@link #newInstance(Ir, String)} once
 * per build; the returned {@link CodeGenerator} runs single-threaded inside that build's JVM.
 *
 * <h2>Allocation behavior</h2>
 *
 * Build-time only — no hot-path concerns. The returned generator allocates freely as it walks the
 * IR and writes per-message {@code .ts} files.
 *
 * <h2>Lifecycle</h2>
 *
 * <ol>
 *   <li>{@code SbeTool} parses {@code trading-schema.xml} into an {@link Ir}.
 *   <li>{@code SbeTool} instantiates this class via reflection (no-arg ctor).
 *   <li>{@code SbeTool} calls {@link #newInstance(Ir, String)}, passing the parsed IR and the
 *       absolute output directory taken from {@code -Dsbe.output.dir=...}.
 *   <li>{@code SbeTool} calls {@link CodeGenerator#generate()} once on the returned instance.
 *   <li>The build task's declared output directory is repopulated atomically.
 * </ol>
 *
 * <h2>Dependencies</h2>
 *
 * Composes per-emitter classes (one per concern: header, enum, message, var-data, uuid composite,
 * router, helpers/constants/index barrel). Each emitter receives the {@link Ir} and writes its
 * portion of the output tree. The orchestration is performed by the {@link CodeGenerator} returned
 * from {@link #newInstance(Ir, String)}; this entry-point class is intentionally small.
 *
 * @see uk.co.real_logic.sbe.generation.TargetCodeGenerator
 * @see uk.co.real_logic.sbe.SbeTool
 */
public final class TypeScriptTargetCodeGenerator implements TargetCodeGenerator {

    /**
     * Required by {@code TargetCodeGeneratorLoader.get(name)}'s reflective fallback path; do not
     * remove. The class is public for the same reason.
     */
    public TypeScriptTargetCodeGenerator() {
        // intentionally empty — see Javadoc
    }

    /**
     * Build the orchestrating code generator that walks {@code ir} and writes per-message
     * TypeScript files into {@code outputDir}.
     *
     * <p>Build-time invocation only. {@code SbeTool} calls this exactly once per build; the
     * returned object is then used exclusively from the calling thread.
     *
     * <p>The supertype's {@link TargetCodeGenerator#newInstance(Ir, String)} declares no checked
     * exceptions, so this override declares none either; any I/O failure that occurs while
     * walking the IR or writing files happens later, inside {@link CodeGenerator#generate()},
     * which the SPI explicitly permits to throw {@link java.io.IOException}.
     *
     * @param ir the parsed SBE intermediate representation; must not be {@code null}
     * @param outputDir absolute path to the directory the generator must populate; the directory
     *     may or may not exist on entry — the generator must create it if necessary. Must not be
     *     {@code null} or blank.
     * @return a {@link CodeGenerator} ready to be invoked once via {@link CodeGenerator#generate()}
     * @throws NullPointerException if {@code ir} or {@code outputDir} is {@code null}
     * @throws IllegalArgumentException if {@code outputDir} is blank
     */
    @Override
    public CodeGenerator newInstance(final Ir ir, final String outputDir) {
        // SPI boundary — fail fast with a named-parameter precondition violation rather than
        // letting a null/blank propagate to a downstream NullPointerException inside Path.of(...)
        // where the stack trace is harder to interpret.
        Objects.requireNonNull(ir, "ir");
        Objects.requireNonNull(outputDir, "outputDir");
        if (outputDir.isBlank()) {
            throw new IllegalArgumentException("outputDir must not be blank");
        }
        // TODO(APP-34): replace with the orchestrating generator that fans out to per-emitter
        // classes (HeaderGenerator, EnumGenerator, MessageGenerator, VarDataGenerator,
        // UuidCompositeGenerator, RouterGenerator, HelpersGenerator, ConstantsGenerator,
        // IndexBarrelGenerator). Until those land, this placeholder produces a minimal valid
        // index.ts so the task is runnable end-to-end and downstream consumers don't break.
        return new PlaceholderTypeScriptCodeGenerator(ir, outputDir);
    }
}
