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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import uk.co.real_logic.sbe.generation.CodeGenerator;
import uk.co.real_logic.sbe.ir.Ir;

/**
 * Pipeline orchestrator for the SBE → TypeScript generator. Fans out to per-emitter classes (one
 * per output concern) and stitches the results together.
 *
 * <h2>Lifecycle</h2>
 *
 * Constructed by {@link TypeScriptTargetCodeGenerator#newInstance(Ir, String)} once per build.
 * {@code SbeTool} then calls {@link #generate()} exactly once on the returned instance.
 *
 * <h2>Runtime call order</h2>
 *
 * Locked. Each emitter is invoked in this fixed sequence so downstream emitters can consume the
 * outputs of upstream ones (e.g. {@link RouterGenerator} and {@link IndexBarrelGenerator} consume
 * the message-decoder name list returned by {@link MessageGenerator#generate(Ir, Path)}):
 *
 * <ol>
 *   <li>{@link HeaderGenerator} — {@code messageHeader.ts}.
 *   <li>{@link EnumGenerator} — one {@code <Enum>.ts} per enum; returns enum names.
 *   <li>{@link MessageGenerator} — one {@code <Message>Decoder.ts} per message (with embedded
 *       {@link GroupGenerator} + {@link VarDataGenerator} + {@link UuidCompositeGenerator} output);
 *       returns decoder names.
 *   <li>{@link HelpersGenerator} — {@code helpers.ts} (content-static).
 *   <li>{@link RouterGenerator} — {@code MessageRouter.ts} (templateId-dispatch flyweight).
 *   <li>{@link ConstantsGenerator} — {@code constants.ts} (price scale + schema identity).
 *   <li>{@link IndexBarrelGenerator} — {@code index.ts} (re-export barrel; whole-schema scan drives
 *       conditional NULL_VAL + UuidValue re-exports).
 * </ol>
 *
 * Reordering is unsafe — Router and barrel emitters depend on the message-decoder name list. The
 * order is locked here in the orchestrator rather than via per-emitter dependency wiring so a
 * future emitter cannot accidentally introduce a dependency-cycle by inserting itself mid-list.
 *
 * <h2>Stateless emitters</h2>
 *
 * Every emitter class is stateless across {@link #generate()} invocations — no static mutable
 * state. Locked so a future Gradle parallel-mode ({@code org.gradle.parallel=true}) cannot silently
 * corrupt builds.
 *
 * <h2>Threading model</h2>
 *
 * Not thread-safe. Build-time only; single-threaded inside the {@code SbeTool} JVM.
 *
 * <h2>Allocation behavior</h2>
 *
 * Build-time only — allocates freely while constructing emitted source.
 *
 * <h2>Naming history</h2>
 *
 * Renamed at chunk 11 from {@code PlaceholderTypeScriptCodeGenerator} (the chunk-1 stub label
 * stopped being accurate once chunks 2–6 wired in real per-emitter classes). The SBE SPI loader
 * hook {@link TypeScriptTargetCodeGenerator} instantiates this class; SPI wiring is unchanged.
 *
 * @see TypeScriptTargetCodeGenerator
 * @see HeaderGenerator
 * @see EnumGenerator
 * @see MessageGenerator
 * @see HelpersGenerator
 * @see RouterGenerator
 * @see ConstantsGenerator
 * @see IndexBarrelGenerator
 */
final class TypeScriptCodeGenerator implements CodeGenerator {

  private final Ir ir;
  private final Path outputDir;

  /**
   * @param ir parsed SBE intermediate representation; consumed by every per-emitter class
   * @param outputDir absolute output directory; created in {@link #generate()} if missing. Caller
   *     (the SPI entry point) has already validated non-null/non-blank
   */
  TypeScriptCodeGenerator(final Ir ir, final String outputDir) {
    this.ir = ir;
    this.outputDir = Path.of(outputDir);
  }

  /**
   * Run the full TypeScript codec generation pipeline. Build-time, single-threaded; allocation
   * freely permitted. The output directory is created if missing.
   *
   * @throws IOException if the output directory cannot be created or any emitted file cannot be
   *     written
   */
  @Override
  public void generate() throws IOException {
    Files.createDirectories(outputDir);

    new HeaderGenerator().generate(ir, outputDir);
    final var enumNames = new EnumGenerator().generate(ir, outputDir);
    final var messageDecoderNames = new MessageGenerator().generate(ir, outputDir);
    new HelpersGenerator().generate(outputDir);
    new RouterGenerator().generate(messageDecoderNames, outputDir);
    new ConstantsGenerator().generate(ir, outputDir);
    new IndexBarrelGenerator().generate(ir, messageDecoderNames, enumNames, outputDir);
  }
}
