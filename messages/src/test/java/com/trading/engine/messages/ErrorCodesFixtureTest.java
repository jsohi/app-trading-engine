/*
 * APP-36 — error-codes JSON fixture emitter.
 *
 * Dumps every value of WebSocketErrorCode (template 67 enum) to
 * web-ui/test/fixtures/error-codes.json as build-time artifact. The
 * web-ui browser-tier ErrorMatrix.test.ts reflects over the JSON to
 * assert that every server enum value is mapped in the client matrix
 * (§2.13). CI fails on enum drift.
 *
 * Threading: single-threaded JUnit invocation; the file write is
 * idempotent given the enum values.
 *
 * Allocation: build-time only — not on a hot path.
 *
 * Plan reference: APP-36 §5.8 / §5.8.3 / §6 row 36.
 */
package com.trading.engine.messages;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.WebSocketErrorCode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Emits {@code error-codes.json} for the cross-stack contract test. */
final class ErrorCodesFixtureTest {

  @Test
  @DisplayName("errorCodes_emitFixture_writesJsonForCrossStackContract")
  void errorCodes_emitFixture_writesJsonForCrossStackContract() throws IOException {
    // Resolve the fixture path relative to the repo root. Gradle test runs
    // with `user.dir` = the module root (websocket-server / messages /…),
    // so we walk up until we find the web-ui/ directory.
    final var repoRoot = findRepoRoot();
    final var fixturesDir = repoRoot.resolve("web-ui/test/fixtures");
    Files.createDirectories(fixturesDir);
    final var target = fixturesDir.resolve("error-codes.json");

    // Build a small JSON document by hand (no Jackson dep in :messages).
    // Schema:
    //   { "schemaVersion": 1, "codes": [ { "name": "...", "value": N }, ... ] }
    final var entries = new ArrayList<String>();
    for (final WebSocketErrorCode code : WebSocketErrorCode.values()) {
      // SBE codegen creates a `NULL_VAL` sentinel; skip — not a real wire value.
      if (code == WebSocketErrorCode.NULL_VAL) {
        continue;
      }
      entries.add("    { \"name\": \"" + code.name() + "\", \"value\": " + code.value() + " }");
    }
    // Per /review MEDIUM (Gemini): use String.join for the JSON array
    // body instead of a manual for-loop; idiomatic and shorter.
    final var sb = new StringBuilder(256);
    sb.append("{\n  \"schemaVersion\": 1,\n  \"codes\": [\n");
    sb.append(String.join(",\n", entries));
    if (!entries.isEmpty()) {
      sb.append('\n');
    }
    sb.append("  ]\n}\n");

    Files.writeString(target, sb.toString(), StandardCharsets.UTF_8);
    assertTrue(Files.exists(target), "fixture not written: " + target);
    assertTrue(Files.size(target) > 0, "fixture is empty: " + target);
  }

  private static Path findRepoRoot() throws IOException {
    final var start = Paths.get("").toAbsolutePath();
    // Loop control variable `cur` is intentionally mutable per CLAUDE.md
    // carve-out for classic for-loop counters; the loop walks parents
    // until `getParent()` returns null (filesystem root). Per Gemini
    // review (MEDIUM): this test utility relies on a sane filesystem
    // root rather than an explicit depth bound; the prior comment
    // claiming depth=10 was out-of-date with the for-loop refactor.
    for (Path cur = start; cur != null; cur = cur.getParent()) {
      if (Files.isDirectory(cur.resolve("web-ui"))
          && Files.isRegularFile(cur.resolve("settings.gradle.kts"))) {
        return cur;
      }
    }
    throw new IOException("could not locate repo root from " + start);
  }
}
