package com.trading.engine.monitoring;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Build-time validator for the {@code :monitoring} subproject (APP-244 Phase 3 Commit C.7).
 *
 * <p>Reads every {@code *.json} dashboard under {@code monitoring/dashboards/} and the {@code
 * monitoring/alerts.yaml} alerting-rules file, then validates each against a project-pinned JSON
 * schema. Exits with status 0 on success and non-zero (with a multi-line diagnostic) on the first
 * batch of violations — emitting all errors per file rather than only the first so a dashboard
 * author can fix everything in a single edit-test cycle.
 *
 * <p><b>Threading model.</b> Single-threaded CLI tool; not used outside the Gradle build.
 *
 * <p><b>Allocation behaviour.</b> Build-time only — allocates freely. Not on any runtime hot path.
 *
 * <p><b>Invariants enforced beyond the JSON schema itself:</b>
 *
 * <ul>
 *   <li>At least one dashboard JSON file must exist (an empty dashboard set is almost always a
 *       packaging mistake).
 *   <li>Dashboard {@code uid} values must be unique across all files — Grafana imports by uid, so
 *       duplicates would silently overwrite each other.
 *   <li>The alerts YAML must parse as a YAML mapping (not as a list or scalar at the root).
 * </ul>
 *
 * <p>Exit codes: {@code 0} on success; {@code 1} on schema violation; {@code 2} on argument /
 * input-file errors.
 */
public final class GrafanaValidator {

  private GrafanaValidator() {
    // utility — instantiated only via main(String[])
  }

  /**
   * Entry point invoked by the {@code :monitoring:validateGrafana} Gradle JavaExec task.
   *
   * @param args positional: {@code <dashboardsDir> <alertsFile> <dashboardSchema> <alertsSchema>
   *     <markerFile>}
   * @throws IOException if any input file is unreadable
   */
  public static void main(final String[] args) throws IOException {
    if (args.length != 5) {
      System.err.println(
          "usage: GrafanaValidator <dashboardsDir> <alertsFile> <dashboardSchema> <alertsSchema>"
              + " <markerFile>");
      System.exit(2);
      return;
    }
    final Path dashboardsDir = Path.of(args[0]);
    final Path alertsFile = Path.of(args[1]);
    final Path dashboardSchemaPath = Path.of(args[2]);
    final Path alertsSchemaPath = Path.of(args[3]);
    final Path markerFile = Path.of(args[4]);

    if (!Files.isDirectory(dashboardsDir)) {
      System.err.println("dashboards directory not found: " + dashboardsDir);
      System.exit(2);
      return;
    }
    if (!Files.isRegularFile(alertsFile)) {
      System.err.println("alerts file not found: " + alertsFile);
      System.exit(2);
      return;
    }

    final var mapper = new ObjectMapper();
    final var schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);

    final JsonSchema dashboardSchema;
    try (InputStream in = Files.newInputStream(dashboardSchemaPath)) {
      dashboardSchema = schemaFactory.getSchema(in);
    }
    final JsonSchema alertsSchema;
    try (InputStream in = Files.newInputStream(alertsSchemaPath)) {
      alertsSchema = schemaFactory.getSchema(in);
    }

    final List<String> errors = new ArrayList<>();

    final List<Path> dashboards = new ArrayList<>();
    try (Stream<Path> stream = Files.list(dashboardsDir)) {
      stream
          .filter(p -> p.getFileName().toString().endsWith(".json"))
          .sorted(Comparator.comparing(p -> p.getFileName().toString()))
          .forEach(dashboards::add);
    }
    if (dashboards.isEmpty()) {
      System.err.println(
          "validateGrafana: no *.json dashboards found under " + dashboardsDir.toAbsolutePath());
      System.exit(1);
      return;
    }

    final Set<String> seenUids = new HashSet<>();
    for (final Path file : dashboards) {
      final JsonNode node = mapper.readTree(file.toFile());
      final Set<ValidationMessage> msgs = dashboardSchema.validate(node);
      if (!msgs.isEmpty()) {
        errors.add(file.getFileName() + ":");
        for (final ValidationMessage m : msgs) {
          errors.add("  - " + m.getMessage());
        }
      }
      // uid uniqueness — Grafana imports by uid; a duplicate would silently overwrite.
      if (node instanceof ObjectNode obj && obj.hasNonNull("uid")) {
        final String uid = obj.get("uid").asText();
        if (!seenUids.add(uid)) {
          errors.add(
              file.getFileName()
                  + ": duplicate uid '"
                  + uid
                  + "' (already used by another dashboard)");
        }
      }
    }

    // ----- alerts.yaml -----
    final var yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
    final Object loaded;
    try (InputStream in = Files.newInputStream(alertsFile)) {
      loaded = yaml.load(in);
    }
    if (loaded == null) {
      errors.add(alertsFile.getFileName() + ": file is empty");
    } else {
      final JsonNode alertsTree = mapper.valueToTree(loaded);
      final Set<ValidationMessage> alertMsgs = alertsSchema.validate(alertsTree);
      if (!alertMsgs.isEmpty()) {
        errors.add(alertsFile.getFileName() + ":");
        for (final ValidationMessage m : alertMsgs) {
          errors.add("  - " + m.getMessage());
        }
      }
    }

    if (!errors.isEmpty()) {
      System.err.println(
          "validateGrafana found "
              + errors.size()
              + " violation(s):\n"
              + String.join("\n", errors));
      System.exit(1);
      return;
    }

    // Write the marker so Gradle up-to-date checks short-circuit subsequent runs.
    Files.createDirectories(markerFile.getParent());
    final StringBuilder marker = new StringBuilder();
    marker
        .append("validateGrafana OK — ")
        .append(dashboards.size())
        .append(" dashboard(s) + 1 alerts file validated\n");
    for (final Path d : dashboards) {
      marker.append("  - ").append(d.getFileName()).append('\n');
    }
    Files.writeString(markerFile, marker.toString());
    System.out.println(
        "validateGrafana: PASS (" + dashboards.size() + " dashboard(s) + alerts.yaml)");
  }
}
