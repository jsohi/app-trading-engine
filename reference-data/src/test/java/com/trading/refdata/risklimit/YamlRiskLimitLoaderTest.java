package com.trading.refdata.risklimit;

import static org.junit.jupiter.api.Assertions.*;

import com.trading.refdata.ReferenceDataLoadException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Objects;
import org.junit.jupiter.api.Test;

/** Tests YAML loading, validation, and edge-case handling for {@link YamlRiskLimitLoader}. */
final class YamlRiskLimitLoaderTest {

  private Path testResource(final String name) {
    final var url = Objects.requireNonNull(getClass().getClassLoader().getResource(name), name);
    try {
      return Path.of(url.toURI());
    } catch (final URISyntaxException e) {
      throw new AssertionError("invalid test resource URI: " + url, e);
    }
  }

  @Test
  void loadValidRiskLimits() throws Exception {
    final var loader = new YamlRiskLimitLoader(testResource("risklimits-valid.yaml"));
    final var records = loader.load();

    assertEquals(2, records.size());

    final var first = records.get(0);
    assertEquals(1L, first.accountId());
    assertEquals(1_000_000_000L, first.maxOrderSize());
    assertEquals(500_000_000L, first.maxOrderNotional());
    assertEquals(10_000_000_000L, first.maxDailyVolume());
    assertEquals("Active", first.status());

    final var second = records.get(1);
    assertEquals(2L, second.accountId());
    assertEquals(0L, second.maxOrderSize());
    assertEquals(0L, second.maxOrderNotional());
    assertEquals(0L, second.maxDailyVolume());
    assertEquals("Suspended", second.status());
  }

  @Test
  void loadEmptyFile() throws Exception {
    final var loader = new YamlRiskLimitLoader(testResource("risklimits-empty.yaml"));
    final var records = loader.load();

    assertTrue(records.isEmpty());
  }

  @Test
  void loadMalformedYamlThrows() {
    final var loader = new YamlRiskLimitLoader(testResource("risklimits-malformed.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("RiskLimit", ex.entityType());
    assertTrue(ex.getMessage().contains("malformed YAML"));
  }

  @Test
  void loadDuplicateAccountIdThrows() {
    final var loader = new YamlRiskLimitLoader(testResource("risklimits-duplicate-id.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("RiskLimit", ex.entityType());
    assertTrue(ex.getMessage().contains("duplicate accountId"));
  }

  @Test
  void loadNegativeAccountIdThrows() {
    final var loader = new YamlRiskLimitLoader(testResource("risklimits-negative-id.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("RiskLimit", ex.entityType());
    assertTrue(ex.getMessage().contains("accountId must be > 0"));
  }

  @Test
  void loadMissingAccountIdThrows() {
    final var loader = new YamlRiskLimitLoader(testResource("risklimits-missing-id.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("RiskLimit", ex.entityType());
    assertTrue(ex.getMessage().contains("accountId"));
  }

  @Test
  void loadMissingFileThrows() {
    final var loader = new YamlRiskLimitLoader(Path.of("/nonexistent/risk-limits.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("RiskLimit", ex.entityType());
    assertTrue(ex.getMessage().contains("cannot read"));
  }

  @Test
  void sourceNameReturnsFileName() {
    final var loader = new YamlRiskLimitLoader(Path.of("/some/path/risk-limits.yaml"));
    assertEquals("risk-limits.yaml", loader.sourceName());
  }

  @Test
  void loadDefaultsOptionalFieldsToZeroAndActive() throws Exception {
    final var loader = new YamlRiskLimitLoader(testResource("risklimits-defaults.yaml"));
    final var records = loader.load();

    assertEquals(1, records.size());
    final var record = records.get(0);
    assertEquals(1L, record.accountId());
    assertEquals(0L, record.maxOrderSize());
    assertEquals(0L, record.maxOrderNotional());
    assertEquals(0L, record.maxDailyVolume());
    assertEquals("Active", record.status());
  }

  @Test
  void recordRejectsMaxDailyLossBpsOverflow() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RiskLimitRecord(1L, 0L, 0L, 0L, "Active"));
  }

  @Test
  void recordRejectsNegativeMaxOrderSize() {
    assertThrows(
        IllegalArgumentException.class, () -> new RiskLimitRecord(1L, -1L, 0L, 0L, "Active"));
  }
}
