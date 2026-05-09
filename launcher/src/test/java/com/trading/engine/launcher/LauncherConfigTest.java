package com.trading.engine.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** Tests for {@link LauncherConfig} — immutable record validation and system property parsing. */
class LauncherConfigTest {

  /**
   * Shorthand for constructing a valid config — avoids repeating 10 args in every negative test.
   */
  private static LauncherConfig valid() {
    return new LauncherConfig(
        "localhost",
        9880,
        3,
        "cluster-data",
        "logs",
        10,
        "accounts.yaml",
        "currencies.yaml",
        "risk-limits.yaml",
        "",
        8192,
        30_000_000_000L,
        100L,
        1_000_000_000L,
        5_000_000_000L,
        0,
        0);
  }

  // ===== Valid construction =====

  @Test
  void constructor_validConfig_allFieldsAccessible() {
    final var config =
        new LauncherConfig(
            "localhost",
            9880,
            3,
            "cluster-data",
            "logs",
            10,
            "accounts.yaml",
            "currencies.yaml",
            "risk-limits.yaml",
            "e2e",
            8192,
            30_000_000_000L,
            100L,
            1_000_000_000L,
            5_000_000_000L,
            0,
            0);

    assertEquals("localhost", config.fixHost());
    assertEquals(9880, config.fixPort());
    assertEquals(3, config.nodeCount());
    assertEquals("cluster-data", config.baseDir());
    assertEquals("logs", config.logDir());
    assertEquals(10, config.driverShutdownTimeoutSeconds());
    assertEquals("accounts.yaml", config.accountsFile());
    assertEquals("currencies.yaml", config.currenciesFile());
    assertEquals("risk-limits.yaml", config.riskLimitsFile());
    assertEquals("e2e", config.aeronDirPrefix());
  }

  @Test
  void constructor_boundaryPorts_accepted() {
    new LauncherConfig(
        "localhost",
        1,
        1,
        "data",
        "logs",
        1,
        "a.yaml",
        "c.yaml",
        "r.yaml",
        "",
        8192,
        30_000_000_000L,
        100L,
        1_000_000_000L,
        5_000_000_000L,
        0,
        0);
    new LauncherConfig(
        "localhost",
        65535,
        1,
        "data",
        "logs",
        1,
        "a.yaml",
        "c.yaml",
        "r.yaml",
        "",
        8192,
        30_000_000_000L,
        100L,
        1_000_000_000L,
        5_000_000_000L,
        0,
        0);
  }

  @Test
  void constructor_emptyAeronDirPrefix_accepted() {
    final var config = valid();
    assertEquals("", config.aeronDirPrefix());
  }

  // ===== fixHost validation =====

  @Test
  void constructor_nullFixHost_throwsIae() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new LauncherConfig(
                    null,
                    9880,
                    3,
                    "data",
                    "logs",
                    10,
                    "a.yaml",
                    "c.yaml",
                    "r.yaml",
                    "",
                    8192,
                    30_000_000_000L,
                    100L,
                    1_000_000_000L,
                    5_000_000_000L,
                    0,
                    0));
    assertEquals("fix.host must not be blank", ex.getMessage());
  }

  @Test
  void constructor_blankFixHost_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LauncherConfig(
                "  ",
                9880,
                3,
                "data",
                "logs",
                10,
                "a.yaml",
                "c.yaml",
                "r.yaml",
                "",
                8192,
                30_000_000_000L,
                100L,
                1_000_000_000L,
                5_000_000_000L,
                0,
                0));
  }

  // ===== fixPort validation =====

  @Test
  void constructor_portZero_throwsIae() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new LauncherConfig(
                    "localhost",
                    0,
                    3,
                    "data",
                    "logs",
                    10,
                    "a.yaml",
                    "c.yaml",
                    "r.yaml",
                    "",
                    8192,
                    30_000_000_000L,
                    100L,
                    1_000_000_000L,
                    5_000_000_000L,
                    0,
                    0));
    assertEquals("fix.port must be in [1, 65535], got: 0", ex.getMessage());
  }

  @Test
  void constructor_portNegative_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LauncherConfig(
                "localhost",
                -1,
                3,
                "data",
                "logs",
                10,
                "a.yaml",
                "c.yaml",
                "r.yaml",
                "",
                8192,
                30_000_000_000L,
                100L,
                1_000_000_000L,
                5_000_000_000L,
                0,
                0));
  }

  @Test
  void constructor_portAboveMax_throwsIae() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new LauncherConfig(
                    "localhost",
                    65536,
                    3,
                    "data",
                    "logs",
                    10,
                    "a.yaml",
                    "c.yaml",
                    "r.yaml",
                    "",
                    8192,
                    30_000_000_000L,
                    100L,
                    1_000_000_000L,
                    5_000_000_000L,
                    0,
                    0));
    assertEquals("fix.port must be in [1, 65535], got: 65536", ex.getMessage());
  }

  // ===== nodeCount validation =====

  @Test
  void constructor_nodeCountZero_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LauncherConfig(
                "localhost",
                9880,
                0,
                "data",
                "logs",
                10,
                "a.yaml",
                "c.yaml",
                "r.yaml",
                "",
                8192,
                30_000_000_000L,
                100L,
                1_000_000_000L,
                5_000_000_000L,
                0,
                0));
  }

  @Test
  void constructor_nodeCountAboveMax_throwsIae() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new LauncherConfig(
                    "localhost",
                    9880,
                    ClusterConfig.MAX_NODES + 1,
                    "data",
                    "logs",
                    10,
                    "a.yaml",
                    "c.yaml",
                    "r.yaml",
                    "",
                    8192,
                    30_000_000_000L,
                    100L,
                    1_000_000_000L,
                    5_000_000_000L,
                    0,
                    0));
    assertEquals(
        "cluster.nodeCount must be in [1, " + ClusterConfig.MAX_NODES + "], got: 4",
        ex.getMessage());
  }

  // ===== baseDir validation =====

  @Test
  void constructor_nullBaseDir_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LauncherConfig(
                "localhost",
                9880,
                3,
                null,
                "logs",
                10,
                "a.yaml",
                "c.yaml",
                "r.yaml",
                "",
                8192,
                30_000_000_000L,
                100L,
                1_000_000_000L,
                5_000_000_000L,
                0,
                0));
  }

  @Test
  void constructor_blankBaseDir_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LauncherConfig(
                "localhost",
                9880,
                3,
                "",
                "logs",
                10,
                "a.yaml",
                "c.yaml",
                "r.yaml",
                "",
                8192,
                30_000_000_000L,
                100L,
                1_000_000_000L,
                5_000_000_000L,
                0,
                0));
  }

  // ===== logDir validation =====

  @Test
  void constructor_nullLogDir_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LauncherConfig(
                "localhost",
                9880,
                3,
                "data",
                null,
                10,
                "a.yaml",
                "c.yaml",
                "r.yaml",
                "",
                8192,
                30_000_000_000L,
                100L,
                1_000_000_000L,
                5_000_000_000L,
                0,
                0));
  }

  @Test
  void constructor_blankLogDir_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LauncherConfig(
                "localhost",
                9880,
                3,
                "data",
                "  ",
                10,
                "a.yaml",
                "c.yaml",
                "r.yaml",
                "",
                8192,
                30_000_000_000L,
                100L,
                1_000_000_000L,
                5_000_000_000L,
                0,
                0));
  }

  // ===== driverShutdownTimeoutSeconds validation =====

  @Test
  void constructor_zeroTimeout_throwsIae() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new LauncherConfig(
                    "localhost",
                    9880,
                    3,
                    "data",
                    "logs",
                    0,
                    "a.yaml",
                    "c.yaml",
                    "r.yaml",
                    "",
                    8192,
                    30_000_000_000L,
                    100L,
                    1_000_000_000L,
                    5_000_000_000L,
                    0,
                    0));
    assertEquals("driver.shutdown.timeout.seconds must be > 0, got: 0", ex.getMessage());
  }

  @Test
  void constructor_negativeTimeout_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LauncherConfig(
                "localhost",
                9880,
                3,
                "data",
                "logs",
                -5,
                "a.yaml",
                "c.yaml",
                "r.yaml",
                "",
                8192,
                30_000_000_000L,
                100L,
                1_000_000_000L,
                5_000_000_000L,
                0,
                0));
  }

  // ===== accountsFile validation =====

  @Test
  void constructor_nullAccountsFile_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LauncherConfig(
                "localhost",
                9880,
                3,
                "data",
                "logs",
                10,
                null,
                "c.yaml",
                "r.yaml",
                "",
                8192,
                30_000_000_000L,
                100L,
                1_000_000_000L,
                5_000_000_000L,
                0,
                0));
  }

  @Test
  void constructor_blankAccountsFile_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LauncherConfig(
                "localhost",
                9880,
                3,
                "data",
                "logs",
                10,
                "  ",
                "c.yaml",
                "r.yaml",
                "",
                8192,
                30_000_000_000L,
                100L,
                1_000_000_000L,
                5_000_000_000L,
                0,
                0));
  }

  // ===== currenciesFile validation =====

  @Test
  void constructor_nullCurrenciesFile_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LauncherConfig(
                "localhost",
                9880,
                3,
                "data",
                "logs",
                10,
                "a.yaml",
                null,
                "r.yaml",
                "",
                8192,
                30_000_000_000L,
                100L,
                1_000_000_000L,
                5_000_000_000L,
                0,
                0));
  }

  @Test
  void constructor_blankCurrenciesFile_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LauncherConfig(
                "localhost",
                9880,
                3,
                "data",
                "logs",
                10,
                "a.yaml",
                "  ",
                "r.yaml",
                "",
                8192,
                30_000_000_000L,
                100L,
                1_000_000_000L,
                5_000_000_000L,
                0,
                0));
  }

  // ===== riskLimitsFile validation =====

  @Test
  void constructor_nullRiskLimitsFile_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LauncherConfig(
                "localhost",
                9880,
                3,
                "data",
                "logs",
                10,
                "a.yaml",
                "c.yaml",
                null,
                "",
                8192,
                30_000_000_000L,
                100L,
                1_000_000_000L,
                5_000_000_000L,
                0,
                0));
  }

  @Test
  void constructor_blankRiskLimitsFile_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LauncherConfig(
                "localhost",
                9880,
                3,
                "data",
                "logs",
                10,
                "a.yaml",
                "c.yaml",
                "  ",
                "",
                8192,
                30_000_000_000L,
                100L,
                1_000_000_000L,
                5_000_000_000L,
                0,
                0));
  }

  // ===== aeronDirPrefix validation =====

  @Test
  void constructor_nullAeronDirPrefix_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LauncherConfig(
                "localhost",
                9880,
                3,
                "data",
                "logs",
                10,
                "a.yaml",
                "c.yaml",
                "r.yaml",
                null,
                8192,
                30_000_000_000L,
                100L,
                1_000_000_000L,
                5_000_000_000L,
                0,
                0));
  }

  // ===== System property parsing =====

  @Test
  void fromSystemProperties_usesDefaults() {
    final String[] keys = {
      "fix.host",
      "fix.port",
      "cluster.nodeCount",
      "cluster.baseDir",
      "log.dir",
      "driver.shutdown.timeout.seconds",
      "accounts.file",
      "currencies.file",
      "risk-limits.file",
      "aeron.dir.prefix"
    };
    // Save and clear any overrides from other tests
    final var saved = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      saved[i] = System.getProperty(keys[i]);
      System.clearProperty(keys[i]);
    }
    try {
      final var config = LauncherConfig.fromSystemProperties();

      assertEquals("localhost", config.fixHost());
      assertEquals(9880, config.fixPort());
      assertEquals(3, config.nodeCount());
      assertEquals("cluster-data", config.baseDir());
      assertEquals("logs", config.logDir());
      assertEquals(10, config.driverShutdownTimeoutSeconds());
      assertEquals("accounts.yaml", config.accountsFile());
      assertEquals("currencies.yaml", config.currenciesFile());
      assertEquals("risk-limits.yaml", config.riskLimitsFile());
      assertEquals("", config.aeronDirPrefix());
    } finally {
      for (int i = 0; i < keys.length; i++) {
        if (saved[i] != null) {
          System.setProperty(keys[i], saved[i]);
        } else {
          System.clearProperty(keys[i]);
        }
      }
    }
  }

  @Test
  void fromSystemProperties_customValues() {
    final String[] keys = {
      "fix.host",
      "fix.port",
      "cluster.nodeCount",
      "cluster.baseDir",
      "log.dir",
      "driver.shutdown.timeout.seconds",
      "accounts.file",
      "currencies.file",
      "risk-limits.file",
      "aeron.dir.prefix"
    };
    final String[] values = {
      "0.0.0.0",
      "5555",
      "1",
      "/var/cluster",
      "/var/log",
      "30",
      "/etc/accounts.yaml",
      "/etc/currencies.yaml",
      "/etc/risk-limits.yaml",
      "e2e"
    };
    // Save originals before overwriting
    final var saved = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      saved[i] = System.getProperty(keys[i]);
      System.setProperty(keys[i], values[i]);
    }
    try {
      final var config = LauncherConfig.fromSystemProperties();

      assertEquals("0.0.0.0", config.fixHost());
      assertEquals(5555, config.fixPort());
      assertEquals(1, config.nodeCount());
      assertEquals("/var/cluster", config.baseDir());
      assertEquals("/var/log", config.logDir());
      assertEquals(30, config.driverShutdownTimeoutSeconds());
      assertEquals("/etc/accounts.yaml", config.accountsFile());
      assertEquals("/etc/currencies.yaml", config.currenciesFile());
      assertEquals("/etc/risk-limits.yaml", config.riskLimitsFile());
      assertEquals("e2e", config.aeronDirPrefix());
    } finally {
      for (int i = 0; i < keys.length; i++) {
        if (saved[i] != null) {
          System.setProperty(keys[i], saved[i]);
        } else {
          System.clearProperty(keys[i]);
        }
      }
    }
  }

  @Test
  void fromSystemProperties_nonNumericPort_throwsNumberFormat() {
    System.setProperty("fix.port", "abc");
    try {
      assertThrows(NumberFormatException.class, LauncherConfig::fromSystemProperties);
    } finally {
      System.clearProperty("fix.port");
    }
  }
}
