package com.trading.engine.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** Tests for {@link LauncherConfig} — immutable record validation and system property parsing. */
class LauncherConfigTest {

  // ===== Valid construction =====

  @Test
  void constructor_validConfig_allFieldsAccessible() {
    final var config =
        new LauncherConfig("localhost", 9880, 3, "cluster-data", "logs", 10, "accounts.yaml");

    assertEquals("localhost", config.fixHost());
    assertEquals(9880, config.fixPort());
    assertEquals(3, config.nodeCount());
    assertEquals("cluster-data", config.baseDir());
    assertEquals("logs", config.logDir());
    assertEquals(10, config.driverShutdownTimeoutSeconds());
    assertEquals("accounts.yaml", config.accountsFile());
  }

  @Test
  void constructor_boundaryPorts_accepted() {
    new LauncherConfig("localhost", 1, 1, "data", "logs", 1, "a.yaml");
    new LauncherConfig("localhost", 65535, 1, "data", "logs", 1, "a.yaml");
  }

  // ===== fixHost validation =====

  @Test
  void constructor_nullFixHost_throwsIae() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> new LauncherConfig(null, 9880, 3, "data", "logs", 10, "a.yaml"));
    assertEquals("fix.host must not be blank", ex.getMessage());
  }

  @Test
  void constructor_blankFixHost_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LauncherConfig("  ", 9880, 3, "data", "logs", 10, "a.yaml"));
  }

  // ===== fixPort validation =====

  @Test
  void constructor_portZero_throwsIae() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> new LauncherConfig("localhost", 0, 3, "data", "logs", 10, "a.yaml"));
    assertEquals("fix.port must be in [1, 65535], got: 0", ex.getMessage());
  }

  @Test
  void constructor_portNegative_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LauncherConfig("localhost", -1, 3, "data", "logs", 10, "a.yaml"));
  }

  @Test
  void constructor_portAboveMax_throwsIae() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> new LauncherConfig("localhost", 65536, 3, "data", "logs", 10, "a.yaml"));
    assertEquals("fix.port must be in [1, 65535], got: 65536", ex.getMessage());
  }

  // ===== nodeCount validation =====

  @Test
  void constructor_nodeCountZero_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LauncherConfig("localhost", 9880, 0, "data", "logs", 10, "a.yaml"));
  }

  @Test
  void constructor_nodeCountAboveMax_throwsIae() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new LauncherConfig(
                    "localhost", 9880, ClusterConfig.MAX_NODES + 1, "data", "logs", 10, "a.yaml"));
    assertEquals(
        "cluster.nodeCount must be in [1, " + ClusterConfig.MAX_NODES + "], got: 4",
        ex.getMessage());
  }

  // ===== baseDir validation =====

  @Test
  void constructor_nullBaseDir_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LauncherConfig("localhost", 9880, 3, null, "logs", 10, "a.yaml"));
  }

  @Test
  void constructor_blankBaseDir_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LauncherConfig("localhost", 9880, 3, "", "logs", 10, "a.yaml"));
  }

  // ===== logDir validation =====

  @Test
  void constructor_nullLogDir_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LauncherConfig("localhost", 9880, 3, "data", null, 10, "a.yaml"));
  }

  @Test
  void constructor_blankLogDir_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LauncherConfig("localhost", 9880, 3, "data", "  ", 10, "a.yaml"));
  }

  // ===== driverShutdownTimeoutSeconds validation =====

  @Test
  void constructor_zeroTimeout_throwsIae() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> new LauncherConfig("localhost", 9880, 3, "data", "logs", 0, "a.yaml"));
    assertEquals("driver.shutdown.timeout.seconds must be > 0, got: 0", ex.getMessage());
  }

  @Test
  void constructor_negativeTimeout_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LauncherConfig("localhost", 9880, 3, "data", "logs", -5, "a.yaml"));
  }

  // ===== accountsFile validation =====

  @Test
  void constructor_nullAccountsFile_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LauncherConfig("localhost", 9880, 3, "data", "logs", 10, null));
  }

  @Test
  void constructor_blankAccountsFile_throwsIae() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LauncherConfig("localhost", 9880, 3, "data", "logs", 10, "  "));
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
      "accounts.file"
    };
    // Save and clear any overrides from other tests
    final String[] saved = new String[keys.length];
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
    } finally {
      for (int i = 0; i < keys.length; i++) {
        if (saved[i] != null) {
          System.setProperty(keys[i], saved[i]);
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
      "accounts.file"
    };
    final String[] values = {
      "0.0.0.0", "5555", "1", "/var/cluster", "/var/log", "30", "/etc/accounts.yaml"
    };
    // Save originals before overwriting
    final String[] saved = new String[keys.length];
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
