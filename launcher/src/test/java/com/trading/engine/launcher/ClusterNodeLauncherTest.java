package com.trading.engine.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.ChannelUri;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Hermetic unit tests for {@link ClusterNodeLauncher}. These only exercise the fail-fast CnC check
 * — spinning up a real Media Driver + Archive + ConsensusModule + ClusteredServiceContainer in a
 * unit test is flaky and belongs to the integration-tests module (APP-16).
 */
class ClusterNodeLauncherTest {

  @Test
  void launchFailsFastWhenAeronDirDoesNotExist(@TempDir final Path baseDir) {
    final String members = ClusterConfig.buildClusterMembers(1);
    final IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                ClusterNodeLauncher.launch(
                    0, baseDir.toString(), "/nonexistent/aeron/dir/does/not/exist", members));
    assertTrue(
        ex.getMessage().contains("cnc.dat missing"),
        "expected 'cnc.dat missing' in message, got: " + ex.getMessage());
  }

  @Test
  void launchFailsFastWhenAeronDirExistsButCncMissing(
      @TempDir final Path baseDir, @TempDir final Path emptyAeronDir) {
    final String members = ClusterConfig.buildClusterMembers(1);
    final IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                ClusterNodeLauncher.launch(
                    0, baseDir.toString(), emptyAeronDir.toString(), members));
    assertTrue(
        ex.getMessage().contains("cnc.dat missing"),
        "expected 'cnc.dat missing' in message, got: " + ex.getMessage());
  }

  @Test
  void launchRejectsInvalidNodeIdBeforeCreatingDirectories(@TempDir final Path baseDir) {
    final String members = ClusterConfig.buildClusterMembers(1);
    assertThrows(
        IllegalArgumentException.class,
        () -> ClusterNodeLauncher.launch(-1, baseDir.toString(), "/nonexistent", members));
    assertThrows(
        IllegalArgumentException.class,
        () -> ClusterNodeLauncher.launch(99, baseDir.toString(), "/nonexistent", members));
    // Critical: no archive-<n> / cluster-<n> directories should have been created on disk.
    assertFalse(Files.exists(baseDir.resolve("archive--1")), "archive--1 must not be created");
    assertFalse(Files.exists(baseDir.resolve("archive-99")), "archive-99 must not be created");
    assertFalse(Files.exists(baseDir.resolve("cluster--1")), "cluster--1 must not be created");
    assertFalse(Files.exists(baseDir.resolve("cluster-99")), "cluster-99 must not be created");
  }

  @Test
  void launchRejectsBlankStringArgs(@TempDir final Path baseDir) {
    final String members = ClusterConfig.buildClusterMembers(1);
    assertThrows(
        IllegalArgumentException.class,
        () -> ClusterNodeLauncher.launch(0, "", "/nonexistent", members));
    assertThrows(
        IllegalArgumentException.class,
        () -> ClusterNodeLauncher.launch(0, "   ", "/nonexistent", members));
    assertThrows(
        IllegalArgumentException.class,
        () -> ClusterNodeLauncher.launch(0, baseDir.toString(), "", members));
    assertThrows(
        IllegalArgumentException.class,
        () -> ClusterNodeLauncher.launch(0, baseDir.toString(), "/nonexistent", ""));
  }

  @Test
  void launchRejectsNullStringArgs(@TempDir final Path baseDir) {
    final String members = ClusterConfig.buildClusterMembers(1);
    assertThrows(
        NullPointerException.class,
        () -> ClusterNodeLauncher.launch(0, null, "/nonexistent", members));
    assertThrows(
        NullPointerException.class,
        () -> ClusterNodeLauncher.launch(0, baseDir.toString(), null, members));
    assertThrows(
        NullPointerException.class,
        () -> ClusterNodeLauncher.launch(0, baseDir.toString(), "/nonexistent", null));
  }

  /**
   * Validates that the snapshot channel URI parses correctly with Aeron's {@link ChannelUri}
   * parser. If this test fails, the snapshot channel falls back to the Media Driver's default IPC
   * term-length (256 KB / 32 KB maxMessageLength), silently breaking atomic snapshot publish. This
   * test MUST pass before any APP-150 changes are merged.
   */
  @Test
  void snapshotChannelUriParsesCorrectly() {
    final ChannelUri uri = ChannelUri.parse(ClusterNodeLauncher.SNAPSHOT_CHANNEL);
    assertEquals("ipc", uri.media(), "snapshot channel must use IPC transport");
    assertEquals(
        "134217728", uri.get("term-length"), "term-length must be 128 MB (134217728 bytes)");
    assertEquals("snapshot", uri.get("alias"), "alias must be 'snapshot'");
  }

  @Test
  void launchFailsFastWithNonLocalhostMembers(@TempDir final Path baseDir) {
    // Locks in that the host-extraction path works for non-localhost clusters. The CnC check
    // still fails first (driver not running), but launch() must have parsed the explicit host
    // without throwing.
    final String members = ClusterConfig.buildClusterMembers(1, "10.0.0.1");
    final IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () -> ClusterNodeLauncher.launch(0, baseDir.toString(), "/nonexistent", members));
    assertTrue(
        ex.getMessage().contains("cnc.dat missing"),
        "expected 'cnc.dat missing' in message, got: " + ex.getMessage());
  }
}
