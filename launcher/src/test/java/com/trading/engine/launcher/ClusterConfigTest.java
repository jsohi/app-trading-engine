package com.trading.engine.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ClusterConfigTest {

  @Test
  void ingressPortPerNode() {
    assertEquals(20110, ClusterConfig.ingressPort(0));
    assertEquals(21110, ClusterConfig.ingressPort(1));
    assertEquals(22110, ClusterConfig.ingressPort(2));
  }

  @Test
  void consensusPortPerNode() {
    assertEquals(20220, ClusterConfig.consensusPort(0));
    assertEquals(21220, ClusterConfig.consensusPort(1));
    assertEquals(22220, ClusterConfig.consensusPort(2));
  }

  @Test
  void logPortPerNode() {
    assertEquals(20330, ClusterConfig.logPort(0));
    assertEquals(21330, ClusterConfig.logPort(1));
    assertEquals(22330, ClusterConfig.logPort(2));
  }

  @Test
  void catchupPortPerNode() {
    assertEquals(20440, ClusterConfig.catchupPort(0));
    assertEquals(21440, ClusterConfig.catchupPort(1));
    assertEquals(22440, ClusterConfig.catchupPort(2));
  }

  @Test
  void archivePortPerNode() {
    assertEquals(8010, ClusterConfig.archivePort(0));
    assertEquals(8011, ClusterConfig.archivePort(1));
    assertEquals(8012, ClusterConfig.archivePort(2));
  }

  @Test
  void portGettersRejectInvalidNodeId() {
    // Sweep every getter — guards against a future refactor that bypasses the shared
    // checkNodeId helper.
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.ingressPort(-1));
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.ingressPort(3));
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.consensusPort(-1));
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.consensusPort(3));
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.logPort(-1));
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.logPort(3));
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.catchupPort(-1));
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.catchupPort(3));
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.archivePort(-1));
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.archivePort(3));
  }

  @Test
  void checkNodeIdAcceptsValidIds() {
    ClusterConfig.checkNodeId(0);
    ClusterConfig.checkNodeId(1);
    ClusterConfig.checkNodeId(2);
  }

  @Test
  void checkNodeIdRejectsInvalidIds() {
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.checkNodeId(-1));
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.checkNodeId(3));
  }

  @Test
  void buildClusterMembersSingleNode() {
    assertEquals(
        "0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010",
        ClusterConfig.buildClusterMembers(1));
  }

  @Test
  void buildClusterMembersTwoNodes() {
    assertEquals(
        "0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010"
            + "|1,localhost:21110,localhost:21220,localhost:21330,localhost:21440,localhost:8011",
        ClusterConfig.buildClusterMembers(2));
  }

  @Test
  void buildClusterMembersThreeNodes() {
    assertEquals(
        "0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010"
            + "|1,localhost:21110,localhost:21220,localhost:21330,localhost:21440,localhost:8011"
            + "|2,localhost:22110,localhost:22220,localhost:22330,localhost:22440,localhost:8012",
        ClusterConfig.buildClusterMembers(3));
  }

  @Test
  void buildClusterMembersRejectsInvalidCount() {
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.buildClusterMembers(0));
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.buildClusterMembers(-1));
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.buildClusterMembers(4));
  }

  @Test
  void buildClusterMembersRoundTripStructure() {
    // Sanity: split by '|' yields nodeCount members, each split by ',' yields exactly 6 fields
    // (memberId + 5 endpoints: ingress/consensus/log/catchup/archive).
    final String members = ClusterConfig.buildClusterMembers(3);
    final String[] perMember = members.split("\\|");
    assertEquals(3, perMember.length);
    for (int i = 0; i < perMember.length; i++) {
      final String[] fields = perMember[i].split(",");
      assertEquals(6, fields.length, "member " + i + " field count");
      assertEquals(Integer.toString(i), fields[0], "member " + i + " id");
    }
  }

  @Test
  void ingressEndpointsThreeNodes() {
    assertEquals(
        "0=localhost:20110,1=localhost:21110,2=localhost:22110", ClusterConfig.ingressEndpoints(3));
  }

  @Test
  void ingressEndpointsSingleNode() {
    assertEquals("0=localhost:20110", ClusterConfig.ingressEndpoints(1));
  }

  @Test
  void ingressEndpointsRejectsInvalidCount() {
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.ingressEndpoints(0));
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.ingressEndpoints(4));
  }

  // -------------------------------------------------------------------------
  // Multi-host overloads (added for APP-14 Gemini review: no more hardcoded localhost)
  // -------------------------------------------------------------------------

  @Test
  void buildClusterMembersWithExplicitHosts() {
    final String members = ClusterConfig.buildClusterMembers(3, "host-a", "host-b", "host-c");
    assertEquals(
        "0,host-a:20110,host-a:20220,host-a:20330,host-a:20440,host-a:8010"
            + "|1,host-b:21110,host-b:21220,host-b:21330,host-b:21440,host-b:8011"
            + "|2,host-c:22110,host-c:22220,host-c:22330,host-c:22440,host-c:8012",
        members);
  }

  @Test
  void buildClusterMembersWithSingleExplicitHost() {
    assertEquals(
        "0,10.0.0.1:20110,10.0.0.1:20220,10.0.0.1:20330,10.0.0.1:20440,10.0.0.1:8010",
        ClusterConfig.buildClusterMembers(1, "10.0.0.1"));
  }

  @Test
  void buildClusterMembersNoHostOverloadStillUsesLocalhost() {
    // Back-compat: the existing single-arg overload remains localhost-only.
    assertEquals(
        ClusterConfig.buildClusterMembers(3, "localhost", "localhost", "localhost"),
        ClusterConfig.buildClusterMembers(3));
  }

  @Test
  void buildClusterMembersRejectsWrongHostCount() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ClusterConfig.buildClusterMembers(3, "host-a", "host-b"));
    assertThrows(
        IllegalArgumentException.class,
        () -> ClusterConfig.buildClusterMembers(2, "host-a", "host-b", "host-c"));
  }

  @Test
  void buildClusterMembersRejectsBlankHost() {
    assertThrows(
        IllegalArgumentException.class, () -> ClusterConfig.buildClusterMembers(2, "host-a", ""));
    assertThrows(
        IllegalArgumentException.class,
        () -> ClusterConfig.buildClusterMembers(2, "host-a", "   "));
    assertThrows(
        IllegalArgumentException.class,
        () -> ClusterConfig.buildClusterMembers(2, "host-a", (String) null));
  }

  @Test
  void buildClusterMembersRejectsNullHostsArray() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ClusterConfig.buildClusterMembers(1, (String[]) null));
  }

  @Test
  void ingressEndpointsWithExplicitHosts() {
    assertEquals(
        "0=host-a:20110,1=host-b:21110,2=host-c:22110",
        ClusterConfig.ingressEndpoints(3, "host-a", "host-b", "host-c"));
  }

  @Test
  void ingressEndpointsNoHostOverloadStillUsesLocalhost() {
    assertEquals(
        ClusterConfig.ingressEndpoints(3, "localhost", "localhost", "localhost"),
        ClusterConfig.ingressEndpoints(3));
  }

  @Test
  void ingressEndpointsRejectsWrongHostCount() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ClusterConfig.ingressEndpoints(3, "host-a", "host-b"));
  }

  @Test
  void hostForMemberExtractsLocalhostDefault() {
    final String members = ClusterConfig.buildClusterMembers(3);
    assertEquals("localhost", ClusterConfig.hostForMember(members, 0));
    assertEquals("localhost", ClusterConfig.hostForMember(members, 1));
    assertEquals("localhost", ClusterConfig.hostForMember(members, 2));
  }

  @Test
  void hostForMemberExtractsExplicitHost() {
    final String members = ClusterConfig.buildClusterMembers(3, "host-a", "host-b", "host-c");
    assertEquals("host-a", ClusterConfig.hostForMember(members, 0));
    assertEquals("host-b", ClusterConfig.hostForMember(members, 1));
    assertEquals("host-c", ClusterConfig.hostForMember(members, 2));
  }

  @Test
  void hostForMemberRejectsMissingNodeId() {
    final String members = ClusterConfig.buildClusterMembers(2);
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.hostForMember(members, 2));
  }

  @Test
  void hostForMemberRejectsBlankString() {
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.hostForMember("", 0));
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.hostForMember(null, 0));
  }

  @Test
  void hostForMemberRejectsMalformedString() {
    // "garbage" has 1 field, fails the strict 6-field check.
    assertThrows(IllegalArgumentException.class, () -> ClusterConfig.hostForMember("garbage", 0));
    // 2 fields — also fails the strict check.
    assertThrows(
        IllegalArgumentException.class, () -> ClusterConfig.hostForMember("0,no-port-here", 0));
  }

  @Test
  void hostForMemberRejectsPartialMemberEntry() {
    // Exactly 5 fields — missing the archive endpoint. Must fail fast rather than parse
    // partial config (per Gemini review feedback on PR #28).
    final String partial = "0,host-a:20110,host-a:20220,host-a:20330,host-a:20440";
    final IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> ClusterConfig.hostForMember(partial, 0));
    assertTrue(
        ex.getMessage().contains("expected 6 fields"),
        "expected 'expected 6 fields' in message, got: " + ex.getMessage());
  }

  @Test
  void hostForMemberRejectsNonNumericMemberId() {
    // Covers the NumberFormatException catch branch. Must have exactly 6 fields to get past the
    // length check and reach the parseInt.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ClusterConfig.hostForMember(
                "abc,host-a:20110,host-a:20220,host-a:20330,host-a:20440,host-a:8010", 0));
  }
}
