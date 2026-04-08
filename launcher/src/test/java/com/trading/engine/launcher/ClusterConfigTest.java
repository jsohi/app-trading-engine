package com.trading.engine.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}
