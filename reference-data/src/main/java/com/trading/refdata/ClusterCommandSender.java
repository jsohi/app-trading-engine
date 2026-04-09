package com.trading.refdata;

import org.agrona.DirectBuffer;

/**
 * Sends an SBE-encoded command to the Aeron cluster ingress. The launcher wires this to {@code
 * clusterClient::offer}.
 */
@FunctionalInterface
public interface ClusterCommandSender {

  /**
   * Send a command buffer to the cluster.
   *
   * @return Aeron publication result (>= 0 on success, negative on failure/back-pressure)
   */
  long send(DirectBuffer buffer, int offset, int length);
}
