package com.trading.engine.launcher;

import io.aeron.archive.Archive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import java.util.Objects;
import org.agrona.CloseHelper;

/**
 * Holder for the three long-lived Aeron Cluster components launched by {@link
 * ClusterNodeLauncher#launch}. Owns their lifecycle: {@link #close()} shuts them down in reverse of
 * construction order ({@code container → consensusModule → archive}) using {@link
 * CloseHelper#closeAll} so a failure in one close does not mask the others.
 */
public final class ClusterComponents implements AutoCloseable {

  private final Archive archive;
  private final ConsensusModule consensusModule;
  private final ClusteredServiceContainer serviceContainer;

  public ClusterComponents(
      final Archive archive,
      final ConsensusModule consensusModule,
      final ClusteredServiceContainer serviceContainer) {
    this.archive = Objects.requireNonNull(archive, "archive");
    this.consensusModule = Objects.requireNonNull(consensusModule, "consensusModule");
    this.serviceContainer = Objects.requireNonNull(serviceContainer, "serviceContainer");
  }

  public Archive archive() {
    return archive;
  }

  public ConsensusModule consensusModule() {
    return consensusModule;
  }

  public ClusteredServiceContainer serviceContainer() {
    return serviceContainer;
  }

  @Override
  public void close() {
    CloseHelper.closeAll(serviceContainer, consensusModule, archive);
  }
}
