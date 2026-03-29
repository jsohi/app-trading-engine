package com.trading.engine.media.driver;

import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Standalone JVM entry point for the Aeron Media Driver. */
public final class MediaDriverLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(MediaDriverLauncher.class);

  private MediaDriverLauncher() {}

  public static void main(final String[] args) {
    final MediaDriverConfig config = MediaDriverConfig.parseArgs(args);

    LOG.info("Starting Aeron Media Driver with config: {}", config);

    final MediaDriver.Context ctx =
        new MediaDriver.Context()
            .aeronDirectoryName(config.aeronDir())
            .threadingMode(config.threadingMode())
            .termBufferSparseFile(true)
            .publicationTermBufferLength(config.termBufferLength())
            .ipcTermBufferLength(config.ipcTermLength())
            .dirDeleteOnStart(config.dirDeleteOnStart())
            .dirDeleteOnShutdown(false);

    final MediaDriver driver = MediaDriver.launch(ctx);

    LOG.info(
        "Aeron Media Driver started -- aeronDir={}, threadingMode={}",
        config.aeronDir(),
        config.threadingMode());

    final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Shutting down Aeron Media Driver");
                  driver.close();
                  LOG.info("Aeron Media Driver closed");
                },
                "media-driver-shutdown"));

    barrier.await();
  }
}
