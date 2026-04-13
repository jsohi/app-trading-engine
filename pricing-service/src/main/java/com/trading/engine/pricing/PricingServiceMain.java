package com.trading.engine.pricing;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;

/**
 * Standalone entry point for the pricing service process. Parses command-line arguments, constructs
 * a default {@link PricingServiceConfig}, launches the pricing service via {@link
 * PricingServiceLauncher#launch}, and blocks until a shutdown signal (SIGINT/SIGTERM) is received.
 *
 * <p><b>Usage:</b>
 *
 * <pre>
 *   java com.trading.engine.pricing.PricingServiceMain \
 *       --aeron-dir=/tmp/aeron-pricing \
 *       [--adapter-type=synthetic] \
 *       [--skew-model=convex]
 * </pre>
 *
 * <p><b>Arguments:</b>
 *
 * <ul>
 *   <li>{@code --aeron-dir=<path>} (required) -- path to the shared Media Driver's Aeron CnC
 *       directory
 *   <li>{@code --adapter-type=<type>} (optional, default "synthetic") -- market data adapter type:
 *       "deterministic" for fixed-rate test mode, "synthetic" for Brownian-motion dev mode
 *   <li>{@code --skew-model=<type>} (optional, default "convex") -- inventory skew model: "convex"
 *       (quadratic, production) or "linear" (test/dev)
 * </ul>
 *
 * <p><b>Shutdown sequence.</b> A JVM shutdown hook is registered before any resources are created.
 * On SIGINT or SIGTERM, the barrier is signalled, the main thread unblocks, and the shutdown hook
 * closes the {@link PricingComponents} in order: agent runner first (drains in-flight responses),
 * then the owned Aeron client.
 *
 * <p><b>Threading.</b> The main thread performs startup orchestration, then blocks on {@link
 * ShutdownSignalBarrier#await()}. The pricing duty cycle runs on its own named thread
 * ("pricing-service").
 *
 * <p><b>Logging.</b> Uses GFLog (zero-allocation) for all logging, consistent with the
 * pricing-service module's hot-path logging convention. No Log4j2 or SLF4J dependency.
 *
 * @see PricingServiceLauncher
 * @see PricingComponents
 * @see PricingServiceConfig
 */
public final class PricingServiceMain {

  private static final Log LOG = LogFactory.getLog(PricingServiceMain.class);

  /** Default Aeron CnC directory when co-located with the gateway media driver. */
  private static final String DEFAULT_AERON_DIR = "/tmp/aeron-gateway";

  /** Default market data adapter type. */
  private static final String DEFAULT_ADAPTER_TYPE = "synthetic";

  /** Default inventory skew model type. */
  private static final String DEFAULT_SKEW_MODEL = "convex";

  private PricingServiceMain() {}

  /**
   * Main entry point. Parses arguments, launches the pricing service, and blocks until shutdown.
   *
   * @param args command-line arguments; see class-level Javadoc for supported flags
   */
  public static void main(final String[] args) {
    final long launchStartNs = System.nanoTime();

    // --- Parse command-line arguments ---
    String aeronDir = DEFAULT_AERON_DIR;
    String adapterType = DEFAULT_ADAPTER_TYPE;
    String skewModel = DEFAULT_SKEW_MODEL;

    for (final String arg : args) {
      if (arg.startsWith("--aeron-dir=")) {
        aeronDir = arg.substring("--aeron-dir=".length());
      } else if (arg.startsWith("--adapter-type=")) {
        adapterType = arg.substring("--adapter-type=".length());
      } else if (arg.startsWith("--skew-model=")) {
        skewModel = arg.substring("--skew-model=".length());
      } else {
        LOG.warn().append("Unknown argument ignored: ").append(arg).commit();
      }
    }

    LOG.info()
        .append("PricingServiceMain starting: aeronDir=")
        .append(aeronDir)
        .append(" adapterType=")
        .append(adapterType)
        .append(" skewModel=")
        .append(skewModel)
        .commit();

    // --- Construct configuration ---
    final PricingServiceConfig config = new PricingServiceConfig(adapterType, skewModel);

    // --- Idle strategy: BackoffIdleStrategy for dev; override for production ---
    // TODO(APP-185): make idle strategy configurable via CLI args or config file
    final IdleStrategy idleStrategy = new BackoffIdleStrategy();

    // --- Register shutdown hook EARLY (before resource creation) ---
    final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
    // AtomicReference ensures safe publication of the PricingComponents reference to the
    // shutdown hook thread, which runs on a different thread from main().
    final AtomicReference<PricingComponents> componentsRef = new AtomicReference<>();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info().append("Shutdown hook triggered -- closing pricing service").commit();
                  final PricingComponents components = componentsRef.get();
                  if (components != null) {
                    components.close();
                  }
                  LOG.info().append("Pricing service shutdown complete").commit();
                },
                "pricing-service-shutdown"));

    // --- Launch ---
    try {
      final PricingComponents components =
          PricingServiceLauncher.launch(aeronDir, config, idleStrategy);
      componentsRef.set(components);

      final long startupMs = (System.nanoTime() - launchStartNs) / 1_000_000L;
      LOG.info().append("Pricing service ready: startupMs=").append(startupMs).commit();

    } catch (final Exception e) {
      LOG.error()
          .append("Pricing service startup failed: ")
          .append(e.getClass().getName())
          .append(" - ")
          .append(e.getMessage())
          .commit();
      System.exit(1);
      return; // unreachable, but clarifies control flow for the compiler
    }

    // --- Block until shutdown signal ---
    barrier.await();
    LOG.info().append("Shutdown signal received -- exiting main()").commit();
  }
}
