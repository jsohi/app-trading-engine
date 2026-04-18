package com.trading.engine.e2e;

/**
 * Thrown when {@code e2e-scenarios.yaml} cannot be parsed or contains invalid field values.
 *
 * <p>Unchecked (extends {@link RuntimeException}) because E2E test scenario loading is a fail-fast
 * operation — there is no recovery path. This is intentionally separate from {@link
 * com.trading.refdata.ReferenceDataLoadException} which is a checked exception for production
 * reference-data loaders. E2E scenarios are test configuration, not reference data.
 *
 * <p><b>Threading:</b> Immutable after construction — safe to share.
 */
public final class E2EScenarioLoadException extends RuntimeException {

  /**
   * Creates a new exception with the given message.
   *
   * @param message description of the parse/validation failure
   */
  public E2EScenarioLoadException(final String message) {
    super(message);
  }

  /**
   * Creates a new exception with the given message and cause.
   *
   * @param message description of the parse/validation failure
   * @param cause the underlying cause (e.g., {@link java.io.IOException} from file read)
   */
  public E2EScenarioLoadException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
