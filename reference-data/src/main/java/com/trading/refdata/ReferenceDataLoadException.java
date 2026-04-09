package com.trading.refdata;

/** Thrown when reference data loading fails — prevents startup. */
public final class ReferenceDataLoadException extends Exception {

  private final String entityType;

  public ReferenceDataLoadException(final String entityType, final String detail) {
    super(entityType + ": " + detail);
    this.entityType = entityType;
  }

  public ReferenceDataLoadException(
      final String entityType, final String detail, final Throwable cause) {
    super(entityType + ": " + detail, cause);
    this.entityType = entityType;
  }

  public String entityType() {
    return entityType;
  }
}
