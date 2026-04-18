package com.trading.refdata.spi;

import com.trading.refdata.ReferenceDataLoadException;

/**
 * Shared validation for status and enum string values used across reference data loaders and
 * encoders. Centralizes the {@code Active/Suspended/Closed} lifecycle check so additions need only
 * one edit.
 *
 * <p>Not thread-safe — but all methods are stateless static, so concurrent use is safe.
 */
public final class StatusValidator {

  private StatusValidator() {}

  /**
   * Validates that {@code status} is one of {@code Active}, {@code Suspended}, {@code Closed}.
   *
   * @param status the status string from YAML
   * @param entityType entity name for error messages (e.g. "Currency", "RiskLimit")
   * @throws ReferenceDataLoadException if the status is not a known value
   */
  public static void validateStatus(final String status, final String entityType)
      throws ReferenceDataLoadException {
    switch (status) {
      case "Active", "Suspended", "Closed" -> {}
      default ->
          throw new ReferenceDataLoadException(entityType, "unknown status: '" + status + "'");
    }
  }

  /**
   * Validates that {@code currencyClass} is one of {@code Fiat}, {@code Metal}, {@code Crypto},
   * {@code Fund}.
   *
   * @param currencyClass the currency class string from YAML
   * @param entityType entity name for error messages
   * @throws ReferenceDataLoadException if the currency class is not a known value
   */
  public static void validateCurrencyClass(final String currencyClass, final String entityType)
      throws ReferenceDataLoadException {
    switch (currencyClass) {
      case "Fiat", "Metal", "Crypto", "Fund" -> {}
      default ->
          throw new ReferenceDataLoadException(
              entityType, "unknown currencyClass: '" + currencyClass + "'");
    }
  }
}
