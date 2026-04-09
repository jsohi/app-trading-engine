package com.trading.refdata.spi;

import com.trading.refdata.ReferenceDataLoadException;
import java.util.List;

/**
 * Loads reference data records from an external source (YAML, CSV, RDBMS, vendor feed).
 *
 * @param <T> the record type (e.g. AccountRecord)
 */
public interface ReferenceDataLoader<T> {

  /** Load all records from the configured source. */
  List<T> load() throws ReferenceDataLoadException;

  /** Human-readable name of the source (e.g. "accounts.yaml"). For logging. */
  String sourceName();
}
