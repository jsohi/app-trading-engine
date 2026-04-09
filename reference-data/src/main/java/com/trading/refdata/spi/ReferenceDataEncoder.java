package com.trading.refdata.spi;

import com.trading.refdata.ReferenceDataLoadException;
import java.util.List;
import org.agrona.MutableDirectBuffer;

/**
 * Encodes a batch of reference data records into an SBE command buffer.
 *
 * @param <T> the record type (e.g. AccountRecord)
 */
public interface ReferenceDataEncoder<T> {

  /**
   * Encode records {@code [fromIndex, toIndex)} into an SBE batch command.
   *
   * @return total encoded length (header + body)
   */
  int encodeBatch(
      List<T> records, int fromIndex, int toIndex, MutableDirectBuffer buffer, int offset)
      throws ReferenceDataLoadException;

  /** SBE templateId of the batch command this encoder produces. */
  int templateId();

  /** Maximum records per batch (constrained by Aeron MTU / SBE group limits). */
  int maxBatchSize();

  /** Human-readable entity type name for error messages (e.g. "Account"). */
  String entityType();
}
