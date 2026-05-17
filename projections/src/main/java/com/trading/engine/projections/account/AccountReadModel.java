package com.trading.engine.projections.account;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.projections.ProjectionUtil;
import java.util.List;

/**
 * Immutable snapshot of an account's state at a point in time. Returned by {@link
 * AccountProjection} query methods to provide a thread-safe, detached view of account state.
 *
 * <p><b>Naming:</b> named {@code AccountReadModel} (not {@code AccountSnapshot}) to avoid import
 * collision with the SBE-generated {@code com.trading.engine.messages.sbe.AccountSnapshotEncoder}
 * and {@code AccountSnapshotDecoder} (template 201), which are used for cluster write-model
 * snapshots.
 *
 * <p><b>Threading:</b> immutable — safe to share across threads without synchronization.
 *
 * <p><b>Allocation:</b> one instance per query result. Created by copying fields from the internal
 * mutable {@link AccountView} under the projection's read lock.
 *
 * @param accountId custom tag 10024: primary account identifier
 * @param parentAccountId custom tag 10040: parent account for give-up arrangements (0 = none)
 * @param accountCode FIX tag 1: account code
 * @param acctIdSource FIX tag 660: account identifier source scheme
 * @param accountName custom tag 10026: account name
 * @param accountType custom tag 10029: account type (House, Client, MarketMaker)
 * @param baseCurrency FIX tag 15: ISO 4217 base currency code
 * @param status custom tag 10027: administrative status (Active, Suspended, Closed)
 * @param complianceStatus custom tag 10041: KYC/compliance status
 * @param capabilities custom tag 10042: raw capability bitfield (bit 0 = CAN_TRADE, bit 1 =
 *     CAN_RFQ)
 * @param canTrade derived from capabilities bit 0
 * @param canRequestQuotes derived from capabilities bit 1
 * @param transactTime FIX tag 60: transaction time (epoch nanos)
 * @param sequenceNumber event sequence number of the most recently applied event
 * @param lastUpdatedAt cluster timestamp (epoch nanos) of the most recent event
 * @param symbolPreferences Phase 3 Commit B — per-account default subscription symbols (validated
 *     against {@code ^[A-Z]{6,8}$} at YAML load time); empty list = use cohort defaults
 * @param panelLayout Phase 3 Commit B — per-account panel-mount preferences; empty list = fall back
 *     to UI defaults
 */
public record AccountReadModel(
    long accountId,
    long parentAccountId,
    String accountCode,
    AcctIDSourceEnum acctIdSource,
    String accountName,
    AccountTypeEnum accountType,
    String baseCurrency,
    AccountStatusEnum status,
    ComplianceStatusEnum complianceStatus,
    long capabilities,
    boolean canTrade,
    boolean canRequestQuotes,
    long transactTime,
    long sequenceNumber,
    long lastUpdatedAt,
    List<String> symbolPreferences,
    List<AccountReadModel.PanelSlot> panelLayout) {

  /**
   * Phase 3 Commit B — sibling of {@code com.trading.refdata.account.AccountRecord.PanelSlot}.
   * Defined here so the {@code projections} module does not need a (wrong-direction) Gradle
   * dependency on {@code reference-data}. The launcher's {@code toReadModel} converts the refdata
   * type into this one at the projections boundary.
   *
   * @param panelId layout-side identifier; e.g. {@code "order-entry"}
   * @param slot grid-slot name; e.g. {@code "right-top"}
   */
  public record PanelSlot(String panelId, String slot) {
    public PanelSlot {
      if (panelId == null || panelId.isBlank()) {
        throw new IllegalArgumentException("PanelSlot.panelId must not be blank");
      }
      if (slot == null || slot.isBlank()) {
        throw new IllegalArgumentException("PanelSlot.slot must not be blank");
      }
    }
  }

  /**
   * Creates an immutable read model by copying all fields from a mutable {@link AccountView}.
   * String fields are decoded from SBE byte arrays using US-ASCII.
   *
   * <p>Must be called under the projection's read lock (or write lock during snapshot creation
   * inside event dispatch).
   *
   * @param v the mutable account view to copy from
   * @return a new immutable read model
   */
  static AccountReadModel from(final AccountView v) {
    return new AccountReadModel(
        v.accountId(),
        v.parentAccountId(),
        ProjectionUtil.asciiString(v.accountCode(), v.accountCodeLen()),
        v.acctIdSource(),
        ProjectionUtil.asciiString(v.accountName(), v.accountNameLen()),
        v.accountType(),
        ProjectionUtil.asciiString(v.baseCurrency(), v.baseCurrencyLen()),
        v.status(),
        v.complianceStatus(),
        v.capabilities(),
        v.canTrade(),
        v.canRequestQuotes(),
        v.transactTime(),
        v.sequenceNumber(),
        v.lastUpdatedAt(),
        // Phase 3 Commit B — projection-internal AccountView does NOT yet carry these fields
        // (extending the cluster's snapshot is out-of-scope for Commit B; the snapshot path is
        // covered by APP-244 follow-up). Default to empty so the read model is well-formed;
        // the LIVE source of preferences is the launcher's toReadModel which propagates from the
        // YAML-loaded AccountRecord directly.
        List.of(),
        List.of());
  }
}
