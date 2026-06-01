# APP-62 — Pre-Trade Risk Control Boundary-Fuzz Evidence Pack

**Regulator-facing artifact.** This file is the canonical evidence pack referenced from PR review for the APP-62 pre-trade risk-control set. SHA-stamped per-run copies live under `build/reports/risk-control-evidence/APP-62-evidence-<sha>.md` (emitted by the `:integration-tests:riskControlEvidence` Gradle task and NOT committed to the repository).

| field                                                         | value                                              |
| ------------------------------------------------------------- | -------------------------------------------------- |
| git commit SHA                                                | `799b678fee26b2abd8342d4c1979107daab5283b (dirty)` |
| schema SHA (`messages/src/main/resources/trading-schema.xml`) | `bdedf0c5a62170826ca486c62255e0be2a89b246`         |
| run timestamp (UTC)                                           | 2026-06-01T10:35:58Z                               |
| plan reference                                                | APP-62 plan §5.3 (boundary-fuzz evidence pack)     |

## Boundary-fuzz table

| checkName           | boundaryCase                                                                | inputValue                                                                                                       | expectedReason        | observedReason        | result |
| ------------------- | --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | --------------------- | --------------------- | ------ |
| SymbolEligibility   | no eligibility record (reject)                                              | `scenario=NO_RECORD, side=Buy`                                                                                   | RegulatoryRestriction | RegulatoryRestriction | PASS   |
| SymbolEligibility   | tradingAllowed=false (reject)                                               | `scenario=TRADING_DISALLOWED, side=Buy`                                                                          | RegulatoryRestriction | RegulatoryRestriction | PASS   |
| SymbolEligibility   | Sell + shortSaleAllowed=false (reject)                                      | `scenario=SHORT_SALE_DISALLOWED, side=Sell`                                                                      | RegulatoryRestriction | RegulatoryRestriction | PASS   |
| SymbolEligibility   | Buy + shortSaleAllowed=false (admit — Phase-1 carve-out)                    | `scenario=SHORT_SALE_DISALLOWED, side=Buy`                                                                       | (admit)               | (admit)               | PASS   |
| SymbolEligibility   | all-permissive (admit)                                                      | `scenario=PERMISSIVE, side=Buy`                                                                                  | (admit)               | (admit)               | PASS   |
| FatFinger           | threshold-1 (admit)                                                         | `thresholdBps=100, actualBps=99, failClosed=true, refSeeded=true`                                                | (admit)               | (admit)               | PASS   |
| FatFinger           | threshold (admit, strict >)                                                 | `thresholdBps=100, actualBps=100, failClosed=true, refSeeded=true`                                               | (admit)               | (admit)               | PASS   |
| FatFinger           | threshold+1 (reject)                                                        | `thresholdBps=100, actualBps=101, failClosed=true, refSeeded=true`                                               | PriceTooFarFromMarket | PriceTooFarFromMarket | PASS   |
| FatFinger           | no-reference, failClosed=true (reject)                                      | `thresholdBps=100, failClosed=true, refSeeded=false`                                                             | PriceTooFarFromMarket | PriceTooFarFromMarket | PASS   |
| FatFinger           | no-reference, failClosed=false (admit)                                      | `thresholdBps=100, failClosed=false, refSeeded=false`                                                            | (admit)               | (admit)               | PASS   |
| FatFinger           | stale-reference, failClosed=true (reject)                                   | `thresholdBps=100, refStaleByHours=1, failClosed=true`                                                           | PriceTooFarFromMarket | PriceTooFarFromMarket | PASS   |
| FatFinger           | per-symbol override (50) tighter than account knob (1000) at 75bps (reject) | `accountBps=1000, overrideBps=50, actualBps=75`                                                                  | PriceTooFarFromMarket | PriceTooFarFromMarket | PASS   |
| FatFinger           | crossed-book skip (no reference cached) (admit)                             | `thresholdBps=100, bid>ask (crossed), failClosed=false`                                                          | (admit)               | (admit)               | PASS   |
| FourEyesViolation   | proposer == approver (reject)                                               | `proposerLen=8, approverLen=8, equal=true`                                                                       | FourEyesViolation     | FourEyesViolation     | PASS   |
| FourEyesViolation   | empty proposer (reject)                                                     | `proposerLen=0, approverLen=8, equal=false`                                                                      | FourEyesViolation     | FourEyesViolation     | PASS   |
| FourEyesViolation   | empty approver (reject)                                                     | `proposerLen=8, approverLen=0, equal=false`                                                                      | FourEyesViolation     | FourEyesViolation     | PASS   |
| FourEyesViolation   | distinct non-empty (admit)                                                  | `proposerLen=5, approverLen=3, equal=false`                                                                      | (admit)               | (admit)               | PASS   |
| RiskLimitsNotLoaded | no risk-limit record loaded for account (reject)                            | `riskLimitStore.contains(account=1)=false`                                                                       | RiskLimitsNotLoaded   | RiskLimitsNotLoaded   | PASS   |
| RiskLimitsNotLoaded | risk-limit record loaded for account (admit)                                | `riskLimitStore.contains(account=1)=true`                                                                        | (admit)               | (admit)               | PASS   |
| PositionLimit       | projected=limit-1 (admit)                                                   | `maxLongPosition=1000000000, workingLong=899999999, orderQty=100000000, observedCheckId=(admit)`                 | (admit)               | (admit)               | PASS   |
| PositionLimit       | projected=limit (admit, strict >)                                           | `maxLongPosition=1000000000, workingLong=900000000, orderQty=100000000, observedCheckId=(admit)`                 | (admit)               | (admit)               | PASS   |
| PositionLimit       | projected=limit+1 (reject)                                                  | `maxLongPosition=1000000000, workingLong=900000001, orderQty=100000000, observedCheckId=PositionLimit`           | PositionLimitExceeded | PositionLimitExceeded | PASS   |
| PositionLimit       | currentLong=Long.MAX_VALUE → safeAdd saturates (reject)                     | `maxLongPosition=1000000000, workingLong=9223372036854775807, orderQty=100000000, observedCheckId=PositionLimit` | PositionLimitExceeded | PositionLimitExceeded | PASS   |
| PositionLimit       | positionLimitEnabled=false at Long.MAX_VALUE (admit-bypass)                 | `maxLongPosition=0, workingLong=9223372036854775807, positionLimitEnabled=false`                                 | (admit)               | (admit)               | PASS   |

## Summary

- total rows: 24
- pass: 24
- fail: 0

## Regulatory cross-reference

- **FINRA Rule 3110(a) — Supervision.** Pre-trade controls must be tested and the test evidence retained as part of the firm's supervisory record. This pack is the artifact referenced from the APP-62 PR for that test evidence.
- **SEC Rule 15c3-5(b) — Market Access Rule.** Brokers providing market access must have risk-management controls reasonably designed to systematically prevent the entry of erroneous orders, by rejecting orders that exceed appropriate price or size parameters or that exceed pre-set credit / capital thresholds. Each row below documents the boundary at which the corresponding 15c3-5(b) control engages.
- **MiFID II RTS 6 Art. 9 (Pre-trade controls) and Art. 17 (Periodic review).** Investment firms engaged in algorithmic trading must apply pre-trade controls on order entry (price collar, max order value, max order volume) and must annually self-assess the calibration. The boundary cases below provide the calibration evidence for the periodic review.
- **MiFID II RTS 6 §1(2) — Four-eyes principle.** Risk-limit changes must be subject to dual control. The `FourEyesViolation` rows below evidence that the cluster rejects single-eye and self-approved risk-limit loads.

_Generated by `:integration-tests:riskControlEvidence` (class `RiskControlEvidenceIT`, listener `EvidenceReportListener`)._
