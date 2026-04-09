# Clock Synchronisation

All trading engine processes use nanosecond epoch timestamps. The cluster provides
deterministic timestamps via `NanosecondClusterClock`; non-cluster processes use
`OffsetEpochNanoClock` (zero-allocation, anchored to `System.nanoTime()`).

Both delegate to the operating system clock. Cross-box timestamp consistency is an
infrastructure concern — the application cannot solve it.

## PTP (IEEE 1588v2) — Preferred

Precision Time Protocol provides sub-microsecond synchronisation with PTP-capable
NICs and a grandmaster clock on the network.

**Linux setup (linuxptp):**

```bash
# Start PTP daemon on the PTP-capable NIC
ptp4l -i eth0 -m -f /etc/ptp4l.conf

# Synchronise the system clock from the PTP hardware clock
phc2sys -a -r -m
```

Expected offset: < 1 microsecond with hardware timestamping.

## chrony with Hardware Timestamping — Fallback

If PTP hardware is unavailable, chrony with hardware timestamping achieves
low-microsecond accuracy.

```bash
# /etc/chrony.conf
server ntp1.example.com iburst
hwtimestamp eth0
```

Expected offset: 1-10 microseconds.

## Verification

```bash
# Check current clock offset and drift
chronyc tracking

# Key fields:
#   System time     : offset from NTP (should be < 1us with PTP)
#   RMS offset      : root-mean-square of recent offsets
#   Frequency       : clock drift rate (ppm)
```

## Alert Thresholds

| Metric | Warning | Critical |
|--------|---------|----------|
| System clock offset | > 100 us | > 1 ms |
| RMS offset | > 50 us | > 500 us |

## Why Application Code Cannot Solve This

`OffsetEpochNanoClock` anchors `System.nanoTime()` to `System.currentTimeMillis()` at
startup and re-samples hourly. If two boxes have OS clocks that diverge by 50 ms, their
epoch-nano timestamps diverge by 50 ms — no amount of application logic can fix this
without a synchronised time source at the OS/network level.
