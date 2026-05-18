# FIX 4.4 Dictionary — Counterparty Integration Artifact

## Purpose

`FIX44.xml` in this directory is the **canonical, exported FIX 4.4 dictionary**
the trading engine's Artio acceptor speaks on the wire. It is intended for
**external counterparty integration** — third-party FIX clients (buy-side OMS,
test harnesses, certification tools such as QuickFIX/J or onixs) can drop this
file into their dictionary loader to obtain an exact, byte-equivalent view of
the messages, fields, components, and required/optional cardinalities accepted
by the gateway.

> This artifact is **not** consumed by the engine itself at runtime; it is the
> downstream/external view. The runtime codec generator (`fix-codecs`) reads
> the same logical dictionary from `fix-codecs/src/main/resources/fix/FIX44.xml`
> at build time, and the `:messages:generateFixDictionary` Gradle task keeps
> these two copies bit-identical via a SHA-256 pin.

## How to consume

1. Copy `FIX44.xml` into your FIX engine's dictionary directory.
2. Point your acceptor's session configuration at it
   (e.g. QuickFIX/J `DataDictionary=FIX44.xml`).
3. Validate against the SHA-256 pin in `FIX44.xml.sha256` before going live:
    ```bash
    shasum -a 256 -c FIX44.xml.sha256
    ```
4. Custom trading-engine tags (Tenor=10001, ProductType=10013, swap-points
   10003 / 10010 / 10011 / 10012) are **not yet** in this dictionary. Tracked
   in APP-45 (Wave 8 — FX Multi-Leg Translators Update).

## SHA-256 pin mechanism

`FIX44.xml.sha256` contains a single `shasum -a 256`-format line:

```
<sha256-hex>  FIX44.xml
```

The `:messages:generateFixDictionary` Gradle task **re-derives** the SHA-256
from the source dictionary on every build and **asserts** the computed digest
matches the committed sidecar. Any drift (manual edit of `FIX44.xml`, an
upstream Artio change, or a stale checkout) fails the build at
`./gradlew :messages:check`. The same task is wired into `:messages:build`
via the `check` lifecycle, so PR pipelines catch drift automatically.

## How to regenerate

When the source dictionary in `fix-codecs/src/main/resources/fix/FIX44.xml`
is intentionally updated (e.g. APP-45 adds the custom FX tags), run:

```bash
./gradlew :messages:generateFixDictionary
```

This re-copies `FIX44.xml` and rewrites `FIX44.xml.sha256` from the new
source. Commit **both** files in the same change set so the pin and the
artifact stay in lockstep.

## Source provenance

The dictionary originates from the QuickFIX/J master `FIX44.xml`
(Apache 2.0) at
<https://github.com/quickfix-j/quickfixj/blob/master/quickfixj-messages/quickfixj-messages-fix44/src/main/resources/FIX44.xml>,
with one local modification: the `ListStatus (35=N)` message is removed
because Artio's flyweight code generator emits broken Java for that
message's `NoRpts (82)` repeating group with this dictionary. The engine
does not speak `ListStatus`, so dropping it is safe. See the comment block
at the top of `FIX44.xml` for the authoritative provenance note.
