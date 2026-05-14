# messageSource — prod-mode wiring (to land with APP-160)

`web-ui/src/main-thread/messageSource.ts` currently ships a **loud stub** for
the prod branch:

```ts
pushConnectionState("DOWN_REQUIRES_USER_ACTION");
console.error("messageSource: prod token provider not landed (APP-160)");
return;
```

This file documents the wiring that replaces the stub when **APP-160**
(JWT auth + prod token provider) lands. The block was previously inlined
as a `/* … */` comment in `messageSource.ts`; moved here so it gets
markdown-lint coverage instead of silently rotting in a JS comment.

## Replacement code

When APP-160 lands, replace the stub with:

```ts
import { skip } from "rxjs";
import { WorkerClient } from "@/main-thread/workerClient";
import { prodTokenProvider } from "@/main-thread/prodTokenProvider"; // ← APP-160 delivers this

// (inside startMessageSource, in the `mode === "prod"` branch)
const client = new WorkerClient({
    tokenProvider: prodTokenProvider,
    wsUrl: import.meta.env.VITE_WS_URL,
});
const subMessages = client.messages$.subscribe((m) => _messages.next(m));

// skip(1): WorkerClient.connectionState$ is a BehaviorSubject seeded with
// "CONNECTING"; subscribing replays it synchronously. The async
// start().catch() resolves on a later microtask, so it cannot beat the
// seed. skip(1) drops the redundant CONNECTING re-push so any
// worker-emitted state (or the catch-pushed WORKER_DEAD) becomes the
// first value to propagate.
const subState = client.connectionState$
    .pipe(skip(1))
    .subscribe(pushConnectionState);
const subErrors = client.errors$.subscribe((e) =>
    console.warn("[worker]", e.code, e.hint),
);
void client.start().catch(() => pushConnectionState("WORKER_DEAD"));

// HMR teardown — mirrors the dev-branch shape:
if (import.meta.hot) {
    import.meta.hot.dispose(() => {
        subMessages.unsubscribe();
        subState.unsubscribe();
        subErrors.unsubscribe();
        client.dispose();
        _messages.complete();
        started = false;
    });
}
```

## Notes on the contract

- **`messages$` MUST NOT be inspected for `type === "connection-state"`.**
  `WorkerClient` already routes those via `connectionState$` (`workerClient.ts`
  inside the `MESSAGE_BATCH` handler); double-bridging would fire each
  transition twice.
- The producer-only `_messages` ReplaySubject stays module-private; the
  public `messages$` is the `defer(() => _messages.asObservable())`
  wrapper. Subscribing `client.messages$` re-emits each `WorkerMessage`
  into `_messages.next(...)` — never via the public `messages$`.
- `skip(1)` on `connectionState$` is load-bearing — see the inline
  comment above.

## When to delete this file

When APP-160 lands and the prod path is real code (not a stub), delete
this file. Its rationale lives in `messageSource.ts`'s header comment
from then on.
