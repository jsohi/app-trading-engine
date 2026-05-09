/**
 * WsUrlValidator — production WebSocket URL safety check.
 *
 * In production builds, the worker MUST connect over `wss://` to an
 * explicit allow-listed host. Refuses `ws://` (cleartext), and refuses
 * loopback / link-local hosts (`localhost`, `127.0.0.1`, `*.local`,
 * `::1`) since those are dev-only.
 *
 * Dev / test mode (`import.meta.env.DEV` or `import.meta.env.MODE === 'test'`)
 * relaxes the loopback ban so `wss://localhost:8443` works for local stacks.
 *
 * Threading: any (pure validation, called once at worker boot from
 * `INIT.wsUrl`).
 *
 * Allocation: small (one URL parse + simple string checks); cold path.
 *
 * Plan reference: §2.5 / §5.1 / §6 row 5.
 */

const LOCAL_HOST_PATTERNS: readonly RegExp[] = Object.freeze([
  /^localhost(?::\d+)?$/i,
  /^127(?:\.\d+){3}(?::\d+)?$/,
  /^\[::1\](?::\d+)?$/,
  /\.local(?::\d+)?$/i,
  /^(?:0\.0\.0\.0|\[::\])(?::\d+)?$/,
]);

export interface ValidatedWsUrl {
  readonly url: string;
  readonly host: string;
  readonly port: number;
}

/**
 * Validate a WebSocket URL string.
 *
 * @param raw URL from `INIT.wsUrl` (build-time `VITE_WS_URL`)
 * @param mode "prod" | "dev" — dev allows loopback hosts
 * @returns parsed { url, host, port } on success
 * @throws Error if URL is malformed, scheme is not wss, or — in prod —
 *   the host matches a loopback pattern
 */
export function validateWsUrl(raw: string, mode: "prod" | "dev" = "prod"): ValidatedWsUrl {
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    throw new Error(`WsUrlValidator: malformed URL ${JSON.stringify(raw)}`);
  }

  // Subprotocols / paths permitted; only scheme + host + port matter for
  // safety. Reject userinfo (token-in-URL leakage path).
  if (parsed.username !== "" || parsed.password !== "") {
    throw new Error(`WsUrlValidator: URL must not contain userinfo (token-in-URL leakage path)`);
  }
  if (parsed.search !== "" || parsed.hash !== "") {
    throw new Error(
      `WsUrlValidator: URL must not contain query string or fragment (token-in-URL leakage path)`,
    );
  }

  if (parsed.protocol !== "wss:") {
    if (mode === "prod") {
      throw new Error(`WsUrlValidator: production scheme must be wss:, got ${parsed.protocol}`);
    }
    if (parsed.protocol !== "ws:") {
      throw new Error(`WsUrlValidator: unsupported scheme ${parsed.protocol}`);
    }
  }

  // Per Gemini review (MEDIUM): URL.host already contains "host:port" when the
  // port is non-default, so concatenating parsed.port back onto parsed.host
  // produced strings like "localhost:8443:8443" that broke the loopback regex
  // tests. URL.host is the correct canonical form on its own.
  const hostport = parsed.host;
  if (mode === "prod") {
    for (const pat of LOCAL_HOST_PATTERNS) {
      if (pat.test(parsed.host) || pat.test(hostport)) {
        throw new Error(
          `WsUrlValidator: production refuses loopback / link-local host: ${parsed.host}`,
        );
      }
    }
  }

  // Default ports: wss = 443, ws = 80. URL parser leaves port "" for defaults.
  const portFromString = parsed.port === "" ? -1 : Number.parseInt(parsed.port, 10);
  const port = portFromString > 0 ? portFromString : parsed.protocol === "wss:" ? 443 : 80;

  return Object.freeze({ url: raw, host: parsed.host, port });
}
