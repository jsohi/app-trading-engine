# monitoring/ — Grafana dashboards + Prometheus alerts

Production observability artefacts for the trading engine. Lives as a Gradle
subproject so the dashboards and alert rules are JSON-schema-validated at
build time and so a hand-edit can never silently break a dashboard import.

APP-244 Phase 3 Commit C.7.

## Layout

```
monitoring/
├── build.gradle.kts                       # registers :monitoring:validateGrafana
├── alerts.yaml                            # Prometheus alerting rules (5 alerts)
├── dashboards/
│   ├── websocket-server.json              # WS sessions, egress, drain latency, JWKS, auth, log appender
│   ├── pricing-service.json               # tick rate, rejects, publish-latency p99, feed-state, snapshot dedup
│   └── cluster.json                       # consensus lag, commit latency, snapshot duration, orders/sec, RFQ in-flight
└── src/main/resources/schema/
    ├── grafana-dashboard-v11.schema.json  # project-pinned subset of the Grafana 11 model
    └── prometheus-alerts.schema.json      # matches promtool's rule-file shape
```

## Validation

`./gradlew :monitoring:validateGrafana` is wired into `check`, so any push that
breaks the dashboard or alert rule shape fails CI. The task:

1. Loads each `*.json` file under `dashboards/`, parses it, and validates against
   `grafana-dashboard-v11.schema.json` (JSON Schema draft 2020-12 via
   `com.networknt:json-schema-validator`).
2. Cross-checks dashboard `uid` uniqueness — Grafana imports by uid, so a
   duplicate would silently overwrite an unrelated dashboard.
3. Loads `alerts.yaml`, converts to a JSON tree, and validates against
   `prometheus-alerts.schema.json`.

The schemas are project-pinned subsets — they cover the load-bearing fields the
import path actually reads. Bumping Grafana major versions is the only event
that requires touching them; adding a new dashboard or panel does not.

## Importing the dashboards

### Manual import (Grafana UI)

1. Open Grafana → **Dashboards** → **New** → **Import**.
2. Click **Upload JSON file** and choose one of the files under `dashboards/`.
3. Select the Prometheus datasource on the prompt and click **Import**.

Each dashboard already declares a `${datasource}` template variable so the UI
will offer the correct picker on import.

### Provisioned (file-based)

Drop the JSON files under your Grafana provisioning path (e.g.
`/etc/grafana/provisioning/dashboards/trading-engine/`) and reference them from
a provisioning YAML:

```yaml
apiVersion: 1
providers:
    - name: trading-engine
      folder: Trading Engine
      type: file
      options:
          path: /etc/grafana/provisioning/dashboards/trading-engine
```

Grafana will pick up edits within `updateIntervalSeconds`.

## Importing the alert rules

Point the Prometheus server at `alerts.yaml` via the `rule_files:` block in
`prometheus.yml`:

```yaml
rule_files:
    - /etc/prometheus/trading-engine-alerts.yaml
```

Validate locally before deploying with:

```bash
promtool check rules monitoring/alerts.yaml
```

The build-time JSON schema enforces structural validity; `promtool` additionally
parses each PromQL expression — both should pass before a production rollout.

## Adding a new dashboard

1. Drop a new `<name>.json` under `dashboards/`.
2. Pick a unique `uid` (kebab-case, ≤40 chars).
3. Run `./gradlew :monitoring:validateGrafana` — fix any schema errors.
4. Commit and PR. CI re-runs the validator on every push.

## Metric inventory

The dashboards reference the following metric families exported by the
production services. New metrics added to the codebase should land here too.

| Family                                          | Owner            | Source                            |
| ----------------------------------------------- | ---------------- | --------------------------------- |
| `websocket_*`                                   | websocket-server | `WebSocketMetrics`                |
| `jwks_refresh_failure_total`                    | websocket-server | `JwtValidator` (C.2)              |
| `log_appender_failure_total`                    | infra            | `Log4j2DiskFullErrorHandler`      |
| `pricing_marketdata_*`                          | pricing-service  | `PricingMetrics`                  |
| `cluster_consensus_position`                    | cluster          | Aeron `ClusterMarkFile` counter   |
| `cluster_commit_latency_seconds`                | cluster          | `TradingClusteredService` timer   |
| `cluster_snapshot_duration_seconds`             | cluster          | snapshot lifecycle timer          |
| `cluster_orders_*`                              | cluster          | order-flow counters               |
| `cluster_rfq_in_flight` / `cluster_rfq_state_*` | cluster          | RFQ state-machine instrumentation |
