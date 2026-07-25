# Rail Connectors

![CI](https://github.com/ai-crypto-onramp/rail-connector/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/ai-crypto-onramp/rail-connector/branch/main/graph/badge.svg)](https://codecov.io/gh/ai-crypto-onramp/rail-connector)

Fiat rail adapters that expose a common interface for card, ACH, SEPA, PIX, and UPI rails.

## Overview / Responsibilities

Rail Connectors are the fiat-side boundary of the on-ramp. They translate the
platform's canonical payment operations into the protocols and message formats of
each underlying rail, and translate the rails' asynchronous settlement and dispute
events back into the platform's event stream.

Responsibilities:

- **Authorize / Capture / Refund** against the active rail family on behalf of
  Payment Orchestration.
- **Status polling and reconciliation** of pending rail transactions.
- **Inbound webhook ingestion** for rail-side state transitions (settlements,
  chargebacks, returns, ACH returns, SEPA R-transactions).
- **Settlement file ingestion** (NACHA, ISO20022, Banco Central SPI reports, NPCI
  settlement files) reconciled against expected captures.
- **Per-rail SLA tracking** — measure auth-to-capture latency, settlement latency,
  and chargeback rates per rail and surface SLO breaches.
- **Error normalization** — map rail-specific decline codes and failure reasons onto
  a common error taxonomy consumed by Payment Orchestration and the Policy Engine.
- **Emit async events** — settlements and chargebacks are published to
  Reconciliation and the Audit / Event Log.

## Language & Tech Stack

- **Language:** Go (1.22+).
- **Deployment:** One deployable binary per rail family, all behind a common
  `RailConnector` interface. The rail family is selected at startup via
  `RAIL_FAMILY`; the binary loads only that rail's adapter implementation.
- **Internal transport:** gRPC for synchronous calls from Payment Orchestration.
- **Inbound transport:** HTTP REST receivers for rail-side webhooks, one path per
  rail (e.g. `/webhooks/card`, `/webhooks/ach`, `/webhooks/sepa`, `/webhooks/pix`,
  `/webhooks/upi`).
- **Storage:** PostgreSQL for request/settlement/chargeback records.
- **Secrets:** per-rail credentials via the platform secret manager; no rail
  credential is shared across families.

## System Requirements

Each deployable binary MUST implement the common `RailConnector` interface and
provide a rail-specific adapter for the configured `RAIL_FAMILY`.

### Common interface

Every rail adapter implements the same six operations:

| Operation   | Purpose                                                         |
|-------------|-----------------------------------------------------------------|
| Authorize   | Reserve / authorize funds on the rail (card auth, ACH pre-note, SEPA init). |
| Capture     | Settle the authorized amount to the platform's rail account.    |
| Refund      | Return captured funds to the end user.                          |
| Status      | Query the current state of a rail request.                     |
| Settle      | Reconcile a settled amount against a settlement file / report.  |
| Chargeback  | Record a rail-initiated dispute / chargeback and emit it downstream. |

### Per-rail adapter implementations

| Rail family | Adapter mechanism                                                        |
|-------------|--------------------------------------------------------------------------|
| Card        | Delegated to a card processor (Stripe, Adyen) via processor API + webhook. |
| ACH         | NACHA file generation / bank API submission; bank-side returns via webhook. |
| SEPA        | ISO20022 `pain.001` messages to the SEPA Instant gateway; `pain.002`/`camt.054` status and settlement. |
| PIX         | Banco Central do Brasil SPI (Sistema de Pagamentos Instantâneos) API.    |
| UPI         | NPCI UPI Collect / Intent APIs; settlement via NPCI settlement files.     |

### Functional requirements

- **Idempotency:** every outbound call carries a stable `IdempotencyKey` derived
  from the platform transaction id and operation; replays are deduped at the rail
  or at this service's request log.
- **Signature-verified inbound webhooks:** each rail webhook receiver verifies the
  rail's HMAC / signature header against the configured shared secret before
  processing. Unverified payloads are rejected with `401`.
- **Settlement reporting files:** daily ingestion of CSV (NACHA summary), ISO20022
  (`camt.053` / `camt.054`), Banco Central SPI reports, and NPCI settlement files.
  Each settled entry is matched to a `rail_requests` row and emitted as a
  settlement event.
- **Per-rail SLA tracking:** the service records auth latency, capture-to-settle
  latency, and chargeback rate per rail and publishes SLO metrics.

## Non-Functional Requirements

- **Per-rail circuit breaker:** each adapter wraps outbound calls in a circuit
  breaker scoped to the rail + endpoint pair; a blown circuit returns a normalized
  `RAIL_UNAVAILABLE` error to Payment Orchestration so the orchestrator can choose
  a fallback rail or fail the order.
- **Retry with jitter:** transient failures (5xx, timeouts) are retried with
  exponential backoff + jitter up to a per-rail max-attempts cap; retries are
  idempotent via the outbound idempotency key.
- **Outbound idempotency keys:** every call to the rail carries
  `Idempotency-Key: <tx_id>:<operation>:<attempt>`.
- **Transport security:** TLS everywhere; mTLS to rail partners that require it
  (SEPA gateway, NPCI, Banco Central SPI). Card processor calls use the processor's
  documented mTLS / token scheme.
- **Secrets per rail:** each rail family's credentials live under a distinct secret
  path (`rail.card.*`, `rail.ach.*`, ...) and are loaded only when that family is
  active. No cross-rail secret access.
- **Observability:** structured logs with `tx_id`, `rail`, `rail_request_id`;
  Prometheus metrics `rail_authorize_latency`, `rail_capture_latency`,
  `rail_settle_latency`, `rail_chargeback_rate`, `rail_circuit_open`.

## Technical Specifications

### Common interface (Go pseudocode)

```go
type RailContext struct {
    TxID            string
    RailRequestID   string
    Amount          decimal.Decimal
    Currency        string
    IdempotencyKey  string
    CustomerRef     string
    RailSpecific   map[string]string
}

type RailResponse struct {
    Status        RailStatus
    RailRef       string
    SettledAmount *decimal.Decimal
    ErrorCode     string
    ErrorReason   string
    RawPayload    []byte
}

type RailStatus int

const (
    RailStatusUnknown RailStatus = iota
    RailStatusAuthorized
    RailStatusCaptured
    RailStatusSettled
    RailStatusRefunded
    RailStatusFailed
    RailStatusChargeback
)

type RailConnector interface {
    Authorize(ctx context.Context, in RailContext) (RailResponse, error)
    Capture(ctx context.Context, in RailContext) (RailResponse, error)
    Refund(ctx context.Context, in RailContext) (RailResponse, error)
    GetStatus(ctx context.Context, in RailContext) (RailResponse, error)
    Settle(ctx context.Context, in RailContext) (RailResponse, error)
    Chargeback(ctx context.Context, in RailContext) (RailResponse, error)
}
```

### Per-rail configuration

| Rail family | Configurable provider | Notes                                                   |
|-------------|------------------------|---------------------------------------------------------|
| Card        | Stripe \| Adyen        | Selected by `RAIL_CARD_PROCESSOR`. Token-based auth.    |
| ACH         | Bank partner API      | NACHA file generation; same partner accepts bank API.   |
| SEPA        | SEPA Instant gateway   | ISO20022 `pain.001` / `pain.002` / `camt.053/054`.      |
| PIX         | Banco Central SPI     | Direct SPI integration; DICT key resolution.           |
| UPI         | NPCI UPI              | Collect / Intent flows; NPCI settlement file ingest.    |

### Endpoints

**gRPC service `railconnectors.v1.RailService`:**

| Method        | Request                   | Response         |
|---------------|---------------------------|------------------|
| `Authorize`   | `AuthorizeRequest`        | `RailResponse`   |
| `Capture`     | `CaptureRequest`          | `RailResponse`   |
| `Refund`      | `RefundRequest`           | `RailResponse`   |
| `GetStatus`   | `GetStatusRequest`        | `RailResponse`   |

**REST webhook receivers (per rail):**

| Path                  | Rail     | Inbound event                          |
|-----------------------|----------|----------------------------------------|
| `POST /webhooks/card` | Card     | Authorization, capture, chargeback, refund notifications. |
| `POST /webhooks/ach`  | ACH      | Returns (RCK, NSF), settlement files. |
| `POST /webhooks/sepa` | SEPA     | `pain.002` status, `camt.054` debit.   |
| `POST /webhooks/pix`  | PIX      | Payment status, refund.               |
| `POST /webhooks/upi`  | UPI      | Collect status, refund, chargeback.   |

### Data model

PostgreSQL tables (one schema per rail family deployable):

- **`rail_requests`** — one row per outbound call to the rail.
  Columns: `tx_id`, `rail`, `rail_request_id`, `operation`, `amount`,
  `currency`, `status`, `idempotency_key`, `rail_ref`, `error_code`,
  `created_at`, `updated_at`.
- **`rail_settlements`** — settled amounts matched from settlement files.
  Columns: `settlement_id`, `rail`, `rail_request_id`, `settled_amount`,
  `currency`, `settled_at`, `source_file_ref`, `raw_payload`.
- **`rail_chargebacks`** — disputes received from the rail.
  Columns: `chargeback_id`, `rail`, `rail_request_id`, `amount`, `reason_code`,
  `received_at`, `status`, `raw_payload`.

### Integrations

- **Inbound (sync):** called by `payment-orchestration` over gRPC for
  `Authorize`, `Capture`, `Refund`, `GetStatus`.
- **Outbound (async):** emits `rail.settlement.completed` and
  `rail.chargeback.received` events to **Reconciliation** and the
  **audit-event-log** service on the event bus.
- **Settlement files:** pulled from the rail partner (SFTP / API) on a schedule and
  matched against `rail_requests`.

### Per-rail error code normalization

Each rail maps its native decline / failure reasons onto the common taxonomy:

| Normalized code       | Meaning                                           |
|-----------------------|---------------------------------------------------|
| `INSUFFICIENT_FUNDS`  | Card NSF, ACH NSF, PIX no-balance.                |
| `DO_NOT_HONOR`         | Generic rail decline with no further detail.      |
| `FRAUD_DECLINE`        | Rail-side risk rejection.                         |
| `EXPIRED_INSTRUMENT`  | Expired card, closed bank account.                |
| `INVALID_REQUEST`     | Malformed message to the rail.                    |
| `RAIL_UNAVAILABLE`    | Circuit breaker open or rail returned 5xx.        |
| `SETTLEMENT_BREAK`    | Settlement file row with no matching `rail_requests` row. |

Normalization maps are per-rail and versioned alongside the adapter implementation.

## Dependencies

- **Per-rail credentials** — API keys, mTLS certs, webhook secrets, sourced from
  the platform secret manager.
- **PostgreSQL** — `rail_requests`, `rail_settlements`, `rail_chargebacks`.
- **audit-event-log** — consumes async audit events for every state transition.
- **Reconciliation** — consumes `rail.settlement.completed` and
  `rail.chargeback.received` events.
- **payment-orchestration** — upstream caller; defines the gRPC contract.

## Configuration

The binary is rail-agnostic; behavior is selected by environment variables. Only
the variables for the active `RAIL_FAMILY` are required; others are ignored.

| Env var                | Required when       | Description                                         |
|------------------------|---------------------|----------------------------------------------------|
| `PORT`                 | always              | gRPC + HTTP listen port.                           |
| `RAIL_FAMILY`          | always              | One of `card`, `ach`, `sepa`, `pix`, `upi`.        |
| `RAIL_CARD_PROCESSOR`  | `RAIL_FAMILY=card`  | `stripe` or `adyen`.                               |
| `RAIL_CARD_API_KEY`    | `RAIL_FAMILY=card`  | Processor API key / token.                         |
| `RAIL_ACH_PARTNER_URL` | `RAIL_FAMILY=ach`   | Bank partner submission API base URL.              |
| `RAIL_ACH_SFTP_URL`    | `RAIL_FAMILY=ach`   | SFTP endpoint for NACHA file exchange.             |
| `RAIL_SEPA_API_KEY`    | `RAIL_FAMILY=sepa`  | SEPA Instant gateway API key.                      |
| `RAIL_SEPA_MTLS_CERT`  | `RAIL_FAMILY=sepa`  | mTLS cert path for the SEPA gateway.               |
| `RAIL_PIX_API_KEY`     | `RAIL_FAMILY=pix`   | Banco Central SPI API key.                         |
| `RAIL_UPI_API_KEY`     | `RAIL_FAMILY=upi`   | NPCI UPI API key.                                  |
| `RAIL_WEBHOOK_SECRET`  | always              | HMAC secret for verifying inbound webhooks.       |
| `DB_URL`               | always              | PostgreSQL DSN for the rail schema.                |
| `AUDIT_EVENT_LOG_URL`  | always              | gRPC endpoint of the audit-event-log service.      |
| `RECON_EVENT_BUS`      | always              | Event bus topic for settlement / chargeback events.|
| `LOG_LEVEL`            | optional            | `debug` \| `info` \| `warn` \| `error` (default `info`). |
| `CIRCUIT_MAX_FAILURES` | optional            | Failures before opening a rail circuit (default 5).|
| `RETRY_MAX_ATTEMPTS`   | optional            | Max retry attempts per outbound call (default 4).  |

## Local Development

The repo follows the **one-deployable-per-rail** pattern: a single Go module
builds one binary per rail family by setting `RAIL_FAMILY` (and the matching
credential env vars) at run time.

### Build

```bash
go build -o bin/rail-connector ./cmd/rail-connector
```

### Run (example: card family)

```bash
RAIL_FAMILY=card \
RAIL_CARD_PROCESSOR=stripe \
RAIL_CARD_API_KEY=sk_test_... \
RAIL_WEBHOOK_SECRET=whsec_... \
DB_URL=postgres://localhost/rails?sslmode=disable \
AUDIT_EVENT_LOG_URL=audit-event-log:9090 \
RECON_EVENT_BUS=recon-bus \
PORT=8080 \
./bin/rail-connector
```

### Test

```bash
go test ./...
go test -race -cover ./...
```

### Lint / typecheck

```bash
go vet ./...
gofmt -l .
```

### Generating gRPC stubs

```bash
buf generate
```
