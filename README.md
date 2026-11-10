# mailq

An outbound mail transfer agent (MTA) in Go that decides, for every SMTP
reply, whether to retry, slow down, bounce or stop sending to a recipient.

## The problem

Handing a message to an SMTP server is the easy part. The hard part is what
to do with the answer:

- **`550` is ambiguous.** `550 5.1.1` means the mailbox does not exist;
  `550 5.7.1` means the receiver does not like the sender. Treat them the
  same and you either keep mailing dead addresses or blacklist good ones.
  Both hurt sender reputation, and reputation decides whether anything
  reaches the inbox.
- **Providers push back.** Gmail answers `421 4.7.28` when you send too fast.
  Retrying at the same rate makes it worse; the sender has to slow down per
  destination domain, then speed up again.
- **Clients retry too.** A timeout on `POST /send` followed by a retry must
  not send the email twice.
- **Workers crash.** A message picked up by a worker that dies must be
  delivered by another one, not lost.

mailq is my answer to those four problems, small enough to read in one
sitting.

## Evidence

Measured with `make demo` against the Docker Compose stack, where
`cmd/fakesmtp` plays a receiving server that misbehaves on purpose:

| scenario | what the server did | result |
|---|---|---|
| normal recipient | `250` | delivered on the first attempt |
| unknown user | `550 5.1.1` | bounced, recipient added to the suppression list |
| policy block | `550 5.7.1` | bounced, recipient **not** suppressed |
| flaky server | `451`, `451`, `250` | delivered on the third attempt |
| full mailbox | `452 4.2.2` every time | 5 attempts, then bounced when the message expired |
| real inbox (Mailpit) | `250` | arrived with the UTF-8 subject intact |
| domain with no route | none | bounced with "no route", nothing sent |
| same `Idempotency-Key` twice | none | same message ids returned, nothing queued |
| new send to a suppressed recipient | none | status `suppressed`, never attempted |
| burst of 40 to one domain | `421 4.7.28` above 5 msg/s | 7 to 8 rate-limit replies; mailq halved the domain's rate on each and delivered all 40 |

### What throttling costs Postgres

`make burst` sends 40 messages to `fake.test` and counts row updates on the
`messages` table with `pg_stat`. Three runs each, from a fresh restart:

| | row updates | seconds to deliver all 40 |
|---|---|---|
| before: throttled messages went back to Postgres | 1,266 – 1,339 | 12 – 14 |
| after: they wait in a per-domain queue in memory | 92 – 93 | 11 – 14 |

The floor is 80 (one claim and one result per message); the rest are the
retries after `421`. Delivery time did not change because it is set by the
receiving server's limit and the AIMD back-off, not by the queue.

### Tests

23 unit tests across the classifier, backoff, throttle, SMTP client, API and
the in-memory ready queue, plus two integration tests against a real
database: the whole pipeline (Postgres queue, worker, SMTP, outcome), and a
throttled burst that must wait in memory without holding up another domain.
Run them with `make itest`. About 2,000 lines of Go plus 750 lines of tests.

## How it works

```
            POST /send                     claim (SKIP LOCKED + lease)
 client ───────────────► API ──► Postgres ◄──────────────────────────── workers
                                 (queue,                                   │
                                 attempts,     per-domain token bucket ◄───┤
                                 suppressions)    (AIMD on 421)            │
                                      ▲                                    ▼
                                      │   outcome + attempt log      SMTP client
                                      └──────────────────────────────  (MX / routes)
                                                                           │
                                                                classifier ◄┘
                                                 delivered / deferred / bounced / suppressed
```

## Run it

Requires Docker and Go 1.26+.

```bash
make up      # postgres, mailpit, fakesmtp and mailq
make demo    # end-to-end scenarios against the running stack
make itest   # unit + integration tests (uses a separate mailq_test database)
make burst   # 40 messages to one throttled domain; prints the Postgres cost
make down
```

- API: http://localhost:8080
- Metrics: http://localhost:8080/metrics
- Mailpit (inbox for `*@mailpit.test`): http://localhost:8025

Nothing leaves your machine: MX delivery is disabled unless
`MAILQ_ALLOW_MX=true`, and the demo only uses the `fake.test` and
`mailpit.test` routes.

### API

```bash
curl -X POST localhost:8080/send \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-123' \
  -d '{"from":"me@mailq.local","to":["ok@fake.test","bounce@fake.test"],
       "subject":"Hi","text":"Hello"}'
# 202 {"messages":[{"id":"…","to":"ok@fake.test","status":"queued"}, …]}

curl localhost:8080/messages/<id>           # status + every attempt with the raw SMTP reply
curl localhost:8080/suppressions
curl -X DELETE localhost:8080/suppressions/bounce@fake.test
```

### The fake receiving server

`cmd/fakesmtp` misbehaves on purpose, based on the recipient's local part:

| recipient    | reply                         | mailq does                          |
|--------------|-------------------------------|-------------------------------------|
| `bounce*`    | `550 5.1.1 User unknown`      | bounce + suppress recipient         |
| `block*`     | `550 5.7.1 spam policy`       | bounce, **no** suppression          |
| `full*`      | `452 4.2.2 Mailbox full`      | retry until the message expires     |
| `defer*`     | `451 4.3.0`                   | retry until the message expires     |
| `flaky*`     | `451` N times, then `250`     | retry, then deliver                 |
| any, > 5/s   | `421 4.7.28` (Gmail-style)    | halve the domain's rate, retry      |
| anything else| `250`                         | delivered                           |

## Design decisions

**Postgres as the queue.** Workers claim batches with
`UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)` and get a lease
(`locked_until`). Many workers can run against the same database without
handing out the same message twice, and messages held by a crashed worker are
picked up again when the lease expires. A dedicated broker (SQS, Kafka) would
scale further, but one database keeps the queue, the attempt history and the
suppression list consistent with a single transaction per attempt.

**Delivery is at-least-once.** If a worker dies after the remote server
accepted the message but before the outcome is committed, the lease expires
and the message is sent again. Exactly-once is not possible across SMTP;
the `Message-ID` header (stable per message) lets receivers deduplicate.

**Idempotent API.** `Idempotency-Key` + recipient has a unique index;
replaying a request returns the original messages instead of creating new
ones, including under concurrent retries.

**Classification uses the enhanced status code first.** `550` alone can mean
"user unknown" or "you look like spam"; `5.1.1` versus `5.7.1` tells them
apart. That distinction matters: an unknown user is suppressed (sending again
hurts reputation), but a policy block is about the sender/IP/content, so the
recipient is not suppressed. Provider-specific rules (Gmail `4.7.28`,
Outlook `4.7.650`, Yahoo `TSS04`) run before the generic logic. A full
mailbox is treated as a soft bounce even when reported as `5.2.2`.

**Throttling per destination domain, adaptive (AIMD).** Each domain has a
token bucket and a cap on concurrent sessions. A rate-limit reply halves the
domain's rate and drains its burst; every success adds a small step back,
never above the configured ceiling. Throttling never counts as an attempt.

**Throttled messages wait in memory, not in Postgres.** A claimed message the
throttle refuses goes into an in-memory line for its domain and is sent as
soon as the domain allows, oldest first. The lease taken at claim time keeps
other workers away, so this costs no database writes. Once a domain has
enough messages waiting, the worker stops claiming that domain (the claim
query skips it) but keeps claiming others, so one slow provider does not
hold up the rest. A message is handed back to Postgres only if it waited
half its lease, or on shutdown. When a delivery finishes, its domain is
asked again at once instead of waiting out the throttle's estimate.

**Retries with jitter and a maximum age.** Default schedule
1m, 5m, 15m, 30m, 1h, 2h, 4h… with ±20% jitter, so a provider outage does not
end in a synchronized retry storm. A message that would be retried after
24h expires and is bounced with the last reply.

**Graceful shutdown.** On SIGTERM the API stops, workers stop claiming, and
in-flight SMTP sessions finish and are recorded.

**Observability.** Prometheus metrics for attempts by domain/class/reason,
SMTP session latency, queue size by status, messages waiting in memory,
messages handed back to Postgres, throttling events and the current adaptive
rate per domain. `GET /messages/{id}` returns every attempt
with host, code, enhanced code, raw reply and duration, which is the first
thing you need when debugging a delivery problem. Logs are structured JSON.

## Layout

```
cmd/mailq          API + workers
cmd/fakesmtp       misbehaving SMTP server for tests and the demo
internal/api       HTTP handlers and request validation
internal/store     Postgres queue, attempts, suppressions (schema.sql)
internal/worker    claim → wait for throttle (in memory) → deliver → classify → record
internal/smtpclient routing (static routes / MX), SMTP session, message build
internal/classify  SMTP reply → delivered / transient / permanent
internal/throttle  per-domain token bucket with AIMD
internal/backoff   retry schedule, jitter, expiry
internal/metrics   Prometheus collectors
```

## Configuration

| variable                   | default                          |
|----------------------------|----------------------------------|
| `MAILQ_DATABASE_URL`       | `postgres://mailq:mailq@localhost:5432/mailq?sslmode=disable` |
| `MAILQ_HTTP_ADDR`          | `:8080`                          |
| `MAILQ_ROUTES`             | `mailpit.test=localhost:1025,fake.test=localhost:2525` |
| `MAILQ_ALLOW_MX`           | `false`                          |
| `MAILQ_HELO`               | `mailq.local`                    |
| `MAILQ_CONCURRENCY`        | `16` deliveries at once          |
| `MAILQ_SMTP_TIMEOUT`       | `30s`                            |
| `MAILQ_RETRY_SCHEDULE`     | `1m,5m,15m,30m,1h,2h,4h`         |
| `MAILQ_MAX_AGE`            | `24h`                            |
| `MAILQ_DOMAIN_RATE`        | `5` msg/s per domain (ceiling)   |
| `MAILQ_DOMAIN_BURST`       | `5`                              |
| `MAILQ_DOMAIN_CONCURRENCY` | `4` sessions per domain          |
| `MAILQ_DOMAIN_RATES`       | per-domain ceilings, e.g. `gmail.com=20,yahoo.com=5` |
| `MAILQ_TLS_INSECURE`       | `false` (skip cert checks on STARTTLS, testing only) |

## What it does not do

Things a real MTA needs that are deliberately out of scope here:

- **Authentication:** DKIM signing, SPF/DMARC alignment, and reverse DNS (PTR)
  for the sending IPs. Without them major providers will reject or junk mail.
- **IP pools and warmup:** a single source IP; no dedicated pools,
  reputation-aware routing or warmup ramps.
- **Asynchronous bounces and feedback loops:** only bounces returned during
  the SMTP session are handled, not DSNs that arrive later by email or
  complaint (FBL) reports.
- **Connection reuse:** one SMTP session per message; real MTAs pipeline many
  messages per connection.
- **MX failover on 4xx:** another MX is tried only when a host is unreachable.
- **Bounded metric cardinality:** the `domain` label should be limited to
  top providers in production.
- **Webhooks** for delivery events and **multi-tenant** limits.

## Next steps

In the order I would do them, each with a way to tell it worked:

Done: ~~per-domain in-memory ready queue~~ (row updates for a throttled burst
went from ~1,300 to ~92, see [Evidence](#what-throttling-costs-postgres)).

1. **Connection reuse.** Send several messages per SMTP session. Done when
   the burst opens a handful of connections instead of 40.
2. **DKIM signing.** Done when Mailpit (or a real inbox) shows `dkim=pass`.
3. **Asynchronous bounces.** Parse DSNs that arrive later by email and feed
   them into the same classifier and suppression list.

## Author

Built by Mario Candido ([github.com/mariocandido](https://github.com/mariocandido)).
This is a learning and portfolio project, not a production MTA. Issues and
questions are welcome.
