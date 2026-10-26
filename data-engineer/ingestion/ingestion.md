# Data Ingestion Patterns: Data Engineer Interview Guide

Getting the first page of orders from an API is usually straightforward. The harder question is whether you can get every order, keep up with changes, and recover after a failure without losing or duplicating work. That is the real job of ingestion: moving source data into a platform where it can be stored, processed, and analyzed, with a clear account of what arrived and what remains.

Start with the requirements: acceptable freshness, volume and peak rate, source capabilities, update/delete behavior, retention, security, and recovery time. A daily reporting table and a fraud detection stream need different designs. Examples below illustrate patterns; API fields and guarantees depend on the source contract.

## Contents

1. [Batch vs. Streaming vs. CDC](#1-batch-vs-streaming-vs-cdc)
2. [REST and GraphQL APIs](#2-rest-and-graphql-apis)
3. [Rate Limiting, Retries, and Exponential Backoff](#3-rate-limiting-retries-and-exponential-backoff)
4. [Webhooks, Polling, and Event-Driven Ingestion](#4-webhooks-polling-and-event-driven-ingestion)
5. [Full Load, Incremental Extraction, and Watermarks](#5-full-load-incremental-extraction-and-watermarks)
6. [Bulk Ingestion, Parallel Extraction, and Chunking](#6-bulk-ingestion-parallel-extraction-and-chunking)
7. [Deduplication, Idempotency, and Ordering](#7-deduplication-idempotency-and-ordering)
8. [Schema Drift, Error Handling, DLQ, and Replay](#8-schema-drift-error-handling-dlq-and-replay)
9. [End-to-End Design Scenarios](#9-end-to-end-design-scenarios)
10. [Interview Questions and Answer Framework](#10-interview-questions-and-answer-framework)

## 1. Batch vs. Streaming vs. CDC

### Separate the dimensions

A common design question is whether to use batch, streaming, or CDC. Before choosing, separate what each term describes: batch and streaming concern how work is processed, while CDC concerns how source changes are captured. They can therefore appear in the same pipeline:

- **Batch:** process a bounded dataset as a job, such as yesterday's files or rows changed in a fixed time interval.
- **Streaming:** continuously consume an ongoing sequence of records. An individual event, small buffer, or micro-batch can be the unit of work.
- **Change Data Capture (CDC):** detect changes to source data, usually inserts, updates, and deletes. Transaction logs, triggers, and change tables are possible capture mechanisms.

CDC events can be delivered continuously or collected into periodic batches. Streaming can carry application events that never came from a database. Polling an API every minute is usually incremental batch ingestion, even if the business calls it near real time.

| Aspect | Batch | Streaming | Log-based CDC |
| --- | --- | --- | --- |
| Typical source | Files, database queries, API exports | Application events, telemetry, message streams | Database transaction log |
| Freshness | Schedule interval plus job duration | Often seconds or less, depending on buffering and load | Depends on capture and downstream lag |
| Work boundary | File set, partition, extraction interval | Offset range, checkpoint, window | Log position and transaction metadata |
| Main advantage | Simple scheduling and efficient bulk work | Low latency and continuous availability of new data | Capture database changes without repeated full scans |
| Main difficulty | Batch windows, reruns, source scans | Backpressure, ordering, state, continuous operations | Snapshot handoff, log retention, source-specific semantics |

### When to choose each pattern

**Batch** fits periodic reporting, large historical loads, and sources that publish daily exports. It amortizes setup costs and can use bulk destination loaders. Its freshness cannot be better than the combined scheduling delay, source availability delay, and processing time.

**Streaming** fits operational dashboards, alerting, and continuously arriving events. It requires capacity for bursts, a durable buffer, recoverable progress, and a policy for late or malformed events. Low latency is a requirement to justify; continuous infrastructure has operational costs.

**CDC** fits keeping an analytical copy of mutable database tables current. Capture keys, operation types, source positions, and relevant schema information. Log access, retained history, supported database features, and a consistent initial snapshot must be available. CDC reduces repeated scanning but still consumes logging, decoding, storage, and network resources.

Use a hybrid when appropriate: bulk snapshot for history, CDC for ongoing changes, and periodic reconciliation to detect gaps.

### A common ingestion architecture

```text
Database / API / files / events
              |
              v
Extractor / webhook receiver / CDC connector
              |
              v
Durable landing area or retained event log
              |
              v
Validation and normalization ----> Quarantine / DLQ
              |
              v
Idempotent destination writes
              |
              v
Published tables and freshness/completeness checks
```

The durable landing boundary separates source availability from downstream availability. Preserve enough original content and metadata to explain and replay a record, subject to access and retention policies. A raw payload is still sensitive data.

Track extraction and publication separately. A source offset durably landed in raw storage can be safe to acknowledge even while curated processing is behind, provided raw retention and independent downstream checkpoints support recovery.

**Interview trap:** A successful scheduler run proves that the job finished its code path. It does not prove that every expected source record arrived or became queryable.

## 2. REST and GraphQL APIs

### REST extraction

An endpoint such as `GET /orders` makes extraction look like a sequence of HTTP requests. Reliable extraction starts earlier, with the API's contract: which orders can you see, how can they be filtered and sorted, and what happens while they change? REST APIs commonly offer pagination and field selection, but those features alone do not establish completeness. Before designing the connector, answer these questions:

- Which records the authenticated account can see, including tenant and region boundaries.
- Whether filtering by creation time, modification time, or a durable change token is supported.
- The ordering, pagination termination rule, and consistency behavior during concurrent writes.
- How deletions, archived records, and historical retention are represented.
- Response size, request quotas, versioning, and available bulk exports.

Illustrative request:

```http
GET /orders?updated_from=2026-10-01T00%3A00%3A00Z&limit=500&cursor=opaque-token HTTP/1.1
Host: api.example.com
Authorization: Bearer <access-token>
Accept: application/json
```

Parameter names are source-specific. A filter named `updated_from` does not establish whether its boundary is inclusive, whether it reflects commit time, or whether results form a snapshot.

Validate status, content type, response shape, and required fields. A response can be syntactically valid JSON while containing an application error or an unexpected envelope. Bound payload size, connection time, and response-read time.

### GraphQL extraction

GraphQL lets clients select fields and traverse relationships. This can reduce unnecessary fields but does not remove pagination, quotas, or server-side execution costs. Deeply nested queries can be expensive even when the response is small.

Many APIs use a connection model with edges, cursors, and `pageInfo`. This is a common convention rather than mandatory behavior for every GraphQL schema. Treat server-issued cursors as opaque. [GraphQL pagination](https://graphql.org/learn/pagination/).

Illustrative schema-dependent query:

```graphql
query Orders($after: String) {
  orders(first: 100, after: $after) {
    edges {
      cursor
      node {
        id
        updatedAt
        status
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

Pass `endCursor` into the next request while `hasNextPage` is true. Detect a repeated cursor or a missing continuation cursor when more pages are reported; otherwise a connector can loop forever or silently stop.

GraphQL can return partial `data` alongside `errors`. Check both; HTTP success alone is insufficient. If required fields or a required connection failed, do not mark that extraction page complete. [GraphQL response format](https://graphql.org/learn/response/).

Nested lists can each have their own pagination. Fetching 100 orders does not prove that all line items for those orders were retrieved. Track each required connection or extract child entities separately.

### Authentication and authorization

| Mechanism | How it works | Ingestion concerns |
| --- | --- | --- |
| API key | A secret identifies a client, commonly through a header | Rotation, scope, account-wide quota, accidental logging |
| OAuth access token | A client presents a token obtained through an authorization flow | Expiry, scopes, audience, grant choice, refresh behavior |
| Basic authentication | Username/password credentials encoded in a header | Requires TLS; encoding is not encryption |
| Mutual TLS or signed requests | Client proves identity using certificates or cryptographic signing | Certificate/key rotation, canonical signing rules, clock requirements |

Use the provider-supported flow. A machine-to-machine client-credentials flow often obtains a new access token when needed; do not assume every OAuth flow issues a refresh token. Coordinate renewal across workers to avoid a refresh storm and persist rotated refresh credentials safely when applicable.

Store credentials in a secret manager or the platform's existing secure configuration mechanism. Use least-privilege read scopes and TLS. Redact authorization headers, tokens, sensitive query parameters, and token-bearing pagination URLs from logs. Bind extraction state to the account/tenant and filters: reusing another tenant's cursor is a correctness and isolation failure.

A `401` can justify one supported token renewal attempt; repeated failures need intervention. A `403` usually needs investigation of permissions, although some providers also use it for throttling. Interpret the documented error body rather than retrying by status alone.

### Pagination strategies

| Strategy | Example | Strength | Failure or limitation |
| --- | --- | --- | --- |
| Offset/page number | `offset=1000&limit=500` | Simple; independent pages on a stable snapshot | Inserts/deletes shift positions; deep offsets may be expensive |
| Keyset/seek | `id > last_id ORDER BY id` | Efficient indexed scans with stable ordering | Requires a suitable ordered key; does not discover updates behind the cursor |
| Opaque cursor | `after=<token>` | Provider owns traversal details | Token may expire or depend on filters/session; no inherent snapshot guarantee |
| Next-page link | Response supplies a continuation URL | Follows the provider's pagination contract | Must validate destination before forwarding credentials |

Follow the documented continuation signal. A short or empty page is not necessarily the last page when a continuation token is present. Do not invent tokens by decoding an opaque cursor.

Persist progress only after that page's records are durable at the chosen checkpoint boundary. If a token expires, restart from a durable extraction boundary using overlap and idempotency, or restart the snapshot if required by the source.

Validate continuation URLs against the trusted scheme, host, and allowed endpoint rules, including redirects, before attaching credentials. A source-provided URL crosses a trust boundary.

**Interview question:** Does cursor pagination prevent missing records? Only if the source's consistency and ordering contract supports that claim. An opaque cursor can still represent an unstable offset.

## 3. Rate Limiting, Retries, and Exponential Backoff

### Rate limits vs. concurrency limits

Adding workers can make an API connector faster until they all compete for the same quota. A **rate limit** bounds work over time, such as 120 requests per minute; a **concurrency limit** bounds requests in flight at once. A source may also restrict bytes, query complexity, daily usage, or individual endpoints. When workers share a credential, they often share its allowance too.

For example, 120 requests/minute with 500 records/request gives an ideal upper bound of 60,000 records/minute. Authentication, empty pages, retries, and smaller responses consume quota and reduce useful throughput. Extra workers cannot exceed the shared allowance.

Use pacing or a token bucket for the relevant quota scope, with bounded concurrency and burst capacity. A per-worker limiter multiplied across ten workers can exceed a global quota by ten times. Reduce issuance when throttling grows; continuously retrying at the same rate prolongs overload.

### Classify failures before retrying

| Failure | Typical response |
| --- | --- |
| Connection reset or temporary timeout | Retry within a budget if the operation can be safely repeated |
| `429 Too Many Requests` | Respect provider delay and quota scope; slow request issuance |
| Transient `502`, `503`, or `504` | Bounded retry with backoff; honor applicable server delay |
| `400` or GraphQL validation failure | Correct the request; repeating it unchanged rarely helps |
| `401` | Renew credentials through the supported flow, then bound further attempts |
| `403` | Inspect permissions and provider-specific throttling indicators |
| `404` | Interpret the contract: deleted object, invalid endpoint, or temporary visibility can need different actions |
| Invalid payload or incompatible schema | Preserve context and quarantine or stop; network retries do not repair it |

`429` indicates rate limiting and may include `Retry-After`. The header can express delay seconds or an HTTP date. Do not treat every rate-limit header name or reset format as universal. [RFC 6585, section 4](https://www.rfc-editor.org/rfc/rfc6585.html#section-4), [RFC 9110, Retry-After](https://www.rfc-editor.org/rfc/rfc9110.html#section-10.2.3).

### Exponential backoff with jitter

For retry number `n`, starting at zero:

```text
backoff_cap(n) = min(max_delay, base_delay * 2^n)
jittered_delay = random value from 0 to backoff_cap(n)
actual_delay = max(jittered_delay, valid_server_minimum_delay)
```

With a 1-second base and a 30-second cap, the random delay ranges are 0–1, 0–2, 0–4, 0–8 seconds, and eventually 0–30 seconds. Jitter spreads retries from clients that failed together. It reduces synchronization; it does not replace quota enforcement.

Validate server delay values. If a valid required wait exceeds the remaining job deadline, defer the work or fail it for later recovery; do not shorten that wait and immediately retry. For an HTTP date, account for clock accuracy.

Bound attempts, total elapsed time, per-request timeouts, and concurrent retries. Check whether the HTTP client, connector, and scheduler already retry: nested retry layers multiply attempts. A circuit breaker can pause traffic to a persistently failing dependency, but recoverable progress and retries are still required.

### A timeout leaves the outcome uncertain

A server may finish an operation even when its response never reaches the client. Repeating a read is generally safe in terms of side effects, but its result may change. Repeating a state-changing request can create duplicate work.

Use a provider-supported idempotency key when creating export jobs or initiating other operations. Reuse the same key for the same logical attempt; generating a new key on every retry defeats deduplication. Scope, lifetime, and conflict behavior come from the provider contract.

HTTP defines idempotency in terms of the intended server effect of repeated requests, not identical response bodies. Automatically retrying a non-idempotent request requires additional knowledge that repetition is safe. [RFC 9110, idempotent methods](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2).

**Interview trap:** Infinite retries turn a permanent failure into an invisible backlog. Failed work needs a visible state, an owner, and a recovery path.

## 4. Webhooks, Polling, and Event-Driven Ingestion

### Compare the triggering mechanisms

How does the pipeline learn that a new order exists? It can ask periodically, receive a notification, or read from a retained event log. These choices change detection delay and recovery options, so compare the triggering mechanism separately from the guarantees of the data transfer.

| Pattern | Initiator | Good fit | Main concern |
| --- | --- | --- | --- |
| Polling | Consumer periodically asks for data | Sources with query APIs or change feeds | Empty calls, quota, missed changes, scheduling delay |
| Webhook | Source sends an HTTP notification | Low-latency SaaS notifications | Authentication, duplicates, limited retries, public receiver availability |
| Broker/event log | Producers publish; consumers subscribe or fetch | Multiple consumers, buffering, retained history | Retention, partition ordering, offsets, consumer lag |

Event-driven describes reacting to events. It does not establish delivery guarantees, persistence, or replay. A webhook is an event delivery mechanism, but it is not automatically a retained event log.

### Polling

Poll a bounded change interval or follow a durable change token. Schedule with jitter so many connectors do not hit the source at the same instant. Avoid overlapping runs that race to update the same checkpoint; use a single owner or a coordinated lease.

For a polling interval `T`, average scheduling wait is approximately `T/2` only when changes arrive uniformly and polls start on schedule. Add source visibility delay and processing duration to estimate freshness.

Conditional requests using `ETag`/`If-None-Match` or modification validators can reduce response transfer when supported. A `304` can avoid downloading unchanged content, but may still consume a request quota. Resource caching does not replace a reliable collection change feed.

Polling current state can miss intermediate transitions. An order changing `created → paid → refunded` between polls may appear only as `refunded`. That can be acceptable for a current-state table and unacceptable for an audit history.

### Webhook receiver design

```text
Receive request
    -> authenticate and validate signature/replay constraints
    -> enforce payload limits and basic envelope validity
    -> durably enqueue or store accepted event
    -> acknowledge promptly

Worker
    -> validate business payload
    -> deduplicate and apply with ordering rules
    -> retry or quarantine
```

Verify signatures using the provider's exact signed representation, timestamp rules, and key rotation procedure. For example, Stripe requires the unmodified raw body for verification and does not guarantee event delivery order. [Stripe webhook documentation](https://docs.stripe.com/webhooks).

Acknowledge after durable acceptance. Returning success before storing the event creates a loss window if the process crashes. Waiting for all downstream transformations can instead cause sender timeouts and unnecessary redelivery. If durable acceptance fails, return the provider-appropriate failure response so it can retry.

Deduplicate using provider event identity within its documented scope. Signature timestamp checks reduce replay attacks but do not replace event deduplication; legitimate retries can arrive later with valid signatures.

Some webhooks contain only an object ID or an incomplete payload. Fetching the current object may be necessary, but adds API quota and can return a later state than the triggering event. It cannot reconstruct intermediate history that the source no longer exposes.

### Event-driven ingestion and the outbox pattern

An application that separately commits a database update and publishes an event has a dual-write failure window: one can succeed while the other fails.

The **transactional outbox** writes the business change and an outbox record in the same database transaction. A relay or CDC process publishes committed outbox records. Publication and acknowledgment can still fail independently, so relay delivery can contain duplicates and consumers still need idempotency.

For retained brokers, acknowledge or commit offsets only after the chosen durable processing boundary. Track partition-specific progress and provision retention for outages plus recovery time. A queue or stream can redeliver, expire, or discard messages according to its configured semantics; verify those semantics.

### Combine push and pull

Use webhooks for fast notification and scheduled incremental polling for reconciliation when the source supports both. The two paths can report the same change, so share identity and version rules.

Reconciliation can repair current-state gaps only within the source's available history. It cannot recover an unseen hard delete or an intermediate transition unless the source exposes that information.

**Interview question:** Are webhooks more reliable than polling? Neither mechanism alone proves completeness. Compare persistence, retry duration, recoverable history, and reconciliation capabilities.

## 5. Full Load, Incremental Extraction, and Watermarks

### Full load

When you first connect a source, you usually need its existing records before you can follow later changes. A **full load** extracts everything within a defined scope. It is also useful for small reference tables, rebuilding a destination, or sources without a reliable incremental interface. The key is to build the new version safely rather than destroy the only usable one before extraction succeeds.

Prefer staging and validated publication over deleting the live table first:

1. Define source scope and snapshot consistency.
2. Extract into an isolated run or staging destination.
3. Validate expected chunks, keys, counts, schema, and representative values.
4. Publish through a supported atomic table swap, transactional replacement, or committed manifest.
5. Retain the prior published version as required for rollback, then clean up safely.

Atomic publication prevents partial visibility; it does not make an inconsistent source scan consistent. Parallel readers need a shared snapshot or a source-supported equivalent if they must represent one point in time.

Do not infer deletes from absence unless the snapshot is complete for exactly the intended scope. A failed page, reduced account permission, or changed filter can otherwise cause mass deletion.

### Incremental extraction

Incremental extraction transfers records changed since a saved boundary. Common boundaries have different guarantees:

| Boundary | Captures | Assumptions and limits |
| --- | --- | --- |
| Increasing row ID | New rows | IDs must become visible monotonically; updates/deletes are not captured |
| Modification timestamp | Inserts and updates | Every relevant update changes it; late visibility and timestamp precision need handling |
| Provider change token | Changes exposed by a change feed | Token validity, retention, scope, and delete semantics are source-specific |
| Transaction-log position | Supported database changes | Log retention, transaction handling, connector semantics, and durable checkpoints |
| File/object manifest | New or changed published objects | Stable identity/version, completion protocol, and discovery guarantees |

Auto-increment allocation is not commit order. A transaction with ID 100 can commit after ID 101 has already been ingested; blindly resuming at `id > 101` misses it.

### Extraction watermark vs. event-time watermark

An **extraction watermark** records source progress, such as the end of a completed timestamp interval or a log position.

An **event-time watermark** in stream processing estimates event-time progress and supports decisions about closing windows or handling late events. It is not a promise that no older event can ever arrive. Allowed lateness and correction policies determine how late data is handled.

Neither should be confused with the ingestion timestamp, which records when the platform received data. Advancing an extraction timestamp to the connector's current wall clock is unsafe without a source contract that makes that boundary complete.

### Bounded windows and composite pagination

Consider a source table with a maintained `updated_at`, a unique `id`, and indexed ordering on `(updated_at, id)`. For one run, fix an extraction interval `[L, H)` and paginate within it:

```sql
SELECT id, updated_at, status
FROM orders
WHERE updated_at >= :window_start
  AND updated_at < :window_end
  AND (
    updated_at > :last_timestamp
    OR (updated_at = :last_timestamp AND id > :last_id)
  )
ORDER BY updated_at, id
LIMIT :page_size;
```

On the first page, omit the last-position predicate. On later pages, use the last emitted row's tuple. If 5,000 records share one timestamp and a page holds 1,000, resuming with only `updated_at > last_timestamp` would skip 4,000 rows.

The tie-breaker solves pagination across equal timestamps. It does **not** solve late commits, rows whose sort key changes during traversal, backdated updates, or changes that reuse the same timestamp and key. A consistent snapshot, a reliable change feed, or bounded overlap plus reconciliation is still needed according to the required guarantee.

### Overlap, safety lag, and checkpointing

For timestamp extraction, a practical run may use:

```text
previous completed interval end = W
window start L = W - overlap
window end H = source reference time - safety lag
extract all pages for [L, H)
make accepted records durable
save completed interval end H
```

Keep `H` fixed across pages and retries. Use a source-derived boundary where possible, preserve source timestamp precision, and define timezone handling. If no valid interval exists yet, wait rather than move progress backward.

Overlap deliberately rereads recent records. Idempotent writes absorb duplicates. Safety lag gives recently committed or eventually visible changes time to appear. Choose both from source behavior and observed delay; neither proves completeness for arbitrarily late or backdated changes.

Save the completed interval only after every required page or chunk is durably accepted. If an interval is empty, advancing to its fixed end can be valid only if the source contract supports complete enumeration of that interval. A transiently empty page is not proof of completeness.

Crash cases explain the ordering:

| Crash point | Recovery |
| --- | --- |
| Before records become durable | Re-extract from the previous durable checkpoint |
| After records are durable, before checkpoint update | Re-extract; idempotency prevents duplicate effects |
| After checkpoint update but before records are durable | Records can be skipped permanently; avoid this sequence |

When data and progress share a transaction, commit them together. Otherwise favor replayable at-least-once delivery: durable writes first, checkpoint second. Use one owner or fenced checkpoint updates so an old worker cannot overwrite newer progress.

### Deletes and snapshot-to-CDC handoff

Timestamp queries against existing rows do not reveal hard deletes. Capture soft-delete markers, a delete feed, tombstones, an audit/change table, or log-based CDC. Periodic full reconciliation is another option when a complete comparable snapshot is available.

For full load followed by CDC, coordinate the snapshot with a recoverable log boundary. Capture or retain changes covering the snapshot interval, complete the snapshot, and reconcile buffered changes according to source/connector semantics. An arbitrary log offset recorded before an unrelated scan does not establish a consistent handoff.

Keep capture positions and source versions so snapshot rows cannot overwrite newer CDC state. If required logs expire before ingestion catches up, stop claiming continuity and recover through a supported resnapshot or other authoritative history.

**Interview question:** Why not use `MAX(updated_at)` from the target? It may reflect one fast partition, transformed data, or partial publication. Store explicit source progress at the correct dataset or partition scope.

## 6. Bulk Ingestion, Parallel Extraction, and Chunking

### Bulk ingestion

If a source holds millions of historical orders, fetching them one at a time wastes requests and transaction overhead. **Bulk interfaces** move many records per operation, often through exported files. They usually make large transfers more efficient, but the unit of recovery becomes the export job or chunk, so you still need to track completion.

For a source bulk-export API, submit a job, persist its identity, poll to a terminal state, enumerate the complete result set, download bounded chunks, and validate completion. Job creation retries need the same uncertain-outcome handling as other state-changing requests. Export links can expire, and an asynchronous job does not necessarily provide snapshot consistency.

For a destination bulk loader, stage supported files and use the existing loader's transaction or manifest mechanism. Check file identity, expected size or supplied checksum, schema, row counts, and load results. A transport checksum proves byte integrity, not business completeness. Do not assume a filename or object ETag universally identifies the same logical content.

Stream large responses or process bounded files instead of loading an entire export into memory. Choose format and compression according to source/destination support and measured bottlenecks.

### Chunking strategies

| Strategy | Example | Watch for |
| --- | --- | --- |
| Key ranges | `id >= 100000 AND id < 200000` | Gaps/skew; numeric width does not predict row count |
| Time ranges | `[2026-10-01, 2026-10-02)` | Hot periods, backdated changes, timezone boundaries |
| Hash partitions | Stable hash of key modulo worker count | Full scans if not source-optimized; stable partition definition |
| Existing source partitions | Tenant, shard, table partition | Uneven sizes, per-tenant quotas, independent progress |
| Files or row groups | Manifest-listed files or supported splits | Incomplete publication; compression may prevent arbitrary splitting |

Use half-open ranges such as `[start, end)` to avoid boundary ambiguity. Chunk definitions must cover the intended source scope without gaps; intentional overlap requires deduplication. Keep the plan fixed for a run so retries reproduce the same scope.

A single opaque cursor chain is generally sequential. Parallelize only across independently enumerable source partitions or provider-supported ranges. Guessing future cursor values or concurrently changing shared cursor state can lose records.

### Parallel extraction and progress

Maintain per-chunk or per-partition state: run identity, scope, fixed bounds, status, last durable position, row/byte counts, and errors. Retry failed chunks rather than restarting a completed terabyte export.

The largest finished chunk does not determine safe global progress. If chunks 1, 2, and 4 finish while chunk 3 fails, the full run is incomplete. Publish only when required chunks are accounted for, or expose partition-level completeness explicitly.

Parallel database extraction can create too many connections, overload indexes or replicas, or retain long-lived snapshots. Use bounded workers, source-approved limits, and compatible snapshot coordination. Reading a lagging replica also changes freshness and boundary assumptions.

### Throughput, skew, and backpressure

End-to-end throughput is bounded by the slowest stage: source reads, API quota, network, decoding, durable landing, or destination writes.

```text
minimum transfer time >= total bytes / sustainable end-to-end bytes per second
```

For example, transferring 1 TB at a sustained 100 MB/s takes at least 10,000 seconds, about 2.8 hours, using decimal units. Parsing, retries, source reads, and destination work can make it longer.

Large chunks reduce setup overhead but increase retry cost and tail latency. Tiny chunks improve balancing but add requests, metadata, small files, and scheduling overhead. Size chunks from measured bytes and duration rather than row count alone.

If one key range contains most rows, split it using observed boundaries or use smaller work units. Avoid changing the partition function mid-run without preserving ownership and coverage.

Backpressure means slowing upstream issuance when downstream capacity is exhausted. Use bounded queues and limit in-flight pages or chunks. Adding unbounded memory buffers delays failure and makes recovery harder. If the source cannot slow down, durable buffering and sufficient retention are required.

**Interview question:** Why did doubling workers reduce throughput? Possible causes include shared throttling, database contention, connection limits, network saturation, and slower destination commits. Measure the bottleneck before increasing concurrency.

## 7. Deduplication, Idempotency, and Ordering

### Delivery guarantees have a boundary

A worker writes an order, then crashes before recording progress. On restart, should it skip the order or try again? The answer depends on where acknowledgment happens and whether repeating the write is safe. Delivery guarantees become useful only when you name the boundary they cover.

| Guarantee | Meaning | Tradeoff |
| --- | --- | --- |
| At-most-once | A record is processed zero or one times | Failures can lose records |
| At-least-once | Recoverable delivery retries until acknowledgment under stated availability/retention assumptions | Duplicate deliveries are possible |
| Exactly-once effects | Each logical change affects a defined destination once despite retries | Requires coordinated state/output or an equivalent idempotent protocol |

A broker's guarantee does not automatically include an external database write, email, or HTTP call. State the scope, failure assumptions, and retention. Idempotent application of at-least-once deliveries can produce exactly-once effects at a particular sink, while events still travel multiple times.

### Deduplication vs. idempotency

**Deduplication** recognizes repeated logical records and removes or ignores them. **Idempotency** means repeating an operation has the same intended final effect as applying it once.

| Workload | Suitable identity | Common mistake |
| --- | --- | --- |
| Append-only application events | Producer event ID, scoped by source/tenant as required | Deduplicating by customer ID removes valid separate events |
| CDC events | Stable source change identity, including row/event position where necessary | Using only transaction ID when one transaction changes many rows |
| Current-state entity table | Entity key plus source version for apply decisions | Keeping the first row for each key forever |
| Delivered files | Source object version or authoritative manifest identity | Assuming the same filename always has identical content |

An entity ID identifies the object; an event ID identifies one change to it. Several valid events can share the same entity ID. A payload hash can be useful when source semantics justify it, but identical payloads can represent distinct events and equivalent JSON can serialize differently.

For an append-only sink, atomically enforce unique event identity or use a transactional inbox. A separate “check then insert” is vulnerable to concurrent workers both observing absence. For current state, atomically compare versions and update the entity.

### Upsert alone does not solve ordering

Suppose updates for order 42 arrive as:

```text
version 11: status = paid
version 10: status = created
```

A plain upsert applies version 10 last and makes the table stale. Use a source-defined comparable version per entity and apply only newer state. Treat equal versions as duplicates only if the source guarantees they identify the same logical state; conflicting equal-version payloads need investigation.

Illustrative apply rule:

```text
if incoming version is newer than stored version:
    atomically apply incoming state and retain its version
else:
    ignore a known duplicate or older state
```

Application timestamps can tie or skew and are not automatically reliable versions. Broker offsets order one partition; they do not establish a global order or correct source order when producers publish out of order.

For delta events such as `balance += 10`, dropping an older event because a newer one arrived can lose a valid change. Preserve required sequence and deduplicate event identity, or reconstruct state from an authoritative ordered history. Version-guarded replacement is appropriate for complete state images under a valid version contract.

### Ordering scope and partition keys

Global ordering can constrain throughput and often exceeds the requirement. Per-entity ordering is commonly sufficient: route related events using a stable entity key and preserve order through processing and retries.

Parallel consumers or asynchronous destination writes can reorder effects even if the input log is ordered. A failed earlier event moved to a DLQ while later events continue also breaks strict sequence. Choose whether to block that key/partition, buffer later events, or use valid version-based state application.

Per-key order does not preserve a transaction spanning multiple keys or tables. If consumers require transaction-level consistency, carry transaction metadata and use an apply/publication mechanism that preserves the required boundary.

### Deletes, deduplication retention, and side effects

When a delete at version 12 is applied, retain a deletion marker/version long enough to reject an arriving version-11 update. Physically removing all version state can resurrect deleted entities during delayed delivery or replay.

An in-memory deduplication cache lasts only until restart. A time-limited store rejects duplicates only within its retention period. Size retention for redelivery and supported replay windows, or use a destination constraint/protocol that remains correct beyond that window.

For increments and external side effects, coordinate the event identity and effect atomically where possible. Recording “processed” before the effect can lose work; recording it after the effect can repeat work after a crash. External APIs may need their own idempotency support.

Keep current-state tables and event history distinct. Deduplicating deliveries should not erase legitimate history. Reprocessing an event through a corrected transformation may need a new output version rather than a global marker that suppresses all future processing.

**Interview question:** Is `MERGE` enough for idempotency? Only with a correct match key, deterministic source rows, version rules, and suitable transaction/concurrency behavior. An unordered merge can repeat or overwrite effects incorrectly.

## 8. Schema Drift, Error Handling, DLQ, and Replay

### Schema drift

A connector can keep receiving successful HTTP responses even after the source changes an amount from a decimal to a string. The transport is healthy, but the data contract has changed. **Schema drift** covers unexpected differences in structure or meaning: fields, types, nesting, nullability, precision, enum values, and semantics all matter.

| Change | Risk | Response |
| --- | --- | --- |
| Added optional field | Strict readers may reject it; sensitive data can appear | Preserve safely in raw; explicitly map or approve curated exposure |
| Removed or renamed field | Required columns become missing | Version the contract or stop/quarantine affected records |
| Integer becomes string | Invalid casts or changed identifier semantics | Validate; preserve original value; avoid silent coercion |
| Decimal precision/scale changes | Rounding, overflow, financial errors | Use compatible exact types and validate ranges |
| New enum value or null | Unhandled branches or invalid assumptions | Define unknown/null handling and alert on contract changes |
| Timestamp unit/timezone changes | Incorrect ordering and partitions | Validate units, timezone, range, and source contract |

Compatibility is directional and depends on the format and reader/writer rules. “Adding a field is always safe” is too broad. CSV positional readers, strict JSON consumers, and serialized schemas have different constraints.

Use explicit contracts at curated boundaries and preserve schema/version metadata with raw records when practical. Quarantine incompatible records with reasons. Broad automatic evolution can leak newly introduced personal data or silently widen a contract, so distinguish raw capture from curated publication.

Do not convert identifiers to numbers merely because they contain digits; leading zeros and large values may matter. Distinguish missing fields from explicit nulls when the source gives them different meanings.

### Error handling by scope

| Error scope | Example | Handling |
| --- | --- | --- |
| Record | One invalid decimal | Quarantine with context if independent records can safely continue |
| Page/file/chunk | Truncated download, malformed envelope | Retry or reacquire the bounded unit; do not checkpoint incomplete content |
| Source/job | Revoked credentials, expired change history | Stop progress, alert, and recover through an explicit procedure |
| Destination | Unavailable warehouse or storage | Backpressure, bounded retries, retain durable input |
| Contract | Required field removed across all records | Stop or quarantine affected scope; alert before producing misleading results |

Partial bulk responses need per-item handling when the interface supports it. Request success can contain failed rows. Retry only the failed subset where identity and ordering allow; otherwise retry the whole unit idempotently.

Choose and expose a policy for invalid data. Continuing past a bad record is acceptable only when the rejected record is durable and the effect on downstream completeness is understood. A financial reconciliation table may need to remain unpublished until its missing rows are resolved.

### Dead-letter queue and quarantine

A **DLQ** holds records that could not be processed under the chosen retry policy. Quarantine is a broader isolation area for invalid or suspicious data. Neither repairs data or proves successful business processing.

Store enough context to reproduce the failure:

- Original payload or a secure reference, plus schema/content type.
- Source, tenant/account scope, entity/event identity, and operation type.
- Source position/version and relevant event/ingestion times.
- Error category, sanitized message, attempt count, and first/last failure time.
- Connector/transformation version and original run or trace identity.

Restrict access and retention because payloads can contain sensitive data. Exclude credentials from diagnostic metadata. If quarantine storage fails, do not advance a checkpoint as if the record were safely retained.

Define ownership, alerts, and a redrive procedure. Monitor both DLQ size and oldest unresolved age. A growing DLQ with green job status is degraded data delivery, even when extraction continues.

### Replay and backfill

**Replay** processes retained input again, often after a bug fix or downstream failure. **Backfill** supplies historical data that was never loaded or needs rebuilding. Both can overlap with live ingestion and require explicit scope and write rules.

For a controlled replay:

1. Select an exact source range, file manifest, or offset range and confirm its retention.
2. Record the schema and transformation version, target, and expected outcome.
3. Reprocess in an isolated consumer/run so live progress is not silently reset.
4. Apply idempotently with version/order rules, or rebuild a staged target.
5. Throttle replay so live freshness remains within its requirement.
6. Validate counts, keys, versions, errors, and business totals before publication.

Replaying source history through an old delete boundary must not resurrect data or trigger unintended external actions. Apply access, deletion, and retention policies to replay inputs and outputs. Raw data is replayable only while it is retained and interpretable.

Replaying a partial delta history cannot reconstruct state unless the required starting state and sequence are available. For corrected transformations, write a new derived version or rebuild the affected partition; ordinary deduplication may otherwise suppress the correction.

### Observability and reconciliation

Track extraction health, durability, and publication separately:

| Signal | What it tells you |
| --- | --- |
| Last successful source contact and checkpoint movement | Whether extraction can reach the source and make progress |
| Source-to-landing and source-to-published freshness | Where delay is introduced |
| Rows/bytes extracted, landed, applied, rejected, and deduplicated | Whether data is being lost, rejected, or repeatedly replayed |
| Pending chunks, oldest unprocessed event, and log/token retention headroom | Recovery risk and time until history becomes unavailable |
| Request latency, throttling, retries, and destination latency | Likely throughput bottleneck |
| Counts, key coverage, checksums, and business totals for aligned scopes | Evidence of completeness and semantic correctness |

Compare equivalent source snapshots or bounded intervals; live source counts naturally change during extraction. Row counts alone can match while different keys are missing and duplicated. Timestamp-based freshness needs trustworthy clocks and clear timestamp semantics.

Separate “no new source data” from “connector stalled.” Source heartbeats or independent source checks help distinguish inactivity from failure. Alert on meaningful lag and unresolved data errors, not only process crashes.

**Interview question:** Can a pipeline commit past a DLQ record? It can commit extraction progress after durable quarantine under an explicit policy, but must still report the unresolved business-data gap. Strict ordered consumers may need to stop that key or partition.

## 9. End-to-End Design Scenarios

### Scenario A: SaaS orders API into an analytical warehouse

Consider a SaaS orders source with shared API quotas and webhook notifications. The warehouse needs historical orders, and later updates must become queryable within 15 minutes. Meeting that requirement means connecting the initial extraction to ongoing changes without leaving an unnoticed gap.

Start with an authorized bulk export for history, if available. Establish an overlapping incremental boundary through a source-supported snapshot/change mechanism; if the API cannot provide a gap-free handoff, state that limitation and reconcile current state.

Poll a change feed or bounded modification-time windows for ongoing extraction. Fix run bounds, paginate to completion, durably land each page, and advance progress after durable acceptance. Use overlap and version-aware upserts where timestamp semantics require them.

Use signed webhooks to improve freshness. Durably accept before acknowledging, share event/version handling with the polling path, and pace enrichment requests under the same account quota. Extract hard deletes through a supported deletion mechanism; webhooks alone may not provide recoverable delete history.

Track freshness through publication, quarantine invalid records visibly, and reconcile order keys and business totals. Explain that fetching the current order supports current state but cannot recreate all historical transitions.

### Scenario B: Mutable database into analytics with low latency

**Requirements:** Capture inserts, updates, and deletes; retain changes for replay; recover after connector restarts.

Use a consistent initial snapshot coordinated with log-based CDC. Retain enough source log for the snapshot and expected outages. Land changes in a durable log or raw store with entity keys, operation, source position, schema, and transaction metadata as needed.

Apply current state with source-version checks and retained delete markers. Keep append-only change history separately if required. Checkpoint only durable work, maintain ordering at the required entity/transaction scope, and monitor log-retention headroom as well as downstream lag.

If retained history expires, use a supported resnapshot procedure and reconcile; do not silently skip to the newest log position. If cross-table atomic visibility is required, include it explicitly in the destination publication design.

### Scenario C: Nightly multi-terabyte file delivery

**Requirements:** Files arrive over several hours; users need one complete daily dataset; failed downloads must be recoverable.

Require a manifest or authoritative completion marker listing the expected versioned files. A prefix containing some files is not evidence that delivery is finished. Extract in bounded parallel tasks and record per-file verification and load status.

Load into a staged daily partition or version. Validate manifest coverage, schema, row/key checks, and business totals, then publish through a supported atomic commit. Keep the previous published version available according to rollback requirements.

Retry failed files idempotently. Handle corrected source files through explicit versions and replacement rules so the same pathname does not cause double counting or hide a changed dataset.

## 10. Interview Questions and Answer Framework

### A structure for a system-design answer

In an interview, walk one record from its source to a published result. Explain where it becomes durable, how the next run knows where to resume, and what evidence would reveal a missing record. The following sequence keeps the answer focused on the boundaries that make the design reliable.

1. **Requirements:** Freshness, volume, burst rate, history, deletes, security, recovery objectives.
2. **Source contract:** Available exports/change feeds, consistency, authentication, quotas, retained history.
3. **Extraction and progress:** Snapshot scope, partitioning, pagination, fixed boundaries, checkpoint ownership.
4. **Durability and apply:** Landing boundary, identities, idempotency, versions, ordering, publication.
5. **Failure recovery:** Timeouts, retries, partial results, quarantine, replay, expired history.
6. **Proof:** Freshness metrics, source/destination reconciliation, key coverage, business checks, cost.

Name the assumptions behind a guarantee. If the source exposes only mutable current state with no reliable change marker, no downstream tool can manufacture a complete history of all changes.

### Common questions

| Question | What a strong answer includes |
| --- | --- |
| When would you choose batch over streaming? | Required freshness, source interface, bulk efficiency, cost, operational complexity |
| Is CDC the same as streaming? | CDC is change detection; streaming is continuous delivery/processing; CDC can feed batches |
| How do you avoid skipping equal timestamps? | Stable `(timestamp, unique key)` pagination; separate discussion of late visibility and overlap |
| When do you advance a watermark? | After all required work through a fixed boundary is durable; account for chunk completeness |
| How do you handle API quotas across workers? | Shared quota scope, pacing, bounded concurrency, provider delays, retry budget |
| What happens after a timeout creating an export? | Outcome is uncertain; reconcile job identity or reuse a supported idempotency key |
| Does an HTTP success mean GraphQL extraction succeeded? | Inspect `errors`, required data, and every paginated connection |
| How do you detect hard deletes? | CDC, explicit delete feed/markers, or validated full reconciliation; timestamps alone cannot |
| How do you prevent duplicate effects? | Stable identity, atomic uniqueness or versioned apply, durable progress, sink-specific boundary |
| Can you discard an older event? | Possibly for complete versioned state; usually unsafe for an unapplied delta |
| What does a DLQ guarantee? | Failed input is retained for investigation/recovery if durably written; processing remains incomplete |
| How do you recover after a week-long outage? | Verify history retention first; replay from durable progress or resnapshot and reconcile if history is gone |
| How do you prove completeness? | Align source scope/boundaries; check expected chunks, keys, counts, versions, and business totals |

### Failure cases to walk through aloud

- A page lands successfully and the connector crashes before saving its cursor: replay that page without duplicate effects.
- Two workers receive the same event simultaneously: enforce atomic destination identity or apply rules, rather than check-then-write.
- A late update arrives after a delete: use retained deletion/version state to prevent resurrection.
- A required GraphQL field fails but HTTP returns success: keep the page incomplete and recover under the error policy.
- One extraction chunk fails after all others finish: preserve completed work but do not publish an allegedly complete dataset.
- A schema change invalidates every incoming record: stop or quarantine the affected scope, alert, and expose the delivery gap.
- A replay reaches already processed events: distinguish duplicate delivery from intentional recomputation through corrected logic.

For deeper database capture details, see [Data Replication](replication.md). For event-log ingestion concepts, see [Kafka](kafka.md). For broader delivery and transaction concepts, see [Core Engineering Concepts](../foundations/core.md).
