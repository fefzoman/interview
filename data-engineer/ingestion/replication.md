# Data Replication: Data Engineer Interview Guide

Copying a table once is a transfer. Keeping that copy useful while the source continues to change is a coordination problem. You need a starting point, an ordered account of later changes, and a recovery path when either side fails. This guide follows that journey from the first snapshot to a destination you can validate and, when needed, cut over to.

This guide covers database replicas and analytical replication pipelines. Examples are illustrative, rather than production configurations. Product capabilities depend on the database, connector, destination, version, and settings; verify those together before promising a guarantee.

## Contents

1. [Fundamentals & Architecture](#1-fundamentals--architecture)
2. [Change Data Capture (CDC)](#2-change-data-capture-cdc)
3. [Consistency & Delivery Guarantees](#3-consistency--delivery-guarantees)
4. [Initial Load & Backfill](#4-initial-load--backfill)
5. [Replication Pipelines & Integration](#5-replication-pipelines--integration)
6. [Schema Evolution & Data Transformation](#6-schema-evolution--data-transformation)
7. [Performance & Scalability](#7-performance--scalability)
8. [Reliability & Recovery](#8-reliability--recovery)
9. [Data Validation & Reconciliation](#9-data-validation--reconciliation)
10. [Production & Advanced Topics](#10-production--advanced-topics)

## 1. Fundamentals & Architecture

### Replication vs. synchronization vs. migration

Before designing the copy, ask what it is for. A read replica, two applications exchanging customer updates, and a database migration may all move the same rows, yet they need different ownership and completion rules. The terms below help separate those goals.

| Term | Purpose | Typical example |
| --- | --- | --- |
| Replication | Maintain another copy by transferring initial data and subsequent changes | PostgreSQL primary to a read replica; OLTP database to a warehouse |
| Synchronization | Bring systems into an agreed state, potentially with changes from either side | Two applications reconcile customer records and resolve conflicts |
| Migration | Move a workload or dataset to a new system, ending with a cutover | Move an on-premises database into a managed database |

These overlap: a migration can use replication to keep the destination current until cutover. Synchronization can use replication in both directions, but still needs rules for ownership, conflicts, and deletes. The terms are sometimes used loosely, so clarify whether both systems can accept writes and whether replication is permanent.

Replication is also different from partitioning. Replication makes copies; partitioning divides the dataset. A distributed database can partition data across nodes and replicate each partition.

### Full load, incremental replication, and CDC

- **Full load:** copy all selected records. Useful for initialization, small datasets, and rebuilding a damaged destination. Repeated full loads cost source I/O and need a safe replacement strategy.
- **Incremental replication:** copy only changes since a saved boundary. It can use an update timestamp, increasing identifier, audit table, or transaction log.
- **Change Data Capture (CDC):** detect and represent data changes. CDC is often log-based, but triggers and polling can also capture changes. CDC by itself does not define how a destination applies them.

An increasing ID captures new rows only if IDs become visible in the expected order; it misses updates and deletes. Even insert-only tables can commit out of ID order. An update timestamp can capture inserts and updates under additional assumptions, but hard deletes require another mechanism.

**Interview question:** Is incremental replication the same as CDC? Explain that incremental describes how much data moves; CDC describes how changes are detected. Log-based CDC is one way to implement incremental replication.

### Physical vs. logical replication

| Aspect | Physical replication | Logical replication |
| --- | --- | --- |
| Replicated representation | Storage-level changes, such as WAL records needed to reproduce database pages | Logical operations or row changes decoded from database activity |
| Compatibility | Usually tightly coupled to the database engine, version, and storage format | Can support different versions, schemas, and database products |
| Selection and transformation | Usually an entire instance or storage unit; limited filtering | Often selected tables, columns, or rows; transformations are possible |
| Common purpose | Standby databases, high availability, read scaling | Data integration, analytics, selective replication, migrations |
| Main concern | Promotion, replay position, storage compatibility | Keys, schema mapping, transaction semantics, apply correctness |

Logical does not mean statement-based. A logical stream can contain row images rather than SQL statements. Physical replication can reproduce schema and storage changes as part of database recovery, while logical replication needs explicit support for each kind of schema change.

### Synchronous vs. asynchronous replication

**Synchronous replication** delays commit acknowledgment until designated replicas reach a configured stage. Receiving bytes, persisting the log, and applying the transaction are different stages. A transaction durable on a standby is not necessarily visible to a query there yet.

**Asynchronous replication** acknowledges the primary independently of replica progress. It usually lowers write latency and allows primary writes during replica outages, but readers can see stale data and failover can lose acknowledged writes that never reached the promoted replica.

Synchronous replication trades write latency and availability for stronger durability under its failure assumptions. Across regions, network round trips can dominate commit latency. Synchronous acknowledgment alone does not establish linearizable reads across arbitrary replicas; read routing, acknowledgment level, and leader fencing also matter. PostgreSQL exposes distinct synchronous acknowledgment behaviors through `synchronous_commit`. [PostgreSQL WAL settings](https://www.postgresql.org/docs/18/runtime-config-wal.html).

### Primary-replica, multi-primary, and active-active

- **Primary-replica:** one writer owns the authoritative order; replicas follow. Simpler conflict handling, but promotion and write routing need coordination.
- **Multi-primary:** several nodes accept writes to replicated data. Concurrent writes can conflict, and circular replication must prevent events from endlessly returning to their origin.
- **Active-active:** multiple sites serve live traffic. This describes deployment behavior; writes might use global coordination, region-owned keys, or asynchronous conflict resolution. It does not automatically mean unrestricted multi-primary writes to every record.

For asynchronous multi-primary systems, discuss disjoint key ownership, conflict detection, convergence, and application invariants. Two regions can independently approve a purchase against the same inventory and oversell even if the final replicated row converges. Globally coordinated writes avoid some conflicts at the cost of latency and partition-time availability.

### Push vs. pull; log-based vs. query-based

Push and pull describe who initiates transfer. A source can push changes to a subscriber; a consumer can poll or maintain a streaming connection. They do not determine whether replication is synchronous, ordered, or reliable.

Log-based and query-based describe the source of change information:

- **Log-based:** decode changes from a durable transaction log. Usually captures intermediate changes and deletes with less repeated table scanning, but needs privileges, log retention, and database-specific decoding.
- **Query-based:** periodically select changed rows or compare snapshots. Easier to introduce where log access is unavailable, but scans, watermark correctness, and missing delete information can be significant limitations.

A connector can pull a log stream and push decoded events to Kafka. These dimensions are independent.

## 2. Change Data Capture (CDC)

### Log-based CDC: WAL, binlog, and redo logs

A database already records changes so it can recover from a crash or maintain replicas. **Log-based CDC** uses that record of work to reconstruct changes, identify their transactions, and establish positions from which capture can resume. The source's log format and configuration determine what a reader can recover.

| Database | Relevant mechanism | Important preparation |
| --- | --- | --- |
| PostgreSQL | Logical decoding of write-ahead log (WAL) | Logical replication configuration, publication, replication slot, suitable replica identity |
| MySQL | Binary log, commonly row-based for CDC | Supported row logging settings, sufficient row images, binlog retention, connector identity |
| Oracle | Online and archived redo; capture through mechanisms such as LogMiner | Required privileges, supplemental logging, redo/archive availability, supported capture configuration |

The MySQL binlog is distinct from InnoDB's internal redo log. Oracle CDC must account for transaction state and supplemental information, rather than treating each redo record as an independent committed row event. [MySQL replication formats](https://dev.mysql.com/doc/refman/8.4/en/replication-formats.html), [Oracle LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html).

CDC avoids repeatedly scanning whole tables, but it is not free: extra logging, retained logs, decoding CPU, snapshot reads, network transfer, and destination writes all consume resources.

### Trigger-based, timestamp-based, and query-based CDC

| Approach | How it works | Strength | Main limitation |
| --- | --- | --- | --- |
| Trigger-based | Row triggers write changes into a change table, ideally in the same transaction | Can record deletes and application-specific values | Adds work to source writes; triggers and change-table cleanup need maintenance |
| Timestamp-based | Poll rows whose update timestamp falls in a selected interval | Simple when the source maintains a reliable indexed timestamp | Misses hard deletes and intermediate updates; timestamp and commit ordering can differ |
| Query-based comparison | Compare keys, versions, or hashes between successive reads | Works without log access | Reads more data and observes states rather than every transition |

For timestamp polling, an indexed composite cursor such as `(updated_at, primary_key)` handles equal timestamps during pagination. Save progress only after the destination write succeeds. A fixed upper boundary makes each poll finite; an overlap window plus deduplication reduces some late-visibility problems.

**Important limit:** none of these makes an application timestamp a commit-order cursor. A transaction can set `updated_at = 10:00`, remain open, and commit after a poll has advanced beyond 10:00. A finite overlap is correct only with a bound on lateness and timestamp behavior. Otherwise use a transactional change table or log-based capture when missing changes is unacceptable.

### INSERT, UPDATE, DELETE, and DDL capture

- **INSERT:** generally an after-image with the new key and values.
- **UPDATE:** after-image, before-image, changed columns, or another representation depending on logging and connector settings. An unavailable before-image is not the same as a SQL `NULL`.
- **DELETE:** identify the old row, usually through its key. A deleted row cannot be discovered by querying its current contents.
- **DDL:** schema changes may appear as schema events, be handled internally, require manual coordination, or be unsupported. Data capture does not imply DDL replay.

Also ask about `TRUNCATE`, bulk operations, generated columns, large objects, and tables without keys. They can have different capture semantics from ordinary row DML. PostgreSQL native logical replication, for example, does not replicate DDL or sequence state. [PostgreSQL logical replication restrictions](https://www.postgresql.org/docs/18/logical-replication-restrictions.html).

### Transaction ordering, commit timestamps, and LSN/SCN

A transaction ID groups related changes. A commit timestamp describes commit time when the source exposes it. A source position identifies a location in the change stream: PostgreSQL uses log sequence numbers (LSNs); Oracle exposes system change numbers (SCNs); MySQL uses binlog coordinates and, where configured, global transaction identifiers (GTIDs).

These identifiers have different meanings. An LSN is not a timestamp; an SCN is not necessarily a unique row-event ID; a transaction ID is not sufficient to order its individual row changes. Transaction logs can interleave records from transactions that commit in a different order.

Use the connector's documented cursor and ordering metadata, potentially including a transaction-local sequence and source incarnation. Do not assume every event has a unique LSN, compare cursors across unrelated databases, or sort formatted positions as arbitrary strings. Timestamps alone can tie or suffer clock skew.

Many CDC tools publish committed transactions, buffering or otherwise tracking open transactions first. Where an implementation streams fragments before commit, downstream logic must honor commit and rollback boundaries before making effects visible.

### Initial snapshot + CDC transition

The correctness requirement is **complete coverage across the snapshot boundary**, with a defined rule for any overlap:

1. Establish a consistent source snapshot and its coordinated CDC position using the database/connector protocol.
2. Retain or durably capture the required change history while scanning the snapshot.
3. Load snapshot rows into a destination generation that is not yet serving inconsistent results.
4. Apply the changes needed to move from that snapshot to the chosen catch-up point.
5. Persist progress and publish the destination only after the required state is complete.

The exact protocol varies. MVCC visibility includes the state of transactions already in progress, so recording the current log position and independently starting a table scan is not sufficient. A transaction that started before the scan and committed during it must be covered correctly.

An overlap is often recoverable with ordered, idempotent apply; a missing interval cannot be reconstructed without another source of history. A late snapshot row must also not overwrite a newer CDC update or resurrect a deleted record. Debezium documents coordinated snapshot and streaming behavior for its PostgreSQL connector. [Debezium PostgreSQL snapshots](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-snapshots).

### Debezium, AWS DMS, and Datastream

| Tool | Common role | Questions to ask |
| --- | --- | --- |
| Debezium | Open-source database change capture, commonly run through Kafka Connect | Which connector and snapshot mode? Where are offsets stored? How are schemas, deletes, and source failover handled? |
| AWS Database Migration Service (DMS) | Managed full load and ongoing replication across supported endpoints | Which source/target pair? What happens to LOBs, DDL, cached changes, apply order, and validation? |
| Google Cloud Datastream | Managed backfill and CDC into supported Google Cloud destinations | How are duplicate and out-of-order events resolved? How is destination freshness measured? |

DMS full-load-plus-CDC processing includes table loading, cached changes, and ongoing replication; loading tables does not make the entire target immediately transactionally consistent. Datastream documents at-least-once delivery and does not guarantee event arrival order; destination handling must account for this. [DMS processing overview](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Introduction.HighLevelView.html), [Datastream events and consistency](https://docs.cloud.google.com/datastream/docs/events-and-streams).

**Interview question:** Which tool is best? Start with source/target support, correctness requirements, operational ownership, recovery, throughput, and cost. A managed service reduces infrastructure work but does not remove the need to verify semantics.

## 3. Consistency & Delivery Guarantees

### Strong vs. eventual consistency

After a customer changes an address, must the next read from a replica show it? That question needs a precise consistency contract. In replication discussions, **strong consistency** often means linearizable operations: a read begun after a completed write sees that write or a newer one. Serializable transactions describe a different property and are not automatically linearizable.

**Eventual consistency** means replicas converge after updates stop, assuming delivery and conflict resolution continue successfully. It does not promise a maximum stale-read interval. A freshness SLO adds a measurable bound or percentile target under defined conditions.

For read-your-writes behavior, route the user to the primary, wait until a replica has applied a returned commit position, or use a database-supported session consistency mechanism. A fixed sleep guesses at lag and is not a correctness guarantee.

### At-most-once, at-least-once, and exactly-once effects

| Guarantee | Typical failure behavior | Practical meaning |
| --- | --- | --- |
| At-most-once | Progress can advance before an output is durable | Changes can be lost; delivery does not retry them |
| At-least-once | A durable output can precede a saved checkpoint | Changes can be replayed; the destination must handle duplicates |
| Exactly-once effects | Output and progress are atomic, or replay cannot alter the logical result | Each logical change affects the result once within a defined boundary |

The classic crash window is:

```text
Read event E -> commit destination update -> crash -> checkpoint not saved
Restart     -> read event E again         -> duplicate attempt
```

Saving the checkpoint before the write merely changes duplicates into possible data loss. Solutions include committing the output and consumed position in the same destination transaction, or combining at-least-once delivery with durable event deduplication or version-aware state replacement.

Kafka transactions can make records and offsets atomic within a supported Kafka processing path. They do not automatically make a database update, file publication, or external API call atomic with those offsets. Always identify the exact source-to-sink boundary and recovery assumptions behind an exactly-once claim.

### Transaction boundaries, atomicity, and ordering

A source transaction might update both an order and its payment. If the two events reach different topics or independently committed target tables, readers can temporarily see only half of the transaction.

Preserving row order, transaction atomicity, and cross-table consistency are separate requirements. Possible approaches include staging transaction events until the commit marker arrives, applying all participating rows in one target transaction, or exposing only a consistent published generation.

Kafka orders records within a partition. Partitioning by row key helps serialize changes for that key, but transactions spanning keys can span partitions. Repartitioning, retries, parallel workers, and destination batching can change processing order. Partition-count changes can also move a key's future events to a different partition and need an ordering-aware transition.

Transaction metadata helps reconstruct boundaries; it does not automatically enforce them in every sink. Global ordering across independent source shards requires additional coordination or a weaker contract.

### Idempotency, deduplication, and replay

**Idempotency** means applying the same operation again has the same effect. Setting `balance = 100` is idempotent for immediate repetition; adding `balance += 10` is not. However, replaying an old state after a newer state can still corrupt an idempotent upsert.

For a current-state destination, store a source version per key and atomically accept only newer changes. Keep version information for deletes too. Otherwise an older replayed insert can resurrect a row after its delete removed both data and version.

```text
key=42, version=100, UPDATE status='paid'  -> apply
key=42, version=100, UPDATE status='paid'  -> duplicate; ignore
key=42, version=99,  UPDATE status='new'   -> stale; ignore
key=42, version=101, DELETE               -> retain deletion version
key=42, version=100, UPDATE status='paid'  -> stale; do not resurrect
```

Here `version` is an illustrative monotonic order for this key, not a promise that one raw LSN uniquely identifies every change. Updates of different fields may require reconstruction before applying a full row replacement. Concurrent consumers need an atomic compare-and-write, not a separate unlocked read followed by a write.

For an event-history destination, deduplicate using a stable source-event identity. A key alone is insufficient because many legitimate events can share it. A broker offset identifies a broker record, but the same source event republished at another offset needs source-level deduplication. Retain deduplication state for the supported replay window.

### Replication lag and data freshness

Measure separate stages: source commit to capture, capture to durable transport, transport to destination apply, and apply to query visibility. A connector can report healthy capture while the warehouse remains hours behind.

- **Time lag:** elapsed time associated with unapplied committed changes or a known replication watermark.
- **Position/byte lag:** distance between source and consumer positions; useful for backlog and retention risk.
- **Consumer lag:** records behind a broker partition; useful operationally but not a direct duration.
- **Business freshness:** how recently a relevant dataset or business entity reflects source changes.

Clock skew distorts timestamp subtraction. Quiet tables can look stale despite being caught up, so use heartbeats or verified watermarks to distinguish inactivity from a stalled pipeline. A captured heartbeat proves only capture progress unless it passes through the same apply path as the data.

### Conflict resolution and split-brain

Conflicts arise when independent writers modify the same logical data. Resolution options include single ownership per key, deterministic source priority, last-write-wins, application merges, or suitable conflict-free replicated data types (CRDTs).

Last-write-wins can silently discard valid updates, and wall-clock timestamps can misorder them. Uniqueness constraints, balances, and inventory reservations often need coordination rather than a generic merge rule. Deletes also need conflict rules to prevent resurrection.

Split-brain occurs when competing nodes both behave as the authoritative writer. Use quorum-based leadership, leases with appropriate assumptions, and fencing that prevents an old primary from continuing writes after promotion. Replication alone does not elect or fence a primary.

## 4. Initial Load & Backfill

### Consistent snapshots and MVCC

A long-running copy can otherwise combine yesterday's customers with today's orders. **Multi-version concurrency control (MVCC)** lets a reader see a stable version while writers continue. For replication, a consistent snapshot should put the selected data at one logical visibility boundary, rather than stitch together independently timed table scans.

An exported/shared snapshot can let parallel readers use the same boundary where the database supports it. Merely starting several repeatable-read transactions does not give them the same snapshot. A replica used for extraction must also support alignment between its visible state and the CDC starting position.

**Interview question:** Why can counts match while the snapshot is wrong? Two tables can have the correct individual row counts but refer to different moments, breaking relationships such as order-to-payment consistency.

### Parallel extraction, chunking, and pagination

Split large tables into non-overlapping ranges using a stable indexed key. Balance work by estimated bytes or sampled key distribution, rather than assuming equal numeric ranges contain equal work. Handle composite keys, sparse IDs, and skew.

Prefer keyset pagination for a suitable key:

```sql
SELECT id, customer_id, amount
FROM orders
WHERE id > :last_id
  AND id <= :snapshot_max_id
ORDER BY id
LIMIT :batch_size;
```

This is illustrative parameterized SQL. It needs a defined starting boundary and snapshot semantics. `snapshot_max_id` bounds the scan; it does not make reads consistent or capture later updates/deletes. Mutable pagination keys can move rows across chunk boundaries.

Large `OFFSET` values often require scanning and discarding preceding rows. Pagination over a changing live dataset can also skip or repeat rows. Durable chunk manifests should record boundaries, completion, row counts, and the snapshot generation so retries do not mix incompatible runs.

### Snapshot isolation and long-running transactions

Snapshot isolation gives stable reads but does not mean the source is locked against all writes or that every business invariant is serializable. The exact locks and allowed DDL depend on the engine and snapshot method.

Long snapshots can retain old row versions, delay vacuum or undo cleanup, increase storage pressure, and hold locks that interfere with DDL. Long source transactions can also delay capture progress or create a large burst of changes at commit.

Monitor source disk, retained history, oldest transactions, and snapshot duration. If a single snapshot is too expensive, use a connector-supported incremental snapshot protocol with chunk boundaries and change-stream reconciliation. Independently scanning chunks at different times is not a globally consistent snapshot.

### CDC buffering during full load

Changes continue while historical rows are copied. Retain them in source logs, a connector spool, or durable transport until they can be reconciled with the snapshot. Size both the source retention and any intermediate storage.

Two broad strategies are snapshot-then-replay and interleaving snapshots with CDC using watermarks and per-key collision handling. Both must ensure that a stale snapshot row cannot overwrite a later change.

If the source produces 20 MB/s of retained change data during a two-hour scan, the raw accumulation is approximately `20 × 7,200 = 144,000 MB`, before safety margin and format overhead. The destination also needs spare capacity to catch up while new changes continue arriving.

### Historical backfills and re-synchronization

A historical backfill loads older data into an existing pipeline. Define whether it restores current state, missing event history, or historical versions. A present-day table snapshot cannot recreate updates and deletes that disappeared before capture began.

For repairs, prefer scoped partitions or key ranges when you can prove the damaged area. Stage the replacement, reconcile it, then merge or atomically publish it under a defined boundary. Avoid letting an old backfill overwrite newer CDC state.

A full re-synchronization should use a new generation or include explicit handling for extra destination keys. Upserting every row from a fresh snapshot into an old target leaves behind rows that were deleted at the source.

### Cutover strategies and zero-downtime migration

A practical migration sequence is:

1. Prepare destination schema, permissions, networking, and required non-table objects.
2. Load a coordinated snapshot and run CDC while applications continue using the source.
3. Validate data and test destination application behavior and performance.
4. Fence or briefly pause source writes; drain in-flight transactions and record a final commit boundary.
5. Wait until the destination has applied that boundary, reconcile critical data, and prepare sequences or generated IDs.
6. Switch traffic and enable destination writes under a single ownership decision; monitor correctness and errors.

Near-zero downtime usually means moving almost all work before a short write pause. A fully interruption-free handover needs an explicit routing and transaction protocol. Naive dual writes create partial-failure windows.

Define rollback before cutover. Once new writes reach the destination, switching back can lose them unless reverse replication or another controlled reconciliation path exists. A metric that reports zero lag is not a substitute for verifying the final transaction boundary.

## 5. Replication Pipelines & Integration

### Database → Database replication

The destination's purpose helps determine the replication path. For high availability or read scaling within one database family, native replication often fits well. For selected tables or a different database engine, logical tools may be more suitable. In either case, check what the destination actually applies: type conversion and apply behavior can change the result.

Check indexes needed for updates/deletes, key uniqueness, foreign keys, generated values, sequences, triggers, and stored procedures. Applying replicated changes through ordinary writes can fire destination side effects or create replication loops. Configure those behaviors deliberately rather than disabling all controls indiscriminately.

A destination that matches row data is not automatically a usable failover database. It also needs appropriate schema objects, identities, privileges, capacity, and a tested promotion process.

### Database → Kafka → Data Lake/DWH

```text
Source DB -> CDC connector -> Kafka topics -> apply/processing job
                |                 |                  |
          source cursor     durable retention    checkpoint
                                                     |
                                     +---------------+---------------+
                                     v                               v
                              raw change history             current-state tables
```

Kafka decouples source capture from downstream processing and supports replay within retention. A raw event layer preserves operation, key, source position, transaction information, schema identity, and capture metadata. A curated layer applies upserts/deletes or creates historical models.

Raw history and current state answer different questions. An append-only set of CDC files does not by itself represent the latest table state. Conversely, a compacted current-state topic may no longer contain all intermediate changes required for an audit.

### Debezium + Kafka Connect

Kafka Connect manages connector configuration, task execution, restarts, and durable source-offset storage. A Debezium source connector reads database changes and emits records, commonly keyed by table primary key. Downstream connectors or processing jobs remain responsible for destination semantics.

Connect worker count does not imply proportional capture parallelism; one database's ordered log can constrain a connector. Some connectors also depend on durable schema history, so protect that state alongside offsets and configuration.

Debezium can expose transaction boundary and ordering metadata where supported. Use it only with a consumer that implements the needed coordination; an ordinary table sink does not automatically reconstruct cross-table atomicity. [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).

### Managed services and native replication

Choose native replication when database-level fidelity and promotion behavior fit the goal. Choose a CDC integration service when supported heterogeneous destinations, selective data movement, or managed capture are more important.

For DMS or Datastream, check endpoint-specific limitations, schema handling, backfill behavior, private connectivity, restart semantics, and destination consistency. Separate service availability from source-to-destination recovery: restarting a healthy service cannot recover source logs that no longer exist.

Test the actual combination with updates, deletes, schema changes, large transactions, and interrupted loads. Avoid deriving an end-to-end guarantee from one component's marketing description.

### Schema Registry, serialization, and event envelopes

Serialization defines the wire representation, such as Avro, Protobuf, or JSON. A Schema Registry can associate records with schemas and enforce configured compatibility rules. It does not automatically migrate warehouse tables or prove that a business meaning remained compatible.

A simplified CDC envelope might be:

```json
{
  "key": {"order_id": 42},
  "op": "u",
  "before": {"order_id": 42, "status": "new"},
  "after": {"order_id": 42, "status": "paid"},
  "source": {"table": "orders", "position": "source-specific-cursor"},
  "transaction": {"id": "tx-17", "event_order": 2}
}
```

This is a teaching format, not an exact connector payload; Kafka keys are commonly serialized separately from values. Preserve the metadata needed for ordering, deletes, diagnostics, and replay even if a transformation flattens the business columns. Treat missing fields, unavailable images, and explicit `NULL` values distinctly.

### Outbox pattern and event-driven integration

The outbox pattern writes business state and an outbox record in one local database transaction. CDC or a relay publishes the outbox record to a broker. This closes the gap where a database commit succeeds but a separate message publication fails.

The relay can still publish twice, so consumers need stable event IDs and idempotent handling. Use an aggregate key for ordering where appropriate, and clean up outbox rows only under a safe publication/retention policy.

Table CDC describes storage changes; outbox events can describe business intent, such as `OrderPlaced`. This reduces coupling to internal table structure. Neither pattern makes downstream side effects automatically exactly-once, and neither should expose sensitive columns indiscriminately.

## 6. Schema Evolution & Data Transformation

### DDL propagation and schema drift

A source team adds a column, but the destination still rejects the next record. Detecting the change did not make every part of the pipeline compatible. **DDL propagation** applies schema changes downstream; **schema drift** is divergence from an expected schema or contract. Detecting a schema change, decoding the new records, and altering the destination are three separate capabilities.

Classify changes before rollout: adding an optional column, widening a type, renaming or dropping a column, changing a key, or changing a field's meaning. Use an expand-and-contract rollout where possible: make readers and destination compatible, introduce the source change, backfill if needed, then retire old fields.

Do not assume a rename will preserve identity in every serializer or sink; some see a dropped field and a new field. Keep the schema history needed to decode older events during replay. Coordinate non-compatible DDL so it cannot leave capture silently dropping or misinterpreting records.

### Type mapping, precision, and timezone handling

| Source concern | Failure mode | Safe approach |
| --- | --- | --- |
| Decimal precision and scale | Rounding, overflow, monetary errors | Preserve decimal representation and validate destination precision/scale |
| Unsigned or large integers | Overflow or JSON consumer precision loss | Use a supported exact integer, decimal, or documented string encoding |
| Timestamp with timezone | Same instant displayed or stored differently | Preserve the instant, normalize consistently, retain zone information if required |
| Timestamp without timezone | Incorrect assumption that local wall time is UTC | Document the intended timezone and daylight-saving ambiguity policy |
| NULL, missing, empty string | Semantically different values collapse | Preserve distinctions or document an explicit mapping |
| Binary, JSON, arrays, LOBs | Encoding changes, unsupported values, truncation | Define representation and test boundary sizes and nested values |

Timezone conversion is not just string formatting. For example, `2026-10-25 03:30` can be ambiguous in a timezone undergoing a daylight-saving transition. Capture an offset or another disambiguation rule when a local timestamp must represent a unique instant.

### Primary key changes and keyless tables

A primary key identifies the destination row to update or delete. A key change can require deleting the old key and inserting the new key, potentially across partitions. Preserve both identities and plan for temporary visibility differences.

For keyless tables, inserts are straightforward, but updates/deletes might require an old full-row image, a configured unique key, or source changes. Duplicate identical rows make full-row matching ambiguous. A hash of all columns changes on update and does not establish stable identity.

PostgreSQL replica identity controls the old-row information available for replication. `REPLICA IDENTITY FULL` increases logged information; it does not create a business key or guarantee efficient target matching. Prefer a stable supported unique identity where possible. [PostgreSQL publication and replica identity](https://www.postgresql.org/docs/18/logical-replication-publication.html).

### Schema compatibility and versioning

- **Backward compatibility:** a new reader can read data written with an older schema.
- **Forward compatibility:** an old reader can read data written with a newer schema.
- **Full compatibility:** both directions are supported under the chosen format's rules.
- **Transitive compatibility:** checks extend across the relevant schema history rather than just the latest version.

Defaults, optional fields, field numbers, and naming rules differ by serialization format. A registry-compatible change can still break SQL models, dashboards, or business logic. Version semantic changes explicitly and test replay of retained data, not only new writes.

### Soft deletes, hard deletes, and tombstones

A soft delete updates a field such as `is_deleted`; the row remains present. A hard delete removes the row and needs a captured delete event or reconciliation to remove it downstream.

In a typical Debezium Kafka flow, a delete event identifies the deleted row and can be followed by a tombstone: the same key with a null Kafka value. Tombstones support removal of old keyed records through compaction. They differ from an envelope whose `after` field is null. [Debezium delete and tombstone events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-delete-events).

Define how sinks interpret each form. Dropping all null-valued messages without understanding the delete path can leave stale destination rows. Compaction is asynchronous, and tombstones have retention limits; a long-offline consumer may require rebuilding rather than assuming every past delete marker remains available.

### Transformation and normalization during replication

Keep capture transformations small when possible: select approved fields, rename columns, map types, and add provenance. Preserve keys, operation type, source ordering, and delete semantics. Apply heavier business transformations downstream when that makes capture easier to recover.

Prefer deterministic transformations for replay. Enriching an old event with today's customer attributes can produce a different result than the original run; use versioned reference data or explicit as-of semantics when historical reproducibility matters.

Filter changes carefully. If a row changes from `region = 'EU'` to `region = 'US'`, a destination containing only EU rows needs a deletion or retraction. Filtering only the after-image can incorrectly leave the old EU row behind.

## 7. Performance & Scalability

### Throughput, latency, and lag

A replication pipeline may handle a large volume and still fall further behind every minute. To see why, distinguish **throughput**, or work per unit time, from **latency**, the elapsed time for one change, and **lag**, the outstanding progress relative to the source. Larger batches may improve throughput while making individual changes wait longer.

If incoming rate is `lambda` and sustainable apply rate is `mu`, backlog drains only when `mu > lambda`. Under a roughly steady workload:

```text
catch-up time ≈ backlog / (apply rate - incoming rate)
```

For a 600 GB backlog, 150 MB/s apply, and 100 MB/s incoming, the estimated drain time is `600,000 / 50 = 12,000 seconds`, about 3 hours 20 minutes using decimal units. Real rates vary with record sizes, transactions, indexes, and compaction, so measure rather than treating this as a service guarantee.

### Snapshot parallelism and batch sizing

Increase snapshot concurrency until source I/O, CPU, network, or destination apply becomes the bottleneck. More readers can increase source latency and compete with the production workload. Key-range skew can leave most workers idle behind one large chunk.

Tune batch size using both rows and bytes. Larger batches amortize round trips but use more memory, take longer to retry, and can delay visibility or exceed destination transaction limits. Small batches reduce retry scope but increase per-request overhead. Snapshot and ongoing CDC usually need different settings.

### Network bandwidth and compression

Estimate transfer from changed bytes, not only transaction count. One large JSON update can outweigh thousands of small inserts; full before/after images increase volume further.

Compression saves bandwidth and storage at a CPU cost. Test compressibility, compression level, TLS overhead, packet loss, and cross-region round trips. Batch enough to use the link efficiently without violating freshness requirements. Avoid logging payloads merely to measure throughput.

### Source overhead and transaction log retention

Log retention must cover snapshot duration, outages, catch-up time, and safety margin for the chosen capture path. Capacity depends on actual log generation, including unrelated database activity where retention is shared.

An approximate storage budget is `peak retained log bytes/second × retention window`, with margin for bursts and long transactions. Measure source logs separately from compressed Kafka or lake data; their sizes are not interchangeable.

PostgreSQL replication slots can retain WAL needed by a consumer. An abandoned or stalled slot can exhaust disk; a configured retention cap can instead make the slot unusable once necessary WAL is removed. Monitor both retained bytes and remaining source disk. [PostgreSQL replication slots](https://www.postgresql.org/docs/18/warm-standby.html#STREAMING-REPLICATION-SLOTS).

### Backpressure and large transactions

Backpressure propagates when downstream apply cannot keep up: queues grow, buffers spill, capture slows, and source logs accumulate. Use bounded buffers, durable spill where supported, and alerts before storage limits are reached. Dropping events to keep a queue small violates replication correctness.

Large transactions can consume capture memory, delay commit visibility, hold dependencies, and create bursts at commit. Support streaming or disk-backed transaction handling where available. Splitting one source transaction into independently visible target batches changes atomicity and must be an explicit contract.

### Partitioning, parallel apply, and hot tables

Partition by a stable row or aggregate key to distribute work while preserving the needed order. More partitions enable more consumers but increase coordination and do not solve a single hot key.

Parallelize independent transactions or key ranges; serialize conflicts or use dependency tracking. Check whether parallel apply preserves commit order and whether readers can observe intermediate states. A source transaction spanning several target partitions can constrain useful parallelism.

For hot tables, identify whether the bottleneck is source capture, one partition, target key lookup, index maintenance, or repeated merges into the same files. Extra workers do not help when every operation contends on the same target row or storage unit.

## 8. Reliability & Recovery

### Checkpointing, offsets, and LSN tracking

After a restart, the pipeline needs more than the last row it happened to read. A **checkpoint** must describe durable progress and the state required to resume safely. Depending on the path, that can include the source cursor, snapshot generation and completion, broker positions, destination progress, and decoding or schema state.

There can be several distinct frontiers: read from source, durably published to transport, applied to destination, and visible to queries. Acknowledge source progress only when the captured data is recoverable from the next durable stage. A destination checkpoint must not claim unapplied records.

Protect checkpoint stores from accidental deletion and incompatible reuse. Reusing an old connector name or cursor with a different source incarnation can cause duplicates, gaps, or incorrect ordering. A cursor string without its source identity is incomplete recovery information.

### Connector restarts, retries, and failover

Restart from the last durable checkpoint, expect replay, and use bounded exponential backoff with jitter for transient failures. Distinguish transient errors from invalid schema, expired credentials, and missing source history; retrying indefinitely does not repair those conditions.

Fence old connector instances so two active writers do not race or incorrectly advance shared state. Preserve the connector's logical identity across intended restarts and verify the recovery behavior after worker rebalances.

Source database failover is separate from connector failover. The promoted source must expose the needed log history and compatible capture state. PostgreSQL logical slot continuity depends on the database version and explicit failover-slot configuration; do not assume every logical slot automatically survives promotion. [PostgreSQL logical slot failover](https://www.postgresql.org/docs/18/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION).

### WAL/binlog expiration and replication slot management

If a saved cursor points to source history that has been purged, the connector cannot resume that missing interval by retrying. Starting from the current position skips changes unless another durable copy covers the gap.

Check whether the interval exists in transport, raw archives, or a supported database log archive. If not, establish a new coordinated snapshot and CDC boundary. Current-state rebuilding can repair present rows, but cannot recover every lost historical change.

Inventory slots and readers, assign ownership, and remove abandoned slots only after confirming the recovery consequences. A slot is a retention mechanism, not an unlimited archive. Similar planning applies to MySQL binlog expiration and Oracle archive-log availability.

### Duplicate, missing, and out-of-order events

- **Duplicates:** use stable event identities, idempotent effects, and source-version checks.
- **Missing events:** investigate checkpoints, filtering, logging prerequisites, retention gaps, and failed/skipped records; reconcile against an aligned source view.
- **Out-of-order events:** preserve per-key order where possible or apply source-version comparisons. Arbitrary arrival order is unsafe for partial updates and noncommutative operations.

Source positions need not be numerically contiguous within a filtered table stream. A gap in LSN values does not itself prove a missing row change. Likewise, contiguous Kafka offsets prove broker consumption progress, not complete source capture.

Do not deduplicate by payload hash alone: two legitimate identical business events can have distinct identities and effects. For Datastream, generated event UUIDs alone are also insufficient across all rereads; use the documented source metadata and destination ordering behavior. [Datastream event metadata](https://docs.cloud.google.com/datastream/docs/events-and-streams).

### Poison records, DLQ, and quarantine

A poison record repeatedly fails decoding, validation, transformation, or apply. Record its source identity, schema, cursor, error category, and enough protected context to repair it. Preserve the original event securely when needed for replay.

A dead-letter queue (DLQ) keeps failures from disappearing, but skipping a failed event changes completeness and possibly transaction atomicity. If correctness requires strict ordering, halt the affected stream or key. If the contract permits quarantine and continued processing, track the unresolved gap explicitly and keep dependent records consistent.

After repair, replay with the original identity and ordering information. Alert on oldest unresolved record and affected keys/tables, not just DLQ count. Apply appropriate encryption, access control, and retention to quarantined sensitive data.

### Disaster recovery and resnapshot strategies

Recover configuration, secrets through the approved secret store, offsets, schema history, broker data, destination state, and deployment definitions as required. Test restoration in a separate environment rather than assuming a saved configuration is enough.

Choose recovery scope deliberately: replay a retained interval, reload a table, repair a key range, or rebuild a full generation. Fence writers during recovery and publish the rebuilt state only after validation. A fresh snapshot merged into old data must also remove keys absent from the new source state.

Resnapshotting restores a baseline, but it can be expensive and does not restore unavailable event history. Preserve raw changes separately when auditability or historical reconstruction requires them.

## 9. Data Validation & Reconciliation

### Schema parity and row-count validation

Before comparing row values, establish whether the source and destination represent the same dataset. Check the required tables and columns, data types, nullability, keys, expected mappings, and intentional omissions. A heterogeneous destination may use different SQL type names; what matters is whether the types preserve the intended meaning.

Counts are a cheap first check, but equal counts can hide one missing row and one extra row. Run comparisons at an aligned snapshot or commit boundary; comparing a changing source with a lagging destination produces false positives and can conceal real differences.

For continuous checks, use a coordinated validation protocol, source snapshots with aligned target watermarks, or delayed/repeated checks under a documented convergence window. Waiting a fixed number of seconds alone does not establish equivalent states.

### Primary-key uniqueness and missing/extra records

Verify destination key uniqueness independently, especially when the engine does not enforce declared constraints. Compare source and target key sets to find missing keys, extra keys, and duplicates.

For large tables, compare ranges or stable hash buckets instead of exporting the entire key set at once. Include composite-key encoding and null rules. Fetch exact records for mismatched ranges to distinguish lag, deletion handling, and incorrect values.

### Checksums, hashes, and bucketed reconciliation

A scalable comparison can use:

1. Assign rows to deterministic buckets, such as `hash(primary_key) mod N`.
2. Canonically encode compared fields with explicit types, lengths, null markers, decimal rules, and timestamp normalization.
3. Compute counts and deterministic digests for each bucket at equivalent source/target boundaries.
4. Compare bucket summaries and inspect exact keys/rows in mismatched buckets.

Do not hash naive concatenation: `('ab', 'c')` and `('a', 'bc')` can produce the same bytes. Unordered hash aggregation needs a deliberate design; XOR can cancel duplicate pairs. Ordered cryptographic digests reduce collision risk but cost sorting or another deterministic construction. Hash matches provide strong evidence under their assumptions, not a mathematical proof of equality.

Hashing is often CPU-intensive and can load the source. Start with low-cost summaries, run bounded checks, and increase detail where discrepancies appear.

### Insert/update/delete propagation validation

Use a controlled fixture or approved canary dataset with a known key:

1. Insert a record and verify the expected destination state.
2. Update several types of fields, including a nullable field, and verify the newer version.
3. Delete it and verify removal or the defined soft-delete representation.
4. Replay duplicate and older events; verify they cannot undo the latest state.
5. Repeat across connector restart and, where required, a multi-table transaction.

Test primary-key changes and transitions into/out of filters separately. Keep canaries outside business reporting or mark them explicitly. A successful insert-only smoke test says little about delete or recovery correctness.

### Data type, NULL, precision, and timezone parity

Include boundary cases: maximum/minimum integers, decimals at the supported precision, fractional seconds, daylight-saving transitions, non-ASCII text, empty strings, SQL `NULL`, missing fields, large objects, and unusual binary values.

Compare normalized semantics where formats differ. Two timestamp strings can represent the same instant; equal-looking local times can represent different instants. Round-trip a decimal exactly rather than comparing floating-point approximations. Investigate truncation and unsupported-value fallbacks explicitly.

### Continuous reconciliation and drift detection

Combine frequent inexpensive checks with rotating deep checks. Prioritize recently changed partitions, critical tables, and known high-risk types. Keep records of validation boundaries, affected ranges, discrepancy age, and repair status.

Detect both structural drift and content drift. Alert on sustained unexplained discrepancies, uniqueness violations, delete failures, and freshness breaches. A running connector and an empty error log do not prove replication correctness.

Repairs should use the authoritative source at a defined boundary and the normal ordering/idempotency rules. Blindly overwriting destination rows can race with CDC and introduce another divergence.

## 10. Production & Advanced Topics

### PostgreSQL logical replication, MySQL binlog, and Oracle redo

A useful way to ground a PostgreSQL replication discussion is to follow an update from WAL to a readable destination row. Explain publication/subscription or logical decoding, the replication slot and resume position, replica identity, retained WAL, and how this differs from physical streaming replication. Before making a logical replica writable at cutover, separately coordinate DDL and prepare sequences. [PostgreSQL logical replication restrictions](https://www.postgresql.org/docs/18/logical-replication-restrictions.html).

For MySQL, explain row-based binlog capture, row image requirements, binlog positions versus GTIDs, expiration, and the continuity checks required after source failover. Statement-based replication can depend on execution context and determinism. [MySQL replication formats](https://dev.mysql.com/doc/refman/8.4/en/replication-formats.html).

For Oracle, explain redo/archive retention, supplemental logging, SCNs, transaction boundaries, and the capture method's supported types and privileges. An SCN is useful for source consistency and restart, but row events still need their full documented identity/order metadata. [Oracle LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html).

### Distributed transactions and cross-table consistency

One local source transaction can already cross tables and Kafka partitions. A distributed transaction can additionally span databases or shards, each with its own log and recovery cursor.

Independent CDC connectors do not automatically reconstruct a global atomic commit. Even when the source uses two-phase commit, downstream readers need transaction metadata and a coordinated visibility mechanism to avoid partial results.

Options include a target with suitable multi-table transactions, a transaction-aware apply coordinator, a consistent published batch, or a documented eventual-consistency contract. Buffering waits for missing participants and increases state and latency; define timeout and recovery behavior.

### Multi-region replication, RPO, and RTO

- **Recovery Point Objective (RPO):** maximum acceptable data loss measured relative to the failure point, usually as time.
- **Recovery Time Objective (RTO):** maximum acceptable time to restore the required service.

Asynchronous replication risks losing acknowledged changes not durably available in the recovery region. Average lag is not a bound on worst-case data loss. Synchronous cross-region replication can reduce RPO under stated failure assumptions, while increasing commit latency and potentially blocking writes when coordination is unavailable.

RTO includes detection, fencing, promotion, log replay, endpoint changes, application recovery, and validation. A standby that exists but has never been promoted in a drill is weak evidence for a short RTO.

### CDC into Iceberg, Delta Lake, Snowflake, and BigQuery

| Destination | Typical apply pattern | Important caveat |
| --- | --- | --- |
| Apache Iceberg | A supported engine applies upserts/deletes through merges or streaming changelog writes | Table format, equality keys, engine support, checkpoint commits, and file maintenance matter |
| Delta Lake | Apply CDC through ordered `MERGE`/stream processing; optionally expose downstream Change Data Feed | Deduplicate/order input and make retries safe; CDF tracks Delta changes rather than capturing the original database log |
| Snowflake | Land CDC in staging, then apply ordered changes; streams/tasks can process changes within Snowflake | A Snowflake stream is a table-change tracking offset, not an external database log reader |
| BigQuery | Datastream integration or supported CDC ingestion through the Storage Write API | Supply correct key/sequence semantics and monitor background apply and query freshness |

Iceberg's documented Flink upsert path requires the supported table format and equality fields, with partition-field requirements for partitioned tables. Atomic commits to one table do not automatically create atomic visibility across several tables. [Iceberg Flink writes](https://iceberg.apache.org/docs/latest/flink-writes/).

Delta Change Data Feed records row changes after it is enabled and is subject to retention. It is not an unlimited audit archive. For current-state merges, reduce multiple changes for a key to the correct latest state only when intermediate events are unnecessary; historical or additive models need those events. [Delta Change Data Feed](https://docs.delta.io/delta-change-data-feed/).

Snowflake standard streams expose the net row changes between offsets, so they need not preserve every intermediate source transition. Consume them before staleness makes required history unavailable. [Snowflake streams](https://docs.snowflake.com/en/user-guide/streams-intro).

BigQuery CDC ingestion uses upsert/delete operations and supports user-supplied `_CHANGE_SEQUENCE_NUMBER` values for ordering. Query freshness and background application must be considered separately from successful ingestion. [BigQuery CDC ingestion](https://docs.cloud.google.com/bigquery/docs/change-data-capture).

### Monitoring, lag metrics, and alerting

Monitor the whole path:

| Layer | Useful signals | What failure can look like |
| --- | --- | --- |
| Source | Log generation/retention, disk free, slot progress, oldest transaction | Disk exhaustion or required logs expiring |
| Capture | Last successful read/checkpoint, reconnects, task state, heartbeat | Connector runs but makes no durable progress |
| Transport | Consumer lag, oldest unconsumed data, retention headroom | Downstream backlog outlives replay retention |
| Apply | Commit rate/latency, retries, rejected records, merge duration | Healthy capture with stale destination tables |
| Business data | Freshness, reconciliation drift, missing deletes, key uniqueness | Silent corruption despite healthy infrastructure |

Alert using explicit freshness and recovery objectives. Track the oldest unapplied change and retention headroom, not only average latency. For DMS, `CDCLatencySource` and `CDCLatencyTarget` help distinguish capture and end-to-target delay, but investigate task state and workload alongside the metric. [DMS latency troubleshooting](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Troubleshooting_Latency.html).

### Security, encryption, and least privilege

Grant only the permissions needed for selected-table reads, snapshotting, log capture, publications/slots, transport writes, and destination apply. Some databases require elevated capture privileges; isolate that identity rather than sharing application credentials. Keep secret rotation compatible with reconnect and recovery.

Encrypt transport with certificate validation and encrypt stored logs, broker data, raw files, checkpoints, and quarantine records as required. Restrict network paths and audit access and configuration changes.

CDC can expose before-images and deleted values that an ordinary current-state query no longer returns. Apply minimization at a trusted boundary, restrict sensitive topics/raw zones, and include replicas, history, and DLQs in deletion/retention policies. Masking a warehouse view does not secure the raw change stream behind it.

### Recovery after prolonged outages

Use this decision sequence:

1. Fence conflicting writers and identify the last durable source, transport, and destination boundaries.
2. Verify which required intervals still exist in source logs, transport, or durable archives.
3. If complete history is available, replay with the original identities/order and enough spare capacity to catch up.
4. If history is incomplete, explicitly identify the gap; rebuild affected current state with a coordinated snapshot and new CDC boundary.
5. Remove stale target-only keys, reconcile, and publish the recovered state.
6. Report unrecoverable historical gaps and adjust retention or capacity based on the incident.

A source slot may have preserved the entire outage backlog but nearly filled the database disk. A broker may have preserved downstream history even after source logs expired. Recovery depends on the actual durable coverage, not the nominal retention setting alone.

### Replication vs. backup and disaster recovery trade-offs

Replication improves availability and can reduce recovery time, but it quickly copies accidental deletes, corrupted application writes, and malicious changes. It is not a replacement for independent, retained backups and tested point-in-time recovery.

Backups preserve recovery points; they require restore and possibly log replay, which affects RTO. Replicas provide ready or nearly ready serving capacity; asynchronous ones can lag, and synchronous ones can share failure or compromise paths.

Use replication, isolated backups, retained recovery logs, access separation, and restoration drills according to the required failure model. Verify recoverability after operator error and credential compromise as well as infrastructure failure.

**Interview scenario:** “Design replication from a busy OLTP database to a warehouse with a five-minute freshness target.” A strong answer establishes keys and delete semantics, chooses a coordinated snapshot plus CDC, defines durable checkpoints and version-aware apply, budgets retention and catch-up capacity, validates at aligned boundaries, and describes both outage recovery and alerting. State whether cross-table atomicity and complete event history are required before choosing parallelism and storage layouts.
