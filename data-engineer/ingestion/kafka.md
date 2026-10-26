# Apache Kafka: Data Engineer Interview Guide

When an order is placed, billing, fulfillment, and analytics may all need the event at different speeds. Kafka lets those applications read independently from ordered, replicated logs and revisit retained history. Understanding it starts with a few practical questions: where is ordering guaranteed, who records progress, and what survives when a producer, consumer, or broker fails?

**Reference baseline:** Apache Kafka 4.1 and the official Java clients unless stated otherwise. This is an explicit documentation baseline, not a claim about the latest release. Client libraries and managed services can have different defaults and feature support. Examples are illustrative; sizing requires workload measurements. Kafka 4.x uses KRaft; ZooKeeper belongs to older deployments. [Kafka introduction](https://kafka.apache.org/41/getting-started/introduction/), [upgrade guide](https://kafka.apache.org/41/getting-started/upgrade/).

## Contents

1. [Architecture & Fundamentals](#1-architecture--fundamentals)
2. [Producers](#2-producers)
3. [Consumers](#3-consumers)
4. [Storage & Replication](#4-storage--replication)
5. [Delivery & Processing Semantics](#5-delivery--processing-semantics)
6. [Performance & Scalability](#6-performance--scalability)
7. [Kafka Ecosystem](#7-kafka-ecosystem)
8. [Security & Operations](#8-security--operations)
9. [Production & Advanced Topics](#9-production--advanced-topics)

## 1. Architecture & Fundamentals

### Brokers, topics, partitions, and replicas

Follow an order event into Kafka. Its topic gives the stream a name, its partition gives it an ordered position, and replicas keep copies on brokers. Those layers have distinct jobs; separating them makes both scaling and failure behavior easier to reason about.

| Concept | Meaning | Interview implication |
| --- | --- | --- |
| Broker | Server that stores partition replicas and serves Kafka requests | One broker can lead some partitions and follow others |
| Topic | Named event stream, such as `orders` | Configuration includes retention, cleanup policy, and partition count |
| Partition | Ordered log within a topic | Ordering and offsets are scoped to a partition |
| Replica | Copy of a partition on another broker | Replication improves fault tolerance, consuming disk and network capacity |

A record typically has a key, value, timestamp, and optional headers. Kafka stores serialized bytes; applications define their meaning. The producer discovers partition leaders through bootstrap brokers, then sends directly to the relevant broker. Bootstrap addresses are discovery endpoints, not a routing layer through which every record passes.

```text
Producer, key=order_id
        |
        v
Topic: orders
  P0: broker A (leader) -> brokers B and C (followers)
  P1: broker B (leader) -> brokers A and C (followers)
  P2: broker C (leader) -> brokers A and B (followers)
        |
        +--> billing group:   consumer 1 owns P0/P1; consumer 2 owns P2
        +--> analytics group: consumer 3 owns P0/P1/P2
```

Both groups can read the same records independently. Consuming a record does not remove it. [Kafka concepts](https://kafka.apache.org/41/getting-started/introduction/).

### Producers, consumers, and consumer groups

Producers append records; consumers fetch them. A consumer group identifies a logical subscriber with its own progress. Within an ordinary subscribed consumer group, a partition belongs to one consumer at a time; a consumer can own several partitions. With three partitions and five consumers, at least two consumers have no partitions from that topic. Different groups can each read every partition.

Group coordination runs on brokers. A group coordinator manages membership and committed offsets; it is distinct from the KRaft controller. Manual partition assignment with `assign()` bypasses automatic group assignment, so the application owns assignment coordination. This guide describes ordinary consumer groups, rather than share-group consumption.

### Leaders, followers, and ISR

Each partition has a leader that accepts writes. Followers fetch the leader's log. The **in-sync replica set (ISR)** contains the leader and followers sufficiently caught up under Kafka's replication rules. ISR membership changes as replicas fail, fall behind, and recover; it is not the same as the configured replica list.

The controller coordinates partition leadership changes. A partition leader is a data-plane role; the active KRaft controller is a cluster metadata role. They are different responsibilities.

### Offsets, retention, and log segments

An offset identifies a position within one partition. `(topic, partition, offset)` identifies a stored record; offset 100 in partition 0 is unrelated to offset 100 in partition 1. A consumer's current position tracks its next fetch position, while its committed offset is its durable restart position.

Partitions are split into segment files. Retention removes eligible old segments according to time or size policy, independently of consumer commits. Kafka can delete data a slow consumer has never read. Offsets are not renumbered after deletion or compaction, so gaps are normal. [Kafka log design](https://kafka.apache.org/41/design/design/).

### KRaft architecture and metadata quorum

KRaft stores cluster metadata in a replicated Raft log. Controller voters elect an active controller; brokers consume metadata updates. Metadata includes topic definitions, partition assignments, and leadership state. User records remain in partition logs, not the metadata log.

A majority of controller voters must be available to make metadata progress. Three voters tolerate one failure; five tolerate two. Production deployments commonly separate controller and broker roles to isolate resources and failure domains. A quorum outage can leave existing data paths working temporarily, but prevents required metadata changes and failovers. [KRaft operations](https://kafka.apache.org/41/operations/kraft/).

**Interview question:** Does adding replicas increase consumer-group parallelism? No. Partitions determine assignment parallelism; replicas provide redundancy.

## 2. Producers

### Partitioning strategies and message keys

If an order is created, paid, and then cancelled, you may need those events processed in sequence. A message key such as `order_id` gives the producer a way to keep them together. With the default Java behavior, a non-null key is hashed to select a partition; routing stays consistent while the serialized key, partitioning rules, and partition count stay the same.

For records without keys, modern Java producers use batching-oriented sticky/adaptive partition selection rather than promising strict round-robin distribution. An explicit partition overrides normal selection; custom partitioners can implement application rules. Avoid custom routing unless the default cannot meet the requirement.

Adding partitions can change the partition chosen for future records of a key. Existing records stay where they are, so ordering across the migration needs a deliberate plan. A dominant key also remains a bottleneck even with many partitions. [Producer configuration](https://kafka.apache.org/41/configuration/producer-configs/), [partition expansion](https://kafka.apache.org/41/operations/basic-kafka-operations/).

### Acknowledgments, retries, and idempotence

| Setting | Successful send means | Failure exposure |
| --- | --- | --- |
| `acks=0` | Producer does not wait for a broker acknowledgment | Broker receipt is unknown; broker errors cannot drive reliable retries |
| `acks=1` | Leader accepted the record locally | Leader failure before replication can lose an acknowledged record |
| `acks=all` | Required ISR replication completed | Durability still depends on ISR policy and surviving replicas |

With `acks=all`, `min.insync.replicas` sets the minimum ISR size required for successful writes. It does not change the acknowledgment rule to “any N replicas.” With replication factor 3, minimum ISR 2, and all three replicas in ISR, the write waits for all three; with only one in ISR, writes fail. [Broker durability configuration](https://kafka.apache.org/41/configuration/broker-configs/).

A lost acknowledgment creates ambiguity: the broker may have stored a record even though the producer timed out. Retries can then duplicate it. Idempotence uses producer identity and per-partition sequence numbers to suppress duplicates from producer retries. It does not deduplicate two application calls that send the same business event, nor arbitrary replays across producer sessions.

In the baseline Java client, idempotence defaults to enabled when configurations do not conflict. It requires `acks=all`, retries enabled, and `max.in.flight.requests.per.connection <= 5`. Explicit incompatible settings can fail configuration validation. Check asynchronous send results; returning from `send()` alone does not prove broker acceptance. [Producer guarantees](https://github.com/apache/kafka/blob/4.1/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java).

### Batching, compression, linger, and buffer memory

| Control | Purpose | Tradeoff |
| --- | --- | --- |
| `batch.size` | Target batch capacity per partition | Larger batches help throughput when traffic fills them |
| `linger.ms` | Wait briefly for more records before sending a batch | Trades batching delay for fewer requests; Java default became 5 ms in Kafka 4.0 |
| `compression.type` | Compress record batches, for example with LZ4 or Zstandard | Saves network/storage at a CPU cost |
| `buffer.memory` | Budget for buffered producer records | Absorbs bursts but cannot fix sustained overload; not a total process-memory cap |
| `max.block.ms` | Bound waiting for metadata or buffer allocation during `send()` | Buffer exhaustion can surface as a caller-side timeout |
| `delivery.timeout.ms` | Bound the delivery attempt after `send()` returns | Includes batching and retries; must accommodate request timeout plus linger |

One broker request can carry batches for multiple partitions. Waiting synchronously after every record or flushing every send limits batching. Compression benefits from repeated values within a batch. [Producer configuration implementation](https://github.com/apache/kafka/blob/4.1/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java).

### Transactional producers and exactly-once semantics

A transactional producer can atomically commit Kafka writes across partitions and topics, optionally including consumer offsets. Set a stable `transactional.id` for a logical producer and use the transaction lifecycle. Downstream consumers need `isolation.level=read_committed` to hide aborted transactional records.

Transactions make Kafka output and Kafka progress atomic; they do not automatically include an external database write, HTTP call, or email. The application can execute processing again after a failure while still exposing one committed Kafka result. Section 5 explains the processing pattern and its limits.

### Ordering guarantees and delivery failures

Kafka provides partition order, not topic-wide order or event-time order. Multiple producers can interleave their writes. Without idempotence, retries with multiple requests in flight can reorder batches if a later batch succeeds before an earlier failed batch is retried.

Retry transient leader/network errors within a bounded delivery budget. Serialization errors, invalid records, and authorization failures require correction. A timeout can be an uncertain outcome; blindly resending a business operation can duplicate it. Use a stable event ID when application retries must be safe. [Producer API](https://github.com/apache/kafka/blob/4.1/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java).

**Interview question:** Does `acks=all` mean every configured replica acknowledged? No. It concerns the ISR, subject to minimum ISR enforcement.

## 3. Consumers

### Consumer groups and partition assignment

Suppose one analytics consumer cannot keep up with the orders topic. Starting another consumer with the same `group.id` lets the group share partitions, subject to available work and downstream capacity. Membership or partition changes can trigger reassignment. Under the classic protocol, client strategies include Range, RoundRobin, Sticky, and CooperativeSticky: Range can distribute multiple topics unevenly, while sticky strategies try to reduce movement.

More consumers help only if partitions and downstream capacity allow it. An application can add worker concurrency within a consumer, but then must preserve any required per-partition processing order and commit only completed work.

### Automatic versus manual offset commits

With `enable.auto.commit=true`, the client periodically commits consumed positions. It has no knowledge of external side effects completing. Handing records to background workers and continuing to poll can commit progress ahead of successful processing.

For manual commits, disable auto-commit and commit after durable processing. If offsets 40 through 44 are complete, commit **45**, the next offset to read. With concurrent processing, never commit past an unfinished record in that partition. `commitSync()` waits for success or failure; `commitAsync()` needs callback error handling and care around overlapping attempts.

If processing succeeds and the process crashes before committing, records are replayed. If the commit succeeds before processing, a crash can skip work. Handle partition revocation so workers do not keep processing or committing work they no longer own. [Consumer API and commit examples](https://github.com/apache/kafka/blob/4.1/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java).

### Rebalancing: eager, cooperative, and the newer protocol

| Mechanism | Assignment behavior | Operational effect |
| --- | --- | --- |
| Classic eager rebalance | Revoke assignments before redistribution | Group-wide processing interruption |
| Classic cooperative rebalance | Incrementally revoke partitions that must move | Less interruption; migration requires compatible assignors |
| New consumer protocol, KIP-848 | Broker computes assignments and members reconcile incrementally | Removes the classic global synchronization barrier |

KIP-848 became generally available in Kafka 4.0. In the 4.1 Java client, opt in with `group.protocol=consumer`; it is not the client default. Assignment uses server-side controls, with optional `group.remote.assignor`, rather than `partition.assignment.strategy`. CooperativeSticky under `classic` and the newer `consumer` protocol are different mechanisms. [Consumer rebalance protocol](https://kafka.apache.org/41/operations/consumer-rebalance-protocol/).

### Polling, heartbeats, and timeouts

`poll()` retrieves records and participates in consumer operation. Heartbeats show membership liveness; they do not prove business processing is making progress.

- `max.poll.interval.ms` limits time between poll calls; long processing can cause loss of ownership even while heartbeats are healthy.
- `max.poll.records` bounds records returned by a poll, helping keep processing within that interval. It does not directly cap network fetch sizes.
- Under `classic`, `heartbeat.interval.ms` controls heartbeat cadence and `session.timeout.ms` controls failure detection.
- Under `consumer`, broker settings `group.consumer.heartbeat.interval.ms` and `group.consumer.session.timeout.ms` control those timings.

Reduce batch work or use bounded workers before merely increasing timeouts. Longer timeouts delay recovery. The Java consumer is not thread-safe for general concurrent calls; coordinate consumer operations on its owning thread. [Consumer configuration](https://kafka.apache.org/41/configuration/consumer-configs/), [consumer threading rules](https://github.com/apache/kafka/blob/4.1/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java).

### Consumer lag, offset reset, and replay

Lag is usually an end-offset minus a consumer position or committed offset; dashboards differ. Offset distance is not always a count of business records because compaction, transactions, and control records introduce gaps. Track lag per partition, its growth rate, and event age. Infrequent commits can make committed-offset lag look worse than processing progress.

`auto.offset.reset` applies when a valid committed position is unavailable, such as a new group or an offset deleted by retention. `earliest` starts at retained history, `latest` at the current end, and `none` raises an error. It does not rewind a healthy existing group on every restart.

Replay using a separate group or a coordinated seek/reset of the existing group. Stop its consumers before an administrative offset reset, inspect the proposed offsets, and ensure downstream writes tolerate replay. Deleted history cannot be recovered through an offset reset. [Consumer configuration](https://kafka.apache.org/41/configuration/consumer-configs/).

**Interview question:** Can heartbeats be healthy while rebalances continue? Yes. Processing can exceed `max.poll.interval.ms` even though the process remains connected.

## 4. Storage & Replication

### Append-only logs, segment files, and indexes

Kafka can replay an old event because the record remains in a partition log until cleanup makes it eligible for removal. New record batches append to the active segment; older segments can later be retained or cleaned according to policy. Sparse offset indexes point to nearby byte positions, and time indexes help locate offsets by timestamp. A reader scans from that position—these are navigation aids for the log, not indexes over arbitrary payload fields.

Typical files include `.log`, `.index`, and `.timeindex`. Segment size and roll timing affect file count and retention granularity. “Append-only” describes normal writes; compaction rewrites eligible segments and recovery can truncate divergent tails. [Kafka implementation](https://kafka.apache.org/41/implementation/).

### Replication factor, ISR, and leader election

Replication factor counts all copies, including the leader. Followers replicate independently of application consumers. A slow application consumer increases consumer lag; a slow replica can shrink ISR and weaken availability or durability.

In the traditional model, clean election chooses a suitable ISR replica. **Version nuance:** Kafka 4.1 enables Eligible Leader Replicas (ELR) by default on new clusters. ELR tracks certain replicas outside the current ISR that remain safe to elect under strict minimum ISR rules. Therefore, “every election outside ISR is unclean” is not universally correct. [Eligible Leader Replicas](https://kafka.apache.org/41/operations/eligible-leader-replicas/).

### High watermark, last stable offset, and leader epochs

- **Log end offset (LEO):** next append offset for a particular replica; replicas can have different LEOs.
- **High watermark (HW):** boundary below which records are replication-committed and eligible for normal consumer reads. It can trail the leader's LEO.
- **Last stable offset (LSO):** for transactional reads, the earlier of HW and the first offset of an open transaction. `read_committed` consumers read below this boundary and filter aborted records.
- **Leader epoch:** generation identifying a partition's leadership history. Epoch information helps replicas detect divergence and truncate inconsistent tails after leadership changes.

Do not confuse replication commit, transaction commit, and consumer offset commit. They solve different problems. An open transaction can hold back later records in its partition for `read_committed` readers, even when those later records are not transactional. [Replication design](https://kafka.apache.org/41/design/design/), [consumer isolation behavior](https://github.com/apache/kafka/blob/4.1/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java).

### Log retention versus log compaction

| Cleanup policy | Purpose | Example |
| --- | --- | --- |
| `delete` | Remove old eligible segments by time or size | Keep seven days of click events |
| `compact` | Eventually remove superseded values for each key | Rebuild the latest customer state |
| `compact,delete` | Apply both mechanisms | Maintain a state stream with a bounded history horizon |

Compaction is asynchronous, so multiple values for a key can coexist. It preserves offsets and retained record order. A keyed null value is a tombstone marking deletion; tombstones themselves can expire. A rebuilding consumer must catch up within the relevant tombstone retention window to avoid retaining stale state.

`retention.bytes` is per partition, not a topic-wide budget. Time and size limits apply independently; either can remove eligible data. `compact,delete` can remove a key's last value through time/size retention, so it is not an unlimited state archive. [Topic cleanup configuration](https://kafka.apache.org/41/configuration/topic-configs/).

### Durability and unclean leader election

A common durability setup is replication factor 3, minimum ISR 2, and producer `acks=all`, with replicas spread across failure domains. One unavailable replica can still permit writes; two generally block them. If ISR falls below the minimum, even `acks=all` cannot make the partition writable by itself.

Unclean leader election allows a replica without the required safe history to become leader, potentially discarding acknowledged data. It exchanges data safety for availability. Conversely, disabling it can leave a partition unavailable until a safe replica returns.

Replication is not a backup: deletion, bad events, and bad retention settings can affect every copy. Acknowledgments also do not imply an `fsync` on every replica for every record; correlated power/storage failures remain a separate durability concern. [Broker replication settings](https://kafka.apache.org/41/configuration/broker-configs/).

**Interview question:** Is compaction immediate deduplication? No. It is background storage cleanup by key, not business-event deduplication before processing.

## 5. Delivery & Processing Semantics

### At-most-once, at-least-once, and exactly-once

A producer retry, a Kafka-to-Kafka transformation, and a payment API call involve different failure boundaries. A guarantee about one does not automatically cover the others. Before using terms such as at-least-once or exactly-once, trace the input, durable effect, and acknowledgment across the particular boundary you mean.

| Semantics | Typical processing arrangement | Crash outcome |
| --- | --- | --- |
| At-most-once | Commit input progress before the effect | Work can be lost; no replay for the committed input |
| At-least-once | Make the effect durable, then commit input progress | Replay can repeat the effect |
| Exactly-once effect | Atomically couple output with progress, or deduplicate atomically at the destination | Retried execution does not duplicate the committed effect |

Idempotent production addresses retry duplicates in Kafka writes. It does not atomically couple consuming, transforming, and publishing. Kafka transactions supply that coupling for Kafka data and offsets. [Producer transaction API](https://github.com/apache/kafka/blob/4.1/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java).

### Atomic Kafka processing with transactions

For a consume-transform-produce application, disable consumer auto-commit and configure a transactional producer. Conceptually:

```text
Initialize transactional producer once for this process generation.
Repeat:
  Poll input records.
  Begin transaction.
  Transform and produce output records.
  Add next input offsets and current consumer group metadata to transaction.
  Commit transaction.
On a recoverable abort:
  Abort as appropriate; return input positions to committed offsets before retry.
On a fatal error or producer fencing:
  Stop this producer instance and recover through a controlled restart.
```

The Java API uses `initTransactions()`, `beginTransaction()`, `sendOffsetsToTransaction(...)`, and `commitTransaction()`. Do not separately commit the same input offsets outside that transaction. Aborting does **not** automatically rewind the consumer's in-memory position. Recreate the consumer or explicitly seek to committed positions before reprocessing. Readers use `read_committed`.

This guarantees committed Kafka effects within the transaction boundary. Multiple output partitions do not become one globally ordered stream, and a consumer poll is not a cross-partition atomic snapshot. [Transactional producer](https://github.com/apache/kafka/blob/4.1/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java), [Kafka delivery semantics](https://kafka.apache.org/41/design/design/).

### External outputs, duplicates, and deduplication

For database sinks, use a unique event ID and atomically insert that ID with the business update in one database transaction. On replay, a uniqueness conflict can suppress the repeated effect. Alternatively, store input offsets with output in the destination transaction and make that store authoritative for recovery.

An upsert is safe only if its semantics match replay: setting a versioned balance can be idempotent; incrementing a balance twice is not. A repeated older update can overwrite newer state unless version checks prevent it. Deduplication state needs a retention policy covering the supported replay horizon.

For application-to-Kafka dual writes, a transactional outbox writes the business change and outbox row in the same database transaction; CDC publishes the row. It closes the database/Kafka dual-write gap, while downstream consumers still need duplicate handling. [Debezium outbox pattern](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html).

### Ordering, retry patterns, and dead-letter queues

Immediate retries preserve partition processing order but block later records. Retry topics let later records continue, so they can break per-key order when a failed event returns later. Kafka does not inherently schedule arbitrary delayed delivery; a retry consumer or framework must enforce delay.

A dead-letter queue (DLQ) holds events that exceeded a deliberate retry policy or cannot be processed. Publish the failed event durably before committing past it. Kafka transactions can couple DLQ publication with the source offset; without that coupling, handle possible duplicate DLQ entries. A DLQ needs ownership, monitoring, and a safe redrive process.

**Interview question:** Does Kafka exactly-once mean a payment API is called once? No. Use a payment idempotency key or another destination-supported atomic mechanism.

## 6. Performance & Scalability

### Partition count, throughput, and parallelism

If consumers need more parallel work, additional partitions can provide independent appends and assignments. They also add metadata, files, replication work, and recovery overhead. Estimate the useful count from measured per-partition throughput and consumer processing capacity, then validate how the system behaves during failure.

For example, if one consumer can durably process 5,000 records/second and input is 30,000 records/second, at least six equally loaded processing slots are needed before headroom. A single hot key can invalidate this estimate. Increasing partition count changes future key routing and cannot be reversed in place; adding brokers alone does not automatically move all existing partitions. [Partition operations](https://kafka.apache.org/41/operations/basic-kafka-operations/).

### Producer batching, compression, and tuning

Measure record size, records per batch, compression ratio, request latency, retries, and buffer exhaustion. Improve batching and compression before changing durability settings. Large batches help throughput only if traffic fills them or acceptable linger permits accumulation. Excessive partitions per producer can spread low traffic across many tiny batches.

Tune against an end-to-end latency objective, not the lowest possible producer latency alone. A little batching delay can reduce broker load enough to improve overall tail latency. [Producer configuration](https://github.com/apache/kafka/blob/4.1/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java).

### Consumer fetch optimization

| Setting | Effect |
| --- | --- |
| `fetch.min.bytes` | Prefer larger responses before returning; improves efficiency at low traffic at a latency cost |
| `fetch.max.wait.ms` | Bound broker waiting when the minimum bytes are unavailable |
| `fetch.max.bytes` | Target upper bound for a fetch response |
| `max.partition.fetch.bytes` | Target upper bound per partition in a fetch |
| `max.poll.records` | Bound records handed to the application per poll |

Fetch limits are not absolute process-memory caps: Kafka can return an oversized first record batch to make progress, and clients may have multiple fetches and decompressed objects in memory. Increase fetch sizes only when network request overhead is the bottleneck. [Consumer fetch settings](https://kafka.apache.org/41/configuration/consumer-configs/).

### Broker disk I/O, page cache, and network

Sequential appends, batched requests, and the operating system page cache are central to Kafka performance. Recent records can often be served from cache. Historical replay can force disk reads and compete with normal traffic; replication and compaction also consume I/O. Reserve memory for page cache rather than assigning all RAM to JVM heap.

Network sizing must include replication and every subscribing group's reads. As a rough local-storage estimate, `stored bytes ≈ stored ingress bytes/sec × retention seconds × replication factor`, before indexes, temporary copies, and headroom. Compression, compaction, and tiered storage alter the estimate. [Kafka efficiency design](https://kafka.apache.org/41/design/design/).

### Hot partitions and partition reassignment

A hot partition can come from skewed keys, oversized records, or unusually expensive processing. Inspect per-partition traffic and lag before scaling the entire group. Salting a key can distribute load but sacrifices straightforward per-key ordering and may require a later aggregation step.

Reassignment moves partition replicas between brokers; it does not split a hot key or repartition the records. Throttle migration, preserve rack/zone placement, monitor replica catch-up, and remove temporary throttles afterward. Balance load by bytes and traffic, not partition count alone. [Replica reassignment](https://kafka.apache.org/41/operations/basic-kafka-operations/).

**Interview question:** Why did doubling consumers not improve throughput? Check partition ownership, key skew, broker limits, and downstream capacity before adding more instances.

## 7. Kafka Ecosystem

### Kafka Connect: sources, sinks, workers, and tasks

Moving records between Kafka and another system does not always need a custom consumer application. **Kafka Connect** runs connectors that integrate with external systems. A connector defines configuration and divides the work; its **tasks** perform the transfer, and **workers** host those connectors and tasks. Keeping those roles separate helps explain what can actually run in parallel.

Distributed workers coordinate and redistribute tasks on failure. `tasks.max` is an upper bound, not a promise of that many useful tasks: source parallelism depends on connector capabilities, and sink parallelism is constrained by input partitions. Distributed Connect persists configuration, status, and source progress in internal Kafka topics.

Converters handle serialization; single message transforms (SMTs) make small per-record changes. Connect is not a general join engine. Exactly-once support depends on the connector and destination; framework support alone does not make every source or sink exactly-once. [Connect user guide](https://kafka.apache.org/41/kafka-connect/user-guide/), [source exactly-once configuration](https://kafka.apache.org/41/configuration/kafka-connect-configs/).

### Kafka Streams: state stores, windowing, and joins

Kafka Streams is a Java library embedded in applications, not a separate broker service. `KStream` models events; `KTable` models keyed updates to a table. Local state stores hold aggregates and join state. Changelog topics permit state restoration; standby replicas can reduce recovery time. [Streams recovery](https://kafka.apache.org/41/streams/developer-guide/running-app/).

Window types include tumbling (fixed, non-overlapping), hopping (fixed, overlapping), sliding (event-relative), and session (activity separated by inactivity gaps). Event-time processing uses record timestamps; grace periods define allowed lateness relative to stream time. Wall-clock waiting alone does not necessarily advance stream time.

A stream-stream join matches events by key within a time window; a stream-table join enriches events from table state. Normal key joins require compatible key partitioning and partition counts; some operations create repartition topics. Global tables and foreign-key joins have different arrangements. State size depends on key cardinality, window retention, and lateness. Use `processing.guarantee=exactly_once_v2` for supported transactional Streams processing. [Streams DSL](https://kafka.apache.org/41/streams/developer-guide/dsl-api/).

### Schema Registry: Avro, Protobuf, and JSON Schema

Schema Registry is an ecosystem component, not a built-in Kafka broker schema validator. It stores schema versions under subjects and checks configured compatibility. Serializers usually include a schema identifier with encoded data; compatible deserializers resolve the writer schema.

- **Avro:** compact binary encoding with writer/reader schema resolution; defaults matter for evolution.
- **Protobuf:** binary messages with numbered fields; avoid reusing removed field numbers.
- **JSON Schema:** defines constraints for JSON values; JSON text alone does not enforce a schema.

**Backward** compatibility lets new readers read older data. **Forward** lets old readers read newer data. **Full** requires both. Transitive modes check the relevant complete version history rather than only the previous schema. Compatibility rules differ by format; compatible structure does not guarantee compatible business meaning. Coordinate rollout order with the chosen mode. [Schema evolution](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html).

### CDC with Debezium

Debezium commonly runs as Kafka Connect source connectors reading database change logs, such as MySQL binlogs or PostgreSQL logical replication. A typical pipeline takes an initial snapshot and then streams changes from tracked source positions. Events can include before/after values, operation type, source metadata, and transaction information.

Monitor connector lag and source-log retention together: losing the required database log position can force resnapshotting. PostgreSQL replication slots can retain WAL and consume database disk when capture falls behind. Handle deletes, snapshot overlap/recovery, schema changes, and duplicates explicitly. Ordering is not automatically global across tables/topics. [Debezium architecture](https://debezium.io/documentation/reference/stable/architecture.html), [PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).

### Kafka versus traditional message queues

| Concern | Ordinary Kafka consumer groups | Typical work queue |
| --- | --- | --- |
| Progress | Per-partition offset position | Per-message acknowledgment |
| Retention | Independent of consumption | Acknowledged messages commonly leave the queue |
| Replay | Read retained offsets again | Often requires separate storage or redrive facilities |
| Fan-out | Separate groups read independently | Separate queues/bindings often implement fan-out |
| Scheduling | Partition ownership and ordered logs | Per-message distribution, priorities, or delays may be native |

Choose according to required replay, ordering, throughput, routing, and task scheduling. Products overlap; neither category universally replaces the other.

**Interview question:** Is Schema Registry enough to prevent all bad events? No. Producers can bypass its serializers, and schema checks do not validate every business rule.

## 8. Security & Operations

### TLS, SASL, ACLs, authentication, and authorization

A client that reaches a Kafka listener still needs to prove who it is and receive permission for the operation. **Authentication** establishes identity; **authorization** controls actions. TLS encrypts connections and can authenticate clients with mutual TLS. SASL supports mechanisms such as SCRAM, Kerberos, and OAuth-based authentication, but SASL alone does not encrypt transport; combine it with TLS as appropriate.

ACLs grant operations on resources such as topics, groups, and transactional IDs. A consumer needs topic and group access; a transactional producer also needs appropriate transaction permissions. Use separate service identities and least privilege. Secure inter-broker and controller communication as well as client listeners.

Clients discover broker addresses through metadata: reaching a bootstrap endpoint is insufficient if `advertised.listeners` returns unreachable names or TLS certificates do not match. Protect Connect REST endpoints, schema services, secrets, and monitoring endpoints too. [Kafka security](https://kafka.apache.org/41/security/).

### KRaft quorum management and broker failures

Monitor controller leadership, quorum replication lag, and voter availability. Kafka 4.1 supports migration from static to dynamic controller membership with the appropriate feature level. Static configuration uses `controller.quorum.voters`; dynamic discovery uses `controller.quorum.bootstrap.servers`. Check the actual quorum mode and feature versions before changing membership.

For a dynamic quorum, let a new controller catch up before adding it as a voter; preserve majority availability during removals and maintenance. Never reformat a live node's storage as a routine failure fix. [KRaft membership management](https://kafka.apache.org/41/operations/kraft/).

For broker failure, establish whether partitions have safe leaders and sufficient ISR, inspect disk/network faults, and restore the broker or replace replicas deliberately. Avoid restarting additional brokers while replication health is degraded.

### Monitoring: lag, ISR, and under-replicated partitions

| Signal | What it tells you | First investigation |
| --- | --- | --- |
| Per-partition lag and event age | Processing delay | Hot key, processing time, sink latency, recent deployment |
| Under-replicated partitions | ISR smaller than assigned replica set | Failed broker, follower I/O, network, reassignment |
| Under-min-ISR partitions | Durability threshold cannot currently be met | Write failures and which replicas are unavailable |
| Offline partitions | No available leader | Safe replica availability and controller health |
| ISR shrink/expand rate | Replica instability | Disk stalls, network jitter, overloaded brokers |
| Request latency, errors, throttling | Client/broker pressure | Quotas, retries, request queues, disk/network saturation |
| Rebalance rate and duration | Assignment churn | Poll overruns, restarts, membership instability |
| Disk free space and growth | Time until capacity exhaustion | Retention, compaction backlog, ingress, replica movement |

Alert on sustained symptoms and service impact; a short under-replication spike during planned movement differs from offline partitions. [Kafka monitoring](https://kafka.apache.org/41/operations/monitoring/).

### Rolling upgrades and partition reassignment

Follow the exact source-to-target upgrade path, client/JVM compatibility requirements, and release notes. For KRaft, separate installing binaries from finalizing metadata/features. Restart nodes incrementally, preserve controller majority and required partition replication, and verify health before continuing. Feature finalization can restrict downgrade options.

Reassignment is a data-copy operation with temporary capacity costs. Plan free disk and network headroom, rate-limit movement, and verify completion. A cluster with no failure headroom is not safely maintained just because the upgrade is called “rolling.” [Upgrade guide](https://kafka.apache.org/41/getting-started/upgrade/), [reassignment operations](https://kafka.apache.org/41/operations/basic-kafka-operations/).

### Disaster recovery and multi-cluster replication

MirrorMaker 2 uses Connect-based components to copy data and maintain offset translation/checkpoints between clusters. Cross-cluster replication is commonly asynchronous, so local acknowledgment does not imply remote durability. Target offsets can differ from source offsets; do not copy numeric positions blindly.

Define recovery point objective (acceptable data loss) and recovery time objective (acceptable outage). Exercise failover and failback, offset translation, duplicate handling, and prevention of simultaneous conflicting writers. Recover schemas, identities, permissions, connector configuration, and applications alongside records. Independent backups are needed when deletion or corruption must not propagate to every recovery copy. [Kafka geo-replication](https://kafka.apache.org/41/operations/geo-replication-cross-cluster-data-mirroring/).

**Interview question:** Is replication factor 3 a disaster recovery strategy? It covers some local failures; regional loss and logical deletion require additional design.

## 9. Production & Advanced Topics

### Rebalancing storms and static membership

A consumer group may have enough capacity on paper yet spend much of its time stopping and redistributing partitions. That is a **rebalance storm**. Crashes, rolling deployments, unstable networks, coordinator pressure, and batches that exceed the poll interval can all contribute. Match rebalance timestamps to processing duration and deployment events before changing timeouts.

Static membership uses a stable, unique `group.instance.id` per logical consumer instance. It can reduce reassignment during short restarts, but duplicates are fenced and failed members can take longer to replace. It does not prevent all rebalances. Cooperative assignment or the newer consumer protocol reduces movement/interruption but does not fix an overloaded consumer. [Static membership configuration](https://kafka.apache.org/41/configuration/consumer-configs/), [rebalance protocol](https://kafka.apache.org/41/operations/consumer-rebalance-protocol/).

### Backpressure and slow consumers

Use bounded work queues and pause assigned partitions when workers or the sink are saturated, while continuing the consumer operations needed to retain membership. Resume after capacity recovers. Pausing does not extend Kafka retention, and already fetched records still consume memory.

For each partition, track a contiguous completed offset boundary. Do not commit the highest completed offset while an earlier record is still running. Scale only if partitions and sink capacity support it. If retained backlog is 1,000,000 records, arrivals are 10,000/sec, and processing is 12,000/sec, catch-up takes roughly `1,000,000 / (12,000 - 10,000) = 500 seconds`, assuming steady rates. Processing equal to ingress never clears the backlog.

### Poison messages, retry topics, and DLQ

A poison message fails repeatedly because of bad bytes, unsupported schema, invalid data, or deterministic application behavior. Separate those failures from transient dependencies. Deserialization can fail before the normal processing loop sees a record, so recovery must preserve the original bytes and exact source position.

Bound retries and use backoff with jitter. Capture source topic/partition/offset, event ID, error category, and attempt history in retry or DLQ records. Restrict access if payloads contain sensitive data. Do not commit past a failed record until its required effect or durable handoff succeeds. Redrive with deduplication and an explicit ordering policy; moving one event aside permits later events to overtake it.

### Transaction recovery and fencing

The transaction coordinator persists transaction state in Kafka's internal transaction topic and recovers that state after failure. A stable `transactional.id` lets a replacement producer initialize recovery and fence an obsolete producer generation. Each concurrently active logical producer needs its own transactional identity.

Fencing prevents a stale “zombie” instance from continuing writes under an identity now owned by its replacement. Treat producer fencing as fatal for that producer instance. Do not endlessly retry it. Long-running transactions can time out and delay `read_committed` visibility, so bound transaction duration and investigate processing stalls. [Producer transaction lifecycle](https://github.com/apache/kafka/blob/4.1/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java).

### Large messages and retention misconfiguration

Large records stress producer buffers, request limits, replication, consumer memory, and tail latency. If required, align producer `max.request.size`, broker/topic batch limits such as `message.max.bytes`/`max.message.bytes`, and follower/consumer fetch settings. These limits apply at different batch/request boundaries and sometimes before versus after compression; do not assume one universal message-size setting.

For large objects, consider storing an immutable object externally and publishing its reference, checksum, and metadata. Then manage authorization, object lifetime, and the consistency of object publication with the event. Chunking instead requires explicit reassembly and missing-chunk handling.

Retention errors can delete unread data or fill disks. Check effective topic overrides, time units, ingress growth, per-partition byte limits, and segment/cleaner behavior. Committed offsets do not protect data from retention, and increasing retention afterward cannot restore deleted records. Size retention for outage plus catch-up time with margin. [Topic limits and retention](https://kafka.apache.org/41/configuration/topic-configs/), [producer limits](https://kafka.apache.org/41/configuration/producer-configs/).

### Kafka on Kubernetes, MSK, and Confluent Cloud

| Deployment | Main benefit | Responsibilities that remain |
| --- | --- | --- |
| Kafka on Kubernetes | Declarative operations, often through an operator such as Strimzi | Persistent volume lifecycle/performance, stable identity, zone placement, disruption control, upgrades, recovery |
| Amazon MSK | AWS-managed Kafka infrastructure | Client networking/authentication, topic and application design, lag, capacity choices, service-specific limits |
| Confluent Cloud | Managed streaming service with ecosystem integrations | Keys/schemas, client behavior, access policies, quotas/costs, retention and recovery design |

On Kubernetes, spread replicas across nodes/zones, preserve controller quorum, and plan storage attachment and replacement. Pod recreation is not data recovery, and a disruption budget only governs supported voluntary disruptions.

For MSK, distinguish AWS control-plane permissions from Kafka data-plane permissions. MSK IAM access control, SASL/SCRAM, and mutual TLS have different policy requirements and support constraints. For either managed service, verify the selected service tier's limits and supported Kafka/client features rather than assuming every broker setting is exposed. [Strimzi deployment guidance](https://strimzi.io/docs/operators/latest/deploying), [MSK IAM action semantics](https://docs.aws.amazon.com/msk/latest/developerguide/kafka-actions.html), [Confluent Cloud overview](https://docs.confluent.io/cloud/current/overview.html).

**Interview exercise:** Design an orders pipeline from a transactional database to Kafka and a warehouse. Explain the CDC/outbox boundary, key choice, schema evolution, partition sizing, offset commits, destination deduplication, retry ordering, retention budget, and regional recovery. For every claimed guarantee, name the failure it handles and the system boundary it covers.
