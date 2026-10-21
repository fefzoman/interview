# Core Engineering Concepts: Data Engineer Interview Guide

Core engineering knowledge explains why a pipeline works, where it fails, and which trade-offs a design makes. In an interview, connect a definition to a concrete workload, a failure scenario, and an observable result.

**Scope:** Platform-neutral fundamentals with selected database, streaming, storage, and Kubernetes examples. Product behavior depends on version and configuration; linked papers and official documentation support the implementation-specific details. Examples and sizing calculations illustrate reasoning rather than prescribe production settings.

**Running example:** An online store writes orders to a transactional database. Change data capture publishes order changes to a broker. Processing jobs validate and enrich events, store analytical tables, and calculate revenue. The pipeline must handle duplicate events, late changes, worker failures, and growing traffic without counting an order twice.

## Contents

1. [Distributed Systems Fundamentals](#1-distributed-systems-fundamentals)
2. [Distributed Data Processing](#2-distributed-data-processing)
3. [Database Engine Internals](#3-database-engine-internals)
4. [Database Transactions & Consistency](#4-database-transactions--consistency)
5. [Data Storage & File Systems](#5-data-storage--file-systems)
6. [Query Optimization](#6-query-optimization)
7. [Performance Engineering](#7-performance-engineering)
8. [Data Infrastructure & Architecture](#8-data-infrastructure--architecture)
9. [Networking & Infrastructure](#9-networking--infrastructure)
10. [Reliability & Production Engineering](#10-reliability--production-engineering)
11. [Advanced Engineering Trade-offs](#11-advanced-engineering-trade-offs)

## 1. Distributed Systems Fundamentals

### Scalability, horizontal vs. vertical scaling

**Scalability** is the ability to handle more work while meeting service objectives as resources increase. Describe the workload first: records per second, dataset size, concurrent queries, or state size. **Elasticity** is the ability to adjust capacity as demand changes.

| Approach | What changes | Benefits | Limits |
| --- | --- | --- | --- |
| Vertical scaling, or scale up | More CPU, RAM, or faster storage on one machine | Simple topology; larger working sets fit in memory | Hardware ceiling, larger failure impact, possible restart |
| Horizontal scaling, or scale out | More machines or workers | More aggregate capacity; work can survive individual node failures | Requires partitioning, coordination, and management of skew |

Adding workers helps only if work can be divided and the bottleneck can use them. Doubling executors does little for a single hot key, a serial coordinator, or an already saturated storage service. Scaling reads with replicas also introduces routing and freshness considerations.

**Example:** Scale up when a hash join narrowly exceeds one worker's memory; consider scaling out when many independent partitions can run concurrently. Measure spill, cost, and runtime before choosing.

### CAP theorem, consistency vs. availability

During a network partition, a distributed read/write service cannot guarantee both **linearizable consistency** and **availability** under the CAP model. Consistency means operations behave as if there were one copy and respect real-time order. Availability means every request reaching a non-failing node eventually gets a valid response; an error is not successful fulfillment of the operation.

If two replicas cannot communicate, accepting conflicting writes on both can violate consistency. Refusing some operations protects consistency but sacrifices availability. Partitions are a failure condition to handle, rather than a feature a networked system can simply omit.

Avoid the slogan “pick any two.” The trade-off concerns behavior during partitions, can differ by operation, and does not describe all normal-operation latency choices. CAP availability also differs from an operational uptime percentage. [Gilbert and Lynch: Perspectives on the CAP Theorem](https://groups.csail.mit.edu/tds/papers/Gilbert/Brewer2.pdf).

### Strong, eventual, causal consistency

| Model | Guarantee | Example and limitation |
| --- | --- | --- |
| Strong consistency, specifically linearizability here | Each operation takes effect at a point between invocation and response; real-time order is preserved | A read started after a completed inventory update sees that update or a later one |
| Eventual consistency | If updates stop and propagation succeeds, replicas converge | A replica may temporarily show an older order status; the model alone gives no staleness bound |
| Causal consistency | Observers see causally dependent updates in dependency order | A reply is not visible before the message it replies to; independent updates can appear in different orders |

“Strong” is ambiguous without a defined contract. **Read-your-writes** and **monotonic reads** are useful session guarantees, but neither alone implies linearizability. Eventual convergence also needs a conflict-resolution rule when concurrent writes disagree.

Causality comes from program order, communication, and transitive dependencies. Wall-clock timestamps alone do not establish it because clocks can differ. Logical clocks help represent ordering; a larger Lamport timestamp does not prove that one event caused another. [Lamport: Time, Clocks, and the Ordering of Events in a Distributed System](https://lamport.azurewebsites.net/pubs/time-clocks.pdf).

### Consensus algorithms: Raft, Paxos

**Consensus** lets nodes agree on a value despite failures. Repeated agreement can establish a replicated log whose commands are applied in the same order by deterministic state machines.

**Raft** separates leader election, log replication, and safety. A leader operates in a numbered term and replicates entries to followers. Elections require a majority and restrict which logs can win. Committing an entry from the leader's current term requires majority replication; earlier entries become committed through the protocol's rules. A follower's acknowledgment alone is not a commit.

**Paxos** uses proposers, acceptors, and learners. Prepare/promise and accept phases use numbered proposals and intersecting quorums so a later proposal preserves an already chosen value. Basic Paxos chooses one value; Multi-Paxos applies the idea to a sequence and can reduce overhead with a stable leader.

Both protect safety in their crash-failure models despite delays and message loss; progress needs enough reachable participants and suitable timing conditions. They do not tolerate arbitrary malicious behavior by default. [Raft paper](https://raft.github.io/raft.pdf), [Paxos Made Simple](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf).

### Leader election, quorum, replication

A **leader** coordinates writes or metadata changes. Election needs a protocol that prevents conflicting committed histories, rather than merely choosing the node with the smallest ID. Terms or epochs distinguish successive leaders; fencing tokens let a downstream resource reject actions from an obsolete leader.

A **quorum** is a set of participants sufficient for a protocol decision. With a fixed membership of `N = 2f + 1` voting replicas, a majority protocol can tolerate `f` unavailable voters while still making progress, assuming the remaining majority communicates. Five voters need three; losing three stops majority decisions even if two nodes remain healthy.

**Replication** maintains multiple copies. Synchronous acknowledgment can protect acknowledged writes against specified failures but adds coordination latency. Asynchronous replication lowers foreground latency while allowing lag and possible loss on failover.

For replicated registers, `R + W > N` creates read/write quorum overlap under fixed-set assumptions. That inequality alone does not guarantee linearizability: concurrent writes, version selection, failure handling, and membership changes still need a correct protocol.

### Network partitions, partial failures, split-brain

A **network partition** prevents some nodes from communicating while others may work normally. A **partial failure** affects only part of a system: one worker, dependency, request path, or availability zone. A timeout cannot distinguish a failed server from a slow response or lost reply.

**Split-brain** occurs when multiple nodes act as authoritative leaders without effective coordination. Heartbeat loss can make both sides believe the other failed. A lease helps only with its timing assumptions; fencing must prevent an expired lease holder from continuing to mutate shared resources.

**Example:** The database commits an order but its response disappears. The caller sees a timeout even though the order exists. Retrying with a new order ID creates a duplicate. Preserve the operation ID and query or retry using that identity.

**Interview check:** In a five-node cluster split into groups of three and two, explain which side can commit majority-based writes, what the minority returns, and how an old leader is fenced.

### Fault tolerance, retries, idempotency

**Fault tolerance** means continuing to meet a defined contract despite specified failures. Redundancy helps only when copies occupy independent failure domains and recovery works. Three replicas on one failed host provide little host-level protection.

A **retry** repeats an operation after a transient failure or uncertain outcome. It increases load and may repeat a successful side effect. Set timeouts, cap attempts, use backoff with jitter, and retry only errors that can reasonably recover.

An operation is **idempotent** when repeating it has the same intended effect as applying it once. Setting an order's status to a specified value can be idempotent; incrementing revenue is not. Even an idempotent status update needs version checks to prevent an old event from overwriting a newer status.

For a payment-like operation, use a stable idempotency key, enforce its uniqueness, and atomically record the key with the business change. A separate “check whether processed” followed by an unprotected write has a race.

## 2. Distributed Data Processing

### Partitioning, sharding, replication

**Partitioning** divides data into subsets. **Sharding** usually means distributing such subsets across nodes. **Replication** copies subsets for durability, availability, or read capacity. A system commonly combines all three.

| Strategy | Useful property | Typical problem |
| --- | --- | --- |
| Hash partitioning | Distributes many distinct keys; colocates equal keys | A popular key remains hot; changing bucket count can move much data |
| Range partitioning | Efficient range access and ordered scans | Uneven ranges or append-heavy keys create hotspots |
| Round-robin partitioning | Spreads rows without a key dependency | Equal keys need reshuffling before joins or aggregations |

Choose the key around access patterns and distribution. Partitioning orders by customer can colocate customer history, while one large customer can dominate a shard. A date directory in a lake is a partition layout; it does not necessarily correspond to a dedicated server.

### Data locality, shuffle, distributed joins

**Data locality** places computation near the data to reduce network transfer. With remote object storage, locality often means using a nearby region and warm worker caches rather than running on the disk-owning host.

A **shuffle** redistributes rows between workers, usually by a join or grouping key. It can involve serialization, network transfer, intermediate files, sorting, and spill. An operation with little arithmetic can still be slow because it moves terabytes.

| Distributed join strategy | How it works | Main constraint |
| --- | --- | --- |
| Broadcast | Copy the small side to participating workers | The expanded build side must fit within each worker's safe memory budget |
| Repartitioned join | Shuffle both sides by compatible join keys | Network cost, skew, and spill |
| Colocated join | Match already compatible data distributions | Matching keys, partitioning rules, and placement are required |

Filter and select needed columns before data movement when semantics allow. A broadcast join is a distribution choice; each worker may execute a local hash join. Compressed file size is not the build hash table's memory size.

### Parallelism, concurrency, scheduling

**Concurrency** means multiple tasks are in progress during overlapping periods. **Parallelism** means multiple tasks execute simultaneously. An asynchronous reader can handle concurrent requests on one CPU; several workers can process partitions in parallel.

A **scheduler** maps runnable tasks onto available resources while honoring dependencies, capacity, fairness, and placement constraints. Too few tasks underuse the cluster; too many tiny tasks spend time on coordination and setup. More concurrent jobs can reduce throughput per job by competing for memory and I/O.

**Speculative execution** starts another attempt of a slow task. It helps with an unusually slow host but often wastes capacity for deterministic skew. Output commits must ensure multiple attempts do not publish duplicate effects.

**Example:** A stage with eight runnable partitions cannot keep 100 single-task slots busy. Increasing partition count may help, provided task overhead and the next stage's distribution do not erase the benefit.

### MapReduce, DAG execution models

**MapReduce** maps input records to intermediate key/value pairs, groups values by key through shuffle, and reduces each group. A word count maps words to `(word, 1)` and reduces by summing counts. A combiner can reduce network traffic only when its aggregation is safe under repeated and partial application; average needs a sum/count representation rather than averaging averages.

A **directed acyclic graph (DAG)** represents operators and dependencies. Engines can pipeline compatible operations, divide work into stages around exchanges, and schedule ready tasks. General DAGs avoid expressing every workflow as separate map/reduce jobs. Some engines recover lost intermediate data by recomputing dependencies; others rely more heavily on persisted state. [Original MapReduce paper](https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/).

```mermaid
flowchart LR
    A[Read order partitions] --> B[Filter and project]
    B --> C[Local partial sums]
    C --> D[Shuffle by customer]
    D --> E[Final sums]
    E --> F[Commit output]
```

The shuffle is a distribution boundary. Partial sums reduce the rows crossing it, while the final commit determines which output attempts become visible.

### Batch vs. stream processing

**Batch processing** operates on bounded input, such as yesterday's files. **Stream processing** handles potentially unbounded input and continuously updates results. Micro-batching processes successive bounded chunks of a stream.

Batch usually makes complete recomputation and large sequential scans easier. Streaming reduces freshness delay but requires decisions about state, late events, out-of-order arrival, and recovery. It is not automatically cheaper or more correct.

**Event time** is when an event occurred; **processing time** is when an operator handles it. A watermark estimates event-time progress so windows can advance. It is a policy about expected lateness, not proof that no older event can arrive. Define whether late changes update previous results, go to a correction path, or are rejected with monitoring.

**Example:** Daily financial totals may use batch reconciliation even when a stream supplies provisional revenue every minute.

### Backpressure, checkpointing, recovery

**Backpressure** limits upstream production when downstream cannot keep up. Bounded queues and flow control keep memory finite, but backlog moves to another layer, such as the source broker. If average arrival rate stays above processing capacity, backlog continues growing.

A **checkpoint** captures recoverable operator state and input progress at a consistent boundary. After failure, the engine restores that state and replays input from the recorded positions. Checkpoints require durable storage and replayable input; source retention must cover recovery delay.

In Flink, checkpoint barriers coordinate distributed snapshots. Exactly-once state recovery does not mean each record executes only once; replay may occur. End-to-end output guarantees also require appropriate source and sink behavior. [Flink stateful stream processing](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/).

Frequent checkpoints reduce replay work but add overhead. Recovery time includes detecting failure, allocating resources, restoring state, and catching up. A checkpoint also preserves bad state, so it is not a substitute for historical backups or reconciliation.

### Delivery guarantees, ordering, deduplication

| Guarantee | Behavior under failure | Typical consequence |
| --- | --- | --- |
| At-most-once | An input can be lost but is not redelivered by the delivery protocol | Acknowledging before durable processing risks loss |
| At-least-once | Unacknowledged input can be redelivered | Processing must tolerate duplicates |
| Exactly-once effects | Each logical input's intended effect becomes visible once within a defined boundary | Requires transactional coordination or an equivalent deduplication design |

**Ordering** must name its scope: connection, partition, key, or entire dataset. Per-partition order does not create global order. Independent retries or concurrent consumers can change the order in which effects appear.

**Deduplication** needs stable event identity and durable state. A TTL-bounded set forgets old IDs, so a replay older than its retention can reapply effects. For changing entities, also compare source versions or sequence numbers; different events for the same order are not necessarily duplicates.

**Interview check:** A worker writes revenue, crashes before acknowledging the input, and receives it again. Explain exactly where deduplication and the revenue update become atomic.

## 3. Database Engine Internals

### Storage engines: row-oriented vs. column-oriented

A **storage engine** manages data representation, access paths, caching, and persistence. Its physical layout shapes which operations are cheap.

| Layout | Strength | Cost |
| --- | --- | --- |
| Row-oriented | Fetching or changing most attributes of a few records | Analytical scans may read many unused attributes |
| Column-oriented | Reading a few columns across many records; compression of similar values | Reconstructing individual rows and handling frequent small updates can require extra work |

An order lookup by primary key often suits row storage. Summing two fields over a billion orders often suits column storage. Hybrid designs use write buffers, delta stores, indexes, and background merges. “Columnar” does not imply read-only, and row layout does not prevent analytical queries.

### B-Trees, LSM-Trees, hash indexes

**B-Trees**, commonly B+ trees in databases, keep ordered keys in a balanced page structure. High fan-out keeps the tree shallow; leaf ordering supports range scans. Inserts may split pages and require updates to multiple structures.

**Log-structured merge trees (LSM-Trees)** buffer writes in memory and flush sorted immutable files to storage. A WAL commonly protects unflushed writes. Compaction merges files and discards versions only when retention and snapshot rules allow it. Reads may consult multiple files; Bloom filters can rule out many absent-key lookups. RocksDB uses memtables, log files, and sorted tables in this pattern. [RocksDB overview](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview).

**Hash indexes** map hashes to buckets and verify actual keys to handle collisions. They suit equality access but do not naturally support ordered ranges.

The choice balances read, write, and space amplification. LSM does not mean every write is cheap forever: compaction consumes I/O and CPU and can cause stalls when it falls behind.

### Pages, blocks, buffer pools, page cache

A **page** is a database unit of storage and caching, often containing multiple records or index entries. A **block** may refer to a filesystem, device, compression, or distributed storage unit. These sizes and meanings differ; a large HDFS block is not a database page.

A **buffer pool** caches database pages in process-managed memory. Reads pin pages while used; modifications mark pages dirty; eviction must respect recovery rules. The **OS page cache** caches filesystem data below the database. Depending on I/O mode, both caches may hold copies, or direct I/O may bypass the OS cache.

A warm cache can hide slow storage. A high cache-hit ratio also does not prove a fast query: it may repeatedly read too many cached pages or spend most time waiting on locks. Track useful work, physical reads, eviction, and latency together.

### WAL, checkpoints, crash recovery

**Write-ahead logging (WAL)** requires a change's recovery log records to reach durable storage before the changed data pages are flushed. With durable commit settings, a transaction's required log records, including its commit record, are durable before success is acknowledged. Data pages can be flushed later; group commit can share a log flush across transactions. [PostgreSQL WAL introduction](https://www.postgresql.org/docs/18/wal-intro.html).

A **checkpoint** establishes a recovery boundary and advances persistence according to the engine's protocol, limiting how far recovery must look back. It need not stop all activity or flush every dirty page instantaneously.

**Crash recovery** uses logs and other recovery metadata to restore a consistent state. Engines may redo changes, undo incomplete work, or leave uncommitted versions invisible through MVCC. The precise algorithm varies.

**Failure timeline:** Commit is durable, acknowledgment is lost, server restarts, and recovery retains the transaction. The client must resolve an unknown outcome; assuming every timed-out transaction rolled back can duplicate work.

### Query parsing, planning, optimization, execution

A typical query path is:

1. **Parse:** Validate SQL syntax and form a parse tree.
2. **Analyze and bind:** Resolve tables, columns, types, and access permissions.
3. **Rewrite and form a logical plan:** Represent relations, filters, joins, and aggregates.
4. **Optimize:** Choose transformations, access paths, join order, distribution, and physical operators.
5. **Execute:** Run operators, obtain a valid transaction view, read data, and produce results.

These stages can overlap or use cached plans. The optimizer searches alternatives within a time budget; it does not prove it found the globally fastest plan. Execution can introduce additional choices, such as adapting a join after observing runtime data.

**Example:** SQL written with a join before a filter can execute as a filtered scan followed by a join. SQL expresses a result; its textual order is not a complete execution schedule.

### Volcano iterator model, vectorized execution

The classic **Volcano iterator model** composes operators through an interface such as `open`, `next`, and `close`. A parent repeatedly requests a tuple from its child. It supports streaming pipelines and early termination, but tuple-by-tuple dispatch can create interpretation and function-call overhead.

**Vectorized execution** operates on batches of values. Tight loops amortize dispatch, improve cache use, and can enable SIMD instructions. Batch execution does not require materializing an entire table, and SIMD is an optimization rather than a synonym for vectorization. [MonetDB/X100 paper](https://ir.cwi.nl/pub/16497/16497B.pdf).

Blocking operators, such as a full sort, still need enough input before producing results. Other engines compile pipelines into machine code; execution techniques can be combined. Columnar storage and vectorized execution complement each other but describe different layers.

### MVCC, locks, transaction isolation

**Multi-version concurrency control (MVCC)** retains versions and uses visibility rules so readers can see an appropriate snapshot while writers create newer versions. Old versions need cleanup once active readers and retention policies no longer require them. Long-running transactions can delay cleanup and increase storage use.

**Locks** coordinate conflicting operations on rows, tables, ranges, or metadata. An MVCC engine can still use locks for writes and schema changes. Ordinary snapshot reads may avoid blocking writers, but that does not make every operation nonblocking.

**Transaction isolation** defines which concurrent effects a transaction may observe. MVCC is an implementation technique, not an isolation level: an engine can use different snapshots or conflict checks to implement different guarantees.

**Interview check:** Explain why a database can support nonblocking reads and still experience write deadlocks or storage growth from an old reader.

## 4. Database Transactions & Consistency

### ACID properties, isolation levels

| Property | Meaning | Engineering implication |
| --- | --- | --- |
| Atomicity | The transaction's changes commit together or do not become committed effects | Avoid an order update without its corresponding ledger update inside the transaction boundary |
| Consistency | Correct transactions preserve declared invariants | Constraints help; ACID does not discover missing business rules |
| Isolation | Concurrent execution obeys the chosen isolation contract | Weaker levels permit some anomalies |
| Durability | Acknowledged commits survive failures covered by the configuration | Asynchronous flushing or failover can weaken the practical guarantee |

ACID consistency concerns application and database invariants. CAP consistency concerns the observed behavior of replicated operations; they are different uses of the word.

The standard's minimum prohibitions are commonly summarized below. An engine may provide stronger behavior under a given level name.

| Isolation level | Dirty read | Non-repeatable read | Phantom read | Serialization anomaly |
| --- | --- | --- | --- | --- |
| Read uncommitted | May occur | May occur | May occur | May occur |
| Read committed | Prevented | May occur | May occur | May occur |
| Repeatable read | Prevented | Prevented | May occur | May occur |
| Serializable | Prevented | Prevented | Prevented | Prevented |

For example, PostgreSQL 18 treats read uncommitted as read committed, and its repeatable read also prevents phantoms. Isolation names are not sufficient to infer every engine's behavior. [PostgreSQL isolation documentation](https://www.postgresql.org/docs/18/transaction-iso.html).

### Dirty reads, non-repeatable reads, phantom reads

- A **dirty read** observes another transaction's uncommitted change. A report sees an order that later rolls back.
- A **non-repeatable read** sees a committed value change when rereading the same row. An order's amount is 100 on the first read and 120 on the second.
- A **phantom read** sees a changed set of rows matching the same predicate. A second query for pending orders includes a newly committed order.

These examples concern repeated reads in one transaction. A stable snapshot can prevent them while still permitting other anomalies, including write skew. A report issuing independent queries without a shared snapshot may combine totals from different database states.

### Snapshot isolation, write skew, serializability

**Snapshot isolation** gives a transaction a consistent snapshot and, in its usual definition, prevents concurrent transactions from both committing conflicting writes to the same item. It can still allow **write skew**, where transactions update different items after reading a shared condition.

**Example:** At least one of two warehouse ingestion workers must stay enabled. Both transactions read that both workers are enabled. One disables worker A, the other disables worker B. Their write sets differ, yet the combined result violates the rule.

**Serializability** requires the committed result to match some serial transaction order. Prevent write skew through serializable execution, correctly locking the relevant predicate, or forcing contenders to update a shared guard row with appropriate conflict handling.

Serializability alone does not require that serial order to respect real-time completion. **Strict serializability** adds that requirement. Likewise, linearizable individual reads and writes do not automatically make a multi-operation transaction serializable.

### Optimistic vs. pessimistic concurrency control

**Optimistic concurrency control (OCC)** permits work, then validates that committing it will not violate the protocol. A version-based conditional update is a common application-level example: update only if `version = expected_version`, and detect whether a row changed. A stale writer must reread and retry or report a conflict.

**Pessimistic concurrency control** reserves access with locks before proceeding. It can avoid wasted work under contention but introduces waiting, deadlocks, and possible long lock holds.

OCC often suits relatively rare conflicts; locking can suit frequent conflicts where waiting is cheaper than redoing work. Neither is universally faster. Keep transactions short and avoid slow external calls while holding locks.

**Example:** Two consumers try to update an order. Comparing a source version prevents an older update from silently overwriting the newer value; unconditional upsert does not provide that protection.

### Deadlocks, conflict detection

A **deadlock** is a cycle of waits: transaction A holds resource X and waits for Y, while B holds Y and waits for X. Engines may detect the cycle and abort a victim, or use timeout and prevention policies. A timeout alone does not prove a deadlock.

Acquire resources in a consistent order, shorten transactions, and index predicates when that reduces the amount of locked work. Applications must still handle deadlock and serialization failures by retrying the complete transaction from a fresh view, within a bounded policy.

**Conflict detection** can compare versions, overlapping write sets, or read/write dependencies. Some conflicts block, some abort, and some are allowed at weaker isolation. Retrying only the failed final statement can preserve earlier decisions based on invalid reads.

### Distributed transactions, 2PC, Saga

A **distributed transaction** coordinates changes across participants. It needs a specified atomicity and isolation contract; distribution alone supplies neither.

**Two-phase commit (2PC)** coordinates an atomic outcome:

1. **Prepare:** Each participant durably records whether it can commit and retains required resources.
2. **Decide:** The coordinator durably records commit if every participant prepared successfully, otherwise abort, then communicates the decision.

A prepared participant may block while the decision is unavailable. Coordinator recovery and replicated decision records improve resilience but do not make classic 2PC a consensus algorithm or automatically supply global serializable isolation. PostgreSQL prepared transactions can retain locks until resolved. [PostgreSQL prepared transactions](https://www.postgresql.org/docs/18/sql-prepare-transaction.html).

A **Saga** chains local transactions and compensating actions. Reserving stock, charging payment, and creating shipment each commit locally; failure may trigger a refund and stock release. Compensation is a new business action, can fail, and cannot erase every external effect. Intermediate states may be visible. [Saga pattern](https://learn.microsoft.com/en-us/azure/architecture/patterns/saga).

### Exactly-once effects, atomic commits

An **atomic commit** publishes a set of related changes together within a stated boundary. To obtain exactly-once effects, make input identity or progress and the business effect participate in the same atomic decision, or use a sink with an equivalent idempotency contract.

For a database-backed consumer, the logical transaction is:

```text
BEGIN
  Attempt to insert event_id into processed_events with a uniqueness constraint.
  If this transaction inserted it, apply the business change.
  If it already exists, leave the business state unchanged.
COMMIT
Acknowledge the broker message only after a successful commit.
```

This is protocol pseudocode, not portable SQL. A failed business change must roll back the event marker too. Store enough identity information to reject reuse of the same key for a different operation. Deduplication records must outlive possible redelivery or replay.

When writing a database and publishing an event, a **transactional outbox** stores the business change and outbound record together. A relay can publish more than once, so downstream deduplication remains necessary. Arbitrary external API calls cannot be included merely by surrounding local code with a database transaction.

**Interview check:** Identify the failure after every step: before commit, after commit but before acknowledgment, and during acknowledgment. State why recovery neither loses nor doubles the business effect.

## 5. Data Storage & File Systems

### HDD vs. SSD, sequential vs. random I/O

**Hard disk drives (HDDs)** use rotating media, so seeks and rotational delay make many small random operations expensive. **Solid-state drives (SSDs)** remove mechanical seeks and support far more random I/O, but controller queues, flash management, endurance, and device bandwidth still matter.

**Sequential I/O** accesses contiguous regions and amortizes per-operation overhead. **Random I/O** touches scattered locations. Compare throughput in bytes per second with **IOPS**, operations per second, and state the request size: 10,000 small reads and 10,000 large reads represent different bandwidth requirements.

An SSD does not make access patterns irrelevant. Columnar scans favor large reads, while poorly localized index probes can produce many small requests. Remote storage adds network and request overhead even when its backing media are fast.

### Block storage vs. object storage vs. file storage

| Type | Interface and abstraction | Common use | Important constraint |
| --- | --- | --- | --- |
| Block storage | Addressable blocks exposed as a device | Database volumes and filesystems | Filesystem and application must manage structure and coordination |
| File storage | Paths, directories, and filesystem operations | Shared directories and applications expecting file APIs | Locking, rename, caching, and durability semantics vary |
| Object storage | Key-addressed objects and metadata through service APIs | Data lakes, archives, and backups | Do not assume POSIX rename, in-place update, or cross-object transactions |

Choose by access semantics as well as price and capacity. A filesystem mount over an object API does not automatically provide the semantics required by a traditional database engine.

### HDFS, S3, distributed file systems

A **distributed file system** presents files whose content or metadata spans machines. It must handle placement, metadata consistency, failed nodes, and reconstruction or replication.

**HDFS** uses a NameNode for namespace and block metadata, and DataNodes for block storage. Clients obtain locations from metadata services and transfer data with DataNodes. Large blocks and distributed replicas suit high-throughput access; high availability for metadata needs an appropriate configuration. Many tiny files create metadata and scheduling overhead. [HDFS architecture](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

**Amazon S3** exposes object keys rather than HDFS blocks or a general POSIX filesystem. It provides strong read-after-write consistency for object writes and deletes, including subsequent GET and LIST operations. Single-key updates are atomic, but S3 does not provide a general atomic transaction across keys. Table-level transactions require a higher-level protocol. Cross-region replication is a separate concern. [S3 consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel).

**Interview pitfall:** Calling all object stores eventually consistent, or assuming a “rename directory” operation atomically commits a dataset, gives the wrong correctness model.

### Row vs. columnar formats: Avro, Parquet, ORC

| Format | Organization | Common fit |
| --- | --- | --- |
| Avro | Schema-based record serialization; object container files organize records into blocks | Event interchange and workflows reading complete records |
| Parquet | Column chunks within row groups, divided into pages | Analytical projection, compression, and metadata-assisted skipping |
| ORC | Columnar stripes with typed streams, indexes, and statistics | Analytical scans and integrations that support its capabilities |

Avro also has reader/writer schema resolution rules for compatible evolution. Parquet and ORC can represent nested data; actual pushdown, encoding, and type support depend on the reader and writer. A row group is not the same as a directory partition. [Avro specification](https://avro.apache.org/docs/1.12.0/specification/), [Parquet format](https://parquet.apache.org/docs/file-format/), [ORC specification](https://orc.apache.org/specification/ORCv1/).

None of these formats alone supplies concurrent table commits, complete table history, or a multi-file transaction log. A table format or database protocol adds those capabilities above the files.

### Compression, encoding, dictionary encoding

**Encoding** represents values efficiently by exploiting structure. Examples include run-length encoding for repeated values, delta encoding for nearby numbers, and bit packing for small integer ranges. **Dictionary encoding** maps repeated values to compact IDs plus a dictionary; it works well when repetition offsets the dictionary's size.

**Compression** applies a codec to encoded data or blocks. It trades CPU work for fewer stored and transferred bytes. Encoding and compression are complementary rather than interchangeable.

**Example:** A status column containing a few repeated strings can dictionary-encode well. Random unique identifiers may gain less and cause a writer to choose another encoding. Compare compressed size, decode cost, and end-to-end scan time; the smallest file is not necessarily the fastest to query.

### Metadata, statistics, data skipping

**Metadata** describes schemas, file locations, layouts, and versions. **Statistics** summarize data using row counts, null counts, min/max bounds, distinct counts, or histograms.

**Data skipping** uses these summaries to avoid reading irrelevant regions. If a file's trustworthy amount bounds are 20–80, a predicate `amount > 100` cannot match it. If bounds overlap the predicate, the engine must still examine candidates; bounds do not prove every row matches. Supported Bloom filters can show definite absence, but a positive result can be a false positive.

Separate statistics used to choose a plan from metadata used to eliminate data safely. Inaccurate cost estimates can produce slow plans; unsafe skipping can produce wrong answers. Correctness-preserving metadata must reflect nulls, types, and comparison semantics conservatively.

### Partitioning, clustering, compaction

**Storage partitioning** groups data by values such as event date, allowing partition pruning. Very high-cardinality directory partitioning can create many tiny files. Choose a granularity that matches both query filters and data volume.

**Clustering** groups similar values physically within files or storage units. Sorting by customer can narrow min/max ranges and improve skipping for customer predicates, but exact clustering semantics vary by engine.

**Compaction** merges or rewrites data to reduce small files, overlapping sorted runs, or obsolete versions. It consumes compute and I/O, and publication must preserve a consistent table view. Deletion of old files must respect active readers, retained snapshots, and recovery requirements.

**Interview check:** For a lake with millions of small daily files, explain how compaction helps request and scheduling overhead, why it does not replace a good partition key, and when old files are safe to remove.

## 6. Query Optimization

### Logical vs. physical execution plans

A **logical plan** states relational operations: filter orders, join customers, and aggregate revenue. A **physical plan** selects implementations: scan certain files, broadcast customers, build a hash table, exchange rows by region, and aggregate on workers.

One logical result can have many physical plans. A fast plan reduces work early and places it well. Read a plan from its data sources through its consumers, identifying estimated or actual rows, scan volume, distribution exchanges, sorts, and materialization.

`EXPLAIN` commonly shows a plan; execution-based variants such as `EXPLAIN ANALYZE` actually run the statement. They can consume resources or execute writes. Confirm engine behavior and use safe environments for mutating statements.

### Rule-based vs. cost-based optimization

**Rule-based optimization** applies valid transformations, such as simplifying expressions or pushing a filter below an inner join when safe. **Cost-based optimization (CBO)** compares alternatives using estimated row counts and costs for CPU, I/O, network, and memory.

Modern optimizers commonly combine both. Costs are estimates, often relative units rather than milliseconds. A wrong estimate or a constrained search can select a poor plan; the optimizer does not exhaust every possibility for a large join graph. [PostgreSQL planner overview](https://www.postgresql.org/docs/18/planner-optimizer.html).

Semantic restrictions matter. Pushing a predicate from an outer join's condition into a post-join filter can remove unmatched rows. A rewrite that saves time but changes results is not an optimization.

### Cardinality estimation, table statistics

**Cardinality** is the number of rows produced by an operator. **Selectivity** is the fraction retained by a predicate. If a million-row table has an estimated 1% selectivity, the optimizer expects 10,000 rows after filtering.

Estimates use table statistics, sampled histograms, distinct counts, and sometimes multicolumn statistics. Stale data, correlated columns, skew, or opaque expressions cause errors. Treating country and city as independent, for example, misestimates many combinations.

A serious underestimate can select a broadcast join that runs out of memory or a nested loop that performs millions of probes. Compare estimates with actual operator output to locate the first major divergence; downstream errors may follow from it. Refresh relevant statistics or improve the predicate before forcing a plan.

### Predicate pushdown, projection pruning

**Predicate pushdown** moves eligible filtering closer to storage or a remote source. A reader may skip whole regions using statistics or evaluate the predicate while decoding; those save different amounts of I/O.

**Projection pruning** reads or carries only needed columns. It reduces columnar I/O, deserialization, row width, network traffic, and hash-table memory. A query selecting two fields should not carry a large unused payload through every shuffle.

Pushdown depends on types, expressions, connector support, and semantics. A function around a partition field may inhibit pruning unless the optimizer can derive a usable range. Verify scan metrics and the physical plan rather than infer success from SQL appearance.

### Join algorithms: nested loop, hash, sort-merge

| Algorithm | Operation | Good fit | Main risk |
| --- | --- | --- | --- |
| Nested loop | For each outer row, find matching inner rows | Tiny outer input or selective indexed probes | Without a useful access path, work can approach `N × M` |
| Hash join | Build a hash table on one side, probe with the other | Equijoins with a manageable build side | Memory pressure, skew, and partitioned spill |
| Sort-merge join | Merge inputs ordered by compatible join keys | Large joins with useful existing ordering or external sorting | Sorting cost and many duplicates per key |

Hash joins usually build on the smaller effective input after filters, not necessarily the table with fewer stored bytes. Sort-merge can spill gracefully but is not free of memory pressure. An output explosion from many-to-many keys affects every algorithm: no join strategy makes billions of required result rows disappear.

### Join ordering, partition pruning

**Join ordering** chooses which relations to combine first. Applying selective joins early can shrink intermediates substantially, but outer joins, dependencies, and other semantics constrain reordering. The smallest table is not always the best first join if it provides little filtering.

**Partition pruning** removes storage partitions that cannot satisfy a predicate. **Dynamic partition pruning** can use values produced by another operator, such as filtered customer IDs, to avoid reading irrelevant partitions when supported.

Pruning is distinct from distributing work across workers. A scan can run in parallel while reading every partition, or prune almost everything while using little parallelism. State both the amount of data avoided and the distribution of the remaining work.

### Materialization, caching, result reuse

**Materialization** stores an intermediate or derived result for later use. A materialized view can precompute daily revenue, exchanging refresh cost and staleness for cheaper queries. A temporary intermediate can avoid recomputing an expensive shared subplan, but adds writes and can block further optimization.

**Caching** keeps data, plans, or complete results for reuse. These are different layers: cached files still require query execution; a result cache may bypass it. A common table expression is not a portable instruction to persist or cache its output.

Reuse must respect source changes, parameters, permissions, and session semantics. A cache that serves another user's restricted result is a correctness and security defect. Measure warm and cold behavior separately.

**Interview check:** A query joins a large fact table to a small dimension and spills. Check estimates, filters, build-side memory, distribution, and skew before requesting a larger cluster.

## 7. Performance Engineering

### Latency vs. throughput, percentiles (P95/P99)

**Latency** is the time for one operation, measured across a defined boundary. **Throughput** is work completed per time unit. Increasing batching or concurrency can improve throughput while making individual requests slower through queueing.

**P95** is the latency at or below which approximately 95% of observations fall; **P99** similarly describes 99%. An average can hide a long tail affecting users or delaying a distributed stage. A request waiting for many parallel subrequests is exposed to the slowest required response.

Do not average per-worker P99 values to obtain a global P99. Combine distributions or suitable histograms, and define the measurement window, population, and treatment of failures. A load generator that stops sending while blocked can hide queueing delays; report the offered load as well as completed throughput.

**Example:** A pipeline processes 100,000 records per second but buffers for five minutes. It has high throughput and poor freshness; both metrics are needed.

### CPU-bound vs. memory-bound vs. I/O-bound workloads

| Bottleneck | What limits progress | Evidence to seek | Possible response |
| --- | --- | --- | --- |
| CPU-bound | Instruction execution | Hot functions, sustained compute use, serialization or codec time | Reduce computation, improve execution efficiency, then add useful CPU parallelism |
| Memory-bound | Memory bandwidth or access latency | Cache misses, stalls, large working sets | Improve locality, reduce width, use compact layouts |
| Memory-capacity constrained | Working set does not fit | Spill, garbage collection, allocation failures | Reduce state/build size, repartition safely, or add memory |
| Storage or network I/O-bound | Data transfer or request latency | I/O queues, transfer rates, read waits | Read fewer bytes, batch requests, improve layout or available I/O capacity |

High reported CPU utilization does not alone prove useful computation: threads can spend cycles stalled on memory. Low CPU can mean I/O wait, locks, throttling, or no runnable work. Diagnose the specific limiting resource before tuning.

### Network bandwidth, latency, serialization overhead

**Bandwidth** limits how many bytes can move per second. **Latency**, including round trips and queueing, penalizes request-heavy workflows. **Serialization** converts structured data into bytes; deserialization, copying, compression, and TLS consume resources around the transfer.

A simple lower-bound model is:

```text
transfer time >= bytes / effective bandwidth
serial request time ≈ number of dependent round trips × round-trip latency
```

Moving 100 GB over an effective 1 GB/s path takes at least about 100 seconds, before other work. A thousand dependent requests at 10 ms each spend about 10 seconds in round trips even with tiny payloads. Parallel requests can overlap latency but do not eliminate bandwidth or concurrency limits.

Reduce unnecessary shuffle, project unused fields away, and batch where latency permits. Compression can shorten transfers while making a CPU bottleneck worse.

### Memory hierarchy: CPU cache, RAM, disk

Registers and CPU caches are small and fast; RAM is larger with higher access latency; SSDs and HDDs provide persistent capacity with much higher access cost. Exact timings vary by hardware and access pattern, so use measured values rather than universal nanosecond claims.

**Locality** matters at every level. Sequential column scans and compact arrays can use cache lines and prefetching efficiently. Pointer-heavy structures can trigger cache misses even when the entire dataset fits in RAM. Moving from RAM to spill files introduces a much larger penalty.

**Example:** A hash table with many per-row objects may use far more memory and CPU time than a compact representation of the same logical keys. Size the in-memory representation, including indexes and buffers, rather than the compressed source file.

### Parallelism limits, Amdahl's Law

**Amdahl's Law** bounds speedup for a fixed-size workload with serial fraction `s` and `p` parallel workers:

```text
speedup(p) = 1 / (s + (1 - s) / p)
```

If 10% is serial, ten workers yield an ideal speedup of about `5.26×`; even infinitely many workers cannot exceed `10×`. Scheduling, network transfer, contention, and skew reduce real speedup further.

Use this to challenge “double the workers, halve the runtime.” Identify the serial coordinator, metadata bottleneck, final single-partition write, or unavoidable ordering first. The model holds workload size fixed; scaling both data and resources asks a different question.

### Data skew, hot partitions, load balancing

**Data skew** means uneven data volume or work across partitions. **Hot partitions** receive disproportionate reads, writes, or state. Equal row counts do not ensure equal work when payload sizes or processing costs differ.

Look for maximum versus median task duration, partition sizes, per-key frequency, and spills. A stage that finishes only when one task ends often points to skew.

Possible responses include filtering early, preaggregating, isolating heavy keys, splitting ranges, or salting a key. **Salting** adds a secondary bucket to distribute one key's work, then combines partial results when the operation permits it. Joins may require duplicating the other side for correctness; exact distinct counts and ordering need particular care.

Adding hash partitions does not split one unchanged hot key. Load balancing must match the unit of indivisible work, rather than merely increase the worker count.

### Profiling, benchmarking, bottleneck analysis

**Profiling** attributes time or resources to functions, operators, waits, or allocations. **Benchmarking** compares behavior under a defined workload. **Bottleneck analysis** identifies the resource or dependency that currently limits useful progress.

Use a controlled sequence:

1. Define success: runtime, sustained throughput, P99, freshness, and cost.
2. Measure a representative baseline with realistic volume, skew, concurrency, and data distribution.
3. Inspect plans, CPU profiles, I/O, network, memory, queueing, and task distributions.
4. Change one relevant factor, repeat measurements, and validate identical results.
5. Check whether the bottleneck moved and whether the improvement survives repeated runs.

Separate cold and warm caches, account for startup and autoscaling, and avoid reporting only the fastest run. A speedup caused by silently dropping late records is a correctness failure.

**Interview check:** If a job's runtime stays flat after doubling workers, distinguish insufficient parallel work, skew, shared I/O saturation, and serial work using specific metrics.

## 8. Data Infrastructure & Architecture

### OLTP vs. OLAP, HTAP

**Online transaction processing (OLTP)** handles operational reads and writes, often a few records at a time, with predictable latency and transactional integrity. **Online analytical processing (OLAP)** scans, joins, and aggregates large datasets for analysis.

**Hybrid transactional/analytical processing (HTAP)** supports both patterns within a coordinated system, sometimes through separate storage representations or replicas. It reduces extraction delay but still needs resource isolation and a defined freshness model between representations.

**Example:** Placing an order is OLTP; calculating monthly revenue by region is OLAP. Running a full historical scan on the order database can delay checkout through shared CPU, cache, and I/O contention. Product categories are not absolute: analytical systems can support transactions, and operational databases can run SQL aggregates.

### ETL vs. ELT, Lambda vs. Kappa architecture

**ETL** extracts, transforms, then loads data into the target. It can enforce shape or remove sensitive fields before landing there. **ELT** extracts and loads first, then transforms using the target platform. It supports retaining raw inputs for reprocessing, with corresponding governance and storage obligations.

**Lambda architecture** combines a batch layer for comprehensive historical computation with a speed layer for recent low-latency results. It requires reconciling their outputs and often maintaining two implementations of business logic.

**Kappa architecture** centers processing on a durable event log and stream-processing path. Reprocessing replays history through a new or reset computation, subject to log retention, source completeness, and replay capacity. It does not remove the need for schemas, corrections, or historical state.

Choose from freshness, replay requirements, complexity, and cost. Using streaming technology alone does not prove a system follows Kappa, nor does every pipeline need both paths.

### Data Warehouse vs. Data Lake vs. Lakehouse

| Architecture | Central idea | Strength | Responsibility or constraint |
| --- | --- | --- | --- |
| Data warehouse | Curated data managed for analytical querying | Integrated SQL, performance management, governed models | Loading, modeling, and platform economics |
| Data lake | Store varied datasets in durable storage, often open files | Flexible formats and processing engines | Table semantics, discovery, governance, and quality require deliberate systems |
| Lakehouse | Add managed table semantics and analytical capabilities over lake storage | Transactional tables and broader engine interoperability | Protocol compatibility, metadata operations, and maintenance |

These labels overlap across products. Ask where the authoritative metadata lives, how concurrent writers commit, which engines support the protocol, and what happens after a failed write. Putting Parquet files in object storage is not sufficient to define a transactionally managed lakehouse.

### CDC, event-driven architecture, message brokers

**Change data capture (CDC)** records source inserts, updates, and deletes through mechanisms such as transaction-log reading or triggers. A log-based feed must handle source transaction order, an initial snapshot, its handoff to ongoing changes, schema evolution, and position retention.

**Event-driven architecture** lets components react to published facts or commands without requiring one synchronous call chain. A **message broker** buffers and routes messages, decoupling producer and consumer lifetimes; delivery, retention, and ordering guarantees depend on the system and configuration.

A row-change record is not automatically a business event. An update to a payment row may not express the business meaning of “payment settled.” An outbox can publish that meaning atomically with local state changes.

**Example:** Carry the source key, operation, source position, and relevant version so consumers can apply deletes, reject stale updates, and recover from replay. A timestamp-only incremental query can miss deletes and changes sharing the boundary timestamp.

### MPP vs. SMP, shared-nothing vs. shared-storage

**Symmetric multiprocessing (SMP)** uses multiple processors sharing one system's memory. **Massively parallel processing (MPP)** distributes work across cooperating processing nodes, often with independent memory and inter-node exchanges.

**Shared-nothing** nodes own their local resources and assigned data. It scales by partitioning, but skew and redistributing data become central. **Shared-storage** designs let multiple compute nodes access a common storage layer, simplifying some placement and elasticity concerns while relying on that layer's throughput and coordination.

These are different axes. An MPP query engine can read shared object storage while using local memory, caches, and shuffle disks on each worker. “Shared storage” does not mean all compute can mutate the same files safely without a protocol.

### Compute-storage separation

**Compute-storage separation** lets durable data outlive an individual compute cluster. Compute pools can scale or stop independently and share a governed storage layer.

Benefits include independent scaling, workload isolation, and less data movement when replacing compute. Costs include remote-read latency, network traffic, cache warmup, metadata dependencies, and potential storage bottlenecks.

Separate compute does not eliminate shared contention: two clusters can overload the same object store, catalog, source system, or network budget. Local caches improve performance but require correct invalidation or immutable version references.

**Example:** Use one compute pool for scheduled transformations and another for interactive dashboards, then measure whether concurrent scans overwhelm a common dependency.

### Metadata catalogs, orchestration, lineage

A **metadata catalog** maps names to schemas, locations, owners, permissions, and sometimes authoritative table versions. Some catalogs participate in commit coordination; others primarily support discovery. State which role is required.

**Orchestration** schedules tasks, dependencies, retries, and backfills. A green task means its configured work completed, not necessarily that the data are fresh, complete, or correct. Publish outputs only after required validation succeeds.

**Lineage** records how sources, transformations, and outputs relate. Table-level lineage helps assess downstream impact; column-level or run-level lineage can explain a particular field or materialization. Dynamic SQL and external side effects can make coverage incomplete.

**Interview check:** For an incorrect revenue dashboard, trace the source version, processing run, transformation code, and published table snapshot. An orchestrator's success status alone cannot establish correctness.

## 9. Networking & Infrastructure

### TCP/IP, DNS, HTTP, TLS

| Component | Responsibility | What it does not guarantee |
| --- | --- | --- |
| IP | Address and route packets across networks | Reliable delivery or application identity |
| TCP | Reliable, ordered byte stream within a connection | Message boundaries, cross-connection ordering, or exactly-once business effects |
| DNS | Resolve names to records such as addresses | Immediate visibility of every service change; caches and TTLs matter |
| HTTP | Define request/response semantics and status codes | A failed response means no server-side effect occurred |
| TLS | Protect transport confidentiality and integrity; authenticate peers according to configuration | Application authorization or protection for data after decryption |

A common HTTPS path is DNS lookup, connection establishment, TLS negotiation, and an HTTP exchange. Reused connections avoid some setup. HTTP/3 uses QUIC over UDP, so HTTP is not synonymous with TCP. TCP delivery and TLS security do not resolve a lost acknowledgment after a database commit. [TCP specification](https://www.rfc-editor.org/rfc/rfc9293).

For secure clients, validate certificate chains and service names; encryption without authenticating the intended peer leaves an important trust gap.

### Sockets, connection pooling, keep-alive

A **socket** is an endpoint for network communication. A TCP socket carries bytes, so an application protocol must frame messages and handle partial reads and writes. One read is not necessarily one complete message.

**Connection pooling** reuses established connections and limits their count. It saves setup cost and protects a server from excessive concurrent sessions. An oversized pool can cause contention; an undersized pool can create long acquisition waits. Set acquisition, connection, and operation timeouts deliberately.

**HTTP keep-alive** supports connection reuse. **TCP keepalive** probes an idle connection to help detect broken peers; it is separate from an application heartbeat and request deadline. Pooled connections can become stale after network or server changes, so clients need appropriate validation and error recovery.

**Example:** Increasing every worker's database pool from 10 to 100 can multiply connections across the cluster and overload the database before useful throughput improves.

### Load balancing, service discovery

**Load balancing** distributes requests or connections across eligible targets. Layer 4 balancing routes transport connections; Layer 7 balancing can use application information such as HTTP paths. Round-robin, least-connections, and consistent-hash policies suit different workloads.

**Service discovery** finds current endpoints through DNS, a registry, or a control plane. Discovery answers where to connect; balancing decides which endpoint to use. Health and readiness checks help avoid unsuitable targets but can lag or miss dependency failures.

Sticky routing can preserve cache locality but create hotspots. Long-lived connections can keep traffic uneven even after adding targets. A broker or database protocol may discover a particular partition leader; a generic load balancer cannot replace its routing rules.

### Containers, Kubernetes, resource scheduling

A **container** packages a process and its dependencies while using operating-system isolation. It shares the host kernel and is not equivalent to a separate virtual machine.

**Kubernetes** manages desired workloads, places Pods on nodes, monitors them, and replaces failed instances. Scheduling considers declared resources and constraints such as placement, affinity, and available volumes. A running Pod is not proof an application is ready; readiness checks control eligibility for traffic.

Restarting a process does not restore lost application state. Stateful systems need persistent storage, replica coordination, and tested restart behavior. A worker must also handle termination during processing: finish safely, checkpoint, or rely on replay with an atomic output protocol.

### CPU/memory limits, cgroups, OOM

Linux **cgroups** account for and constrain groups of processes. In Kubernetes, resource **requests** guide scheduling; resource **limits** constrain runtime use. Requests are not simply a live reading of current consumption.

CPU limits can cause **throttling**, delaying work even when an application has runnable threads. Memory-limit enforcement can cause an **out-of-memory (OOM) kill** rather than a clean application exception. Node memory pressure can also lead to eviction. Inspect the termination reason and node events before labeling every restart an application bug. [Kubernetes resource management](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/).

For JVM workers, account for heap, direct buffers, native memory, thread stacks, and other charged memory. Setting heap equal to the container's entire limit leaves no headroom. A high-memory join may fail because of its build representation, skew, or excess concurrent tasks, even if source files are small.

### Autoscaling, resource isolation

**Autoscaling** adjusts replicas, worker capacity, or node capacity based on observed demand. Signals can include CPU, queue depth, broker lag, or task backlog. Choose a signal that tracks the actual bottleneck: a blocked I/O workload can have growing lag and low CPU.

Scaling has delay from detection, provisioning, startup, state transfer, and cache warmup. Use headroom for bursts, dampen oscillation, and make scale-down safe for in-flight tasks and state. More consumers than available partitions may not add useful concurrency.

**Resource isolation** uses quotas, separate pools, admission control, or placement to limit noisy-neighbor effects. Compute isolation alone does not isolate shared database connections, catalog requests, or storage bandwidth.

### Cloud networking, IAM, infrastructure security

Cloud workloads communicate through networks, subnets, routes, firewalls, and sometimes private service endpoints or NAT. Reachability depends on the full path, including name resolution and return traffic. Cross-zone or cross-region placement affects both latency and transfer cost.

**Identity and access management (IAM)** determines who may perform which actions on which resources under specified conditions. Network access does not imply authorization, and authorization does not create a network route.

Use least-privilege workload identities, short-lived credentials where supported, secret management, encryption, and audit trails. Validate untrusted inputs at service boundaries, parameterize database queries, and avoid logging secrets or sensitive records. Separate read, write, and destructive maintenance permissions according to actual duties.

**Interview check:** A worker cannot read storage. Separate DNS and routing, connection and TLS, authentication, resource authorization, and encryption-key access instead of treating every error as “networking.”

## 10. Reliability & Production Engineering

### High availability, disaster recovery

**High availability (HA)** keeps a service usable through expected component failures, often using redundancy and automated failover. **Disaster recovery (DR)** restores service and data after a larger event, such as a regional outage, corruption, or destructive deployment.

HA and DR address different scopes. An online replica can reproduce a destructive delete, so it does not replace independent historical recovery. Define failure domains, dependencies, capacity after failure, and who can execute the recovery procedure.

**Example:** Multiple workers in one region can survive a worker failure; recovering after the region is unavailable may require another region's data, credentials, encryption keys, catalog metadata, and compute capacity.

### RPO, RTO, SLA, SLO, error budgets

| Term | Meaning | Example |
| --- | --- | --- |
| Recovery point objective (RPO) | Maximum acceptable loss of recent data, expressed as a time gap | Recover to a point no more than five minutes before the incident |
| Recovery time objective (RTO) | Target time to restore the defined service after disruption | Resume correct ingestion within one hour |
| Service level indicator (SLI) | Measured service behavior | Fraction of expected order changes available within ten minutes |
| Service level objective (SLO) | Internal target for an SLI over a window | At least 99.9% of changes meet that freshness target monthly |
| Service level agreement (SLA) | An agreement with consequences for missing commitments | Contractual service credits |
| Error budget | Allowed unreliability under the SLO | At a 99.9% success target, 0.1% of eligible events may miss it |

For a time-based 99.9% availability objective over 30 days, the budget is 43.2 minutes. An event-based freshness objective instead budgets events; its failures cannot automatically be converted into downtime minutes. Define the denominator, exclusions, window, and observation method. [Google SRE: Service Level Objectives](https://sre.google/sre-book/service-level-objectives/).

RPO and RTO are objectives, not guarantees established merely by buying replicas. Validate them through recovery exercises, including catching up and verifying correctness.

### Replication, failover, backup and restore

**Replication** maintains additional copies of current data. **Failover** transfers service to a replacement. Promotion must fence the former writer, select an appropriately current copy, and update routing. Asynchronous lag can turn failover into acknowledged-data loss.

A **backup** preserves recoverable state independently of the live system. Point-in-time recovery often combines a base backup with a complete retained log sequence. Include schemas, catalog state, permissions, configuration, and keys needed to interpret and access the data.

**Restore testing** proves that backups are readable and complete enough to meet the recovery objective. A successful backup job does not prove a working restore. Measure restoration at realistic volume, validate row counts and business invariants, and confirm downstream jobs can resume from compatible positions.

### Retry strategies, exponential backoff, circuit breakers

Retry transient connection errors, throttling, or documented retryable conflicts under a bounded policy. Validation failures and permanent authorization errors generally need correction rather than repetition. An ambiguous write outcome needs idempotency or status resolution.

**Exponential backoff** increases the delay between attempts up to a cap. **Jitter** randomizes delays to keep clients from retrying in lockstep. Respect service retry guidance and the caller's overall deadline. Avoid independent retry loops at every layer: three attempts at five layers can amplify one request into as many as `3^5 = 243` attempts.

A **circuit breaker** temporarily rejects calls to a failing dependency, then permits limited probes before reopening normal traffic. It can reduce cascading failures but needs an intentional policy for queued or rejected data. Bound queues and shed work explicitly when required rather than silently losing records. [Google SRE: Handling Overload](https://sre.google/sre-book/handling-overload/).

### Observability: metrics, logs, traces

**Metrics** quantify rates, distributions, and resource use. **Logs** describe discrete events with context. **Traces** follow work across components and show where time is spent. Use stable correlation identifiers while controlling sensitive data and high-cardinality metric labels.

For a data pipeline, observe both infrastructure and data outcomes:

- Throughput, consumer lag, queue depth, task failures, retry volume, and checkpoint age.
- CPU throttling, memory, spills, disk and network pressure, and connection-pool waits.
- End-to-end freshness, missing or duplicate keys, null rates, schema changes, and reconciliation differences.

A healthy process can produce incorrect data. Source silence can also produce zero lag while expected business events are missing. Alert on user-visible objectives and correctness symptoms, then use lower-level signals to diagnose causes.

### Capacity planning, cost optimization

Estimate arrival rate, retained bytes, state growth, peak concurrency, and expected growth. Include replication, indexes, temporary shuffle, compaction headroom, checkpoint storage, and recovery capacity rather than count only raw input bytes.

If input arrives at `λ` records/s and processing sustains `μ`, backlog grows when `λ > μ`. With existing backlog `B` and `μ > λ`, ideal catch-up time is `B / (μ - λ)`. A service operating exactly at normal arrival capacity cannot clear a failure backlog while new traffic continues.

Track **cost per successful unit of work**, such as cost per million validated events or per completed report. Smaller compute may run longer; larger compute may waste idle capacity. Reduce scans, unnecessary transfers, small files, redundant recomputation, and idle resources before assuming hardware price alone determines cost.

### Data correctness, reconciliation, recovery

**Data correctness** includes valid values, complete expected coverage, uniqueness, consistent relationships, and appropriate ordering and freshness. Define invariants such as “each accepted payment contributes to net revenue once,” including refunds and corrections.

**Reconciliation** compares independent views: source versus target counts, sums by business key or time range, unmatched keys, and source progress. Matching row counts alone can hide one missing and one duplicate record; monetary totals can also cancel unrelated errors.

**Recovery** needs retained input, compatible schemas, deterministic or versioned transformation logic, and safe publication. Backfills should use the same identity and version rules as live processing. Validate staged results before an atomic cutover when supported; coordinate overlapping live changes rather than overwrite them blindly.

**Interview check:** A day of revenue is wrong although every task succeeded. Explain how to locate the affected input range, correct the computation, replay safely, reconcile the result, and publish without double counting.

## 11. Advanced Engineering Trade-offs

### Consistency vs. latency vs. availability

Coordination can increase latency and make operations depend on enough reachable replicas. Serving local replicas improves read latency and independence but may expose older values. Even without a partition, cross-region coordination costs round trips; CAP alone does not explain that everyday latency trade-off.

Choose guarantees per operation. Inventory reservation may need a coordinated conditional change, while an approximate product-view counter can tolerate delayed convergence. State what a caller sees during failure and how conflicting updates are resolved.

### Replication vs. partitioning

Replication copies data; partitioning divides it. Replicas improve resilience or read capacity but consume storage, network, and write propagation work. Shards increase aggregate data and processing capacity but complicate cross-shard joins, transactions, and rebalancing.

Most large systems need both: distribute orders across shards and replicate each shard. Adding replicas does not automatically increase write capacity when every write still passes through the same shard leader.

### Throughput vs. resource efficiency

More parallelism and spare capacity can improve peak throughput or tail latency while reducing average utilization. Packing workloads tightly can lower cost but increases contention and leaves little room for bursts, failover, or backlog recovery.

Compare useful throughput per unit of cost while meeting latency and reliability objectives. A highly utilized cluster with unstable queues is not necessarily efficient, and idle failover capacity can be an intentional reliability expense.

### Batch size vs. processing latency

Larger batches amortize request, transaction, scheduling, and serialization overhead and can compress better. They increase buffering delay, peak memory, retry work, and the time before a result becomes visible.

Use both a size limit and a time limit when the workload needs bounded waiting under low traffic. Measure from event creation to committed output; measuring only the time spent inside the batch hides the batching delay.

### Indexing vs. write amplification

Indexes accelerate selective reads but need maintenance on writes and consume space and cache. Covering indexes can avoid base-table reads while making index entries larger. An index unused by important queries still imposes a maintenance cost.

**Write amplification** is physical data written relative to logical user data written. Index updates, WAL, LSM compaction, and replication all contribute work at different layers; define the measurement boundary. Add an index for a demonstrated access pattern and verify its effect on both reads and ingestion.

### Compression vs. CPU utilization

Compression can lower storage cost and network time while adding encode and decode CPU work. A stronger codec may help an I/O-bound scan but slow a CPU-bound pipeline. Repeatedly decompressing the same data can alter the balance again.

Benchmark the actual data and read/write ratio. Use end-to-end throughput, latency, and cost alongside compression ratio. There is no universal best codec or compression level for every stage.

### Precomputation vs. query-time computation

Precomputed aggregates, indexes, and materialized views move work from reads to ingestion or refresh. They make recurring queries cheaper but add storage, maintenance, correction logic, and potential staleness. Historical changes can require revisiting already computed results.

Query-time computation offers flexibility and fresh derivation at the cost of repeated scans and higher latency. Precompute common stable questions when their reuse justifies maintenance, and retain sufficient detail to answer new questions and repair old aggregates.

### Managed services vs. self-hosted infrastructure

Managed services reduce some provisioning, patching, and availability work. They can impose service limits, pricing constraints, network charges, reduced low-level control, or portability costs. Self-hosting offers more control while making upgrades, security, scaling, backup, and on-call expertise the team's responsibility.

Compare total operating cost and demonstrated requirements rather than infrastructure price alone. A managed service still requires correct schemas, access rules, recovery objectives, monitoring, and client behavior. Self-hosting only pays when the needed control or economics justify that operational burden.

**Interview check:** For every choice, name the workload, the required guarantee, the limiting resource, and the failure behavior. Then state which measurement or requirement would make you change the design.
