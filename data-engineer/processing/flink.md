# Apache Flink & Stream Processing: Data Engineer Interview Guide

A five-minute revenue total sounds simple until an order arrives ten minutes late or a worker fails halfway through an update. Flink handles this kind of ongoing, stateful computation over bounded and unbounded streams. To understand the result it produces, follow three things together: the records, the progress of event time, and the state that must survive failure.

**Reference baseline:** Apache Flink 2.2, using the established Java DataStream API and Table API/SQL. This is an explicit documentation baseline, not a claim about the latest release. DataStream API V2 and connector releases have their own compatibility considerations. Examples are illustrative fragments, not complete applications. [Flink documentation](https://nightlies.apache.org/flink/flink-docs-release-2.2/).

## Contents

1. [Architecture: JobManager, TaskManagers, and Slots](#1-architecture-jobmanager-taskmanagers-and-slots)
2. [DataStream API, Table API, and Flink SQL](#2-datastream-api-table-api-and-flink-sql)
3. [Event Time, Processing Time, and Watermarks](#3-event-time-processing-time-and-watermarks)
4. [Windows, Triggers, and Allowed Lateness](#4-windows-triggers-and-allowed-lateness)
5. [Stateful Operators, Keyed State, and RocksDB](#5-stateful-operators-keyed-state-and-rocksdb)
6. [Checkpoints, Savepoints, and Fault Tolerance](#6-checkpoints-savepoints-and-fault-tolerance)
7. [Exactly-Once Processing and Transactional Sinks](#7-exactly-once-processing-and-transactional-sinks)
8. [Backpressure, Rescaling, and State Redistribution](#8-backpressure-rescaling-and-state-redistribution)
9. [Interview Questions and Review Checklist](#9-interview-questions-and-review-checklist)

## 1. Architecture: JobManager, TaskManagers, and Slots

### Control plane and workers

A Flink job has two kinds of work: coordinating execution and processing the stream itself. Records normally travel between TaskManagers; coordinating services schedule that work and organize recovery. The roles below explain which component to inspect when a job cannot start, a task fails, or checkpoints stop completing.

| Component | Responsibility |
| --- | --- |
| JobManager | Coordinates execution, scheduling, checkpoints, and recovery |
| Dispatcher | Accepts submissions and starts a JobMaster for each job |
| ResourceManager | Manages available slots and interacts with the deployment environment |
| JobMaster | Coordinates one job's execution |
| TaskManager | Worker JVM that executes tasks and exchanges stream data |
| Task slot | Unit of scheduling resources within a TaskManager |

The JobManager contains coordinating services; records normally flow between TaskManagers rather than through the JobManager. High availability provides leader failover, while durable snapshots provide recoverable application state. [Flink architecture](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/concepts/flink-architecture/).

```text
                         JobManager
                 scheduling / checkpoints / recovery
                           |       |
                   TaskManager A   TaskManager B
                       |                 |
Records: source -> parse/filter -> keyBy exchange -> aggregate -> sink
                       |                 |
                       +---- durable checkpoint storage ----+
```

### Operators, subtasks, and chaining

An operator is a transformation such as `map`, a window aggregation, or a sink. Parallelism is its number of parallel subtasks. Compatible subtasks can be chained into one task thread, avoiding serialization and network handoff between those operators. A `keyBy` repartitions records and commonly creates an exchange boundary.

Slots reserve a share of managed memory; a slot is neither a CPU core nor a dedicated JVM. With default slot sharing and one slot-sharing group, required slots are generally the job's highest operator parallelism, rather than the sum across operators. Separate groups and resource requirements change that calculation. [Tasks, chains, and slots](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/concepts/flink-architecture/#tasks-and-operator-chains).

**Example:** two TaskManagers with four slots each offer eight slots. A source at parallelism four, aggregation at eight, and sink at four can fit under the sharing assumptions above. Eight slots alone do not prove the workers have enough CPU, memory, disk, or network capacity for the workload.

**Interview trap:** adding slots to an already saturated worker changes scheduling capacity; it does not create additional hardware capacity.

## 2. DataStream API, Table API, and Flink SQL

### Choosing an API

You can express a revenue aggregation as a continuous SQL query or build a custom event-handling application. Both run on Flink, but they expose different levels of control. Choose the API around the operation you need to describe, especially whether you need direct access to keyed state and timers.

| API | Model | Typical use |
| --- | --- | --- |
| DataStream API | Explicit record transformations, keys, state, and timers | Custom event handling, routing, and state machines |
| Table API | Relational expressions over tables | Programmatic filters, joins, aggregations, and reusable relational pipelines |
| Flink SQL | Declarative continuous queries | Streaming ETL and analytics expressed in SQL |

Table API and SQL share a relational planner and can be combined. Their queries compile into runtime operators; SQL uses the same distributed execution foundation. Choose DataStream when application behavior requires direct control over state or timers, and relational APIs when the operation fits their model. [Table API and SQL concepts](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/common/).

### DataStream example

Assume `events` is a `DataStream<Tuple3<String, Long, Long>>` containing `(customer_id, count, event_timestamp_millis)`. Imports, source construction, and sink configuration are omitted.

```java
WatermarkStrategy<Tuple3<String, Long, Long>> watermarks =
    WatermarkStrategy
        .<Tuple3<String, Long, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner((event, previousTimestamp) -> event.f2)
        .withIdleness(Duration.ofMinutes(1));

OutputTag<Tuple3<String, Long, Long>> tooLate =
    new OutputTag<Tuple3<String, Long, Long>>("too-late") {};

SingleOutputStreamOperator<Tuple3<String, Long, Long>> counts = events
    .assignTimestampsAndWatermarks(watermarks)
    .keyBy(event -> event.f0)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
    .allowedLateness(Duration.ofMinutes(1))
    .sideOutputLateData(tooLate)
    .sum(1)
    .uid("customer-window-counts");

DataStream<Tuple3<String, Long, Long>> lateEvents = counts.getSideOutput(tooLate);
```

This aggregates field `f1` per customer and window. The tuple's `f2` is not a window boundary; use a window function when output needs explicit start/end fields. The fragment assigns watermarks after the source for simplicity; source-level assignment is preferable when the connector supports per-partition tracking. Both normal results and the late side output need sinks in a complete job. [Watermark strategies](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/event-time/generating_watermarks/), [window API](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/windows/).

### Flink SQL example

Assume a registered streaming table `orders` with `customer_id`, `amount DECIMAL(18, 2)`, and an event-time attribute `event_time TIMESTAMP(3)`. Its source DDL declares `WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND`; connector options are omitted here.

```sql
SELECT
    customer_id,
    window_start,
    window_end,
    SUM(amount) AS total_amount
FROM TABLE(
    TUMBLE(TABLE orders, DESCRIPTOR(event_time), INTERVAL '5' MINUTES)
)
GROUP BY customer_id, window_start, window_end;
```

`TUMBLE` assigns rows to fixed, non-overlapping windows. The watermark declaration marks the timestamp column as an event-time attribute. Epoch-based timestamps commonly use `TIMESTAMP_LTZ` after conversion; choose timestamp types and session time zone deliberately. [SQL window aggregation](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/window-agg/), [time attributes](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/concepts/time_attributes/).

### Dynamic tables and changelog compatibility

A dynamic table changes as records arrive. A continuous query updates its result accordingly. A non-windowed `GROUP BY customer_id` can revise an existing total repeatedly, whereas the standard event-time window aggregation above emits its result when the window closes.

Results may contain inserts, updates, and deletes. An append-only sink cannot accept every changelog. Upsert sinks need a suitable key, and converting a Table to a DataStream must preserve the required change information; use changelog conversion for updating tables. [Dynamic tables and stream conversion](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/concepts/dynamic_tables/).

**Interview answer:** SQL can express stateful streaming computations. The planner manages the stateful operators, but the engineer still owns time semantics, state growth, and sink compatibility.

## 3. Event Time, Processing Time, and Watermarks

### Two clocks

An order can be created at `10:02` and reach the pipeline at `10:07`. Should it count toward the interval when the customer placed it or the interval when a worker received it? That choice separates event time from processing time and determines how delays and replay affect the answer.

| Clock | Meaning | Consequence |
| --- | --- | --- |
| Event time | When the event occurred, according to its assigned timestamp | Supports business-time windows despite transport delays |
| Processing time | Worker wall-clock time when an operation runs | Simple and low overhead, but results depend on arrival and execution timing |

An order created at `10:02` and processed at `10:07` belongs to the `10:00–10:05` event-time window. Processing-time windows place it according to `10:07`. Event time supports reproducible business-time computation, provided timestamps, ordering assumptions, lateness rules, and application logic are suitable. Replaying the same input under processing-time windows can produce different groups. [Timely stream processing](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/concepts/time/).

### What a watermark means

A watermark is an estimate of progress in event time. It tells downstream operators that earlier timestamps are no longer expected under the chosen strategy. It is an assumption about arrival behavior, not a guarantee that older events cannot appear. Event-time timers and windows use this progress signal.

For bounded out-of-orderness, a useful approximation is:

```text
watermark ≈ largest observed event timestamp - out-of-orderness bound
```

The built-in Java generator includes a one-millisecond boundary adjustment, and periodic emission adds delay. A ten-second bound does not mean each event waits exactly ten wall-clock seconds. If no newer events arrive, observed event time may stop advancing. [Bounded watermark generator](https://nightlies.apache.org/flink/flink-docs-release-2.2/api/java/org/apache/flink/api/common/eventtime/BoundedOutOfOrdernessWatermarks.html).

### Propagation, idle inputs, and alignment

An operator's watermark is constrained by the minimum watermark of its active inputs. Source connectors can track progress per partition or split. One slow partition can therefore hold back downstream windows even while other partitions are current.

Idle-source detection excludes an input after configured inactivity. When it resumes, old records can be late relative to downstream progress. Watermark alignment instead slows faster sources to limit their lead over slower ones, reducing excessive buffering and state growth. Alignment depends on connector support; it does not repair a slow sink. [Watermark propagation, idleness, and alignment](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/event-time/generating_watermarks/).

**Diagnostic example:** input throughput is healthy, but five-minute windows never emit. Check watermark values per subtask, timestamp extraction, missing watermark generation, and idle partitions before changing the window duration.

**Interview trap:** watermarks do not sort the stream. Operators still receive out-of-order records; event-time logic decides how to use them.

## 4. Windows, Triggers, and Allowed Lateness

### Assigning records to windows

Before calculating a total over a stream, decide which records belong together. A fixed reporting interval, a rolling view of recent activity, and a browsing session each imply a different window. That membership rule affects not only the answer but also how much state the job must retain.

| Window | Membership | Example |
| --- | --- | --- |
| Tumbling | Fixed size, no overlap | Revenue per five-minute interval |
| Sliding | Fixed size and slide; can overlap | Last ten minutes of activity, recomputed every minute |
| Session | Groups activity separated by less than an inactivity gap | Customer browsing sessions |
| Global | One ongoing window per key; needs a suitable trigger | Custom count-driven grouping |

A ten-minute sliding window with a one-minute slide can assign an event to ten windows, increasing work and state. Sessions merge when an event bridges two activity periods. Keyed windows group independently per key; `windowAll` is a non-keyed operation and can become a single-subtask bottleneck. [Window assigners and keyed windows](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/windows/).

### Assigners, functions, and triggers have different jobs

- **Assigner:** decides which windows receive an event.
- **Window function:** calculates the result, such as a sum.
- **Trigger:** decides when to evaluate and emit a result.
- **Allowed lateness:** controls how long an event-time window remains available for late updates after its normal firing boundary.

Default event-time triggers fire when the watermark reaches the window's maximum timestamp. A custom trigger replaces the default; a count trigger alone does not automatically retain event-time firing. Firing and purging are separate decisions: emitting a result does not necessarily clear window contents. [Trigger behavior](https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/dev/datastream/operators/windows.md#triggers).

Use incremental `reduce` or `aggregate` when possible. A full `ProcessWindowFunction` can retain all records; combining it with an incremental aggregate provides window metadata without retaining the entire input. [Window functions](https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/dev/datastream/operators/windows.md#window-functions).

### A late-event timeline

Consider a tumbling window `[12:00, 12:05)` with one minute of allowed lateness. Window progress follows the operator watermark, not the time on a wall clock.

| Event or progress update | Result with the default event-time trigger |
| --- | --- |
| Records at `12:01` and `12:04` arrive before closure | Included in the window |
| Watermark advances to `12:05:00` | Initial result has fired; state is retained for late updates |
| Record timestamped `12:03` arrives while watermark is `12:05:30` | Included and causes a late firing with an updated result |
| Watermark advances beyond `12:06:00` | Window state has been cleaned up |
| Another `12:03` record arrives after cleanup | Sent to the configured late side output, or dropped if none exists |

For millisecond time windows, the exact firing boundary is `end - 1 ms`, and the cleanup boundary is that timestamp plus allowed lateness. The table uses times beyond those boundaries to keep the example readable. Default allowed lateness is zero. [Allowed lateness and late firings](https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/dev/datastream/operators/windows.md#allowed-lateness).

### Watermark delay versus allowed lateness

A watermark bound delays the initial conclusion that event time has advanced. Allowed lateness retains window state after the normal firing so older records can update its answer. Increasing either can increase state retention, but their effects on output timing differ.

If a window first emits `100` and later emits `105`, these are successive answers for the same window. Adding both downstream would produce an incorrect total of `205`. Use a stable identity such as `(customer_id, window_start, window_end)` and an output contract that handles updates.

DataStream custom triggers, allowed lateness, and side outputs are explicit APIs. Do not assume the SQL window query inherits those settings or emits the same late revisions; standard SQL window aggregation produces a final window result according to its supported time semantics. [SQL window result emission](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/window-agg/).

## 5. Stateful Operators, Keyed State, and RocksDB

### Why state exists

A filter can often decide what to do from one event. A running customer count cannot: it needs to remember earlier events, as do stream joins, deduplication, and sequence detection. **Flink-managed state** makes that memory part of checkpoint and restore operations. Keeping a value in an ordinary mutable field does not automatically make it recoverable.

After `keyBy(customer_id)`, keyed state is scoped to the current key. An operator subtask can own many customers, while each customer belongs to one subtask for that keyed operator. State updates are local to the owner; Flink redistributes the ownership when parallelism changes. [Stateful stream processing](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/concepts/stateful-stream-processing/).

### Keyed state primitives

| Primitive | Stores | Example |
| --- | --- | --- |
| `ValueState<T>` | One value per key | Last observed sequence number |
| `ListState<T>` | A list per key | A bounded set of pending events |
| `MapState<K, V>` | A map within each outer stream key | Event IDs already seen for a customer |
| `ReducingState<T>` | An incrementally reduced value | Running maximum |
| `AggregatingState<IN, ACC, OUT>` | An accumulator with distinct input/output types | Running average from sum and count |

`MapState` keys are nested inside the outer `keyBy` key. A customer with many nested entries still occupies the ownership partition for that customer. State descriptors identify managed state and its serialization contract. [Working with keyed state](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/state/).

### Operator state, broadcast state, and timers

Operator state belongs to an operator instance rather than a stream key. For list operator state, redistribution can split entries across new subtasks; union state gives each restored subtask the combined list. Broadcast state is replicated to downstream parallel instances and is useful for a relatively small rule set. These are different contracts from keyed-state partitioning. [Operator state](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/state/#using-operator-state), [broadcast state](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/broadcast_state/).

A keyed process function can register event-time or processing-time timers. Timers are checkpointed with managed state. Event-time timers wait for watermark progress; processing-time timers depend on worker clocks. Neither a timer nor a checkpoint makes arbitrary external calls transactional. [Process functions and timers](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/process_function/).

### Bounding state with cleanup and TTL

Retain only the history the business rule needs. Windows, explicit timer-driven cleanup, and state TTL address different retention needs. In the established keyed-state API, TTL uses processing time; physical cleanup depends on backend and cleanup strategy. TTL is not event-time allowed lateness and does not promise immediate deletion from every historical snapshot.

For deduplication, a one-day TTL means an ID can be accepted again after expiry. Choose the horizon from replay and duplicate-arrival requirements, and account for restarts and backlog. [State TTL](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/state/#state-time-to-live-ttl).

### RocksDB versus heap state

| Backend | Working-state storage | Main tradeoff |
| --- | --- | --- |
| `HashMapStateBackend` | Java objects on JVM heap | Fast access, constrained by heap capacity and garbage collection |
| `EmbeddedRocksDBStateBackend` | Serialized keys/values in embedded local RocksDB | Large state with serialization, native-memory, disk, and compaction costs |

RocksDB uses local disk plus memory for caches and write buffers. It is not a remote shared database, and choosing it does not make local state survive worker loss by itself. Recovery still requires durable checkpoint data. Incremental RocksDB checkpoints can reuse unchanged SST files; restore needs the complete set of referenced files, not just the latest uploaded differences. Never delete shared checkpoint files manually. [State backends and incremental checkpoints](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/state_backends/).

Keep **state backend** and **checkpoint storage** separate: the backend manages working state; checkpoint storage holds durable recovery snapshots. Flink 2.2 also documents ForSt, so RocksDB is not the only backend choice. [Backend choices](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/state_backends/#available-state-backends).

**Interview answer:** choose RocksDB when state size and heap pressure justify its access and operational costs. Measure state access latency, compaction, checkpoint size, local disk, and native memory before tuning it.

## 6. Checkpoints, Savepoints, and Fault Tolerance

### What a checkpoint captures

If a worker fails after processing several orders, restoring its totals without restoring the matching input position can produce a wrong answer. A **checkpoint** captures a consistent recovery position across source progress, managed operator state, and participating sink state. Unaligned checkpoints also capture in-flight channel data. The snapshots must survive worker failure, and the source must retain enough history to replay from that position.

Checkpointing is disabled by default in a plain DataStream environment. Enabling it is necessary but insufficient: configure durable storage, recovery behavior, timeouts, and source/sink support. A checkpoint does not copy an entire Kafka topic or provide infinite replay retention. [Checkpointing requirements and configuration](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/checkpointing/).

### Aligned checkpoint sequence

1. The coordinator starts a checkpoint, and sources inject barriers into their output streams at recorded positions.
2. Barriers travel through the same logical channels as records.
3. At a multi-input operator, an aligned checkpoint holds channels that have received a barrier until the remaining barriers arrive.
4. Operators snapshot a consistent state and forward barriers; persistence commonly continues asynchronously.
5. After all required acknowledgments, the coordinator marks the checkpoint complete. Participating sinks can finalize the associated output according to their protocol.

Alignment separates records before and after the snapshot across inputs. It does not require stopping the entire job for the full upload duration. [Distributed snapshots and barriers](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/concepts/stateful-stream-processing/#state-persistence).

### Failure and replay example

```text
Completed checkpoint C: source next offset = 501; customer count = 100
Process offsets 501..520:                         customer count = 120
Worker fails before the next checkpoint completes
Restore C:             source next offset = 501; customer count = 100
Replay offsets 501..520:                          customer count = 120
```

The computation executes those records again, but restored state does not retain their earlier, uncheckpointed effects. An external append performed during the first attempt could still remain; sink coordination is covered in the next section.

For Kafka sources, checkpointed source positions drive recovery. Broker-committed consumer offsets can expose progress to monitoring or other consumers, but they are not a replacement for Flink's coordinated snapshot. [Kafka source fault tolerance](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/#fault-tolerance).

### Aligned versus unaligned checkpoints

Unaligned checkpoints include buffered records so checkpoint barriers can progress without waiting for normal alignment. They can help when backpressure makes barrier travel and alignment dominate checkpoint duration. They increase snapshot I/O and can increase restore work; they do not increase a saturated sink's sustainable throughput.

Measure start delay, alignment time, snapshot/upload time, and snapshot size separately. A slow checkpoint caused by storage I/O needs a different fix from one delayed behind network buffers. Buffer debloating can reduce the amount of in-flight data. [Checkpointing under backpressure](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/checkpointing_under_backpressure/).

### Checkpoints versus savepoints

| Property | Checkpoint | Savepoint |
| --- | --- | --- |
| Primary purpose | Automatic recovery after failure | Planned update, migration, or operational restart |
| Creation | Usually periodic and coordinated by Flink | Explicitly requested |
| Lifecycle | Flink manages retention; externalized retention can be configured | User manages retention and disposal |
| Format | Backend-specific, potentially incremental | Canonical or backend-native |
| Portability | Depends on format and compatibility | Canonical format provides broader migration flexibility |

Both can be restore inputs for supported rescaling. A savepoint is a consistent job-state snapshot, not a backup of external systems or a guarantee that arbitrary code changes are compatible. Native savepoints trade portability for efficiency. [Checkpoint and savepoint comparison](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/checkpoints_vs_savepoints/).

Assign stable operator UIDs, preserve state descriptor names, and check serializer and key compatibility before an update. Automatically generated operator IDs can change when the job graph changes. Do not discard unmatched state merely to make an incompatible restore succeed. Restoring an older savepoint also rewinds source progress; plan how already-published external output will be reconciled. [Savepoints and operator identity](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/savepoints/).

**Interview tradeoff:** frequent checkpoints reduce the potential replay interval but add overhead. Recovery time also includes scheduling, state download, source replay, and catching up to the live input rate.

## 7. Exactly-Once Processing and Transactional Sinks

### Define the boundary of the guarantee

After restoring a checkpoint, Flink may execute some input records again. The question is whether the repeated execution changes the committed result. Separate the guarantee for managed state from the guarantee for an external destination: a correct state snapshot alone cannot make an arbitrary external write atomic.

| Guarantee | Failure behavior |
| --- | --- |
| At-most-once | Records can be lost; failed attempts are not retried in a way that duplicates them |
| At-least-once | Records are retried; effects can appear more than once |
| Exactly-once state consistency | Recovery and replay leave managed state equivalent to applying each logical input once |
| End-to-end exactly-once effects | External visible results also avoid duplicate effects, under the source and sink protocol |

Exactly-once does not mean each record executes only once. The recovery example above executes records twice. It means repeated execution does not create repeated committed effects within the promised boundary. [State consistency guarantees](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/concepts/stateful-stream-processing/).

End-to-end guarantees require replayable input, consistent snapshots, and a sink whose transactions or idempotent protocol cover the output effects. Calls to a payment API, email service, or arbitrary database write inside `map` are outside Flink's managed state contract. [Fault-tolerance concepts](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/learn-flink/fault_tolerance/).

### Transactional sink lifecycle

A checkpoint-aware transactional sink connects output publication to recovery progress. Conceptually:

1. Write records into an uncommitted transaction.
2. At a checkpoint boundary, prepare the current transaction and record enough sink state to recover its outcome.
3. Continue writing subsequent records into a new transaction.
4. When the checkpoint succeeds, commit the prepared output.
5. On failure, recover pending commits or abort transactions according to the connector protocol.

Failure after the durable checkpoint but before the commit response is a critical case: the connector must safely resolve the transaction without silently losing or duplicating output. Two-phase commit is a common model; actual sink implementations can use committers or other checkpoint-coordinated mechanisms. [Sink API and committers](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/sinks/).

### Kafka transactional sinks

For `KafkaSink`, the exactly-once delivery mode uses Kafka transactions coordinated with checkpoints. Configure a transactional ID prefix unique among concurrently running applications on the Kafka cluster. Downstream consumers must use `isolation.level=read_committed` to hide aborted and uncommitted records.

Transaction timeouts must cover the checkpoint and recovery lifecycle, with margin, and fit broker limits. Successful checkpoints affect when output becomes visible, so checkpoint cadence is also a publication-latency tradeoff. A Flink checkpoint setting alone does not configure the sink's delivery guarantee. [Kafka sink guarantees and timeout requirements](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/).

### Idempotence and multiple external effects

An upsert keyed by `(customer_id, window_start, window_end)` can replace a repeated window result safely. By contrast, retrying `UPDATE totals SET amount = amount + 5` increments twice unless a transaction or deduplication protocol prevents it. Stable identities, suitable unique constraints, and version rules must match the business operation.

An idempotent final row does not automatically deduplicate database triggers, audit records, or notifications. Similarly, two sinks that each provide exactly-once delivery do not automatically create one atomically visible transaction across both destinations. Identify every external effect when stating the guarantee. [Related Kafka delivery semantics](../ingestion/kafka.md#5-delivery--processing-semantics).

**Interview trap:** an updated result from a late window firing is a legitimate revision. Checkpoint-based exactly-once does not turn it into the same thing as a failure-induced duplicate.

## 8. Backpressure, Rescaling, and State Redistribution

### How backpressure propagates

Suppose a database sink slows down while incoming orders continue at the same rate. Its output buffers fill, so upstream tasks have to wait; that pressure can travel back through the job until input backlog grows in Kafka or another retained source. This is **backpressure**, and following its direction helps distinguish the bottleneck from the tasks waiting behind it.

```text
records:       source -> parse -> aggregate -> slow database sink
backpressure:  source <- parse <- aggregate <- sink bottleneck
```

The task reported as backpressured is often waiting on a downstream bottleneck. Examine `busyTimeMsPerSecond`, `idleTimeMsPerSecond`, and `backPressuredTimeMsPerSecond` by subtask; a job-level aggregate can hide skew. Busy time is task execution time, not a direct CPU-utilization measurement. [Monitoring backpressure](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/monitoring/back_pressure/).

### Diagnose before adding resources

| Observation | Possible cause | Useful next check |
| --- | --- | --- |
| Most upstream tasks wait; sink requests are slow | Destination capacity or throttling | Sink latency, errors, connection limits, destination load |
| One keyed subtask is busy; peers are idle | Hot key or uneven keys | Per-subtask rates and key-frequency distribution |
| RocksDB latency rises with local disk usage | Compaction pressure or disk saturation | Disk latency, compaction metrics, state growth |
| Long JVM pauses | Heap pressure or allocation rate | GC logs and heap/native-memory split |
| Checkpoint alignment rises sharply | Barriers delayed by buffered data | Backpressure path, start delay, buffer volume |
| Windows stall without high input lag | Slow or idle watermark input | Per-input watermarks and idleness configuration |

These are diagnostic hypotheses, not one-to-one rules. For example, a sink that waits on network I/O can be slow despite low CPU. Increasing parallelism can worsen pressure on a destination that already limits request capacity.

**Capacity example:** input arrives at 100,000 records/second, but the sink sustains 80,000. Backlog grows by 20,000 records/second. After fixing the limit, capacity must exceed 100,000 to drain existing backlog while new events continue arriving.

### Key groups and maximum parallelism

Flink hashes keys into key groups. A key group is the unit of keyed-state redistribution, and the configured maximum parallelism determines how many groups exist. Each subtask owns a range of groups; rescaling changes the ranges assigned to subtasks while retaining each key's state. [Key groups](https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/concepts/stateful-stream-processing.md#keyed-state).

```text
max parallelism = 8, so key groups are 0..7

parallelism 2: subtask A owns 0..3; subtask B owns 4..7
parallelism 4: subtask A owns 0..1; B owns 2..3; C owns 4..5; D owns 6..7
```

A normal restore cannot arbitrarily change maximum parallelism because that changes the keyed-state partitioning contract. Plan headroom before deployment. A single hot key still belongs to one subtask after scaling; splitting it requires a valid application-level aggregation strategy. [Maximum parallelism and capacity planning](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/large_state_tuning/#capacity-planning).

### Planned rescaling

1. Confirm the bottleneck can benefit from more parallelism and that source/sink capacities permit it.
2. Check available slots, maximum parallelism, operator UIDs, serializer compatibility, and snapshot storage.
3. Stop with a savepoint without `--drain` for a job that will resume, then restore with the intended parallelism.
4. Flink redistributes keyed state by key groups and operator state by its redistribution contract.
5. Verify state restoration, checkpoints, watermarks, output correctness, throughput, and backlog drain.

Draining advances event time to its end, firing timers and closing windows. Reserve it for permanent termination; resuming from a drained pipeline can produce incorrect results. [Stopping with savepoints](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/cli/#stopping-a-job-gracefully-creating-a-final-savepoint).

Adding TaskManagers does not automatically change every running job's parallelism. Adaptive scheduling, reactive mode, or an external autoscaler can automate supported rescaling workflows, but deployment mode and scheduler configuration matter. Budget for recovery and state transfer; automation does not imply interruption-free scaling. [Elastic scaling](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/elastic_scaling/).

**Interview trap:** a Kafka source with fewer partitions than source subtasks cannot keep all subtasks actively reading partitions. Downstream operators can still have different parallelism after repartitioning.

## 9. Interview Questions and Review Checklist

### Questions to practice aloud

Practice by explaining a job whose windows stopped emitting or whose throughput did not improve after adding workers. A useful answer follows the relevant boundary—watermark progress, state ownership, checkpoint recovery, or sink capacity—rather than naming a setting in isolation. The questions below give you concrete cases to work through.

| Question | What a strong answer covers |
| --- | --- |
| Does one slot equal one core? | Scheduling and managed memory; shared CPU; worker resource sizing |
| Why use SQL instead of DataStream? | Relational planning versus explicit state/timer control; changelog and sink requirements |
| Why are event-time windows not emitting? | Watermark generation and minimum active-input progress; timestamp errors; idle partitions |
| Is an event behind the watermark always dropped? | Operator-specific policy; window lifetime; allowed lateness; side output |
| Does a trigger remove a window? | Firing, purging, and cleanup are separate decisions |
| Does RocksDB eliminate memory concerns? | Native caches and buffers, heap, disk, serialization, compaction, durable checkpoints |
| How do checkpoints provide exactly-once state? | Consistent source/state snapshot, restore, replay, and rollback of uncheckpointed effects |
| What happens if the sink commits but the response is lost? | Recoverable transaction identity or idempotent commit protocol |
| Can I upgrade any job from a savepoint? | Stable UIDs, state/key/serializer compatibility, snapshot format, external-output reconciliation |
| Why did adding workers not improve throughput? | Actual parallelism, hot keys, source partitions, sink capacity, or another shared bottleneck |

### End-to-end scenario

**Prompt:** build five-minute purchase totals per customer from Kafka, tolerate out-of-order events, publish recoverable results, and handle a traffic spike.

A clear answer walks through these decisions:

1. Validate event schemas, business timestamps, and keys; decide how malformed records are handled without silently dropping them.
2. Use an event-time watermark strategy based on measured arrival behavior, with an explicit idle-input policy.
3. Key by customer and use an incremental window aggregate. Define whether late events revise totals, go to reconciliation, or are rejected.
4. Bound state through window cleanup and any required deduplication horizon. Choose a backend from state size and measured access costs.
5. Enable checkpoints to durable storage and retain enough Kafka history for the intended recovery window.
6. Choose a compatible sink guarantee. If publishing to Kafka transactionally, configure transaction identity, timeouts, and committed reads.
7. Give revised window totals a stable identity so downstream consumers handle them as updates.
8. Observe lag, watermarks, per-subtask load, checkpoint health, state size, and sink latency; scale the stage that limits sustainable throughput.

Before finishing an interview answer, name the guarantee's boundary, the failure you recover from, and the business tradeoff: freshness versus late-data completeness, state cost versus retention, or checkpoint overhead versus recovery work.
