# Apache Spark: Data Engineer Interview Guide

A Spark job that looks like a few DataFrame operations can send terabytes across a cluster. Another job may finish quickly until one unusually large customer leaves a single task running long after the rest. To explain either outcome, follow the work: where it runs, how data moves, what state it builds, and how it recovers. This guide uses that path to connect Spark's execution model to practical tuning decisions.

**Reference versions:** Apache Spark 3.5.6 and 4.0.0. These are explicit documentation baselines, not a claim about the latest release. Examples use PySpark and assume an existing `SparkSession` named `spark`. Table names, paths, and Kafka endpoints are illustrative. Iceberg and Delta Lake require compatible connectors and catalog configuration; their features depend on the installed version. Sizing examples are estimates to validate with measurements.

## Contents

1. [Architecture & Execution](#1-architecture--execution)
2. [Query Optimization](#2-query-optimization)
3. [PySpark Internals](#3-pyspark-internals)
4. [Partitioning & Shuffling](#4-partitioning--shuffling)
5. [Joins & Aggregations](#5-joins--aggregations)
6. [Memory & Resources](#6-memory--resources)
7. [Storage & Lakehouse](#7-storage--lakehouse)
8. [Structured Streaming](#8-structured-streaming)
9. [Performance & Production](#9-performance--production)

## 1. Architecture & Execution

### Driver, executors, and cluster manager

Submitting a Spark application starts a conversation between the process planning the work and the processes executing it. The driver decides what needs to run, executors perform tasks, and a cluster manager supplies resources. Knowing which role owns a problem prevents treating every memory error or delay as an executor issue.

| Component | Responsibility | Typical failure or limit |
| --- | --- | --- |
| Driver | Runs the application entry point, builds plans, schedules work, tracks progress | Large `collect()` results or oversized planning metadata exhaust driver memory |
| Executor | Application-specific JVM process that runs tasks and holds cache/shuffle data; Python workers are separate processes | Task failures, heap exhaustion, container termination, lost local files |
| Cluster manager | Allocates resources to applications: Spark Standalone, YARN, or Kubernetes | Resource shortages leave executors pending |

The driver schedules tasks; the cluster manager allocates their execution resources. Executors usually run multiple tasks concurrently. With ordinary CPU tasks, available slots are approximately executor cores divided by `spark.task.cpus`, subject to other resource requirements. A worker machine can host several executors. In client deploy mode the driver runs with the submitting client; in cluster mode it runs inside the cluster. [Spark cluster overview](https://spark.apache.org/docs/3.5.6/cluster-overview.html).

### Jobs, stages, tasks, and DAG scheduling

- **Application:** driver plus its executors.
- **Job:** computation submitted to produce an action's result. One SQL action can involve multiple jobs, for example for broadcast preparation or adaptive execution.
- **Stage:** tasks that can run together without crossing a shuffle dependency.
- **Task:** processes one partition for a stage attempt. Retries create additional task attempts.
- **DAG:** directed acyclic graph of dependencies. The DAGScheduler creates stages; the TaskScheduler submits their tasks to executors. [DAGScheduler implementation](https://github.com/apache/spark/blob/v3.5.6/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala).

```text
Read partitions -> filter -> project -> partial aggregate
                                           |
                                        shuffle
                                           |
                                  final aggregate -> write
```

The diagram shows a typical aggregation. Filtering and projecting can be pipelined into each reading task; the shuffle redistributes intermediate results by grouping key. Exact stage and job counts depend on the physical plan, available partitioning, and AQE.

### Lazy evaluation and dependencies

Transformations describe work; actions such as `count()`, `collect()`, and writes request execution. Laziness lets Spark optimize the whole query before processing rows. Schema inference, file discovery, and planning can still perform work before the main action.

| Dependency | Data movement | Examples |
| --- | --- | --- |
| Narrow | Within a dependency, each parent partition feeds at most one child partition | `filter`, projections, RDD `map`, DataFrame `coalesce` |
| Wide | Parent partitions contribute to multiple downstream partitions through a shuffle | `repartition`, aggregations or joins that need new distribution |

An operation's name does not prove it shuffles: a broadcast join avoids shuffling the large side, and Spark may reuse an already suitable distribution. Lazy execution and recomputation are core RDD properties. [RDD programming guide](https://spark.apache.org/docs/3.5.6/rdd-programming-guide.html).

```python
from pyspark.sql import functions as F

orders = spark.read.parquet("/data/orders")
totals = (
    orders.filter(F.col("status") == "paid")
    .groupBy("customer_id")
    .agg(F.sum("amount").alias("total_amount"))
)
totals.explain("formatted")
totals.show(10)
```

Before `show`, `totals` represents a query. Predict a scan, filter, partial aggregate, exchange, and final aggregate, then verify the actual plan.

### Fault tolerance, lineage, and retries

Lineage records dependencies needed to recompute lost partitions. Executor loss can require rebuilding cached blocks or shuffle output. A fetch failure can cause an upstream shuffle stage to run again. Task retries handle transient faults; repeatedly failing code or an oversized partition normally fails again. Driver failure requires application-level recovery, rather than lineage alone. [DAGScheduler failure handling](https://github.com/apache/spark/blob/v3.5.6/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala).

**Interview question: Can a task run more than once?** Yes: retries and speculative execution can create multiple attempts. Therefore, an external payment, API call, or database insert inside task code needs an idempotency strategy. Spark's ability to recompute data does not make arbitrary side effects exactly once.

## 2. Query Optimization

### Catalyst, logical plans, and physical plans

Writing a filter in SQL or as a DataFrame expression may produce the same execution plan. Both use **Catalyst**, which represents queries as trees and applies analysis, rewrite rules, and planning. Changing syntax alone therefore does not fix a slow query; the useful question is whether the change alters the work in the plan. [Catalyst design](https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html).

| Plan | Question it answers | Example |
| --- | --- | --- |
| Parsed logical | What operations were requested? | Unresolved table and column names |
| Analyzed logical | Are references and types valid? | Resolved columns and inserted casts |
| Optimized logical | Which equivalent relational expression is cheaper? | Removed columns, folded constants, moved filters |
| Physical | Which operators and distributions implement it? | `BroadcastHashJoin`, `SortMergeJoin`, `Exchange` |

Use `df.explain("extended")` for logical and physical plans, `"formatted"` for physical operator details, `"cost"` for available estimates, and `"codegen"` for generated code. With AQE, also inspect the completed query in the UI: a pre-execution plan may not be final. [EXPLAIN reference](https://spark.apache.org/docs/3.5.6/sql-ref-syntax-qry-explain.html).

### Predicate pushdown, partition pruning, and CBO

These optimizations act at different boundaries:

- **Predicate pushdown:** a data source receives a supported filter so it can reduce data returned to Spark. Unsupported expressions, including many Python UDF predicates, cannot be pushed down.
- **Partition pruning:** eliminate storage partitions from a scan using partition predicates. Dynamic partition pruning can derive a filter from another side of a join.
- **Column pruning:** read only needed columns.

For a Parquet scan, examine `PartitionFilters`, `PushedFilters`, and `ReadSchema`; a pushed predicate does not guarantee every irrelevant row is filtered inside the reader. [Spark Parquet filtering](https://spark.apache.org/docs/3.5.6/sql-data-sources-parquet.html).

**Cost-based optimization (CBO)** uses estimates such as row counts and column statistics to compare alternatives. Stale statistics can underestimate a join or choose a poor order. For supported catalog tables:

```sql
ANALYZE TABLE orders COMPUTE STATISTICS;
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS customer_id;
```

Collecting statistics has a cost; available statistics and `ANALYZE` support depend on the catalog. `spark.sql.cbo.enabled` enables CBO, while join reordering has its own setting, `spark.sql.cbo.joinReorder.enabled`. [ANALYZE TABLE](https://spark.apache.org/docs/3.5.6/sql-ref-syntax-aux-analyze-table.html), [SQL configuration](https://spark.apache.org/docs/3.5.6/configuration.html).

### Tungsten and Whole-Stage CodeGen

Tungsten names Spark's work on compact binary data, explicit memory management, CPU cache efficiency, and code generation. It reduces object allocation and interpretation overhead. It does not imply all Spark memory is off-heap. [Original Tungsten design](https://www.databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html).

Whole-Stage CodeGen fuses compatible physical operators into generated Java code. A scan/filter/project pipeline can avoid repeated virtual calls and intermediate objects. Exchanges and unsupported operators break fusion; Python UDF bodies are not compiled into this Java pipeline. A codegen stage identifier is not a DAGScheduler stage identifier. [Whole-stage code generation implementation](https://github.com/apache/spark/blob/v3.5.6/sql/core/src/main/scala/org/apache/spark/sql/execution/WholeStageCodegenExec.scala).

### Adaptive Query Execution (AQE)

AQE revises eligible physical plans using runtime statistics, including shuffle output sizes. It can coalesce small post-shuffle partitions, split eligible skewed join partitions, and change join strategies. It is enabled by default in upstream Spark 3.5.6, but verify runtime settings. Its advisory partition size is a target influenced by other settings, not a hard limit. [AQE documentation](https://spark.apache.org/docs/3.5.6/sql-performance-tuning.html#adaptive-query-execution).

**Interview question: Does AQE replace good query design?** No. It cannot remove required output rows, make an arbitrary UDF transparent, or undo every scan and shuffle already performed. First reduce unnecessary data and validate join semantics; then inspect the adaptive result.

## 3. PySpark Internals

### Python, the JVM, and Py4J

A PySpark application is written in Python, but its rows do not necessarily pass through Python one at a time. In classic PySpark, driver code uses Py4J to build and submit work to the driver JVM. Built-in DataFrame expressions normally run in JVM operators on the executors. That execution boundary matters when comparing a built-in expression with a Python UDF.

Python RDD functions and Python UDFs require executor-side Python workers. Functions, dependencies, and data cross a serialization boundary. Python workers can be reused, but retain their own process memory and imported libraries. [PySpark process architecture](https://spark.apache.org/docs/4.0.0/api/python/development/debugging.html).

PySpark uses cloudpickle to serialize Python functions/closures; ordinary Python object transfer uses pickle-based serialization, while eligible paths use Arrow batches. A large captured closure increases distribution cost. Configuring a JVM serializer such as Kryo does not eliminate Python serialization or worker execution costs. [PySpark serialization implementation](https://github.com/apache/spark/blob/v3.5.6/python/pyspark/serializers.py).

```text
Classic DataFrame expressions:
Python driver --Py4J--> driver JVM --tasks--> executor JVM

Python UDF execution adds:
executor JVM <--serialized data / Arrow batches--> Python worker
```

Spark Connect changes the client boundary: it sends unresolved plans using a remote protocol based on gRPC rather than direct client-side Py4J access to the driver JVM. RDD APIs and direct JVM access are not generally available through Connect. [Spark Connect overview](https://spark.apache.org/docs/4.0.0/spark-connect-overview.html).

### DataFrame versus RDD

| Property | DataFrame | Python RDD |
| --- | --- | --- |
| Representation | Schema and relational expressions | Distributed Python objects and functions |
| Optimizer visibility | Columns, filters, joins, aggregates | Python function bodies are opaque |
| Execution | Built-in SQL operators usually stay in JVM execution | Python transformations require Python workers |
| Typical choice | Structured ETL, SQL, joins, aggregations | Work that genuinely needs the lower-level API |

Choose DataFrames first for relational work. Converting through `.rdd` introduces a boundary that loses relational optimization across the Python function. RDDs still provide partition control and dependency-based recovery. [Structured query execution](https://spark.apache.org/docs/3.5.6/sql-programming-guide.html), [RDD API model](https://spark.apache.org/docs/3.5.6/rdd-programming-guide.html).

### Python UDFs, Pandas UDFs, and Arrow

| Approach | Execution model | Main cost or constraint |
| --- | --- | --- |
| Built-in expression | Native Spark SQL operator | Limited to supported expressions |
| Regular Python UDF | Scalar Python function; default transport uses Python serialization | Python calls, object conversion, opaque logic |
| Arrow-optimized Python UDF | Scalar Python semantics with Arrow transport | Faster transfer does not vectorize the function body |
| Pandas UDF | Arrow batches passed to pandas/NumPy operations | Batch memory and pandas conversion; speed depends on the implementation |

Arrow is a columnar interchange format, not an optimizer for Python logic. Modern Spark supports `udf(..., useArrow=True)`; verify type coercion and compatibility when enabling it. Prefer a built-in such as `F.upper("name")` over writing an equivalent Python UDF. [Python UDF execution options](https://spark.apache.org/docs/4.0.0/api/python/user_guide/udfandudtf.html).

Arrow batch sizing can control peak transfer memory, but it does not make `toPandas()` distributed: the result still has to fit on the driver. Grouped pandas operations such as `applyInPandas` can load an entire group into a worker; the usual Arrow batch row limit does not split that group. One hot key can therefore cause Python OOM even when typical batches are small. [Arrow and pandas memory considerations](https://spark.apache.org/docs/4.0.0/api/python/tutorial/sql/arrow_pandas.html).

**Interview question: Is PySpark always slower than Scala?** No. Equivalent built-in DataFrame expressions often reach the same JVM plan. The difference grows when the workload repeatedly crosses into Python or spends substantial time in Python code.

## 4. Partitioning & Shuffling

### Three meanings of partition

A discussion about increasing partitions can go wrong if two people mean different things by the word. Spark execution, table layout, and Kafka each have their own partition boundary. Identify which one you are changing before predicting an effect on parallelism or data movement.

- **Execution partition:** a unit of distributed processing for a stage.
- **Storage partition:** a table layout such as directories by date or an Iceberg partition transform.
- **Kafka partition:** an ordered source log with its own offsets.

Their counts need not match. A table storage partition can contain many files; Spark can split or combine file inputs into execution partitions. A later shuffle creates a new distribution independent of the input layout.

### `repartition()` versus `coalesce()`

| API | Behavior | Appropriate use |
| --- | --- | --- |
| `df.repartition(n)` | Shuffle to a requested number of execution partitions | Increase parallelism or redistribute work |
| `df.repartition(n, "key")` | Shuffle using hash partitioning by expressions | Establish a key distribution |
| `df.coalesce(n)` | Reduce partition count with a narrow dependency | Modest reduction after filtering when balance is already acceptable |

`coalesce` does not increase partitions beyond the current count and does not rebalance records through a full shuffle. A drastic reduction, especially `coalesce(1)`, can collapse upstream parallelism. `repartition` pays redistribution cost but can produce better balance. Hashing by a skewed key still leaves that key together. [Repartition API](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html), [coalesce API](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce.html).

### Hash and range partitioning

Hash partitioning maps equal keys to the same partition; it promises neither balanced key frequencies nor order. Range partitioning samples values to choose boundaries and assigns contiguous key ranges. Sampling can produce imperfect balance, and `repartitionByRange` does not sort rows inside each partition; add `sortWithinPartitions` when that ordering is required. [Range partitioning API](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartitionByRange.html).

### Shuffle mechanics and spilling

Shuffle producers assign records to destination partitions, buffer/organize them, and write shuffle blocks. Downstream tasks fetch their blocks, deserialize them, and perform the next operation. Costs include network traffic, serialization, disk I/O, and scheduling. Local disks therefore matter even for data stored remotely. Memory pressure can spill intermediate sort/aggregation state to disk; spill can prevent OOM but increase runtime. [Shuffle behavior](https://spark.apache.org/docs/3.5.6/rdd-programming-guide.html#shuffle-operations).

### Skew, salting, and sizing

**Skew** means a few partitions receive disproportionate work. Look for high maximum task duration relative to the median, uneven shuffle bytes, or one hot key. More executors do not split a single hash key automatically.

For a skewed inner equijoin, first consider broadcasting a genuinely small side or AQE skew handling. If salting is needed, distribute hot-key rows on the large side across salt values, replicate matching small-side rows across those values, and join on `(key, salt)`. Salting only by hashing the hot key assigns every such row the same salt and solves nothing. This approach increases data volume; outer joins and deduplication require separate correctness analysis.

For decomposable aggregates, aggregate by `(key, salt)` and then by `key`. Sum partial sums; compute averages from total sum and total count, rather than averaging averages. Do not apply that recipe blindly to exact distinct counts or medians.

Estimate partitions as `ceil(expected stage bytes / target bytes per partition)`, then check task duration, available slots, skew, and in-memory expansion. For example, 256 GiB divided by a trial 128 MiB gives 2,048 partitions. This is an arithmetic starting point, not a Spark default or universal recommendation. Input split size, shuffle partition count, and output file size are separate tuning decisions.

**Interview question: Why can 1,000 partitions still use one core at the end?** An operator may require a single partition, one skewed task may dominate, or a final coalesce may have reduced parallelism. Inspect the stage that is actually slow.

## 5. Joins & Aggregations

### Join algorithms

To join a large orders table with customer data, Spark must arrange for matching keys to meet. It can copy a small side, shuffle and sort both sides, or build hash tables after a shuffle. The best choice depends on the actual input size and distribution, especially the memory required by the largest build partition.

| Strategy | How it works | Good fit | Main risk |
| --- | --- | --- | --- |
| Broadcast Hash Join | Distribute the build side; probe its hash relation while processing the other side | Equijoin with a safely small build side | Broadcast construction, transfer, and per-executor memory |
| Sort-Merge Join | Establish compatible key distribution and sort both inputs, then merge matching keys | Large equijoins with sortable keys | Shuffle, sort, spill, and hot keys |
| Shuffled Hash Join | Establish compatible distribution, then build a local hash table per partition | Equijoin where the build partition fits memory | One unexpectedly large build partition can exhaust memory |

Existing compatible distribution or ordering can remove some shuffle/sort work. A broadcast hint on a non-equijoin can lead to a broadcast nested-loop join rather than a hash join. The actual choice also depends on join type and key support. [Spark join selection implementation](https://github.com/apache/spark/blob/v3.5.6/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala).

### Choosing a strategy, hints, and AQE

Start with semantics: inner, left, full, semi, or anti join; key uniqueness; null behavior; expected output size. A many-to-many join can produce huge output even when the implementation is efficient. Ordinary equality does not match nulls; SQL `<=>` is null-safe equality when that behavior is required.

```python
from pyspark.sql import functions as F

customers = spark.table("customers").select("customer_id", "segment")
enriched = spark.table("orders").join(
    F.broadcast(customers), "customer_id", "left"
)
enriched.explain("formatted")
```

Only broadcast after establishing that the build relation fits. Compressed file size is not its in-memory hash-table size. Confirm that duplicate customer keys will not multiply order rows.

Hints include `BROADCAST`, `MERGE`, and `SHUFFLE_HASH`. They influence planning but cannot make an unsupported strategy valid. Spark prioritizes broadcast over merge over shuffle-hash when hints conflict. Inspect the final plan rather than treating a hint as proof. [Join hints](https://spark.apache.org/docs/3.5.6/sql-ref-syntax-qry-select-hints.html).

AQE can discover that an initially large-looking side is small after filtering, or that a shuffle partition is skewed. Runtime correction helps when estimates are poor, but an appropriate initial plan can avoid work that AQE discovers only after materialization.

### Partial aggregation

For aggregates such as sum and count, Spark can reduce records locally before shuffling partial buffers to a final aggregate. This can shrink network traffic dramatically when many local rows share a key. High-cardinality groups reduce that benefit, and large per-key values such as `collect_list` can still exceed memory. Recognize partial/final aggregate operators around an exchange in an explain plan. [Aggregate plan example](https://spark.apache.org/docs/3.5.6/sql-ref-syntax-qry-explain.html).

Example: to average order values per customer, combine `(sum, count)` buffers. If one partition has one value of 100 and another has nine values of 0, the correct average is 10; averaging partition averages gives 50.

### Window functions

`groupBy` reduces rows to groups. A window function computes values over related rows while usually retaining one output row per input row. `partitionBy` defines the window group; `orderBy` defines sequence; the frame defines which neighboring rows participate. Distribution and sorting can be expensive, especially for a global window.

```python
from pyspark.sql import Window, functions as F

running_window = (
    Window.partitionBy("customer_id")
    .orderBy("order_time", "order_id")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
running = spark.table("orders").withColumn(
    "running_total", F.sum("amount").over(running_window)
)
```

Here `order_id` must break timestamp ties deterministically. `ROWS` frames count physical positions; `RANGE` frames use ordering values and can include peers sharing the same value. Declare the frame explicitly when that difference matters. [Window semantics](https://spark.apache.org/docs/3.5.6/sql-ref-syntax-qry-select-window.html).

**Interview question: Why is a left join suddenly producing twice as many rows?** Inspect duplicate keys on the right before tuning execution. Broadcasting changes the algorithm, not the multiplicity of matches.

## 6. Memory & Resources

### JVM heap, off-heap, and Python memory

An executor can be terminated even when its JVM heap appears to have room. PySpark workers, native libraries, buffers, and off-heap allocations also consume memory. Treat the container's total demand as the starting point, then identify which budget covers each part.

| Memory area | Examples | Relevant controls |
| --- | --- | --- |
| Executor JVM heap | Query objects, managed execution/storage memory | `spark.executor.memory` |
| Managed off-heap | Spark-managed allocations outside the heap when enabled | `spark.memory.offHeap.enabled`, `spark.memory.offHeap.size` |
| Other process/native memory | JVM non-heap, native libraries, buffers, Python workers | `spark.executor.memoryOverhead` |
| Explicit PySpark allowance | Python memory requested separately on supported cluster managers | `spark.executor.pyspark.memory` |
| Driver memory | Plans, metadata, collected results, driver-side Python | Driver heap and deployment-specific overhead |

On YARN/Kubernetes, the executor container request includes heap, overhead, configured managed off-heap, and an explicit PySpark allowance when set. Without that PySpark setting, Python shares the overhead budget. Python limiting depends on OS support; setting it is not a universal hard cap on every platform. [Executor resource configuration](https://spark.apache.org/docs/3.5.6/configuration.html).

An illustrative request of 12 GiB heap + 3 GiB overhead + 2 GiB managed off-heap + 1 GiB explicit Python allowance totals 18 GiB. This does not mean a task can use 18 GiB: tasks share resources, and several areas serve other purposes.

### Execution versus storage memory

Execution memory supports sorts, joins, shuffles, and aggregation. Storage memory holds cached blocks. Spark's unified manager lets these share a managed region: execution can reclaim borrowed storage space, while storage cannot evict active execution memory. The whole heap is not available to this region, and Python process memory is outside it. Cache pressure, task concurrency, and object expansion therefore matter alongside heap size. [Unified memory management](https://spark.apache.org/docs/3.5.6/tuning.html#memory-management-overview).

### Caching, persistence, and checkpointing

| Mechanism | Purpose | Recovery behavior |
| --- | --- | --- |
| `cache()` | Reuse a computed dataset with the API's default storage level | Lost blocks can be recomputed from lineage |
| `persist(level)` | Choose an appropriate memory/disk storage policy | Retains lineage; blocks may be evicted or lost |
| Reliable `checkpoint()` | Materialize data in configured durable storage and truncate lineage | Recovery uses checkpoint data |
| `localCheckpoint()` | Truncate lineage using executor-local caching | Executor loss can make recovery impossible |
| Streaming checkpoint | Restore offsets, commits, and state for a streaming query | A separate streaming recovery mechanism |

Caching is lazy; declaring it does not populate all blocks. Cache only a reused, expensive result, and release it with `unpersist()` when finished. DataFrame persistence defaults differ from RDD defaults; in PySpark 3.5.6 DataFrame `persist()` defaults to `MEMORY_AND_DISK_DESER`. [DataFrame persistence](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.persist.html).

```python
from pyspark import StorageLevel
from pyspark.sql import functions as F

paid = spark.table("orders").filter(F.col("status") == "paid")
paid.persist(StorageLevel.MEMORY_AND_DISK)
try:
    paid.groupBy("country").count().show()
    paid.groupBy("payment_method").count().show()
finally:
    paid.unpersist()
```

Reliable DataFrame checkpointing needs a checkpoint directory; `checkpoint(eager=True)` materializes immediately. Unlike cache, it deliberately cuts the earlier logical plan. Checkpointing is useful for growing iterative plans, but adds storage and I/O cost. Local checkpoints sacrifice reliable storage and can be lost with executors. [DataFrame checkpoint](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.checkpoint.html), [local checkpoint limitations](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.localCheckpoint.html).

### Executor sizing, dynamic allocation, and OOM

Choose cores and memory together. More concurrent tasks share the same executor heap and may create more Python workers. Very large executors can suffer long garbage collection pauses and lose more work on failure; tiny executors increase process overhead. Measure the workload instead of adopting one executor shape universally.

Dynamic allocation changes the number of executors as demand changes. It needs a supported shuffle-preservation mechanism, such as external shuffle service, shuffle tracking, or a supported decommission/reliable-storage arrangement. It does not resize individual executor heaps, and state/cache movement can make scaling costly. [Dynamic resource allocation](https://spark.apache.org/docs/3.5.6/job-scheduling.html#dynamic-resource-allocation).

| Symptom | Investigate first | Possible targeted response |
| --- | --- | --- |
| JVM `OutOfMemoryError` | Largest partition, hash table, cache, concurrent tasks | Reduce working set, partition better, reduce concurrency, then size heap |
| Container `OOMKilled` / overhead exceeded | Process RSS, Python/native allocations, configured limits | Address the non-heap consumer and overhead request |
| Python worker crash | Python logs, batch/group size, native library failures | Bound batches/groups or replace the Python operation |
| Driver failure after collection | Returned data volume and driver logs | Keep work distributed or collect a bounded result |

**Interview question: Why did increasing executor heap fail to fix OOM?** The failing process may be Python, the container may be hitting its total memory limit, or one task may still hold an unbounded structure.

## 7. Storage & Lakehouse

### Parquet internals, compression, and vectorization

A revenue query may need only a few columns from a wide orders table. Parquet's layout lets the reader avoid fetching every attribute: a file contains row groups, each row group contains column chunks, and those chunks are divided into pages. The footer describes the schema and file metadata, while available statistics can rule out irrelevant data. Encoding, such as dictionary encoding, is separate from compression; choose a codec by measuring the CPU, storage, and transfer trade-off on realistic reads and writes. [Parquet file format](https://parquet.apache.org/docs/file-format/).

Spark's vectorized Parquet reader decodes batches to reduce per-row overhead. Support depends on types, configuration, and reader capabilities; verify the scan plan. Columnar storage, vectorized reading, and Whole-Stage CodeGen are related optimizations at different layers. [Spark Parquet configuration](https://spark.apache.org/docs/3.5.6/sql-data-sources-parquet.html).

### Pushdown and data skipping

Filtering can avoid data at several levels: storage partitions, table files, Parquet row groups, and finally individual rows. Delta's file statistics, including min/max values, support data skipping; clustering similar values together can make those bounds more selective. An optional format feature such as a page index or Bloom filter is useful only if the writer produces it and the reader uses it. [Delta data skipping](https://docs.delta.io/optimizations-oss/#data-skipping), [Parquet reader feature support](https://parquet.apache.org/docs/file-format/).

Example: a file spanning every customer ID has weak min/max selectivity for a customer filter. Files containing narrower customer ranges can be skipped more effectively. A selective filter is valuable only when the physical layout and reader can exploit it.

### Small files and compaction

Small files increase listing, opening, footer reading, scheduling, and table metadata costs. Avoid overly granular storage partitions and excessive output parallelism. One task can write several files when it touches several table partitions, so task count is not a guaranteed file count.

Compaction rewrites small files into larger ones while preserving logical rows. Iceberg exposes procedures such as `rewrite_data_files`; Delta offers `OPTIMIZE` in supported releases. These perform real I/O and require scheduling. Use the table's transaction-aware maintenance tools, and retain old files long enough for active readers and recovery. Manual file deletion can invalidate snapshots. [Iceberg file maintenance](https://iceberg.apache.org/docs/latest/spark-procedures/#rewrite_data_files), [Delta compaction](https://docs.delta.io/optimizations-oss/).

### Schema evolution and table formats

Reading raw Parquet with `mergeSchema` reconciles compatible file schemas at read time and can add metadata cost. It is not a transactional table schema-management system. [Parquet schema merging](https://spark.apache.org/docs/3.5.6/sql-data-sources-parquet.html#schema-merging).

| Aspect | Iceberg | Delta Lake |
| --- | --- | --- |
| Table state | Metadata, snapshots, manifests, tracked data/delete files | Transaction log and checkpoints tracking active files and metadata |
| Evolution | Field IDs support schema changes; partition specs can evolve | Schema enforcement/evolution; capabilities depend on protocol and features |
| Row changes | `MERGE`, updates/deletes with compatible Spark integration | `MERGE`, updates/deletes with compatible Delta integration |
| Operational work | Rewrite files/manifests, expire snapshots safely | Compact files and manage obsolete-file retention safely |

Iceberg's hidden partitioning derives storage partition filters from source-column predicates; changing a partition spec does not eagerly rewrite historical files. [Iceberg evolution](https://iceberg.apache.org/docs/latest/evolution/). Delta's schema evolution must be explicitly supported/enabled for the operation; do not assume arbitrary incompatible type changes are accepted. [Delta table schema behavior](https://docs.delta.io/delta-batch/).

Both add table metadata and atomic commit semantics around data files. Plain Parquet alone does not provide a table transaction log. Table-format compatibility includes Spark version, connector version, catalog, and enabled table features; configuring only `.format(...)` may be insufficient. [Iceberg Spark integration](https://iceberg.apache.org/docs/latest/spark-configuration/), [Delta batch integration](https://docs.delta.io/delta-batch/).

### `MERGE` operations

A merge matches source rows to target rows, applies conditional updates/deletes, and inserts unmatched rows. Physical work can include scans, joins, shuffles, and rewriting affected files; supported row-level modes may instead record changes for readers to reconcile. It is not an in-place update to a single Parquet row.

This example assumes `source_unique` already has one valid, non-null row per `order_id`, and `version` defines a reliable per-order update order:

```sql
MERGE INTO target_orders AS t
USING source_unique AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.version > t.version THEN
  UPDATE SET status = s.status, version = s.version
WHEN NOT MATCHED THEN
  INSERT (order_id, status, version)
  VALUES (s.order_id, s.status, s.version);
```

Reject or resolve duplicate/conflicting source keys deterministically before merging. Multiple source rows updating one target row are ambiguous and can fail; duplicate unmatched rows can also create unwanted inserts. A merge does not automatically enforce primary-key uniqueness. Partition restrictions must preserve match semantics: a target filter that hides an existing key can accidentally turn an update into an insert. [Iceberg merge semantics](https://iceberg.apache.org/docs/latest/spark-writes/#merge-into), [Delta merge semantics](https://docs.delta.io/delta-update/).

**Interview question: Why can a small incremental merge scan a large table?** Matching rows may be scattered across many files, the predicate may not prune the target, or statistics/layout may be ineffective. Inspect scan and rewrite metrics before increasing cluster size.

## 8. Structured Streaming

### Micro-batches and triggers

Instead of repeatedly recomputing all orders, a streaming query can maintain a result as new input arrives. **Structured Streaming** models that stream as an incrementally updated table. In micro-batch mode, a trigger processes a bounded input range, updates state, and writes output. The default starts another batch when the previous one finishes. A processing-time trigger requests a cadence, but slow batches do not pile up as overlapping executions of the same query.

`availableNow=True` processes the data available for the run and terminates, potentially using multiple batches. It is useful for scheduled incremental processing. Continuous processing is a distinct mode with narrower operator support and different guarantees. [Trigger API](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.trigger.html).

### Output modes

| Mode | Rows emitted | Typical implication |
| --- | --- | --- |
| Append | Newly finalized result rows | Window aggregates wait until a watermark permits finalization |
| Update | Result rows changed in the batch | Sink needs to interpret updates correctly |
| Complete | Entire current result table | Repeated large output; aggregation state is retained |

Operator and sink combinations restrict valid modes. Append does not universally mean every arriving input row is emitted immediately. [Output mode API](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.outputMode.html).

### Event time, watermarks, and state

Event time comes from the event; processing time is when the engine handles it. A watermark tracks observed event-time progress minus an allowed delay. It bounds eligible state retention and finalization, rather than measuring lateness from the wall clock. It may advance only after subsequent processing.

Suppose the maximum observed event time is 12:20 and the delay is 10 minutes: the watermark is approximately 12:10. For a qualifying windowed aggregation, data within the allowed delay is guaranteed not to be dropped for lateness; older data may or may not be processed. A watermark is not an exact rule that rejects every older event. [Watermark API](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withWatermark.html), [watermark guarantees](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html#semantic-guarantees-of-aggregation-with-watermarking).

Stateful operations retain information across batches: aggregates, deduplication, and stream-stream joins. For bounded join state, use appropriate event-time watermarks and a time-range condition so old rows can no longer match. A hot key or insufficient cleanup can make state grow. RocksDB can reduce JVM heap pressure, but still needs native memory, disk, and checkpoint I/O. [Stateful streaming operations](https://spark.apache.org/docs/4.0.0/streaming/apis-on-dataframes-and-datasets.html).

```python
from pyspark.sql import functions as F

events = (
    spark.readStream.schema("event_time TIMESTAMP, customer_id STRING")
    .parquet("/data/incoming_events")
)
counts = (
    events.withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "5 minutes"), "customer_id")
    .count()
)
query = (
    counts.writeStream.format("parquet")
    .outputMode("append")
    .option("path", "/data/window_counts")
    .option("checkpointLocation", "/checkpoints/window_counts")
    .trigger(processingTime="30 seconds")
    .start()
)
```

These paths must be on storage visible to the cluster; the checkpoint needs durable storage and a unique location per query. Production ingestion also needs validation/quarantine for malformed data and invalid event timestamps. Append output may stall when event-time progress stalls.

### Checkpointing and delivery guarantees

Streaming checkpoints preserve source progress, commit information, and state needed to resume. Reuse a compatible checkpoint after a restart; deleting it changes recovery behavior and can replay or skip data. Stateful schema/operator changes may require a planned migration rather than a restart against the old state.

End-to-end exactly-once results require replayable input, deterministic processing, recoverable state, and a sink/commit protocol that handles replay. `foreachBatch` is at-least-once by default: use a transactional or idempotent write, often identifying the query plus `batchId`. Writing a marker separately from the data leaves a crash window. Continuous processing has at-least-once guarantees. [Streaming recovery and sink guarantees](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html).

### Kafka integration and offsets

Spark reads Kafka's topic/partition/offset ranges and tracks streaming progress in its checkpoint; it does not depend on Kafka consumer-group offset commits for recovery. `startingOffsets` applies to a new query; restarting from an existing checkpoint resumes checkpointed progress. Kafka retention must cover the recovery window. Do not suppress missing-offset errors without understanding the data loss.

Kafka keys/values arrive as binary data and need explicit decoding and validation. `maxOffsetsPerTrigger` can bound micro-batch intake. Kafka source partitions and downstream shuffle partitions serve different purposes. Spark's Kafka sink provides at-least-once writes, so duplicates remain possible; retain a stable event identity for downstream deduplication. [Kafka integration and guarantees](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html).

**Interview question: Does adding a checkpoint make Kafka-to-Kafka exactly once?** No. It gives Spark recoverable progress, but the Kafka sink can duplicate records during replay or retries.

## 9. Performance & Production

### Spark UI, plans, and metrics

When a job slows down, begin with the query and the work on its critical path. Connect its physical operators to stages and tasks, then ask where time and data accumulate. The Spark UI views below provide different parts of that explanation; a configuration guess is useful only after it fits the evidence.

| View | What to inspect |
| --- | --- |
| SQL | Physical/adaptive plans, operator rows, scan/exchange/join metrics |
| Jobs and Stages | Dependencies, task distribution, duration percentiles, retries |
| Executors | Failures, garbage collection, memory-related metrics, shuffle and task activity |
| Storage | Whether expected cached partitions are actually present |
| Environment | Effective configuration rather than assumed defaults |

Enable event logging to inspect completed applications through the History Server. Compare task maximums and percentiles, shuffle read/write, fetch wait, spill, and executor logs. Memory-spilled and disk-spilled bytes measure different representations and need not match. Streaming progress adds batch duration, input/processing rates, watermark, state size, and source offsets. [Spark monitoring](https://spark.apache.org/docs/3.5.6/monitoring.html).

### Diagnose the bottleneck before tuning

The following are diagnostic hypotheses to test, rather than one-to-one rules:

| Evidence | Possible bottleneck | Useful next experiment |
| --- | --- | --- |
| Most tasks slow with high CPU use | Expensive expressions, Python work, decoding/compression | Profile a representative task; replace avoidable UDFs |
| High GC, spill, or repeated memory failures | Working set exceeds available task memory | Reduce columns, build-side size, concurrency, or partition size |
| High shuffle bytes and fetch waits | Redistribution/network/disk pressure | Filter/project earlier; examine join and aggregation plans |
| Large scan volume with low selectivity | Ineffective pruning or data layout | Inspect scan filters, file statistics, and selected columns |
| A few tasks vastly slower than the median | Skew or unusually large input partitions | Compare keys and per-task input/shuffle bytes |
| Many tiny tasks with little useful work | Scheduling/file overhead | Reduce small files or excessive partition count |
| Pending executors despite runnable tasks | Cluster capacity, quota, or placement constraints | Inspect resource-manager scheduling events |

For example, if 199 join tasks finish in 20 seconds and one takes 15 minutes, investigate skew first. Doubling executors may leave the critical task unchanged. If every task takes 15 minutes, investigate a shared CPU/I/O limitation or oversized partitions instead.

### Kubernetes, YARN, and autoscaling

On Kubernetes, drivers and executors run in pods; diagnose pending pods, requests/limits, eviction/OOM reasons, image dependencies, networking, and local disk availability. A node autoscaler can add capacity only when its placement and capacity constraints allow it. [Spark on Kubernetes](https://spark.apache.org/docs/3.5.6/running-on-kubernetes.html).

On YARN, Spark uses an ApplicationMaster and executor containers. Check queue capacity, container allocations, application/container diagnostics, and aggregated logs. Client versus cluster mode determines driver placement and affects network reachability and recovery. [Spark on YARN](https://spark.apache.org/docs/3.5.6/running-on-yarn.html).

Spark dynamic allocation requests executors; infrastructure autoscaling supplies nodes. Their response times interact. More executors help only when enough parallel work exists and the bottleneck can scale. Stateful streaming may pay substantial restore costs when executors move. [Spark allocation policy](https://spark.apache.org/docs/3.5.6/job-scheduling.html).

### Monitoring, retries, idempotency, and backfills

Production design should cover both execution success and data correctness:

1. **Monitor outcomes:** freshness, lag, processed/rejected rows, duplicate keys, nulls, and reconciliation totals alongside CPU/memory metrics.
2. **Classify failures:** retry transient infrastructure errors with bounded backoff. Repeated schema, permission, or deterministic code errors need a correction.
3. **Make replay safe:** use stable event/business keys, deterministic conflict resolution, and atomic sink commits. Plain append writes can duplicate on rerun.
4. **Bound backfills:** fix a source snapshot or explicit offset/time range. Define timezone and interval boundaries, usually half-open intervals such as `[start, end)`.
5. **Validate before replacement:** compare row counts, key uniqueness, aggregates, and rejected records. Restrict overwrite scope and coordinate overlapping writers.
6. **Preserve recovery:** retain source history, table snapshots, and checkpoints for the required recovery period; keep backfill checkpoints separate from live streaming queries.

These are operational design recommendations. The exact implementation depends on source replayability, sink transaction guarantees, and the business definition of a duplicate or late correction.

**Interview question: How would you investigate a job that became twice as slow?** Compare input volume/distribution, schema and file count, effective configuration, statistics, final query plan, and cluster capacity against a known good run. Locate the changed stage and its resource evidence, make one targeted change, then compare runtime, cost, and output correctness on representative data.

### Final interview checklist

For a proposed Spark pipeline, be ready to explain:

- Where the driver, JVM operators, and Python workers execute.
- Which operations move data and why each shuffle is necessary.
- Why the chosen join and partition sizes fit the observed data.
- What happens after executor loss, task replay, and driver restart.
- How the sink handles duplicate attempts and partial failures.
- How state, files, and metadata remain bounded over time.
- Which plan nodes and metrics would confirm or disprove your diagnosis.
