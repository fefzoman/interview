# Cost Engineering (FinOps): Data Engineer Interview Guide

A query can finish twice as fast and cost exactly the same. A cheaper worker can make a daily pipeline more expensive if it creates retries and missed deadlines. **Cost engineering** connects resource use to the price of delivering useful, reliable data. To reason about savings, first identify what is billed, then change the workload and verify both the billable effect and the result's correctness.

**FinOps** brings engineering, finance, and business teams together to manage technology spending and business value. A lower bill is useful only if the system still delivers the required results. This framing follows the [FinOps Foundation framework](https://www.finops.org/framework/).

**Scope:** Platform-neutral concepts with selected cloud examples. All numerical prices below are hypothetical teaching inputs, not current provider quotes. Actual billing depends on service, region, contract, metering rules, and deployment. References were checked on October 25, 2026.

**Running example:** An e-commerce platform ingests order events, keeps raw data in object storage, transforms it into analytical tables, and serves dashboards. Daily processing must finish within two hours, dashboards must be fresh within 15 minutes, and retained raw data must support recovery and audit requirements.

## Contents

1. [Compute, Storage, and Network Cost Models](#1-compute-storage-and-network-cost-models)
2. [Serverless vs. Provisioned Infrastructure](#2-serverless-vs-provisioned-infrastructure)
3. [Resource Sizing, Utilization, and Autoscaling](#3-resource-sizing-utilization-and-autoscaling)
4. [Query Cost, Shuffle, and Data Scanned Optimization](#4-query-cost-shuffle-and-data-scanned-optimization)
5. [Storage Lifecycle, Compression, and Retention](#5-storage-lifecycle-compression-and-retention)
6. [Spot/Preemptible vs. On-Demand Compute](#6-spotpreemptible-vs-on-demand-compute)
7. [Cost Attribution, Budgets, and Monitoring](#7-cost-attribution-budgets-and-monitoring)
8. [Performance vs. Cost vs. Reliability Trade-offs](#8-performance-vs-cost-vs-reliability-trade-offs)
9. [Interview Scenarios and Answer Framework](#9-interview-scenarios-and-answer-framework)

## 1. Compute, Storage, and Network Cost Models

### Start with the billing unit

Start with a simple question: what event makes the bill increase? An idle provisioned worker may still be charged, while a faster query may cost the same under scan-based billing. A smaller stored file may not reduce billable query bytes either. Resource consumption and financial cost are connected through a specific meter, so identify that meter before proposing an optimization.

| Category | Common billing units | Easily missed costs |
| --- | --- | --- |
| Compute | Instance-seconds/hours, vCPU-hours, memory GB-seconds, slot-hours, warehouse credits | Idle time, startup, minimum billing periods, retries, licenses, managed-service fees |
| Query processing | Billable TB/TiB processed or compute capacity over time | Repeated dashboard queries, failed work subject to service billing rules, maintenance jobs |
| Object storage | GB-month plus requests and retrieval | Object versions, replicas, snapshots, transition requests, minimum durations |
| Block/file storage | Provisioned GB-month, sometimes IOPS and throughput | Detached volumes, snapshots, unused provisioned performance |
| Network | GB transferred by route and direction; sometimes gateway-hours and processing GB | Cross-zone/region traffic, NAT processing, replication, exports |
| Supporting services | Requests, ingestion volume, retained logs, catalog operations | Orchestration, observability, encryption-key requests, connectors |

Use a complete boundary for the estimate:

```text
Cloud cost = compute + storage + requests/retrieval + network + supporting services
Total cost of ownership = cloud cost + engineering/operations + migration costs
```

For capacity billed over time:

```text
Compute cost = sum(resource count × billed duration × unit rate)
```

**Example:** Eight workers run for 1.5 hours at $0.50 per worker-hour. Worker compute costs `8 × 1.5 × $0.50 = $6`. A billed coordinator, retries, storage, and transfer belong in the job's total as well.

For storage, use time-weighted average billable capacity rather than only the end-of-month size. If 10,000 billable GB remain for a whole month at $0.02/GB-month, capacity costs $200 before requests, retrieval, and other charges. Check whether the service meters physical compressed bytes, logical bytes, or provisioned space. Keep GB/TB and GiB/TiB consistent.

### Network follows the route

Draw the data path: source → processing → storage → consumers. Check each hop's region, zone, service, and gateway. “Same cloud” does not establish that transfer is free. Provider rules also differ by direction and service pair; [EC2 pricing](https://aws.amazon.com/ec2/pricing/on-demand/) separates transfer from instance charges.

**Example:** Moving 5,000 billable GB over a route priced at $0.02/GB adds $100. Repeating that transfer daily for 30 days adds $3,000 before gateway charges. Filtering or aggregating near the source can reduce traffic, if it preserves required detail and access controls.

**Interview check:** A stopped cluster still generates charges. Investigate retained volumes, snapshots, reservations/commitments, storage, network appliances, and other independent resources rather than assuming the compute meter explains the entire bill.

## 2. Serverless vs. Provisioned Infrastructure

**Serverless** means the provider manages infrastructure provisioning and operation. It does not imply one billing model or that every component scales to zero. **Provisioned infrastructure** means capacity is explicitly allocated; it can still be scheduled, resized, or autoscaled.

| Dimension | Serverless | Provisioned capacity |
| --- | --- | --- |
| Good starting workload | Bursty or intermittent processing with uncertain demand | Sustained, predictable work that keeps capacity useful |
| Cost exposure | Usage growth, expensive individual operations, scaling ceilings | Idle capacity, oversized resources, unused commitments |
| Operations | Less host and cluster management | More control over configuration and placement |
| Performance | Startup and scaling behavior depend on the service | Warm capacity can make latency more predictable |
| Responsibility | Query design, data layout, quotas, retries, and ownership remain with the team | Those responsibilities plus more infrastructure operation |

**Important distinction:** Architecture and pricing are separate decisions. BigQuery offers both on-demand query billing and capacity-based billing while remaining a managed serverless service. Capacity pricing uses slots over time. See [BigQuery pricing](https://cloud.google.com/bigquery/pricing).

### Compare cost at the same service level

Imagine choosing between a serverless job at a hypothetical $0.08 per successful run and an always-on provisioned pool at $240 per month. Before comparing their prices, establish that both meet the same service requirements. Under that assumption, the simplified compute break-even is:

```text
Monthly runs at break-even = $240 / $0.08 = 3,000
```

Below that volume, serverless has lower compute cost under these assumptions. Above it, the pool might be cheaper only if its throughput and concurrency are sufficient. Include scheduling, idle periods, operations, storage, and failure recovery before deciding. Scheduling the provisioned pool off when idle changes the comparison.

Measure the same input, output, freshness, concurrency, and failure policy. Comparing a delayed batch with an interactive service hides a product trade-off inside the price comparison.

**Interview trap:** “Serverless is always cheaper because you only pay for use.” High sustained use, repeated scans, minimum charges, and premium managed pricing can change the result.

## 3. Resource Sizing, Utilization, and Autoscaling

### Size for the actual bottleneck

Low average CPU can tempt you to shrink a worker that is already struggling with memory or I/O. **Right-sizing** means selecting CPU, memory, storage performance, and parallelism that meet the workload's requirements at an acceptable cost. The relevant evidence is the actual bottleneck, which average CPU alone cannot identify.

| Observation | Possible explanation | What to check before changing size |
| --- | --- | --- |
| High CPU, little spill | CPU-bound execution | CPU time, serialization, compression, runnable tasks |
| Low CPU, high disk spill | Insufficient memory or oversized/skewed partitions | Peak memory, partition sizes, spill, garbage collection |
| Low CPU, long read waits | Storage/network bottleneck | Throughput, request latency, throttling, file layout |
| A few tasks finish much later | Skew or an uneven source | Task duration distribution, hot keys, largest partitions |
| Long queue but idle workers | Scheduling or parallelism constraint | Runnable tasks, quotas, locks, source partitions |

Low utilization may mean waste, but it can also represent recovery headroom or spare capacity for latency targets. High utilization can increase queues and tail latency. Use representative peaks, concurrent jobs, and recovery behavior rather than choosing an arbitrary universal target.

**Example:** Ten workers at $1/hour finish in 60 minutes: $10. Twenty workers finish in 40 minutes: about $13.33. The larger cluster is faster but costs more. If it finished in 25 minutes, compute would fall to about $8.33. Benchmark the full job, including startup and shutdown.

### Scale on demand and useful parallelism

Autoscaling needs a signal connected to work: queue depth, oldest-event age, consumer lag, runnable tasks, or query queue time. CPU can be useful, but an I/O-bound backlog can grow while CPU remains low.

Configure minimum and maximum capacity, scale-up speed, and scale-down stabilization. Allow for startup time and drain in-flight work safely. Scaling cannot fix a single hot partition, a serial stage, or a saturated dependency; extra workers can amplify throttling.

A simple streaming estimate, assuming uniform load and measured sustainable throughput:

```text
Incoming rate = 12,000 events/second
Per-worker capacity = 2,000 events/second
Chosen target utilization = 75%
Workers = ceiling(12,000 / (2,000 × 0.75)) = 8
```

This is a starting estimate. Catching up after an outage requires capacity above the arrival rate. Validate source partition limits, skew, state size, downstream capacity, and checkpoint performance.

**Interview check:** Why might autoscaling make a job more expensive? Workers can arrive too late to help, repeat data loading, trigger rebalancing, or remain idle during serial stages. Compare successful output and deadline compliance per dollar before and after scaling.

## 4. Query Cost, Shuffle, and Data Scanned Optimization

### Identify which meter the optimization affects

An optimization saves money only if it changes a billed quantity or avoids future capacity. Under scan-based billing, that may mean fewer billable bytes. Under capacity billing, it may mean less resource-time or more throughput on existing resources. A faster job on an already committed pool can free capacity without immediately shrinking the invoice.

Do not equate storage size, physical I/O, and billable scan bytes. For Athena's scan-based SQL pricing, compression and columnar projection can reduce charged scans; capacity billing is also available. See [Athena pricing](https://aws.amazon.com/athena/pricing/). Check the target engine's rules rather than carrying this assumption to every warehouse.

### Avoid reading unnecessary data

| Technique | How it helps | Limitation |
| --- | --- | --- |
| Partition pruning | Skips partitions outside the requested range | Predicates must be compatible with the partition layout and engine |
| Column projection | Reads required columns instead of every column | Greatest I/O benefit with columnar storage |
| Clustering/sorting | Groups related values so metadata can skip blocks | Maintenance and writes also consume resources |
| Predicate pushdown | Applies supported filters near the scan | A filter can reduce rows without reducing billable scanned bytes |
| Incremental processing | Processes changed data instead of full history | Needs reliable change tracking, late-data handling, and replay |
| Materialized aggregates | Reuses repeated expensive computation | Adds refresh/storage cost and a freshness contract |

For a table partitioned on `order_date`, a dashboard might use:

```sql
SELECT customer_id, SUM(amount) AS revenue
FROM orders
WHERE order_date >= DATE '2026-10-01'
  AND order_date < DATE '2026-10-08'
GROUP BY customer_id;
```

This expresses a bounded date range and requests only needed columns. Confirm pruning and bytes processed in the plan and job statistics; SQL text alone does not prove savings. Avoid transformations of partition columns unless the engine can still prune them.

**Scan example:** At a hypothetical $4 per billable TiB, scanning 20 TiB costs $80. Reducing billable input to 0.5 TiB costs $2, a 97.5% reduction in that query's scan charge. It does not imply a 97.5% reduction in the platform's total cost.

In BigQuery, dry runs help estimate bytes and maximum-bytes-billed settings can guard individual queries. A `LIMIT` on a non-clustered table does not reduce scan charges. Its cost controls differ between on-demand and capacity pricing. See [BigQuery cost controls](https://docs.cloud.google.com/bigquery/docs/best-practices-costs).

### Reduce shuffle and spill

**Shuffle** redistributes data among workers for operations such as joins, grouping, sorting, and repartitioning. It consumes network, CPU, memory, and potentially temporary disk. Network transfer billing depends on placement; shuffle can increase compute cost even where no separate transfer fee applies.

Inspect input size, shuffle volume, spill, and task duration distribution. Filter and project early when semantics allow. Pre-aggregate only when the aggregation remains correct. Broadcast a small join side only when it fits safely in worker memory; splitting skewed partitions and choosing appropriate partition counts can reduce stragglers. Spark's adaptive execution can adjust join strategies and shuffle partitions using runtime statistics. See [Spark SQL performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html).

Avoid forcing all data into one output partition for convenience. Too few partitions reduce parallelism and may exhaust memory; too many increase scheduling and file overhead. A cache is worthwhile only when reuse saves more than its memory, materialization, and eviction costs.

**Correctness trap:** Replacing an exact distinct count with an approximation changes the answer contract. Likewise, incremental loads must handle updates, deletes, duplicates, and late events rather than silently dropping them to save work.

**Interview check:** A query scans the same bytes but runs twice as fast after join tuning. Scan-based query charges may stay the same; capacity use and interference with other queries may improve. Explain both outcomes.

## 5. Storage Lifecycle, Compression, and Retention

### Choose tiers using access and recovery requirements

Old data may be rarely queried yet essential to a recovery deadline. Before moving it to a cheaper tier, compare access frequency with retrieval cost and restore time. The cheapest storage rate is useful only when the data remains available in the way the workload requires.

| Data class | Possible policy | Decision to verify |
| --- | --- | --- |
| Frequently queried recent data | Keep in an online tier with suitable layout | Query latency and request cost |
| Older, occasionally queried data | Use a lower-cost online tier when economical | Retrieval charges, minimum duration, minimum billable size |
| Rarely accessed recovery/audit history | Consider archive storage | Restore delay, restore cost, recovery deadline |
| Recomputable intermediate output | Expire after a defined recovery window | Upstream replay availability and recomputation cost |

Lifecycle transitions have costs of their own. Amazon S3 documents minimum storage durations and transition-request charges; moving many small objects can cost more than it saves. See [S3 lifecycle considerations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-transition-general-considerations.html).

For a simple monthly comparison:

```text
Net saving = capacity saving
             − retrieval/restore cost
             − transition requests
             − minimum-duration charges
             − additional operational cost
```

**Example:** Moving 10,000 billable GB from $0.02 to $0.005/GB-month saves $150/month in capacity. If retrieval, transition, and other incremental charges total $180 that month, the move increases cost by $30.

### Compression and file layout

Compression trades CPU work for fewer stored and transferred bytes. Columnar formats such as Parquet and ORC also support selective analytical reads. Measure compression ratio, encode/decode time, and end-to-end query behavior; a better ratio alone does not establish lower total cost.

Many tiny files increase request, listing, metadata, and scheduling overhead. Compaction can reduce those costs, but rewriting data has its own compute and I/O bill. Very large files can hinder parallelism or increase rewrite work. Choose sizes from workload measurements rather than a universal rule.

### Retention includes recovery dependencies

Define separate retention for raw events, curated tables, staging output, logs, snapshots, old object versions, and temporary query results. Include incomplete uploads and obsolete resources in cleanup policies where applicable.

Before deleting anything, check audit obligations, legal holds, privacy requirements, downstream consumers, and replay dependencies. A derived table is only reproducible if the source, transformation code, schema, and required reference data still exist. Raw data is not automatically a complete database backup.

For transactional lake tables, expire snapshots and clean orphan files through supported table-format maintenance. Deleting files directly from storage can break current tables or readers using older snapshots. Coordinate cleanup with concurrent jobs and the required recovery window.

**Interview trap:** “Keep raw data forever because storage is cheap.” Retention also creates request, replication, security, and governance costs. Conversely, deleting the only replay source to save capacity can make an otherwise recoverable failure permanent.

## 6. Spot/Preemptible vs. On-Demand Compute

**Spot/preemptible compute** offers spare capacity at a lower price in exchange for possible reclamation. Provider names, interruption behavior, and availability rules differ. **On-Demand compute** avoids that spare-capacity reclamation model, but hardware failures and capacity shortages remain possible.

| Purchase approach | Appropriate use | Main risk |
| --- | --- | --- |
| On-Demand | Variable work or critical stages needing flexible compute | Higher rate; launch capacity is still not guaranteed |
| Spot/preemptible | Restartable batch tasks, flexible backfills, replaceable workers | Lost work, unavailable capacity, correlated interruptions |
| Usage commitments | Stable baseline demand after right-sizing | Paying for unused commitments or losing flexibility |

For EC2, Savings Plans discount eligible usage through a spending commitment; they do not reserve capacity. Capacity Reservations address availability separately. See [AWS purchasing options](https://docs.aws.amazon.com/decision-guides/latest/decision-guides/ec2-purchasing-options-aws-how-to-choose.html).

### Design for interruption before comparing prices

A discounted worker is useful only if losing it does not lose the only recoverable state or expose partial output. Keep recovery state in durable storage, choose a measured checkpoint interval, and make retries idempotent. Publish through atomic commits or a transactional protocol so interruption cannot leave partial results or duplicated records. Those recovery properties come before comparing compute prices.

Diversify suitable capacity pools and provide an explicit fallback when a deadline requires it. Keeping critical coordinators on stable capacity while using Spot for replaceable workers can reduce risk, but lost shuffle/state may still cause recomputation. Treat interruption notices as helpful signals rather than the recovery mechanism. [EC2 interruption documentation](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-interruptions.html).

### Compare successful work, including retries

```text
Effective Spot job cost = billed initial and retry compute
                          + checkpoint/storage/network cost
                          + fallback compute cost
```

**Example:** A job needs 100 worker-hours on On-Demand at $1/hour: $100. Spot costs $0.35/hour and interruptions add 40 worker-hours, making compute `140 × $0.35 = $49`. If incremental checkpoint and recovery costs add $8, total cost is $57. That saves $43 only if the deadline and correctness requirements still hold.

A 65% lower hourly rate therefore does not establish 65% job savings. Frequent checkpoints reduce lost work but increase writes and pauses. Long intervals lower checkpoint overhead but expose more recomputation. Measure interruption patterns, recovery duration, and successful completion times.

For commitments, distinguish **coverage** (how much eligible usage receives the committed discount) from **utilization** (how much purchased commitment is actually consumed). High coverage can coexist with waste from overcommitting. Right-size first, then commit to a defensible baseline.

## 7. Cost Attribution, Budgets, and Monitoring

### Assign cost to accountable owners

A shared platform bill tells you the total, but it does not explain who generated the work or who can change it. Associate spending with accountable teams, products, environments, and pipelines using account or project boundaries, tags or labels, query metadata, and job IDs. Shared resources still need an explicit allocation rule: tags alone cannot divide a worker's bill. [FinOps allocation capability](https://framework.finops.org/framework/capabilities/allocation/).

**Example:** A shared platform costs $12,000/month. Team A accounts for 25% of measured slot-seconds, so a usage-proportional policy allocates $3,000 to A. State separately how idle capacity, shared storage, discounts, and support charges are handled. The allocation is an accounting rule, not proof that removing A would save $3,000.

**Showback** reports costs to owners. **Chargeback** transfers those costs to their budgets. Both need understandable rules, consistent metadata, and reconciliation to billing totals. Keep unallocated costs visible rather than silently excluding them.

For job comparisons, decide whether costs are list-price, discounted, or amortized. Amortization spreads commitment payments over the covered period. Avoid mixing upfront cash payments in one month with amortized costs in another when assessing efficiency.

### Measure cost per useful outcome

Useful metrics include cost per successful pipeline run, per million valid events delivered, per processed TiB, or per customer served. Define the time window, included shared costs, and output quality. The [FinOps unit economics guidance](https://www.finops.org/framework/capabilities/unit-economics/) distinguishes resource efficiency from business outcomes.

**Example:** Spend rises from $10,000 to $12,000 while valid delivered volume rises from 100 million to 150 million events. Cost per million events falls from $100 to $80, a 20% improvement. Spend alone would miss that gain. Track retries, correctness, and freshness so the denominator does not reward repeated or incomplete work.

### Budgets need operational guardrails

| Control | Purpose | Limitation |
| --- | --- | --- |
| Budget alerts | Notify owners about actual or forecast spending | Billing/notification delay allows overshoot |
| Anomaly detection | Find unexpected changes against a baseline | Launches and backfills can create legitimate spikes |
| Query byte limits, timeouts, concurrency limits | Bound individual operations or simultaneous demand | Can reject or delay legitimate work |
| Worker/slot limits and quotas | Restrict resource growth | Can cause backlog and missed freshness targets |
| Idle shutdown and expiration | Remove avoidable resource lifetime | Needs ownership and recovery-aware exclusions |

A budget notification is not an immediate spending cap. AWS documents billing and notification delays; budget actions are separate controls. See [AWS Budgets behavior](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html).

Combine delayed billing exports with faster operational signals: bytes scanned, worker runtime, retries, queue age, storage growth, and egress. Alert on absolute spend and unit cost. Give each alert an owner and response, such as reviewing an unbounded query or pausing an approved noncritical backfill.

**Interview check:** A daily cost doubles. First separate more legitimate demand from a higher cost per unit; then break down by service, owner, job, and meter. Check new deployments, repeated retries, full-refresh jobs, missing filters, retention changes, and transfer routes.

## 8. Performance vs. Cost vs. Reliability Trade-offs

### Optimize within explicit service requirements

Before removing spare capacity or extending batch intervals, state the service the cheaper design must still deliver: freshness, throughput, latency percentiles, successful completion, and recovery objectives. **RPO** defines the acceptable data-loss window; **RTO** defines the target time to restore service. A lower-cost design that misses those requirements is not an equivalent alternative.

| Choice | Potential saving or benefit | Cost or reliability trade-off |
| --- | --- | --- |
| Larger batch intervals | Less scheduling and per-job overhead | Older data and bigger recovery batches |
| More workers | Lower runtime when parallelism exists | Higher spend if speedup is sublinear |
| Spot workers | Lower compute rate | Retries, capacity gaps, variable completion time |
| Archive old data | Lower storage capacity charges | Restore delay and retrieval costs |
| More replicas/regions | Better resilience to specified failures | More storage, transfer, coordination, and operations |
| Materialized results | Faster repeated reads | Refresh, storage, and stale-result handling |
| Lower spare capacity | Less idle spend | Slower recovery and greater queueing during peaks |

**Example:** With a 15-minute freshness objective, changing ingestion to hourly batches violates the requirement even if it cuts cost. A five-minute microbatch might qualify, but only after accounting for ingestion delay, queueing, transformation, and publication time.

### Include the cost of failure

```text
Decision cost = infrastructure + operations
                + expected recovery effort + expected business impact
```

Expected business impact can be estimated using scenario probabilities and impact, but rare correlated failures are difficult to model. Use scenario and sensitivity analysis; an average-cost estimate does not replace hard recovery requirements.

To validate an optimization, compare representative workloads with the same input and output contract. Record total and unit cost, runtime, p95/p99 latency or completion time, freshness, retries, and recovery behavior. Verify row counts and business invariants such as order totals. Distinguish actual invoice savings from freed capacity and avoided future growth.

**Interview trap:** “Use one availability zone to avoid transfer fees.” Evaluate the resulting failure exposure against availability and recovery requirements before treating it as a viable saving.

## 9. Interview Scenarios and Answer Framework

### Scenario: A dashboard causes a scan-cost spike

A dashboard refreshes every minute, repeatedly scanning a year of order history, and query spending spikes. The tempting fix is to make each read smaller, but the useful comparison includes the cost of maintaining any replacement aggregate and preserving correct totals. What would you investigate and change first?

**Answer outline:** Verify billing mode and identify queries responsible for the spike. Measure refresh frequency, billable bytes, users, and required freshness. Check whether filters prune partitions and whether the query selects unnecessary columns. Consider a maintained daily aggregate or reusable result if its refresh cost is lower. Validate late updates, deletes, and dashboard totals. Compare the combined refresh-and-read cost rather than only the cheaper read query.

### Scenario: Cheap workers miss the daily deadline

**Question:** Moving the nightly job to Spot halves its normal compute bill, but some runs miss the two-hour deadline.

**Answer outline:** Inspect interruption frequency, lost work, capacity wait, skew, and recovery time. Reduce restart scope and persist recoverable progress. Use stable capacity for critical state and a deadline-aware fallback for remaining work. If required reliability cannot be achieved economically, retain more On-Demand capacity. Report cost per successful on-time run, not the cheapest normal execution.

### Scenario: Storage grows after deleting old data

**Question:** Logical rows were deleted, but billed storage barely fell. Why?

**Answer outline:** Check the billing model and time delay, then inspect snapshots, time travel, object versions, replicas, and unexpired files. A logical deletion may not immediately reclaim physical storage. Use supported maintenance with an approved recovery window; verify downstream readers and retention requirements before reclaiming data.

### Scenario: Commitments look attractive

**Question:** Should a team commit to its current compute footprint for a discount?

**Answer outline:** Remove idle capacity and benchmark right-sizing first. Separate the stable baseline from seasonal bursts and planned migrations. Model coverage, utilization, and the cost of unused commitment under lower-demand scenarios. Preserve flexible capacity for uncertain demand, and evaluate capacity guarantees separately from rate discounts.

### A reusable interview answer

1. **Define the contract:** volume, growth, freshness, latency, correctness, RPO/RTO, and deadlines.
2. **Identify the meter:** scanned bytes, resource-time, storage, requests, or transfer; include supporting costs.
3. **Establish a baseline:** total cost, unit cost, utilization, retries, and representative peak behavior.
4. **Change the main driver:** avoid unnecessary work, tune execution and layout, then evaluate purchase discounts.
5. **Verify the result:** measure savings and service quality, test recovery where affected, and monitor regression.

A strong answer names the billable unit, explains why the proposed change affects it, and states the evidence needed to keep the saving without losing required behavior.
