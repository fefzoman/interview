# Data Platform Testing: Data Engineer Interview Guide

Data platform testing verifies transformation logic, data correctness, system integration, recovery behavior, and performance. A successful job can still publish duplicated, incomplete, stale, or semantically incorrect data. Explain how you detect those failures and prevent consumers from using invalid results.

The running example is an orders pipeline: source database → ingestion → raw data → transformed tables → daily revenue. Orders have an `order_id`, `customer_id`, `amount_cents`, `status`, and update metadata. Examples assume refunds are modeled separately; real quality rules must follow the business definition.

## Contents

1. [Unit, Integration, and End-to-End Testing](#1-unit-integration-and-end-to-end-testing)
2. [Data Quality, Schema, and Contract Testing](#2-data-quality-schema-and-contract-testing)
3. [Source-Target Reconciliation and Checksums](#3-source-target-reconciliation-and-checksums)
4. [Property-Based and Parametrized Testing](#4-property-based-and-parametrized-testing)
5. [Incremental Pipeline, CDC, and Backfill Testing](#5-incremental-pipeline-cdc-and-backfill-testing)
6. [Idempotency, Replay, and Failure Injection](#6-idempotency-replay-and-failure-injection)
7. [Performance, Load, and Scalability Testing](#7-performance-load-and-scalability-testing)
8. [CI/CD Quality Gates and Test Environments](#8-cicd-quality-gates-and-test-environments)
9. [Interview Scenarios](#9-interview-scenarios)

## 1. Unit, Integration, and End-to-End Testing

### What each level proves

| Level | Scope | Orders pipeline example | Main limitation |
| --- | --- | --- | --- |
| Unit | One transformation or small piece of logic with controlled inputs | Given paid, canceled, and refunded orders, calculate expected revenue | Does not prove connectors or deployment configuration work |
| Integration | A boundary between real components | Write a batch through the actual database connector and verify committed rows | Does not prove the complete consumer journey |
| End-to-end (E2E) | The deployed path from input to consumer output | Insert a source order and verify its appearance in the revenue table | Slower, harder to isolate, and more sensitive to shared infrastructure |

The boundary matters more than the label. A SQL model test in a real warehouse might be called a unit test because it isolates one model, while still depending on an execution engine. State what is real and what is replaced.

Use many fast logic tests, enough integration tests to exercise important boundaries, and a small set of representative E2E journeys. There is no universal percentage split.

### Unit testing transformations

Give each fixture a small input and an independently specified expected output. Test row values, row counts, keys, and output grain.

- Filtering: paid orders contribute to revenue; canceled orders do not.
- Joins: missing dimensions follow an explicit policy; duplicate dimension keys do not silently multiply facts.
- Aggregation: multiple orders for the same day produce exactly one daily row.
- Boundaries: empty input, null values, duplicate keys, rounding, timestamp boundaries, and daylight-saving transitions where relevant.
- Determinism: freeze the clock, make ordering explicit, and use a defined tie-breaker for otherwise equal records.

For example, input orders of 1,000 and 500 cents with status `paid`, plus 700 cents with status `canceled`, should produce 1,500 cents of revenue. Write that expectation directly; computing it with the same expression as the transformation can repeat the same bug.

Use exact integer cents or an agreed decimal representation for money. Do not introduce floating-point tolerance to hide an incorrect financial result.

### Integration testing

Exercise behavior mocks cannot establish:

- Actual connector authentication, serialization, type mapping, and null handling.
- Transaction commit and rollback, retries, timeouts, and connection loss.
- Table creation, partition writes, merge behavior, and permissions.
- Unicode, decimal precision, timestamp zones, and large payloads crossing a system boundary.
- Warehouse-specific SQL and differences between declared and enforced constraints.

Mocks are useful for forcing a timeout or testing retry decisions. They do not establish that the real destination applies a merge correctly. A local substitute also cannot prove that production identity and network configuration work.

### End-to-end testing asynchronous pipelines

1. Create a unique test tenant, key range, schema, or dataset.
2. Submit a small, known source change through the normal ingestion path.
3. Wait for a specific observable condition: the expected source position has been applied and the expected result is queryable.
4. Assert consumer-visible values, uniqueness, freshness, and relevant side effects.
5. Capture diagnostics and clean up owned resources even if assertions fail.

Use bounded polling with a deadline. A fixed sleep can be too short on a slow run and unnecessarily long on a fast run. Seeing one expected row is insufficient if duplicates or partial aggregates can still appear.

**Interview question:** Why do we need integration tests if unit tests have high coverage? Unit tests can verify transformation decisions while missing connector behavior, transactional boundaries, type conversions, and deployment configuration.

## 2. Data Quality, Schema, and Contract Testing

### Three related checks

| Check | Question | Example |
| --- | --- | --- |
| Schema | Does the physical or serialized structure match expectations? | `amount_cents` is an integer and required columns exist |
| Data quality | Are the actual values fit for their intended use? | Order keys are unique and paid orders have valid amounts |
| Contract | Does the producer satisfy the consumer-facing agreement? | Grain, meanings, units, schema evolution, freshness, ownership, and failure handling are defined |

A column can retain its name and integer type while changing from cents to whole dollars. A schema check passes, but the semantic contract breaks. A contract needs executable checks and an ownership/process agreement; a document alone does not enforce it.

### Choose assertions from the data contract

Use [data quality dimensions](data-quality.md#1-data-quality-dimensions) to choose the assertions and [freshness monitoring](data-quality.md#4-freshness-volume-schema-and-distribution-monitoring) to define progress. This guide owns how those assertions are exercised with fixtures, integration runs, replay, and failure injection.

Make fixtures reflect the declared grain: one current row per order differs from an order-history table. Include missing or late dimension records and idle-source progress, so tests cover agreed grace periods and heartbeat behavior.

### SQL checks should return violating rows

For a current-state `orders` table with `order_id`, `customer_id`, `amount_cents`, and `status`, the following assertions pass when they return no rows:

```sql
SELECT order_id, COUNT(*) AS occurrences
FROM orders
GROUP BY order_id
HAVING order_id IS NULL OR COUNT(*) > 1;
```

```sql
SELECT order_id
FROM orders
WHERE amount_cents IS NULL
   OR amount_cents < 0
   OR status IS NULL
   OR status NOT IN ('pending', 'paid', 'canceled', 'refunded');
```

Explicit null checks matter: SQL comparisons involving null generally evaluate to unknown, so `amount_cents < 0` alone does not catch a missing amount.

Check relationships separately, with an agreed policy for missing or late customers:

```sql
SELECT o.order_id, o.customer_id
FROM orders AS o
LEFT JOIN customers AS c ON o.customer_id = c.customer_id
WHERE o.customer_id IS NOT NULL
  AND c.customer_id IS NULL;
```

### Schema evolution and consumer compatibility

Use the reader/writer directions and format-specific examples in [contract compatibility](data-contracts.md#4-backward-forward--full-compatibility). Turn the supported combinations into fixtures using the [contract test matrix](data-contracts.md#8-contract-testing--consumer-driven-contracts).

Test old fixtures with the new consumer and new fixtures with supported old consumers as required. Include semantic changes such as units, timezone interpretation, key meaning, and enum expansion. Keep historical schemas available for replay.

### What happens when a check fails?

Choose the action according to the data's use:

- Block publication for financial discrepancies, broken keys, or incompatible contracts.
- Quarantine invalid records when partial publication is acceptable; account for and retain rejected records for repair.
- Warn on an explainable statistical anomaly, with an owner and escalation threshold.

Store failure counts, affected partitions, rule versions, run IDs, and safe diagnostic samples. Mask sensitive values in test output. An empty dataset can pass every row-level rule, so independently check expected arrivals and freshness.

**Interview question:** Are data quality checks tests or monitoring? The same assertion can serve both. CI uses controlled fixtures to catch code regressions; production checks inspect changing real data and trigger operational action.

## 3. Source-Target Reconciliation and Checksums

### Establish the comparison boundary first

Reconciliation asks whether the target represents the expected source state after agreed transformations. Define the selected columns, keys, filters, time range, delete policy, and comparison position before comparing values.

For a full copy, compare equivalent snapshots. For CDC, coordinate a source snapshot or known commit boundary with the target's applied progress. If the target has already advanced beyond that boundary, use a target snapshot/version or a controlled pause that represents the same state. A watermark alone does not freeze either side.

A consistent source snapshot prevents its own queries from observing changing states, but does not automatically align another system. For an example of transaction-level snapshot behavior, see [PostgreSQL's Repeatable Read documentation](https://www.postgresql.org/docs/16/transaction-iso.html#XACT-REPEATABLE-READ).

For transformed targets, compare the expected transformed source result with the target. A raw order count cannot equal a daily aggregate row count. For streaming systems, define a convergence deadline and account explicitly for permitted late arrivals.

### A practical reconciliation ladder

1. Verify schema mappings and the expected grain.
2. Compare row counts by partition, date, tenant, or key range.
3. Compare distinct key counts and check duplicate keys independently.
4. Compare business totals, null counts, and selected distributions.
5. Compare exact key sets to find missing and extra rows.
6. Compare values or row digests for matching keys.
7. Drill into differing buckets and classify lag, missing changes, duplicates, mapping errors, or incorrect deletes.

Equal counts can hide one missing row and one extra row. Equal sums can hide offsetting errors. Neither establishes equality.

### Missing and extra keys

Assume `expected_orders` and `actual_orders` already represent the same boundary and population. Both must pass non-null and unique-key checks first.

```sql
SELECT 'missing' AS problem, e.order_id
FROM expected_orders AS e
WHERE NOT EXISTS (
    SELECT 1 FROM actual_orders AS a WHERE a.order_id = e.order_id
)
UNION ALL
SELECT 'extra' AS problem, a.order_id
FROM actual_orders AS a
WHERE NOT EXISTS (
    SELECT 1 FROM expected_orders AS e WHERE e.order_id = a.order_id
);
```

`NOT EXISTS` avoids the null-related surprises of `NOT IN`. Set operations such as `EXCEPT` may discard duplicate multiplicity; they do not replace uniqueness checks. Compare matched values separately with explicit null-safe rules appropriate to the engine.

### Checksums and canonical representation

A digest is useful only if equal logical values produce equal bytes on both systems. Define:

- Column order and data type markers.
- Null encoding distinct from empty strings and literal text such as `"null"`.
- Unambiguous field boundaries, such as length prefixes or an agreed structured encoding.
- Decimal precision and scale, timestamp timezone and precision, and binary encoding.
- Whether Unicode normalization or case folding is intended; do not normalize away meaningful differences.
- Handling for floating-point special values and any explicitly allowed numerical tolerance.

Naive concatenation is ambiguous: `('ab', 'c')` and `('a', 'bc')` both become `abc`. Engine-specific hash functions may use different algorithms or encodings. An approximate numeric comparison also cannot generally be replaced by exact hashing.

For large tables, assign each row to a stable bucket using an agreed hash of its business key. Compare bucket counts and deterministic digests, then inspect mismatches. Ensure the bucket assignment itself is consistent across engines.

An ordered digest must use a deterministic total order and preserve duplicates. A commutative digest must deliberately account for multiplicity; XOR alone can cancel identical pairs. Cryptographic hashes reduce collision risk but are probabilistic evidence, not a proof of equality. Checksums also do not establish that a published business definition is correct.

Use bounded partitions and off-peak execution when reconciliation would burden the source. Sampling can detect problems cheaply, but a clean sample does not establish complete reconciliation.

**Interview question:** Source and target counts match. Are we done? No. Check equivalent boundaries, key sets, duplicates, mapped values, and business totals; decide whether exact comparison or digest-based evidence is required.

## 4. Property-Based and Parametrized Testing

### Parametrized tests

A parametrized test applies one assertion to multiple explicitly chosen cases. It reduces repeated test code while making important boundaries visible.

This standard-library example uses subtests so each failing case is identified independently:

```python
import unittest


class StatusNormalizationTest(unittest.TestCase):
    def test_normalization(self):
        cases = [(" paid ", "PAID"), ("Canceled", "CANCELED"), ("", "")]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(raw.strip().upper(), expected)


if __name__ == "__main__":
    unittest.main()
```

In a real project, call the production normalization function in place of the inline expression. This example demonstrates the test structure; validation of unsupported statuses is a separate responsibility.

Useful parameter sets include empty/single/many rows, supported operation types, null placement, partition boundaries, decimal limits, and timestamp precision. Avoid a huge Cartesian product when most combinations exercise the same behavior.

### Property-based tests

Instead of listing only examples, generate many inputs from a defined domain and assert an invariant. A generator should produce structurally meaningful cases, including boundaries. Tools can shrink a failing input into a smaller counterexample; retain that example as a regression test. See the [Hypothesis introduction](https://hypothesis.readthedocs.io/en/latest/tutorial/introduction.html) and [its testing phases](https://hypothesis.readthedocs.io/en/latest/settings.html#hypothesis.settings.phases).

| Property | Data engineering example | Required assumption |
| --- | --- | --- |
| Round trip | Decode an encoded record and recover the original logical values | Encoding preserves the tested types and precision |
| Idempotence | Applying the same batch twice leaves the same final state | Input identities and conflict rules are stable |
| Partition invariance | Whole-input aggregation equals combining partition aggregates | Aggregation is decomposable and numerical rules are respected |
| Order invariance | Shuffling rows leaves the result unchanged | The operation is defined as order independent |
| Conservation | Accepted rows plus quarantined rows equal received rows | Categories are disjoint and cover every input occurrence |
| Reference equivalence | Optimized output equals a simple, trusted implementation | Both implement the same specified semantics |

Do not assert false properties. A CDC state machine is not invariant to arbitrary reordering unless it uses source versions to reject older changes. Global deduplication is not generally equivalent to independent chunk deduplication followed by concatenation.

### Stateful and metamorphic testing

Stateful tests generate action sequences: insert, update, delete, retry, crash, restore, and replay. Compare the system with a small in-memory reference model after each action or convergence point. This catches failures that isolated rows cannot expose.

Metamorphic testing checks a known relationship between related executions when a complete expected output is difficult to construct. For example, duplicating every input row should double a simple sum without deduplication, while rerunning a batch with identical event IDs should leave an idempotent target unchanged.

Use controlled seeds or a framework's reproduction mechanism, retain failing examples, and keep fixture generation deterministic where possible. Generated coverage complements deliberately chosen business cases; it does not prove correctness for every possible input.

**Interview question:** How is property-based testing different from random data generation? It combines a meaningful input domain with an invariant and reproducible failures; random rows without an oracle may reveal little beyond crashes.

## 5. Incremental Pipeline, CDC, and Backfill Testing

### Incremental processing boundaries

Test state across runs, rather than testing each batch in isolation. A pipeline can be correct on a fresh table and wrong when updating existing output.

| Scenario | Expected assertion |
| --- | --- |
| Initial run | Correct state is created with no prior checkpoint or target |
| Empty next interval | Existing data remains correct and progress follows the documented policy |
| Record exactly on a boundary | It is eventually included without an unintended duplicate |
| Several rows share a timestamp | Cursor pagination and filtering do not skip tied rows |
| Record commits after an extraction started | It appears in a later eligible run |
| Late update outside the lookback | It is repaired or explicitly falls outside the promised guarantee |
| Existing key changes partition | The old partition is corrected as well as the new one |
| Retry after partial failure | The same logical result is obtained without loss |

Half-open windows such as `[start, end)` avoid overlap at adjacent time boundaries, but do not solve late visibility or late updates. Timestamp cursors may need a composite tie-breaker, a bounded overlap with deduplication, or a different capture method. A monotonically allocated ID can still commit out of order.

Check the ordering between durable output and progress persistence. Advancing a checkpoint before durable writes can lose data; persisting it afterward can cause replay after a crash. Tests must verify the chosen recovery protocol.

### CDC operation and ordering matrix

Start with a known source state and track expected state after each change:

1. Insert an order, update its amount, then delete it.
2. Deliver the same event again; verify the documented duplicate behavior.
3. Deliver an older update after a newer update; verify version-aware application where promised.
4. Deliver an old insert after a delete; verify that stale replay cannot resurrect the row within the supported replay horizon.
5. Change a primary key and verify the connector's actual representation and target handling.
6. Apply a multi-row transaction and check the promised visibility boundary.
7. Introduce supported and unsupported schema changes and verify handling.

Distinguish a delete event, a soft-delete flag, and a broker tombstone; they need not have the same meaning. Source positions or versions require source-specific ordering rules. Wall-clock timestamps alone are often insufficient tie-breakers, and one global ordering may not exist across partitions.

If the serving layer promises transaction-level atomicity, readers must never observe half a transaction. Otherwise, test the documented convergence guarantee and avoid assuming cross-table atomicity.

### Snapshot-to-stream handoff

Generate writes during the initial snapshot, including updates and deletes to rows being scanned. Verify that the handoff accounts for changes between snapshot boundaries and streaming start without omissions or stale overwrites.

Include connector restarts, buffered event replay, large transactions, and the supported log-retention limit. When required source history has expired, the expected result may be a visible failure and controlled resnapshot, rather than silent continuation.

### Backfill correctness and isolation

- Use explicit affected intervals or partitions and a versioned transformation definition.
- Compare backfill output against an independent expected result for the same historical inputs.
- Run overlapping backfills and a concurrent live pipeline; verify the intended serialization or conflict-resolution policy.
- Verify overwrite scope: neighboring partitions, newer live records, and unrelated tenants remain intact.
- Fail halfway and resume; verify no duplicate output and no publication of an incomplete partition.
- Rebuild affected downstream aggregates and invalidate relevant cached results.

Test safe publication using the platform's transaction, staging-and-swap, or atomic metadata commit mechanism where available. A backfill should not expose partly rewritten results when consumers require a complete version.

For historical joins, use the dimension state valid at the event time when that is the business definition. Joining old facts to today's dimension can silently change historical reporting. Deleted raw inputs and expired schema versions can also make a faithful replay impossible.

**Interview question:** How would you test an incremental model? Run initial load, insert/update/delete sequences, boundary and late-arrival cases, retries, and overlapping runs; compare the converged result with a full recomputation over the same logical input state.

## 6. Idempotency, Replay, and Failure Injection

### Idempotency is an observable state guarantee

For state `S` and batch `B`, an idempotent application satisfies `apply(apply(S, B), B) = apply(S, B)`. Define whether this includes only the target table or also notifications, audit records, billing, and other side effects.

A merge on a stable key is not automatically idempotent. `total = total + incoming_amount`, nondeterministic timestamps, unstable event IDs, and arrival-order conflict handling can all change the result on retry.

Test by applying the batch, recording its logical result, applying it again, and comparing keys, values, totals, and side effects. Ignore only explicitly permitted metadata changes, such as an attempt counter. Also test concurrent attempts; sequential reruns do not expose every race.

### Replay and deduplication

Replay old input from a saved position into either the existing target or an isolated rebuilt target, according to the recovery design. Verify:

- Stable event identity and the scope of deduplication keys.
- Retention of deduplication state, source history, and deleted-key versions for the promised replay horizon.
- Supported historical schemas and a defined code/configuration version.
- Deterministic event time, enrichment inputs, and business rules where reproducibility is required.
- Explicit treatment of external side effects, including transactional outbox/inbox behavior when used.

Distinguish reproducing the original output from recomputing history under new rules. Both are valid operations, but they have different expected results.

### Failure injection matrix

| Injected failure | What the test should establish |
| --- | --- |
| Crash before any write | Restart eventually processes the input |
| Crash during a non-atomic batch | Recovery removes or safely completes partial effects |
| Target commits, but acknowledgment is lost | An uncertain success can be retried without duplicate business effects |
| Crash after target commit, before checkpoint | Replay preserves the intended final state |
| Checkpoint write fails | Progress is not falsely reported as durable |
| One destination succeeds and another fails | Recovery follows the documented cross-system consistency protocol |
| Poison record | The pipeline blocks or quarantines according to policy, with no silent loss |
| Network timeout or rate limit | Retries are bounded and do not amplify load indefinitely |
| Disk full or unavailable storage | Failed publication does not corrupt the last good dataset |
| Worker restart or rebalance | Ownership changes preserve the documented ordering and delivery behavior |

Inject failures at known commit boundaries and make each test repeatable. A random process kill can reveal faults, but a targeted failpoint makes the recovery claim easier to prove.

### Exactly-once claims and recovery objectives

At-least-once delivery with idempotent application can provide one logical effect within a defined boundary. Exactly-once processing inside one transactional system does not automatically cover an unrelated database or API. Test every claimed boundary, including side effects and ambiguous commit outcomes.

Measure recovery time, maximum recoverable backlog, and data loss against stated objectives. Check alerts, runbooks, and reconciliation after recovery. A restarted job that reports success while skipping records has not recovered correctly.

Run destructive failure experiments in isolated environments unless a controlled production exercise has an explicit scope and recovery process.

**Interview question:** What is the most revealing retry test? Let the destination commit, then lose its acknowledgment before progress is saved. The retry must handle an outcome the caller cannot distinguish from failure.

## 7. Performance, Load, and Scalability Testing

### Different tests answer different questions

| Type | Question | Example |
| --- | --- | --- |
| Performance baseline | How fast and costly is a representative workload? | Process a normal day's data and measure runtime and resource use |
| Load | Does expected traffic meet objectives? | Sustain the peak expected arrival rate with concurrent consumers |
| Stress | What happens beyond planned capacity? | Increase arrival rate until lag grows or a resource saturates |
| Spike | Can sudden changes be absorbed? | Burst ingestion after a source reconnects |
| Soak | Does behavior remain stable over time? | Run long enough to expose leaks, state growth, and compaction pressure |
| Scalability | How does behavior change with more data or resources? | Compare the same workload across worker counts and larger datasets |
| Recovery/catch-up | Can the system drain an outage backlog? | Resume ingestion while new data continues arriving |

Define the workload and acceptance criteria before measuring. A benchmark without a representative distribution can reward a design that fails in production.

### Representative workloads

Include realistic key skew, wide rows, null rates, compression, file sizes, partition counts, update/delete ratios, transaction sizes, and join cardinality. Test hot keys and small-file accumulation, not just uniformly distributed inserts.

Use anonymized or synthetic data without production secrets. Preserve characteristics that affect the execution plan; masking every customer to one key creates unrealistic skew.

Measure both cold and warm behavior where relevant. Keep code, configuration, data volume, and hardware recorded. Repeat measurements enough to distinguish regressions from noise, with comparable cache and concurrency conditions.

### Metrics and correctness under load

- Throughput: rows, bytes, or events processed per second.
- Latency: end-to-end p50/p95/p99, not only averages or task duration.
- Freshness and backlog: source-to-publication lag, oldest unprocessed event, and backlog growth.
- Resources: CPU, memory, spill, disk and network I/O, connection usage, and source load.
- Efficiency: cost per processed unit, source query pressure, and retry amplification.
- Correctness: missing/duplicate records, invalid output, lost deletes, and reconciliation differences.

Keep the offered arrival rate independent of pipeline slowdown when modeling an external producer. A generator that waits for each response can unintentionally reduce load and hide overload. Separate event-time age from processing latency so replayed old events do not distort interpretation.

### Catch-up capacity and scaling limits

If new data arrives at 100,000 events/minute and sustained processing is 150,000 events/minute, net backlog reduction is 50,000 events/minute. A backlog of 1,000,000 events takes about 20 minutes to drain under those stable assumptions. If processing capacity is no greater than the ongoing arrival rate, the backlog cannot drain.

Adding workers does not guarantee linear speedup. Bottlenecks include source limits, partition parallelism, shuffle, hot keys, state-store access, metadata operations, and destination write contention. Measure the actual limiting stage and cost as resources increase.

For a fixed workload, doubling resources and reducing runtime from 60 to 35 minutes gives about 1.71× speedup. Whether that is acceptable depends on the deadline and cost objective. Also test growing workloads at a proportional resource increase.

**Interview question:** A pipeline meets throughput targets but misses its freshness objective. How? Bursty arrival, queueing, skew, slow publication, or long tail latency can leave old records waiting even when average throughput looks healthy.

## 8. CI/CD Quality Gates and Test Environments

### Put checks where their feedback is useful

| Stage | Typical checks | Gate |
| --- | --- | --- |
| Local/pre-commit | Parsing, linting, fast unit and fixture tests | Catch obvious errors before review |
| Pull request | Changed transformations, relevant downstream tests, schema/contract compatibility, isolated integration tests | Block incompatible or incorrect changes |
| Pre-release | Representative E2E journeys, migrations, recovery tests for affected paths | Establish deployment readiness |
| Scheduled | Wider regression, reconciliation, performance, and longer recovery tests | Detect issues too costly for every change |
| Production publication | Required quality, freshness, and reconciliation rules | Publish, quarantine, or retain the last good version |

Use lineage to select affected descendants, while recognizing blind spots such as dynamic SQL, shared macros, configuration, and external consumers. Run broader checks when impact cannot be determined reliably.

Code coverage is supporting evidence, not a complete gate. A test can execute a join without checking fanout or run a merge without testing delete behavior. Risk and meaningful assertions determine confidence.

### Test environments and fixtures

- Give each run isolated schemas, buckets/prefixes, topics, consumer groups, and checkpoint locations as applicable.
- Match the important production engine versions, serialization formats, collation, timezone, and permission boundaries.
- Use disposable local services for fast feedback, then test platform-specific behavior in a representative managed environment.
- Use least-privilege test identities; ensure CI cannot accidentally write production data.
- Version small deterministic fixtures and expected results; use controlled generators for larger workloads.
- Include empty, malformed, duplicate, late, out-of-order, boundary, and historical-schema cases.
- Register cleanup in the test lifecycle and use expiry policies for resources left behind by interrupted runs.

Small fixtures cannot prove distributed execution behavior. Keep separate representative tests for shuffle, file layout, concurrency, and scale. Conversely, a full production clone is usually unnecessary for a simple transformation regression.

### Migration, deployment, and rollback

Test schema changes against both deployed and proposed application versions when they overlap during rollout. An expand-and-contract migration can introduce a compatible representation, move consumers, and remove the old representation only after support is no longer needed.

Verify migration restart behavior, pre-existing data, permissions, and unsupported states. A code rollback cannot automatically undo destructive DDL, published incorrect data, or changed checkpoint formats. Test a data repair or forward-fix path where reversal is unsafe.

For important datasets, build a candidate version separately, compare it with the current version, run required checks, and switch publication using the platform's supported mechanism. Keep a known good version long enough for the recovery requirement.

### Failure artifacts and flaky tests

Retain the commit/version, fixture or generator reproduction information, run ID, engine versions, source positions, query/job IDs, and masked failure samples. These make CI failures reproducible.

Treat retries as a diagnostic or operational mechanism, not evidence that a failing assertion is harmless. Investigate time-dependent waits, shared fixtures, unordered results, eventual-consistency assumptions, and resource contention. Any temporary test quarantine needs an owner and a deadline, especially if it removes a release gate.

**Interview question:** Which checks should block a release? Deterministic correctness, incompatible contracts, and recovery regressions on affected critical paths should block it. Statistical anomalies and performance changes need defined thresholds, comparable measurements, and an explicit response policy.

## 9. Interview Scenarios

### Design tests for a new orders pipeline

Start with the output grain, paid/revenue definition, allowed lateness, freshness target, and recovery guarantee. Then propose:

1. Unit fixtures for transformations, joins, and aggregation boundaries.
2. Schema, uniqueness, relationship, amount, and freshness rules.
3. Integration tests for connectors, types, transactions, and merges.
4. CDC sequences including deletes, duplicates, late changes, and snapshot handoff.
5. An E2E journey plus source-target reconciliation at a defined boundary.
6. Retry, replay, backfill concurrency, and ambiguous-commit tests.
7. Peak load, skew, catch-up capacity, and publication gates.

Explain which assertion catches which failure and who responds when it fails.

### Revenue doubled after a backfill

Check grain and keys first. Compare affected partitions, join cardinalities, append-versus-replace behavior, concurrent writers, and replayed events. Reconcile detail and aggregates against the same input boundary. Repair the affected scope, then add a rerun and overlapping-backfill regression test.

### Counts match, but dashboards disagree

Check duplicate/missing key compensation, value mappings, status filters, timezone cutoffs, decimal precision, dimension validity, and dashboard cache/version. Compare logical populations before trusting counts or digests.

### A restart resurrected deleted records

Reproduce a delete followed by stale replay. Inspect ordering/version checks, delete-marker retention, deduplication expiry, checkpoint recovery, and snapshot overwrite behavior. The regression test must cover both immediate retry and replay near the supported retention boundary.

### Everything passes in CI, but production fails

Look for an untested boundary: different engine behavior, real identity/network configuration, historical schemas, skew, scale, concurrent runs, late data, or failure recovery. Add the smallest representative test for the actual failure mode instead of merely increasing coverage.
