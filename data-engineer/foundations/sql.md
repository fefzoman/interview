# Advanced SQL: Data Engineer Interview Guide

Advanced SQL interviews test whether you can produce the right result, explain how the database computes it, and preserve correctness as data volume and concurrency grow. Start an answer with the required grain and semantics, then discuss the plan, trade-offs, and validation.

**Scope:** All ten topics below combine relational fundamentals with analytical and production patterns. Examples use PostgreSQL 18 syntax unless another engine is named. Features such as `FILTER`, `GROUPS`, `QUALIFY`, partial indexes, and `MERGE` vary by engine and version. Documentation reviewed: 2026-10-25.

**Running example:** An online store has `customers(customer_id, region)`, `orders(order_id, customer_id, ordered_at, amount, status)`, and `events(event_id, user_id, event_ts)`. Each table's primary ID is unique and non-NULL; order and event timestamps are non-NULL. Money uses an exact decimal type. Time examples assume UTC unless a business timezone is specified. Additional tables are introduced where needed. Examples are independent teaching snippets, not a script to run in order; engine-specific examples have not been executed against those services.

**Study order:** Query semantics → windows and joins → plans and optimization → transactions and incremental processing → distributed execution and production diagnosis.

## Contents

1. [SQL Fundamentals & Execution](#1-sql-fundamentals--execution)
2. [Analytical SQL](#2-analytical-sql)
3. [Query Execution Plans](#3-query-execution-plans)
4. [Query Optimization](#4-query-optimization)
5. [Indexing & Data Access](#5-indexing--data-access)
6. [Joins & Aggregations](#6-joins--aggregations)
7. [Transactions & Concurrency](#7-transactions--concurrency)
8. [Data Modeling & Incremental SQL](#8-data-modeling--incremental-sql)
9. [Distributed SQL & MPP](#9-distributed-sql--mpp)
10. [Performance & Production](#10-performance--production)

## 1. SQL Fundamentals & Execution

### Logical query processing vs. physical execution

SQL describes a result. Its **logical order** explains which rows and values are available at each stage; its **physical plan** chooses algorithms and access paths to compute an equivalent result.

A useful conceptual order for a query block is:

1. `FROM`, including joins and `ON` conditions.
2. `WHERE`, which filters individual rows.
3. `GROUP BY` and aggregate calculation.
4. `HAVING`, which filters groups.
5. Window calculation over the remaining rows or groups.
6. Final `SELECT` projection, including window outputs.
7. `DISTINCT`.
8. `ORDER BY`.
9. `OFFSET` / `LIMIT` / `FETCH`.

This is a reasoning model, not a promise about expression evaluation timing. A window expression belongs to the select list, but its input has already passed `WHERE`, grouping, and `HAVING`. Engines with `QUALIFY`, including BigQuery and Snowflake, can filter window results before `DISTINCT`; PostgreSQL uses an outer query. Set operators combine query blocks; use parentheses and an outer ordering when necessary.

Consequences: a window result cannot be filtered by the same query block's `WHERE`; a select-list alias is generally unavailable there; `LIMIT` without a deterministic `ORDER BY` does not identify a stable subset. Alias rules differ across dialects.

The optimizer may reorder inner joins, push filters into scans, and combine operations. A filter written late can execute early if equivalence is preserved. Outer joins, NULLs, volatile functions, and limits constrain those transformations.

### Joins, subqueries, CTEs, and correlation

| Construct | Meaning | Main correctness question |
| --- | --- | --- |
| `INNER JOIN` | Emit every matching pair | Can either key have multiple matches? |
| `LEFT JOIN` | Emit matches plus a NULL-extended row for each unmatched left row | Does a later filter accidentally remove unmatched rows? |
| `RIGHT JOIN` / `FULL JOIN` | Preserve the right side / both sides | Which columns can become NULL in the output? |
| `CROSS JOIN` | Emit every combination | Is the product intentional and bounded? |
| Scalar subquery | Produce one value; zero rows becomes NULL, multiple rows is an error | Is one row guaranteed, perhaps by aggregation? |
| Derived table | Subquery in `FROM` | What is its output grain? |
| CTE | Name a query result within one statement | Does the engine inline or materialize it? |
| Correlated subquery | Refer to a value from an enclosing query | Can the optimizer decorrelate it, or will work repeat? |

Keep a right-side eligibility filter in `ON` when unmatched customers must survive:

```sql
SELECT c.customer_id, o.order_id
FROM customers AS c
LEFT JOIN orders AS o
  ON o.customer_id = c.customer_id
 AND o.status = 'paid';
```

Putting `o.status = 'paid'` in `WHERE` removes NULL-extended rows and changes the result. Moving predicates mechanically across an outer join is unsafe.

A correlated aggregate can express a useful condition directly:

```sql
SELECT o.order_id, o.amount
FROM orders AS o
WHERE o.amount > (
    SELECT AVG(o2.amount)
    FROM orders AS o2
    WHERE o2.customer_id = o.customer_id
);
```

The subquery logically depends on the current customer. The physical plan might compute customer averages once and join them, or repeatedly execute a subplan. Read the plan before claiming that correlation always means one execution per row. A CTE improves readability but is neither a persistent table nor an automatic performance improvement.

### Set operations: UNION, INTERSECT, EXCEPT

| Operator | Result | Duplicate behavior |
| --- | --- | --- |
| `UNION ALL` | Rows from both inputs | Preserves all occurrences |
| `UNION` | Rows present in either input | Removes duplicates |
| `INTERSECT` | Rows present in both inputs | Distinct by default |
| `EXCEPT` | Rows present in the left input and absent from the right | Distinct by default |

Inputs need the same number of columns in compatible positions and types. Ordinary set operators match by position, not by column name. Where supported, `INTERSECT ALL` keeps the minimum occurrence count, and `EXCEPT ALL` keeps `max(left_count - right_count, 0)`. Support varies by dialect; Oracle also uses `MINUS` for set difference.

`UNION ALL` avoids duplicate elimination, often removing a hash or sort step, but use it only when duplicate preservation is correct. Deduplication compares the entire projected row; two records with one business key and different timestamps both survive `UNION`. For set duplicate elimination, NULLs are treated as equal to each other, unlike ordinary `NULL = NULL` predicates.

### NULL semantics and three-valued logic

NULL represents missing or unknown information. Comparisons with NULL ordinarily yield **UNKNOWN**, and `WHERE`, `HAVING`, and join matching accept only TRUE. Use `IS NULL`, `IS NOT NULL`, or a supported NULL-safe comparison such as PostgreSQL's `IS NOT DISTINCT FROM`.

| Expression | Result |
| --- | --- |
| `NULL = NULL` | UNKNOWN |
| `NOT UNKNOWN` | UNKNOWN |
| `TRUE AND UNKNOWN` | UNKNOWN |
| `FALSE AND UNKNOWN` | FALSE |
| `TRUE OR UNKNOWN` | TRUE |
| `FALSE OR UNKNOWN` | UNKNOWN |

`COUNT(*)` counts rows; `COUNT(amount)` counts non-NULL amounts. Most ordinary aggregates ignore NULL inputs. `SUM` on no qualifying rows returns NULL, while `COUNT` returns zero. `COALESCE(SUM(amount), 0)` defines a zero result when that matches the business meaning; replacing missing measurements with zero before averaging changes the denominator.

NULLs form one group in `GROUP BY`. Unique-constraint NULL handling differs by engine and options. In PostgreSQL, a `CHECK` rejects FALSE but permits UNKNOWN, so `CHECK (amount > 0)` alone does not enforce `NOT NULL`.

### EXISTS vs. IN, anti-joins, and semi-joins

A **semi-join** returns left rows with at least one match, without multiplying them by right-side matches. An **anti-join** returns left rows with no match. `EXISTS` and `NOT EXISTS` express these operations naturally:

```sql
SELECT c.customer_id
FROM customers AS c
WHERE NOT EXISTS (
    SELECT 1
    FROM orders AS o
    WHERE o.customer_id = c.customer_id
);
```

This returns customers without any matching order. `SELECT 1` communicates that only existence matters; it is not an optimization over `SELECT *` inside ordinary `EXISTS`.

`IN` tests membership; `EXISTS` tests whether a query returns a row. Optimizers can produce equivalent semi-join plans, so neither is universally faster. The major semantic trap is `NOT IN`: `2 NOT IN (1, NULL)` is UNKNOWN and is filtered out. A NULL in a subquery can therefore remove otherwise expected results. `NOT EXISTS` avoids that contamination, although NULL join keys still require an explicit matching policy. [PostgreSQL subquery semantics](https://www.postgresql.org/docs/18/functions-subquery.html).

`LEFT JOIN ... WHERE right_key IS NULL` can express an anti-join when the tested right column cannot be NULL on a real match. Testing an unrelated nullable right column can misclassify matched rows. Semi-joins preserve existing left duplicates; they do not deduplicate the left relation.

**Interview check:** Explain why a left join followed by `COUNT(*)` counts an unmatched customer once, while `COUNT(o.order_id)` counts zero orders.

## 2. Analytical SQL

### Window functions: ranking and neighboring rows

A window function computes a value across related rows while retaining the input grain. `PARTITION BY` defines independent groups; the window's `ORDER BY` defines sequence and peers. It does not guarantee final output order.

| Function | Purpose | Tie behavior or boundary |
| --- | --- | --- |
| `ROW_NUMBER()` | Assign a position | Distinct numbers; tied ordering is nondeterministic without a tie-breaker |
| `RANK()` | Competition ranking | `1, 1, 3` |
| `DENSE_RANK()` | Rank distinct ordering values | `1, 1, 2` |
| `LAG(x)` | Read a previous row's value | Default offset 1; missing previous row yields NULL unless a default is supplied |
| `LEAD(x)` | Read a following row's value | Analogous to `LAG`, in the other direction |

Use `ROW_NUMBER` for exactly three orders per customer; use `RANK` or `DENSE_RANK` when ties have a business meaning. Adding `order_id` to a revenue ranking removes revenue peers, so it changes ranking semantics. Put a tie-breaker in a separate row-number window when necessary.

```sql
WITH ranked AS (
    SELECT order_id, customer_id, amount,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY ordered_at DESC, order_id DESC
           ) AS rn
    FROM orders
)
SELECT order_id, customer_id, amount
FROM ranked
WHERE rn = 1;
```

This selects one latest order deterministically under the stated non-NULL timestamp/key contract. If timestamps are nullable, specify a NULL ordering policy; PostgreSQL defaults to NULLs first for descending ordering. `LAG` and `LEAD` use partition order rather than the aggregate window frame.

### Window frames: ROWS, RANGE, GROUPS

The **partition** is the whole group; the **frame** is the subset used for the current row's frame-sensitive calculation.

| Frame unit | Boundary measures | Example use |
| --- | --- | --- |
| `ROWS` | Positions in the ordered row sequence | Last seven observations |
| `RANGE` | Ordering values and peer groups | Events within the preceding hour |
| `GROUPS` | Groups of equal ordering values | Current and previous distinct price levels |

For ordered aggregates in PostgreSQL, the default frame includes the partition start through the current row's last peer. Thus a default running total on amounts `10, 10, 20`, ordered by amount, produces `20, 20, 40`. An explicit `ROWS` frame with a deterministic tie-breaker produces `10, 20, 40`. Without window ordering, the default frame covers the partition.

`ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` means up to seven rows, not seven calendar days. For PostgreSQL time-offset `RANGE`, use one ordering expression, for example `ORDER BY event_ts RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW`. `GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW` includes the previous peer group and the current peer group; support differs across engines. [Window frame syntax](https://www.postgresql.org/docs/18/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS).

`LAST_VALUE` is frame-sensitive: under the default ordered frame, it commonly returns the current peer group's last value. To request the last value in the entire partition, specify `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`. Ranking functions do not become rolling rankings merely because a frame is added. [PostgreSQL window functions](https://www.postgresql.org/docs/18/functions-window.html).

### Running totals, moving averages, and percentiles

Assume `daily_sales(sale_date, revenue)` has exactly one row for every calendar day, including zero-sales days:

```sql
SELECT sale_date, revenue,
       SUM(revenue) OVER (
           ORDER BY sale_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS running_revenue,
       AVG(revenue) OVER (
           ORDER BY sale_date
           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ) AS seven_day_average
FROM daily_sales
ORDER BY sale_date;
```

Early averages contain fewer than seven observations. For a strict seven-day denominator, define the prehistory and missing-day policy. If dates are sparse, join to a calendar first when zero-activity days should contribute; a time-range frame alone does not invent those rows.

PostgreSQL ordered-set aggregates calculate exact percentiles:

```sql
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median,
       PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY amount) AS p95_observed
FROM orders;
```

`PERCENTILE_CONT` interpolates, while `PERCENTILE_DISC` returns an observed value. NULL inputs are ignored. Approximate quantile functions trade exactness for lower resource use at scale; state the error contract. Do not average daily medians or p95 values to obtain a monthly percentile. `NTILE(100)` allocates rows to buckets and can split ties; it is not an exact percentile-value function.

### Conditional aggregation: FILTER and CASE

```sql
SELECT customer_id,
       COUNT(*) AS total_orders,
       COUNT(*) FILTER (WHERE status = 'paid') AS paid_orders,
       SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS paid_amount
FROM orders
GROUP BY customer_id;
```

`FILTER` restricts input to one aggregate. `CASE` works across more dialects and can express richer classification. `COUNT(CASE WHEN condition THEN 1 END)` counts matches; adding `ELSE 0` counts every row because zero is non-NULL. An aggregate filter keeps other rows available for other aggregates; a `WHERE` filter removes them from all aggregates.

For ratios, use decimal arithmetic and protect zero denominators, for example `paid_orders::numeric / NULLIF(total_orders, 0)` in an outer query. Decide whether an undefined ratio should stay NULL rather than silently turning into zero.

### GROUPING SETS, ROLLUP, CUBE

These calculate multiple aggregation grains in one statement:

```sql
SELECT region, status,
       GROUPING(region) AS region_is_total,
       GROUPING(status) AS status_is_total,
       SUM(amount) AS revenue
FROM order_reporting
GROUP BY GROUPING SETS ((region, status), (region), ());
```

Here `order_reporting` contains order amounts with region and status. The grouping sets produce region/status totals, region totals, and a grand total. `ROLLUP(region, status)` expands to that same hierarchy. `CUBE(region, status)` also includes totals by status across regions; a cube over `n` expressions expands to `2^n` grouping sets.

Subtotal dimensions are represented with NULLs, which can also exist in source data. `GROUPING(column)` distinguishes a rolled-up placeholder from a genuine NULL dimension. Never sum all output rows indiscriminately: details and subtotals overlap. [Grouping sets and grouping indicators](https://www.postgresql.org/docs/18/queries-table-expressions.html#QUERIES-GROUPING-SETS).

### Gaps and islands

A **gap** is a break in continuity; an **island** is a maximal consecutive run. Define continuity explicitly: consecutive dates, adjacent integer IDs, or overlapping intervals require different rules.

For consecutive active dates, deduplicate dates before numbering:

```sql
WITH days AS (
    SELECT DISTINCT user_id, event_ts::date AS active_date
    FROM events
), labeled AS (
    SELECT user_id, active_date,
           active_date - (ROW_NUMBER() OVER (
               PARTITION BY user_id ORDER BY active_date
           ))::integer AS island_key
    FROM days
)
SELECT user_id, MIN(active_date) AS start_date,
       MAX(active_date) AS end_date, COUNT(*) AS active_days
FROM labeled
GROUP BY user_id, island_key;
```

The date minus row position is constant within a consecutive run. Date extraction must use the intended business timezone. For overlapping intervals, compare each start to the **running maximum** prior end; comparing only to the immediately preceding interval's end fails on nested intervals.

### Sessionization

Define a session as events for one user separated by no more than 30 minutes. A gap strictly greater than 30 minutes starts another session:

```sql
WITH previous AS (
    SELECT event_id, user_id, event_ts,
           LAG(event_ts) OVER (
               PARTITION BY user_id ORDER BY event_ts, event_id
           ) AS previous_ts
    FROM events
), boundaries AS (
    SELECT event_id, user_id, event_ts,
           CASE WHEN previous_ts IS NULL
                     OR event_ts - previous_ts > INTERVAL '30 minutes'
                THEN 1 ELSE 0 END AS starts_session
    FROM previous
)
SELECT event_id, user_id, event_ts,
       SUM(starts_session) OVER (
           PARTITION BY user_id ORDER BY event_ts, event_id
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS session_number
FROM boundaries;
```

Separate query blocks are needed because window functions cannot be nested directly. Define whether exactly 30 minutes is continuous. Late events can bridge two existing sessions, so incremental sessionization must revisit affected history; a row-number-based session identifier is not stable under arbitrary late inserts.

### Cohort analysis

A cohort groups entities by a common starting event, such as signup month. **Retention** measures how many cohort members satisfy an activity rule at each age. Define the denominator, activity event, timezone, and observation cutoff before writing SQL.

Assume `users(user_id, signup_at)` has one row per user. This PostgreSQL example returns counts of users active in each signup-month/activity-month pair:

```sql
WITH cohorts AS (
    SELECT user_id, DATE_TRUNC('month', signup_at)::date AS cohort_month,
           signup_at
    FROM users
), activity AS (
    SELECT DISTINCT c.user_id, c.cohort_month,
           DATE_TRUNC('month', e.event_ts)::date AS activity_month
    FROM cohorts AS c
    JOIN events AS e ON e.user_id = c.user_id
    WHERE e.event_ts >= c.signup_at
)
SELECT cohort_month, activity_month, COUNT(*) AS active_users
FROM activity
GROUP BY cohort_month, activity_month;
```

Divide each count by that cohort's size computed from **all signed-up users**, not only active users. Derive month age with calendar year/month arithmetic, not elapsed days divided by 30. To show zero retention, generate a cohort-by-age calendar and left join the counts. Exclude or mark future and partially observed periods: a two-week-old cohort has no fully observed month-two retention. Deduplicating user/month prevents frequent users from inflating the numerator.

**Interview check:** Explain how ties, missing dates, late events, and an incomplete observation window can each change an analytical result without any SQL error.

## 3. Query Execution Plans

### Logical vs. physical plans; EXPLAIN vs. EXPLAIN ANALYZE

A logical plan represents relational operations such as filter, join, and aggregate. A physical plan chooses implementations: sequential scan, hash join, sorted aggregate, parallel exchanges. Engines may expose different levels of detail and may adapt execution at runtime.

In PostgreSQL, inspect estimates first, then collect runtime evidence for a safe workload:

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT customer_id, SUM(amount)
FROM orders
WHERE ordered_at >= TIMESTAMP '2026-10-01 00:00:00'
GROUP BY customer_id;
```

`EXPLAIN` describes the chosen plan without executing the statement. `EXPLAIN ANALYZE` executes it and adds runtime measurements. On DML it performs the writes; use an appropriate disposable environment. A rollback does not undo every possible effect, such as sequence consumption or external trigger effects. Profiling also adds overhead. Syntax and available runtime statistics differ by engine.

### Scans and common operators

| Operator | What happens | What to inspect |
| --- | --- | --- |
| Sequential scan | Read table pages and filter rows | Fraction of table needed; bytes read; rows removed |
| Index scan | Find matching index entries, then fetch table rows as needed | Selectivity and scattered heap access |
| Index-only scan | Read required values from the index | PostgreSQL may still need heap visibility checks |
| Bitmap index/heap scan | Collect candidate row locations, then visit heap pages | Combined predicates, lossy pages, rechecks |
| Nested Loop | For each outer row, scan or probe the inner input | Outer row count × inner work |
| Hash Join | Build a hash table, then probe it | Build size, skew, memory, batches/spill |
| Merge Join | Walk ordered join-key inputs | Existing ordering and sort cost |
| Sort | Order rows, sometimes only a top-N subset | Input width, memory, external sort |
| HashAggregate | Maintain aggregate state by group key | Distinct groups and spill |
| GroupAggregate | Aggregate rows already ordered by group key | Whether sorting was needed |
| Materialize | Buffer a result for reuse | Rescans avoided, buffered size, disk use |

A sequential scan can be optimal for a broad analytical query. An index scan can be slower when it fetches a large fraction of a table through random accesses. A `Materialize` plan node is an execution buffer, distinct from a persistent materialized view.

### Estimated vs. actual cardinality and execution statistics

Read a tree from scans upward to understand data flow, then locate the expensive path. A small final result can hide huge intermediate work. An illustrative PostgreSQL fragment is:

```text
Nested Loop  (cost=0.60..500.00 rows=10 width=24)
             (actual time=0.05..900.00 rows=100000 loops=1)
  -> ... outer input ...
  -> Index Scan ... (actual time=0.01..0.02 rows=10 loops=10000)
```

The top node underestimated output by 10,000 times. The inner scan produced an average of ten rows per execution across 10,000 executions: about 100,000 rows in total. Costs are planner units, not milliseconds. PostgreSQL's `actual time` values are startup/total milliseconds per execution; `loops` counts executions. Parent timings include child work, so summing every node double-counts time. Parallel plans require attention to worker detail.

Inspect cardinality errors early in the tree: they can cause a bad join order, undersized hash table, or nested loop over far too many rows. Buffer hits indicate database-cache access; buffer reads do not prove physical device I/O because the OS may cache pages. Look for temporary reads/writes, external sorts, hash batches, rows removed by filters, and planning/execution time. [PostgreSQL EXPLAIN interpretation](https://www.postgresql.org/docs/18/using-explain.html).

**Interview check:** A query returns 20 rows but takes 40 seconds. Explain which intermediate counts, loops, sorts, and reads you would inspect before adding an index.

## 4. Query Optimization

### Rule-based vs. cost-based optimization

**Rule-based transformations** apply equivalences, such as constant folding or removing a redundant expression. **Cost-based optimization (CBO)** estimates alternative plans using row counts, widths, selectivity, available ordering, memory, I/O, and sometimes network cost. Modern optimizers commonly combine rules with cost-based choices.

The chosen plan is the cheapest among alternatives explored under the optimizer's model and search budget. It is not guaranteed to be globally optimal, and estimates can be wrong. A rule can enable a new access path while the cost model decides whether to use it.

### Predicate pushdown and projection pruning

**Predicate pushdown** evaluates filters closer to the data source, reducing rows reaching later operators. **Projection pruning** reads and carries only required columns, reducing I/O, decompression, hash-table width, and network traffic. In a columnar engine, avoiding 190 out of 200 columns can matter more than replacing one expression.

Pushdown has semantic and connector limits. An outer-join filter, a window-dependent predicate, a volatile expression, or a remote source with different comparison rules may prevent a safe rewrite. Confirm the scan's pushed predicates and columns in the plan. A filter that appears early in SQL is not proof that a remote source executed it.

### Join ordering and join elimination

Inner joins can often be reordered. Joining selective inputs early can reduce intermediate results; doing so is useful only when estimates capture filter selectivity and key multiplicity. Outer joins and lateral dependencies constrain reordering.

**Join elimination** removes a join whose contribution is provably unnecessary. For example, an unused right side of a left join can be removable if uniqueness proves it cannot multiply rows. Eliminating an inner join also requires proving that it cannot filter rows, including NULL and missing-match behavior. Whether an optimizer uses foreign-key or uniqueness metadata is engine-specific. Warehouse constraints that are informational rather than enforced must reflect real data; false assumptions can produce incorrect results when used by optimization.

### Subquery decorrelation and CTE materialization

**Decorrelation** converts a dependent subquery into operations such as joins, grouped aggregates, or semi-joins. `EXISTS` is a common candidate. Scalar-subquery rewrites must preserve the zero-row NULL result and the error on multiple rows; replacing them with an unrestricted inner join changes semantics.

**Inlining** lets an optimizer combine a CTE with its caller, pushing filters and pruning columns across the boundary. **Materialization** computes a result for reuse, potentially avoiding repeated expensive work but adding storage and sometimes preventing caller-specific filtering.

PostgreSQL 18 can fold a non-recursive, side-effect-free CTE referenced once into its parent. A multiply referenced CTE is normally materialized; `MATERIALIZED` and `NOT MATERIALIZED` influence eligible cases. `NOT MATERIALIZED` can duplicate expensive calculation. Recursive or volatile CTEs have different restrictions. Do not generalize these rules to other engines or older PostgreSQL versions. [CTE materialization](https://www.postgresql.org/docs/18/queries-with.html#QUERIES-WITH-CTE-MATERIALIZATION).

### SARGability and partition pruning

A **SARGable** predicate can be used as an efficient search condition for an access path. Functions or implicit conversions on an indexed column can obstruct that path, unless the optimizer rewrites the predicate or a matching expression index exists.

Prefer a half-open timestamp interval over extracting a date from every row:

```sql
SELECT order_id, amount
FROM orders
WHERE ordered_at >= TIMESTAMP '2026-10-01 00:00:00'
  AND ordered_at <  TIMESTAMP '2026-10-02 00:00:00';
```

This avoids guessing the last representable instant of a day and can support range access. `BETWEEN` includes both endpoints, so adjacent timestamp batches can overlap. For local business dates, convert boundaries to the storage timezone; daylight-saving days need not last 24 hours.

**Partition pruning** skips partitions that cannot satisfy predicates on the partitioning scheme. It is distinct from index access inside a remaining partition. Runtime pruning may use parameter values or join information when supported. A SARGable predicate does not guarantee pruning if it constrains a different column or an unsupported expression. Inspect partitions or bytes actually scanned.

### Statistics, histograms, and cardinality estimation

Statistics approximate data distribution: table row count, NULL fraction, distinct-value count, frequent values, histograms, and sometimes relationships between columns. Histograms summarize ranges; most-common-value lists represent hot values more directly.

Suppose a million-row table has ten statuses. Assuming uniformity estimates 100,000 rows per status; if 99% are `complete`, the estimate is badly wrong for both hot and rare statuses. Correlated predicates cause another failure: multiplying independent selectivities for `country = 'US'` and `state = 'CA'` ignores their dependency.

Refresh statistics after relevant data changes using the engine's mechanism; PostgreSQL uses `ANALYZE`. For supported within-table relationships, extended statistics can improve estimates:

```sql
CREATE STATISTICS orders_customer_status_stats (dependencies, mcv)
ON customer_id, status FROM orders;

ANALYZE orders;
```

Create these only for observed estimation problems. Extended statistics have scope limits and do not solve arbitrary cross-table join estimates. Better sampling or statistics targets may help irregular distributions, at a collection and planning cost. [PostgreSQL planner statistics](https://www.postgresql.org/docs/18/planner-stats.html).

### Query rewrites and plan regression

Useful rewrites reduce work while preserving duplicate, NULL, and ordering behavior: replace join-plus-deduplication with `EXISTS` for existence checks; pre-aggregate at a valid grain; prune columns before a wide exchange; replace a non-searchable date expression with equivalent boundaries.

Avoid blanket rules such as “CTEs are faster,” “joins beat subqueries,” or “replace every `OR` with `UNION ALL`.” An `OR` rewrite can duplicate rows satisfying both branches; `UNION` can remove legitimate input duplicates.

A **plan regression** is a worse execution strategy after changes to statistics, data distribution, parameters, indexes, settings, or engine version. Save the query, parameter values/types, data snapshot, old/new plan, runtime distribution, and resource metrics. Prepared statements may use generic or parameter-dependent plans; hot and rare values can need different access paths. Compare representative parameters before applying hints or pinning a plan, and reassess any workaround after the underlying cause changes.

**Interview check:** A query slows down after a large load without a SQL change. Explain how you would distinguish stale estimates, a new hot key, lost pruning, and cache effects.

## 5. Indexing & Data Access

### B-Tree, Hash, Bitmap, and BRIN

An index trades storage and write work for a cheaper way to locate or order data. Its usefulness depends on predicate shape, distribution, required columns, and the fraction of the table needed.

| Structure | Strength | Limitation |
| --- | --- | --- |
| B-Tree | Equality, ranges, ordered retrieval, many top-N lookups | Broad matches can make table access expensive |
| Hash | Equality lookup | Does not provide range ordering; support and durability details depend on engine |
| Persistent bitmap index | Compact bitmaps can combine low-cardinality predicates efficiently | Frequent concurrent updates can be costly; common in analytical rather than OLTP use |
| BRIN | Small summaries of physical block ranges; useful for very large, physically correlated data | Lossy candidates require rechecks; weak correlation means little skipping |

PostgreSQL supports B-Tree, Hash, and BRIN index access methods, among others. Its **bitmap index scan** is a scan strategy that builds a bitmap from indexes; it is not a user-created persistent bitmap index type. PostgreSQL Hash indexes support equality; BRIN works especially well when values such as append-time timestamps track physical location. [PostgreSQL index types](https://www.postgresql.org/docs/18/indexes-types.html).

### Clustered vs. non-clustered indexes

A clustered organization makes table-row placement follow an index key; a non-clustered or secondary index stores keys plus row locators. Range reads can benefit from physical locality, but wide clustering keys can enlarge secondary indexes and random inserts can cause page splits.

Terminology is engine-specific. SQL Server clustered rowstore indexes store table rows at the leaf level; InnoDB organizes rows by the primary key. PostgreSQL normally stores rows in a separate heap, and `CLUSTER` performs a one-time reordering that subsequent writes do not continuously maintain. Snowflake and BigQuery clustering concerns placement and pruning of columnar data rather than a conventional clustered B-Tree. A primary-key declaration does not imply the same physical layout everywhere.

### Composite, covering, partial, and expression indexes

PostgreSQL examples illustrate distinct purposes:

```sql
CREATE INDEX orders_customer_time_idx
ON orders (customer_id, ordered_at DESC, order_id DESC)
INCLUDE (amount, status);

CREATE INDEX orders_open_time_idx
ON orders (ordered_at)
WHERE status = 'open';

CREATE INDEX customers_lower_email_idx
ON customer_contacts (LOWER(email));
```

The first is a **composite** index supporting a customer's ordered history, with extra payload columns to make some queries **covering**. `INCLUDE` columns do not become search or ordering keys. The second is a **partial** index storing only open orders; the query predicate must imply its condition. Parameterized predicates can prevent that proof for a generic plan. The third is an **expression** index for queries on `LOWER(email)` in an additional `customer_contacts` table.

For a B-Tree on `(customer_id, ordered_at)`, equality on the first key and a range on the next is a useful pattern. A predicate only on `ordered_at` may be less efficient; some engines and PostgreSQL 18 can use skip scans in suitable distributions. “The leftmost key must always appear or the index is unusable” is too absolute. Choose column order from actual equality, range, and ordering needs, not a universal “most selective first” rule. [Multicolumn indexes](https://www.postgresql.org/docs/18/indexes-multicolumn.html).

A covering index does not guarantee a heap-free PostgreSQL scan. The engine must also establish MVCC visibility, often through the visibility map. Recently modified pages can still require heap fetches, even under an `Index Only Scan` plan. [Index-only scans](https://www.postgresql.org/docs/18/indexes-index-only-scans.html).

### Selectivity, cardinality, and scan strategies

**Cardinality** can mean row count or distinct-value count; state which one you mean. Here, predicate **selectivity** is the matching fraction: lower means fewer rows. A high-distinct-count column often supports selective equality lookups, but a heavily skewed value can still match most rows.

For a tiny match set, an index probe can win. For a moderate scattered set, a bitmap strategy can reduce repeated heap-page access. For a large fraction, a sequential or columnar scan can win. There is no portable percentage threshold: row width, clustering, cache state, storage, ordering, and covering all matter. `ORDER BY ... LIMIT` can favor an ordered index even when the predicate itself is broad.

### Index maintenance and write amplification

Each relevant insert, update, or delete can maintain extra index entries, generate log records, dirty pages, and consume replication or compaction work. Wide and overlapping indexes increase storage and cache pressure. Updates can cause page splits or dead entries; maintenance differs by engine.

Measure index use and write cost across a representative workload before adding or removing an index. A rarely used index may still enforce uniqueness or support a critical monthly job. Bulk index creation can lock or consume substantial resources; online/concurrent options have their own restrictions. Do not automatically add one index per filtered column.

### Columnar storage, data skipping, and zone maps

Columnar storage groups values by column, supporting compression, vectorized processing, and projection pruning. It is effective for reading a few columns across many rows; point updates and row reconstruction have different costs from row storage.

**Zone maps** or similar block metadata summarize values, often min/max, so the engine can skip blocks outside a predicate's range. Clustering or sorting narrows those ranges. If every block contains dates from January through December, a July filter skips little despite good SQL. These summaries are lossy exclusion aids, not exact row-level lookup indexes. NULL metadata, bloom filters, and file statistics may add other skipping mechanisms depending on the format and engine.

**Interview check:** Explain why a 20-billion-row date-correlated table might benefit from BRIN or columnar block skipping while a customer point lookup might need a B-Tree.

## 6. Joins & Aggregations

### Join algorithms and complexity

Let `N` and `M` be input row counts and `K` output rows. The simplified costs below exclude disk/network effects and assume ordinary equality joins where stated:

| Algorithm | Approximate work | Good conditions and risks |
| --- | --- | --- |
| Naive nested loop | `O(N × M + K)` | Small inputs; general join predicates |
| Indexed nested loop | Roughly `O(N log M + K)` | Small filtered outer input and efficient inner probes; random I/O can dominate |
| Hash join | Expected `O(N + M + K)` | Equijoins and a manageable build side; hashing, skew, and spill can add cost |
| Merge join | `O(N + M + K)` after ordering | Inputs already sorted; otherwise add sorting costs |

For unsorted inputs, merge join may add `O(N log N + M log M)` sorting. Hash joins need memory proportional to the build-side representation; it is often larger than compressed storage size. No algorithm avoids producing `K` output rows when the query requires them. Large duplicate-key groups can make `K` dominate all other costs.

### Broadcast vs. repartitioned joins in MPP engines

A **broadcast** join sends the filtered build input to every participating worker. It avoids moving the large probe side, but replicates memory and network work. With `W` workers and build size `B`, transfer is roughly proportional to `W × B`, subject to topology and implementation.

A **repartitioned** join hashes inputs on compatible join keys so matching rows meet on the same worker. When neither side is already correctly placed, moving both inputs costs roughly their combined bytes. Existing compatible distribution can eliminate one or both moves. A small compressed dimension may still be too large to broadcast after hash-table expansion.

Choose based on post-filter size, memory per worker, worker count, skew, and concurrency. Broadcasting a small dimension can be excellent; broadcasting a misestimated large input can fail many workers simultaneously. Distribution choice is separate from the local join algorithm.

### Hash vs. sort-based aggregation

Hash aggregation maintains one state per group, with expected linear input processing. Memory grows with group count, key width, and aggregate state; a nearly unique grouping key can be expensive. Sorted aggregation can stream one group at a time from ordered input, but sorting unordered input costs CPU, memory, and possible spill.

MPP engines often use local partial aggregation, exchange, then final aggregation. `SUM` and `COUNT` combine naturally. `AVG` combines as sum/count, not the average of worker averages. Exact `COUNT(DISTINCT)` can require large deduplication state and additional exchange; approximate sketches need a supported merge function and a known error contract.

### DISTINCT vs. GROUP BY

`SELECT DISTINCT customer_id FROM orders` and `SELECT customer_id FROM orders GROUP BY customer_id` return the same set of customer IDs. The engine may use the same physical operations; neither spelling is automatically faster. `GROUP BY` additionally expresses aggregate calculations, while `DISTINCT` removes duplicate projected rows.

`COUNT(DISTINCT customer_id)` counts non-NULL IDs. `SELECT DISTINCT customer_id` can emit one NULL row. Applying `DISTINCT` after a faulty join can hide duplicated entities while leaving inflated sums unchanged; fix the join's grain instead.

### Join skew and cardinality explosion

If a key occurs `a` times on the left and `b` times on the right, an equijoin emits `a × b` matching pairs for that key. Joining orders to both order lines and payments can multiply line rows by payment rows. Summing the order amount afterward overstates revenue.

**Skew** means uneven work distribution, often from a hot customer, a default key, or many NULLs routed to one aggregation partition. Inspect maximum worker rows/time, not only averages. SQL equality does not match NULL join keys, but NULL group keys can still form a huge aggregation group.

Possible remedies include filtering invalid keys, handling hot keys separately, pre-aggregation, or broadcasting a genuinely small side. Salting distributes a hot key, but the other side and final recombination must preserve matches and prevent duplication. It is a targeted rewrite with correctness costs, not the first default.

### Pre-aggregation and semi-join optimization

When a report needs customer totals, aggregate each many-side relation to customer grain before joining:

```sql
WITH order_totals AS (
    SELECT customer_id, SUM(amount) AS ordered_amount
    FROM orders
    GROUP BY customer_id
), payment_totals AS (
    SELECT customer_id, SUM(amount) AS paid_amount
    FROM payments
    GROUP BY customer_id
)
SELECT c.customer_id, o.ordered_amount, p.paid_amount
FROM customers AS c
LEFT JOIN order_totals AS o ON o.customer_id = c.customer_id
LEFT JOIN payment_totals AS p ON p.customer_id = c.customer_id;
```

Here `payments(customer_id, amount)` contains individual payment rows. Each CTE contributes at most one row per customer, preventing a cross-product between orders and payments. Preserve every dimension required by later filters or grouping; aggregating away product before computing product revenue is invalid. Keep sums/counts when later calculating weighted averages.

If only customer eligibility matters, `EXISTS` avoids bringing order detail into the result. Optimizers can implement it with hash, merge, or nested-loop semi-joins, or dynamic filters in some distributed engines. Benefit depends on how much data can actually be excluded.

**Interview check:** A report doubles revenue after joining a new table. State each input's grain, check per-key multiplicity, and explain why a final `DISTINCT` is insufficient.

## 7. Transactions & Concurrency

### Transaction guarantees to recall

The definitions of ACID, MVCC, isolation levels, anomalies, and locking live in [Database engine internals](database.md#4-mvcc-transaction-isolation-and-locking). Use this section to apply them in SQL: choose transaction boundaries, prevent stale writes, and handle concurrent updates.

Before writing a transaction, state the invariant and the snapshot it requires. A multi-row invariant needs protection across all relevant rows; locking only the row being updated can still allow write skew.

### Locks, deadlocks, and optimistic concurrency

When diagnosing blocked SQL, inspect the wait graph and transaction age. Keep transactions short and acquire shared resources in a consistent order. Indexes can affect the amount of locked work, but the behavior depends on the engine.

**Optimistic concurrency** verifies that state has not changed since the client read it. Assume `documents(document_id, body, version)`:

```sql
UPDATE documents
SET body = $1, version = version + 1
WHERE document_id = $2 AND version = $3;
```

The application must check the affected-row count. Zero means the expected version was absent or stale; re-read and resolve the conflict. Use bounded retries for retryable failures, retry the whole transaction after a serialization failure, and preserve idempotency for any external effects.

### Transaction boundaries and atomic updates

An application-side read, check, then unconditional write creates a race. Express a single-row invariant in one conditional statement when possible:

```sql
UPDATE inventory
SET available = available - $1
WHERE product_id = $2 AND available >= $1
RETURNING available;
```

This assumes one inventory row per product, non-NULL nonnegative inventory, and a validated positive quantity. Exactly one returned row indicates success; no row means missing product or insufficient stock. A database constraint should also enforce the inventory invariant. The conditional update avoids a stale application read; stricter isolation may still surface a retryable conflict.

When one business operation changes multiple tables, put the required writes in one transaction. Commit a pipeline checkpoint with the corresponding target changes when they share a transactional store. Across separate systems, use an explicit protocol such as a transactional outbox plus idempotent consumption; a SQL transaction alone does not make an external message publication atomic.

### MERGE, UPSERT, and concurrency conflicts

**UPSERT** means insert a new key or update its existing row. **MERGE** matches source and target and applies conditional actions. Neither spelling automatically makes an incremental pipeline correctly ordered, deduplicated, or safe under arbitrary concurrent writers.

PostgreSQL `INSERT ... ON CONFLICT DO UPDATE` uses a unique arbiter to handle competing inserts. PostgreSQL `MERGE` has different concurrent-insert behavior and can raise a uniqueness violation rather than take the update action. Do not treat the statements as interchangeable. [PostgreSQL MERGE notes](https://www.postgresql.org/docs/18/sql-merge.html).

Before either operation, ensure one relevant source row per target key or a deliberately defined action sequence. Multiple source matches can cause an error or engine-specific nondeterminism. Check the engine's target-key enforcement, supported clauses, isolation, and retry behavior. An atomic statement protects its own changes; it does not validate stale source versions or prevent an unrelated writer from violating the pipeline's ownership assumptions.

**Interview check:** Explain why “MVCC means no locks” and “MERGE guarantees exactly once” are both incorrect.

## 8. Data Modeling & Incremental SQL

### Star schemas, facts, dimensions, and SCD Type 2

A **fact table** records a business process at a declared grain, such as one order line. **Dimensions** hold descriptive context such as customer or product attributes. A **star schema** links facts to dimensions, often through surrogate keys, simplifying repeated analytical joins.

Declare the grain before choosing keys and measures. Transaction facts record events; periodic snapshots record state at intervals; accumulating snapshots track lifecycle milestones. Revenue is generally additive across appropriate dimensions; account balance is often not additive across time; ratios need their underlying numerator and denominator.

**Slowly changing dimension Type 2 (SCD2)** preserves attribute versions. A customer dimension might contain `customer_sk`, `customer_id`, `region`, `valid_from`, and `valid_to`, where NULL `valid_to` marks the open version. Use non-overlapping half-open validity intervals:

```sql
SELECT f.order_id, d.customer_sk, d.region
FROM orders AS f
JOIN dim_customer AS d
  ON d.customer_id = f.customer_id
 AND f.ordered_at >= d.valid_from
 AND (f.ordered_at < d.valid_to OR d.valid_to IS NULL);
```

Joining by natural key alone multiplies facts by every historical version; joining only the current dimension rewrites history. Validate exactly one applicable version per fact, at most one open version per natural key, and no overlapping intervals. Close the old interval and insert the new one atomically under an appropriate concurrency protocol.

Distinguish **business effective time** from **system arrival time**. A correction learned today may be effective last month. One validity timeline cannot answer every “what did we believe then?” question; bitemporal modeling adds that capability when required.

### Deduplication and deterministic latest-record selection

Deduplication requires an identity and a winner policy. `DISTINCT` removes identical projected rows; it does not decide which conflicting version of one business key is authoritative.

Assume `staged_changes` contains complete after-images with a non-NULL `order_id`, comparable non-NULL per-key `source_version`, unique stable `event_id`, operation `op`, and payload columns. Conflicting payloads for the same authoritative source version should be rejected or reconciled; a tie-breaker is not a substitute for a trustworthy source order.

```sql
WITH ranked AS (
    SELECT s.*,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY source_version DESC, event_id DESC
           ) AS rn
    FROM staged_changes AS s
)
SELECT order_id, customer_id, amount, source_version, event_id, op
FROM ranked
WHERE rn = 1;
```

The immutable event ID makes repeat selection deterministic for equal versions. Choose ordering from a source commit sequence, log position, or a documented composite sequence; wall-clock event time and ingestion time may disagree with commit order. For partial patches or arithmetic deltas, keeping only the last event can discard required changes: fold the ordered sequence or reconstruct full after-images first.

### Incremental loads and watermark-based processing

A **watermark** records progress through a source ordering. It can be an extraction timestamp, sequence, or log offset; it is distinct from the target's maximum business event time. A robust batch protocol is:

1. Read the last committed checkpoint and capture a stable upper bound from the source.
2. Extract a bounded interval using a stable snapshot/cursor contract, or a deliberate overlap for replay.
3. Validate keys, schema, ordering, and source completeness; deduplicate as required.
4. Apply changes with version guards and record the completed interval.
5. Advance the checkpoint only after durable target success; retry ambiguous outcomes idempotently.

A timestamp-only condition `updated_at > last_watermark` can miss equal-timestamp rows. A composite cursor such as `(updated_at, primary_key)` fixes ties only if the source guarantees visibility/order compatible with that cursor; it does not fix transactions committed later with older timestamps. Use CDC commit positions, a suitable snapshot protocol, overlap plus deduplication, and periodic reconciliation according to source guarantees.

For a half-open interval `[lower, upper)`, the next interval starts at `upper`. Do not move the checkpoint to the job's wall-clock finish time unless that is a valid source completeness boundary. Preserve enough source retention to replay after failures.

### CDC transformations and MERGE patterns

**Change data capture (CDC)** represents inserts, updates, and deletes. Preserve source identity, ordering, operation, and any transaction information required by the target contract. Detect log gaps, schema changes, and primary-key changes; the last may need deletion under the old key plus insertion under the new one.

The following PostgreSQL-style `MERGE` assumes `latest_changes` is the validated one-row-per-key result above. `order_state` is a current-state table with a unique non-NULL order key, a non-NULL source version, payload, and an `is_deleted` tombstone flag. Writers for an overlapping key range are serialized, and this source provides full after-images:

```sql
MERGE INTO order_state AS t
USING latest_changes AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.source_version > t.source_version THEN
    UPDATE SET customer_id = s.customer_id,
               amount = s.amount,
               source_version = s.source_version,
               is_deleted = (s.op = 'D')
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, amount, source_version, is_deleted)
    VALUES (s.order_id, s.customer_id, s.amount, s.source_version, s.op = 'D');
```

Validate `op` as a permitted non-NULL operation before applying the batch. Tombstone rows must permit whatever payload a delete carries; a key-only delete need not have customer or amount values. The version guard prevents an older or replayed event from overwriting a newer target version. Equal versions are no-ops only under the contract that one version determines one authoritative state.

A retained tombstone also prevents an old insert from resurrecting a physically deleted record. If tombstones must be purged, retain equivalent ordering metadata or prove old events cannot be replayed. Reads must exclude `is_deleted` rows where appropriate.

This example updates **current state**, not SCD2 history. Collapsing a batch to one latest row discards intermediate transitions needed for full history. SCD2 needs the ordered effective changes and interval reconstruction. Also, do not use `WHEN NOT MATCHED BY SOURCE THEN DELETE` against a partial incremental batch: absence from the batch does not establish deletion from the source.

### Late-arriving data and historical corrections

Late data can be a new old event, a correction to an old fact, or an old version arriving after a newer version. These need different handling: accept a previously unseen fact, recompute affected derived results, or reject a stale state update by source ordering.

A bounded lookback catches lateness only within that bound. Keep a backfill path for older corrections and monitor their frequency. Reprocess affected partitions and downstream dependencies, including windows whose frame crosses partition boundaries. If an order moves from one business date to another, correct both the old and new date aggregates.

For a late SCD2 change, split or adjust the affected historical intervals and revisit facts assigned to those intervals. A late dimension can use a controlled unknown member followed by restatement; joining only to today's dimension loses historical meaning. Coordinate backfills with live writers so one cannot overwrite newer results.

### Idempotency and reconciliation

An **idempotent** load reaches the same intended target state when replayed. Stable keys, deterministic transforms, immutable batch inputs, version checks, and atomic publication/checkpointing support it. `INSERT ... SELECT` without uniqueness or replacement semantics usually appends duplicates on retry. `amount = amount + delta` also needs event-level deduplication; replaying a delta twice changes state twice.

For partition replacement, build the complete authoritative partition and publish it atomically using the engine's supported mechanism. Delete-then-insert in separate commits exposes partial or missing data and creates failure windows.

**Reconciliation** checks source and target at aligned boundaries: key counts, missing/extra keys, duplicate keys, deletion counts, sums by partition, and source-version coverage. Total row counts alone cannot detect offsetting missing and extra rows. Checksums need consistent NULL/type/order rules and are evidence rather than an absolute collision-free proof. Compare full rows or keys for discrepancies, and record checkpoint, batch ID, counts, and failure state for recovery.

**Interview check:** Design a load that survives a timeout after commit, a delete followed by an older insert, a source update with a tied timestamp, and a correction outside the normal lookback.

## 9. Distributed SQL & MPP

### Distributed execution and exchange operators

**Massively parallel processing (MPP)** divides query work among workers. A coordinator plans stages/fragments, schedules work, and collects results. Local scans and partial aggregates run in parallel; exchange operators connect stages where data must change placement.

| Exchange | Purpose | Typical cost |
| --- | --- | --- |
| Gather | Bring distributed rows to fewer workers or a coordinator | Bottleneck for large final results or serial work |
| Repartition / shuffle | Route rows by a hash or range of keys | Network, serialization, buffers, and skew |
| Broadcast / replicate | Copy one input to workers | Repeated transfer and replicated memory |
| Merge ordered streams | Combine compatible ordered inputs | Coordination and maintaining ordering |

A logical `GROUP BY customer_id` can become local aggregation → hash exchange on customer ID → final aggregation. A global `ORDER BY` or single huge window partition may impose a less parallel stage. Scanning twice as fast does little if most runtime is spent exchanging or serially processing the result.

### Partitioning, locality, pruning, clustering, and sorting

**Storage partitioning** divides data into physical units, often by date. **Execution partitioning** distributes rows among workers, often by hash. Filtering one date partition does not guarantee that a subsequent customer join is local.

**Data locality** means required data is available where computation runs. Compatible hash distribution on join keys can enable colocated joins. Shared object storage separates compute from durable placement; local caches can still reduce remote reads. Broadcast supplies locality by replication, with the costs described earlier.

Partition pruning skips whole storage partitions; clustering and sorting improve block-level skipping and sometimes ordered processing inside them. They are complementary. Excessive tiny partitions/files add metadata and scheduling overhead; a poor distribution key creates skew. Choose layout for common access patterns and measure pruning, not just the existence of a partition or clustering declaration.

### Memory spilling, parallelism, and data skew

When a sort, hash join, or aggregate exceeds its memory budget, an engine may spill intermediate data to disk or remote storage. Spill preserves execution where supported but adds I/O and serialization; some operators/configurations fail rather than spill. Input compression is not a reliable estimate of in-memory state size.

More workers can reduce per-worker input, but adds exchanges and scheduling, and may replicate broadcast state more widely. A single hot key remains concentrated unless the algorithm can split that work. High query concurrency divides shared capacity and increases memory pressure. Compare maximum vs. median task time, spill by stage, blocked time, and bytes moved before scaling.

### BigQuery, Snowflake, Redshift, and Trino differences

These are broad execution distinctions, not interchangeable tuning recipes:

| Engine | Execution and placement | Main profiling focus |
| --- | --- | --- |
| BigQuery | Managed distributed execution through stages and slots; partitioned/clustered storage can reduce scans | Execution graph, stage input/output, shuffle bytes/spill, slot time, and wait/compute/read/write timing |
| Snowflake | Virtual warehouses process shared columnar storage; micro-partition metadata supports pruning | Query Profile operators, rows, partitions scanned, local/remote spill, and queue/execution time |
| Amazon Redshift | MPP execution on columnar tables; distribution and sort design influence locality and scan work | `EXPLAIN` redistribution/broadcast operators, runtime step statistics, skew, and spill |
| Trino | Distributed stages/tasks query connector-backed sources; statistics and pushdown capabilities vary by connector | Distributed plan, exchanges, input/output rows, CPU/blocked time, memory, and connector pushdown |

BigQuery can change its stage graph during execution, so inspect runtime stage information rather than assuming a fixed tree. Slot time sums resource use across parallel work and is not wall-clock latency. [BigQuery query plan and timeline](https://cloud.google.com/bigquery/docs/query-plan-explanation).

For Snowflake, distinguish queued, compilation, and execution time before resizing a warehouse. Query Profile shows which operators consume work and whether scanning, spilling, or row expansion dominates. [Snowflake execution analysis](https://docs.snowflake.com/en/user-guide/performance-query-exploring), [Query History and profiles](https://docs.snowflake.com/en/user-guide/ui-snowsight-activity).

Redshift's estimated plan does not include all parallel runtime detail. Compare planned data movement with actual step statistics after execution; sort/distribution choices matter alongside row estimates. A scan in this columnar warehouse should not be diagnosed using OLTP B-Tree assumptions. [Redshift query plans](https://docs.aws.amazon.com/redshift/latest/dg/c-the-query-plan.html).

Trino uses connector-provided statistics for cost-based join order and distribution selection. Broadcast requires the build input on each participating worker; partitioning spreads build state. A missing statistic or unsupported pushdown can change behavior substantially. [Trino cost-based optimization](https://trino.io/docs/current/optimizer/cost-based-optimizations.html).

**Interview check:** A distributed join has low average CPU and one worker running ten times longer than the others. Explain why skew, blocked exchanges, or spill may fit better than “add more parallelism.”

## 10. Performance & Production

### CPU, memory, I/O, and network bottlenecks

| Bottleneck | Evidence | Possible response after confirming cause |
| --- | --- | --- |
| CPU | High CPU time; expensive expressions, sorts, hashing, or decompression | Reduce rows/columns and repeated work; improve algorithm or compute capacity |
| Memory | Large hash/aggregate state, spill, allocation pressure, failures | Reduce build/state size; pre-aggregate; adjust layout or memory within workload limits |
| Storage I/O | Large scans, poor skipping, random fetches, temporary-file I/O | Improve access paths, pruning, projection, or locality |
| Network | Heavy redistribution, remote reads, blocked exchange stages | Reduce transferred data, colocate compatible keys, use a suitable join distribution |
| Locking or queues | Long wait time before little actual execution | Identify blockers, transaction scope, admission limits, and workload contention |

Low CPU does not imply spare useful capacity: a query can wait on disk, network, another worker, a lock, or admission to a queue. CPU time across workers can exceed elapsed time. A bottleneck can migrate after one stage is improved.

### Query profiling and runtime statistics

Use a repeatable diagnostic sequence:

1. Capture the exact query, typed parameters, engine/version, data boundary, and end-to-end latency.
2. Separate queue, planning/compilation, execution, and result transfer time.
3. Inspect the critical path, actual vs. estimated rows, worker imbalance, reads, exchanges, and spill.
4. Form one explanation and make the smallest relevant change.
5. Compare correctness, latency distribution, resource use, and cost under comparable conditions.

In PostgreSQL, plans plus buffer statistics explain individual executions, while facilities such as `pg_stat_statements`, when installed, summarize recurring workloads. Service query histories provide analogous evidence with different fields. Aggregated query statistics can hide parameter-specific outliers, so connect a slow execution to its actual plan and values. Avoid logging sensitive literal values indiscriminately.

### Materialized views and result caching

A **materialized view** stores a query result for reuse. It can reduce repeated joins and aggregation, at the cost of storage, refresh work, and potential staleness. Refresh can be full or incremental depending on the engine and definition; not every view supports incremental maintenance or automatic query rewrite.

A **result cache** reuses a prior query's answer under engine-specific validity rules. It differs from a buffer or local data cache, which can speed reading data while still executing the query. Schema/data changes, session context, access policies, nondeterministic expressions, and engine rules can affect eligibility. Do not infer uncached production performance from a cached benchmark. Define freshness and invalidation expectations before depending on either mechanism.

### Pagination: OFFSET vs. keyset

`ORDER BY ordered_at, order_id OFFSET 1000000 LIMIT 100` typically requires locating and discarding many earlier rows. Under changing data, separate pages can also skip or repeat rows. Always use a total ordering, including a unique tie-breaker.

**Keyset pagination** continues after the last returned ordering key:

```sql
SELECT order_id, ordered_at, amount
FROM orders
WHERE (ordered_at, order_id) > ($1, $2)
ORDER BY ordered_at, order_id
LIMIT 100;
```

Here parameters are the previous page's final non-NULL timestamp and ID, and a suitable `(ordered_at, order_id)` index supports access. Row-value comparisons are dialect-dependent; an equivalent lexicographic condition is `ordered_at > $1 OR (ordered_at = $1 AND order_id > $2)`. Reverse comparisons for descending traversal, accounting for mixed sort directions if present.

Keyset access avoids deep offsets but does not support arbitrary page-number jumps naturally. It also does not create a snapshot: concurrent inserts before the cursor may be missed, and changes to ordering keys can move rows across page boundaries. Use a stable snapshot or export boundary for consistent extraction, and define nullable-key behavior explicitly.

### Parameterized queries and SQL injection prevention

Bind values through the driver rather than interpolating user input into SQL text. Parameterization separates data from SQL syntax and can support plan reuse. Placeholder syntax differs: PostgreSQL's server-style examples use `$1`, whereas client libraries may use other forms.

Parameters represent values, not arbitrary identifiers, operators, or sort directions. If a user can choose a column or ordering, map that choice through a fixed allowlist to known SQL fragments. Use the driver's identifier-quoting facility where dynamic identifiers are necessary, and authorize access separately. Stored procedures and ORMs are not automatically safe if they concatenate untrusted SQL internally. Least-privilege database roles limit exposure if application validation fails.

Parameterization is also a performance consideration: a plan chosen for a common value can be poor for a rare one, or vice versa. Preserve injection protection while diagnosing parameter-sensitive planning; embedding user literals is not an acceptable tuning fix.

### Workload concurrency and resource management

Optimize the workload, not just one query in isolation. A larger hash-memory budget can speed one query while causing many concurrent queries to exhaust memory. Per-operation budgets can multiply across plan nodes and workers. More simultaneous queries can increase throughput until CPU, memory, I/O, or network saturates, after which queuing and latency grow.

Use the engine's admission controls, resource groups/queues, priorities, workload isolation, statement timeouts, and capacity policies. Keep short operational queries from competing blindly with large backfills. Distinguish scaling up one query's resources from scaling out for more concurrent queries, and measure cost as well as latency. Cancelled jobs and retries should leave checkpoints and published data in a recoverable state.

### Benchmarking and query regression testing

A meaningful benchmark fixes or records data snapshot, scale, skew, NULL rates, engine version, parameters, layout, statistics, compute capacity, concurrency, and cache state. Include hot and rare keys, large groups, empty results, and representative peak loads. Use repeated runs, report median and tail latency, and include bytes scanned, CPU/slot time, spill, throughput, and monetary cost where relevant.

Correctness comes before speed. Compare rows as **multisets** when duplicates matter; ordinary `EXCEPT` can miss multiplicity errors. Use `EXCEPT ALL` where available or compare grouped row counts with NULL-safe equality. Ignore row order only when no ordering contract exists. For money use exact comparison; for floating-point aggregates define an appropriate tolerance and account for changed parallel summation order.

Regression cases should exercise the failure modes introduced by a change: tied window keys, NULL-sensitive anti-joins, many-to-many joins, duplicate CDC events, older updates after deletes, empty groups, and retry behavior. Concurrency claims need concurrent sessions; a single-session query cannot prove an isolation guarantee.

Treat plan shape as diagnostic evidence rather than a universal assertion. A different plan can be faster and correct as data grows. Track operational bounds such as p95 latency, cost, scan volume, spill, and reconciliation failures, with enough context to explain changes.

### Production interview scenarios

| Prompt | What a strong answer establishes |
| --- | --- |
| “The same query is suddenly slower.” | Same parameters and data? Changed estimates, plan, cache, queue, pruning, or distribution? |
| “Revenue increased after adding a join.” | Intended grain, per-key match counts, filter placement, and aggregate multiplicity |
| “A job was retried and doubled totals.” | Stable event identity, atomic publication, checkpoint timing, and delta deduplication |
| “The merge sometimes fails on duplicate keys.” | Source uniqueness, target constraints, concurrent writers, and engine conflict behavior |
| “One partition is much slower.” | Data size, hot keys, worker skew, file layout, and spill rather than only average runtime |
| “Retention looks poor for recent users.” | Cohort denominator, activity deduplication, and incomplete observation periods |
| “A deleted record reappeared.” | Out-of-order replay, tombstone/version retention, and full vs. incremental source semantics |

**Answer pattern:** State the required result and invariants → describe the relevant execution mechanism → identify evidence that would confirm the cause → propose the smallest valid change → explain how to test correctness and workload impact.
