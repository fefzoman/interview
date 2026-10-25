# Data Engineer Interview Preparation

Interview guides grouped by subject. Start with foundations, then follow the topic tree below. Choose cloud and platform guides to match your target role; study AI when relevant to the interview.

## Topic tree

- **foundations/**
  - [Core engineering concepts](foundations/core.md)
  - [Python](foundations/python.md)
  - [SQL](foundations/sql.md)
  - [Database engine internals](foundations/database.md)
- **ingestion/**
  - [Data ingestion patterns](ingestion/ingestion.md)
  - [Replication and CDC](ingestion/replication.md)
  - [Kafka](ingestion/kafka.md)
- **processing/**
  - [Spark](processing/spark.md)
  - [Flink and stream processing](processing/flink.md)
- **storage-and-modeling/**
  - [Data warehousing and modeling](storage-and-modeling/dwh.md)
  - [Iceberg](storage-and-modeling/iceberg.md)
  - [ClickHouse](storage-and-modeling/clickhouse.md)
  - [MongoDB](storage-and-modeling/mongodb.md)
  - [Semantic layer and metrics](storage-and-modeling/semantic-layer.md)
- **pipelines/**
  - [Airflow orchestration](pipelines/airflow.md)
  - [dbt transformations](pipelines/dbt.md)
- **quality-and-governance/**
  - [Data contracts and schemas](quality-and-governance/data-contracts.md)
  - [Data quality and observability](quality-and-governance/data-quality.md)
  - [Data platform testing](quality-and-governance/testing.md)
  - [Governance and security](quality-and-governance/governance.md)
- **cloud-and-platforms/**
  - [AWS](cloud-and-platforms/aws.md)
  - [GCP](cloud-and-platforms/gcp.md)
  - [Databricks](cloud-and-platforms/databricks.md)
  - [Snowflake](cloud-and-platforms/snowflake.md)
- **system-design/**
  - [Data platform system design](system-design/data-platform-system-design.md)
  - [Cost engineering and FinOps](system-design/finops.md)
- **ai/**
  - [AI data engineering](ai/ai.md)

## Recall flow before an interview

Follow one dataset from source to consumer. At each step, recall the concepts without opening the guide, explain a trade-off and a failure scenario, then review only what you missed.

1. **Foundations:** solve a SQL query and a short Python task; recall joins, window functions, indexes, transactions, and distributed systems basics.
2. **Ingestion:** explain batch, streaming, and CDC; trace offsets, retries, deduplication, schema changes, and replay.
3. **Processing:** walk through a Spark or Flink job; recall partitioning, shuffles, skew, state, watermarks, and recovery.
4. **Storage and modeling:** choose a warehouse or lakehouse layout; explain grain, facts and dimensions, partitioning, table formats, and query performance.
5. **Pipelines:** connect ingestion, transformation, and serving; explain dependencies, incremental runs, idempotency, backfills, and orchestration.
6. **Quality and governance:** explain how you validate results, detect failures, enforce contracts, trace lineage, and protect sensitive data.
7. **Cloud and platforms:** map the flow to the stack in the job description; recall service choices, operational limits, and cost drivers. Review AI topics if the role requires them.
8. **System design:** sketch the complete flow from memory; state volume, freshness, reliability, and cost requirements, then defend your choices.
9. **Final recall:** revisit the gaps you noted and rehearse project examples covering a design decision, a production incident, and a measurable improvement.
