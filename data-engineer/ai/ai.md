# AI Data Engineering: Data Engineer Interview Guide

AI data engineering builds the pipelines, indexes, metadata, and controls that let AI systems use reliable data. In an interview, connect each model or framework choice to data freshness, retrieval quality, permissions, failure recovery, latency, and cost.

**Scope:** Platform-neutral concepts and production design trade-offs. Framework descriptions follow their official documentation; APIs and capabilities depend on the installed versions and integrations. Numbers below illustrate sizing and evaluation rather than prescribe production settings. This guide contains no executable application or provider-specific pricing.

**Running example:** A data catalog assistant answers questions about datasets, metric definitions, ownership, and pipeline failures. It ingests documentation, schemas, transformation definitions, and lineage events. Users should see only authorized information, answers should cite evidence, and any proposed pipeline change must pass ordinary engineering checks.

## Contents

1. [LLM Fundamentals, Embeddings, and Tokenization](#1-llm-fundamentals-embeddings-and-tokenization)
2. [RAG Architectures, Ingestion, and Chunking](#2-rag-architectures-ingestion-and-chunking)
3. [Vector Databases, ANN Indexes, and Similarity Search](#3-vector-databases-ann-indexes-and-similarity-search)
4. [Hybrid Search, Reranking, and Retrieval Evaluation](#4-hybrid-search-reranking-and-retrieval-evaluation)
5. [Knowledge Graphs, GraphRAG, and Metadata Discovery](#5-knowledge-graphs-graphrag-and-metadata-discovery)
6. [LangChain, LangGraph, LlamaIndex, and MCP](#6-langchain-langgraph-llamaindex-and-mcp)
7. [AI Agents for Migration, Data Quality, and Lineage](#7-ai-agents-for-migration-data-quality-and-lineage)
8. [LLM Evaluation, Observability, Security, and Cost Optimization](#8-llm-evaluation-observability-security-and-cost-optimization)

## 1. LLM Fundamentals, Embeddings, and Tokenization

### What an LLM learns

A **large language model**, or LLM, learns statistical patterns from training data. Many generative LLMs use a decoder-only Transformer trained to predict the next token from preceding tokens. Repeated next-token prediction generates a response; it does not perform an authoritative database lookup.

**Self-attention** lets a token's representation depend on other tokens. A causal mask prevents a decoder from attending to future tokens during next-token training. Positional information represents token order. The Transformer introduced attention-based sequence modeling; modern model architectures vary. [Attention Is All You Need](https://arxiv.org/abs/1706.03762).

Separate three stages:

| Stage | What changes | Data engineering concern |
| --- | --- | --- |
| Pretraining | Model parameters learn broad patterns | Corpus provenance, deduplication, quality, licensing, and leakage |
| Fine-tuning or adaptation | Parameters or adapters learn task behavior | Curated examples, train/test separation, versioning, and regression evaluation |
| Inference | A deployed model processes a request | Context construction, retrieval, authorization, latency, and cost |

Instruction tuning and preference optimization can improve usefulness, but an apparently confident answer can still be wrong. **Hallucination** includes unsupported facts, invented citations, and plausible but nonexistent table names.

### Tokenization and context budgets

A **tokenizer** converts text into token IDs. Tokens may represent words, word fragments, punctuation, bytes, or special markers. Byte-pair encoding, WordPiece, and Unigram are common tokenizer families. A token is not reliably one word or a fixed number of characters, especially across languages, code, and identifiers. [Hugging Face tokenizer overview](https://huggingface.co/learn/llm-course/en/chapter6/1).

Tokenization affects:

- **Capacity:** Prompts, retrieved passages, conversation history, tool definitions, and generated output consume model budgets.
- **Cost:** Providers or serving systems may account for input, output, cached, or other token categories differently.
- **Chunking:** Character-based splitting can produce uneven token lengths or exceed an embedding model's input limit.
- **Identifiers:** A long table path or error code may split into many tokens while requiring exact matching in search.

For a simplified model with a shared 32,000-token context budget, reserving 2,000 tokens for instructions, 3,000 for conversation and tool overhead, and 3,000 for output leaves at most 24,000 for retrieved content. Actual models may have separate limits or additional accounting; count with the relevant tokenizer and leave headroom.

A larger context window does not guarantee that every relevant fact is used correctly. Long-context evaluations have shown sensitivity to the position of evidence. Test your model and workload rather than assuming that adding documents improves answers. [Lost in the Middle](https://arxiv.org/abs/2307.03172).

### Embeddings and representation spaces

An **embedding** maps an input to a numeric vector. Retrieval embeddings are trained so useful query-document pairs receive favorable similarity scores. They support semantic search, clustering, deduplication, and candidate generation. Sentence-BERT illustrates efficient sentence representations that can be compared without jointly encoding every pair. [Sentence-BERT](https://arxiv.org/abs/1908.10084).

Distinguish:

- **Token embeddings:** Internal representations associated with tokens in a model.
- **Document or chunk embeddings:** Fixed-size representations used to search a collection.
- **Query embeddings:** Representations produced by the compatible query encoder, sometimes with a query-specific prefix or instruction.
- **Sparse representations:** Weighted vocabulary features; learned sparse retrieval differs from both dense vectors and conventional keyword scoring.

Use compatible query and document encoders, their required preprocessing, and the intended similarity metric. Equal vector dimensions do not establish compatibility. An English general-purpose embedding model may perform poorly on multilingual runbooks or SQL identifiers.

Version the embedding model, dimensions, preprocessing, and chunking configuration with each index generation. A model change normally requires re-embedding documents and routing queries to a matching index. Compare old and new retrieval on held-out queries before switching traffic.

### Generation settings and structured output

**Temperature** adjusts a sampling distribution; lower values usually reduce variation. **Top-p** limits sampling to a probability mass. Neither setting guarantees factual correctness, and temperature zero is not a general reproducibility guarantee across serving environments.

Structured output can constrain responses to a schema, such as a dataset recommendation with a dataset ID and evidence references. Schema validity does not prove that the dataset exists, that evidence supports the recommendation, or that the user may access it. Validate those properties outside the model.

### RAG versus fine-tuning

| Need | Typical starting point | Reason |
| --- | --- | --- |
| Frequently changing private facts | Retrieval-augmented generation | Facts can change without updating model weights |
| Consistent formatting or specialized task behavior | Prompting, then fine-tuning if evaluation justifies it | Training examples can shape behavior |
| Exact current revenue or row counts | Authorized SQL or another deterministic tool | Retrieve authoritative values rather than estimate them from prose |
| Evidence and citations | Retrieval with source tracking | Connect claims to documents and versions |

RAG and fine-tuning can be combined. Fine-tuning is not a dependable replacement for an access-controlled, current source of truth.

**Interview question:** Why not embed an entire warehouse table as one vector? An embedding loses row-level detail and exact arithmetic, the input may exceed model limits, and updates become expensive. Embed useful descriptions or suitable records for discovery; query actual data through authorized tools when exact results matter.

## 2. RAG Architectures, Ingestion, and Chunking

### End-to-end architecture

**Retrieval-augmented generation**, or RAG, retrieves external evidence and supplies it to a generator. Retrieval can use keyword search, vectors, structured queries, or graphs. A vector database is one implementation option. The original RAG work combined a retriever with a generative model for knowledge-intensive tasks. [RAG paper](https://arxiv.org/abs/2005.11401).

```text
Ingestion:
Sources -> extract -> validate -> normalize -> chunk -> embed -> publish index
                |                  |                    |
          source versions     metadata + ACLs       index manifest

Serving:
Authenticated question -> resolve authorization scope -> retrieve candidates
 -> fuse/rerank -> assemble authorized evidence -> generate -> check citations
 -> answer with sources, or abstain
```

Keep ingestion and serving independently scalable. A model provider outage should not lose source changes; an indexing backlog should have a measurable freshness impact.

### Ingestion is a data pipeline

Treat source acquisition as ordinary production ingestion:

1. Discover changes through snapshots, CDC, events, or incremental polling, depending on the source.
2. Record durable source identifiers, versions, timestamps, and deletion events.
3. Parse documents while preserving headings, tables, page references, and code structure where relevant.
4. Validate extraction quality; quarantine corrupt files, empty OCR, and unsupported content.
5. Normalize text without removing meaningful syntax, units, or identifiers.
6. Attach trusted metadata and authorization information before publishing searchable records.
7. Produce embeddings in bounded batches with retries, rate-limit handling, and progress tracking.
8. Reconcile the published index against source manifests and expose freshness metrics.

PDF extraction is not solved by receiving a successful HTTP response. A table may become unreadable text, an image may need OCR, and a heading may be detached from its section. Track both ingestion success and useful extracted content.

### Chunking strategies and trade-offs

A **chunk** is the retrieval unit. Choose boundaries that keep a useful fact and its qualifiers together.

| Strategy | Useful for | Main limitation |
| --- | --- | --- |
| Fixed token windows | A simple, measurable baseline | Can split definitions, code, and table rows |
| Recursive or structure-aware splitting | Documents with headings and paragraphs | Depends on extraction and document structure |
| Semantic boundary detection | Prose with shifting subjects | Extra processing; boundaries can vary with the model |
| Parent-child retrieval | Precise small-chunk matching with broader answer context | Parent expansion consumes context and must preserve permissions |
| Record or entity chunks | Catalog objects, tickets, or product entries | Records may need linked context to make sense |

Overlap can preserve boundary context but increases embedding work and duplicate evidence. For example, 500-token windows advancing by 400 tokens have about 25% overhead for a long document, before edge effects: `500 / 400 = 1.25`. These are trial values, not recommended defaults.

For a metric definition, keep its formula, grain, units, exclusions, owner, and validity period together. For SQL, preserve a complete logical unit and its source identifiers where possible. Tune chunking against representative questions, not visual neatness.

### Metadata, provenance, and reproducibility

A searchable chunk should carry enough information to locate, interpret, authorize, and rebuild it:

```text
document_id, source_version, chunk_id, source_uri, section_or_page
content_hash, source_updated_at, ingested_at
tenant_id, authorization_scope, classification
parser_version, chunking_version, embedding_model, index_generation
```

This is an illustrative record, not a mandatory universal schema. Prefer stable catalog IDs over names that can be reused. Distinguish source modification time from ingestion time so a recently indexed stale document does not look current.

A source-to-chunk manifest supports deduplication, deletes, and audits. A content hash helps skip unchanged content, but identical text can occur under different owners or permissions. Do not merge records in a way that broadens visibility.

### Updates, deletes, and index publication

Use idempotent writes keyed by source identity, version, chunk identity, and index generation as appropriate. Version ordering or conditional writes must prevent an old retried job from overwriting a newer publication.

When chunk boundaries change, upserting new chunks alone leaves obsolete chunks searchable. Reconcile the full chunk set for that source version and remove superseded records. Handle source deletes and permission revocations across lexical indexes, vector indexes, graph projections, and caches.

For a full rebuild, construct and validate a new generation, then switch a routing pointer or alias if the storage system supports it. Keep a rollback path. Multiple stores rarely share one transaction, so use publication manifests, reconciliation, and authorization checks rather than assuming a globally atomic update.

Deletion lag must not become an authorization bypass. Enforce current policy at retrieval and before evidence reaches a model or external reranker, even if physical cleanup is asynchronous.

### RAG architecture variants

| Variant | Behavior | Cost or failure concern |
| --- | --- | --- |
| Basic RAG | Retrieve once, generate once | Can miss ambiguous queries or evidence distributed across sources |
| Query rewriting or expansion | Reformulate a question or generate several searches | Added calls; may change intent or introduce unsupported assumptions |
| Multi-step retrieval | Retrieve, identify a missing fact, retrieve again | Needs iteration limits and evidence tracking |
| Routed RAG | Choose documentation, SQL, graph, or another source | A routing error can choose an inappropriate authority |
| Agentic RAG | Model selects tools and retrieval steps dynamically | More variable latency, permissions exposure, and evaluation difficulty |

Start with a measurable retrieval baseline. Add a stage when it resolves an observed failure and its quality improvement justifies latency and operational cost.

**Interview question:** The answer quotes an outdated owner after a source update. Trace source change detection, extraction, index generation, stale chunks, duplicate candidates, cache keys, and citation versions. Increasing model size does not repair a stale index.

## 3. Vector Databases, ANN Indexes, and Similarity Search

### Vector search versus a vector database

**Vector search** ranks items by a vector similarity or distance. A **vector database** additionally manages records, metadata, persistence, filtering, updates, and operational behavior, with capabilities varying by product.

Faiss is a vector search library with several index families; it does not by itself supply a complete distributed database service. A relational database with vector support may be sufficient when data volume, permissions, joins, and operational needs fit. [Faiss documentation](https://faiss.ai/).

Evaluate storage using workload requirements: vector count and dimensions, ingest and update rate, tenant distribution, filter selectivity, recall target, concurrency, p95/p99 latency, durability, backup/restore, and deletion semantics. A fast benchmark without the real filters or update workload is weak evidence.

### Similarity metrics

For nonzero vectors `q` and `x`:

```text
dot product       = sum(q_i * x_i)
cosine similarity = dot(q, x) / (norm(q) * norm(x))
Euclidean distance = sqrt(sum((q_i - x_i)^2))
```

Higher dot product or cosine similarity is better; lower Euclidean distance is better. Some APIs expose a distance or a negated score instead of similarity, so check ordering semantics.

When both vectors have unit length, cosine similarity equals dot product, and squared Euclidean distance is `2 - 2 * dot(q, x)`. These metrics then give equivalent rankings, aside from ties and numerical effects. Without normalization, vector magnitude can change rankings. Reject or handle zero vectors explicitly because cosine is undefined for them.

Use the metric expected by the embedding model. A cosine score of 0.8 is not an 80% probability of relevance, and a threshold does not transfer automatically between models or corpora.

### Exact nearest neighbors versus ANN

**Exact nearest-neighbor search** computes the true nearest vectors under the chosen metric and eligible corpus. A straightforward scan costs roughly `O(N * d)` per query for `N` vectors of dimension `d`. It can be a sensible baseline for small collections or highly selective filters.

**Approximate nearest-neighbor search**, or ANN, avoids examining every vector, trading some neighbor recall for resource savings. Approximation quality and semantic relevance are different: an exact vector neighbor can still be irrelevant to the user's question.

### HNSW

**Hierarchical Navigable Small World**, or HNSW, organizes vectors in a multilayer proximity graph. Search traverses sparse upper layers and explores a candidate neighborhood at lower layers. It offers useful speed/recall trade-offs at the cost of graph memory and construction work. [HNSW paper](https://arxiv.org/abs/1603.09320).

Common controls include graph connectivity, construction search effort, and query search effort. Increasing query exploration usually improves recall while increasing latency. Parameter names and update behavior depend on the implementation.

### IVF and product quantization

An **inverted file**, or IVF, partitions vectors into lists associated with learned centroids. A query searches selected lists rather than the entire collection. Probing more lists raises work and generally improves recall. Centroid training should represent the indexed distribution; data drift can reduce index quality.

**IVFFlat** stores full vectors in those lists. **Product quantization**, or PQ, compresses vectors into codes built from smaller learned codebooks. It saves memory but introduces distance approximation; retaining original vectors for exact rescoring adds storage and fetch cost. IVF and PQ can be combined. [Faiss index families](https://github.com/facebookresearch/faiss/wiki/Faiss-indexes).

| Approach | Main advantage | Main trade-off |
| --- | --- | --- |
| Exact flat search | Ground truth for vector neighbors; simple behavior | Work grows with eligible corpus size |
| HNSW | Strong latency/recall behavior for many in-memory workloads | Graph memory and build/update costs |
| IVF | Search a tunable subset of clustered vectors | Training quality and probe count affect recall |
| Quantized indexes | Reduced vector memory footprint | Approximation error and possible rescoring overhead |

### Filtering, tenancy, and authorization

Filtering can happen before candidate search, during traversal, or after ANN candidate generation. The strategy changes both performance and result quality. If a search retrieves 100 candidates and only two pass a filter, returning those two may miss better authorized matches that were never explored.

In pgvector, approximate-index filtering can occur after the index scan; its documentation describes iterative scans and alternatives such as filter indexes, partial indexes, and partitioning. Do not assume another engine has identical behavior. [pgvector indexing and filtering](https://github.com/pgvector/pgvector#filtering).

Benchmark with actual tenant and ACL distributions. Candidate oversampling may help recall, but it is not a security control. Unauthorized content must not reach the generator, an unapproved reranker, shared caches, logs, or the caller.

### Memory sizing and index maintenance

One million 768-dimensional float32 vectors require `1,000,000 * 768 * 4 = 3.072 GB`, about 2.86 GiB, for raw vectors alone. Ten million require 30.72 GB. Text, metadata, graph edges, auxiliary indexes, replicas, and build buffers add overhead.

Track recall and latency during concurrent writes, compaction, deletes, and rebuilds. Understand whether deletion immediately excludes a record, when space is reclaimed, and how snapshots recover both data and the intended index generation.

**Interview question:** Why did a tenant filter make ANN slower and reduce results? Selective filtering may discard most candidates or force more traversal. Compare against exact search on the same authorized subset, inspect filter execution, then evaluate search effort, partitioning, and indexing options.

## 4. Hybrid Search, Reranking, and Retrieval Evaluation

### Lexical and dense retrieval

**Lexical retrieval** uses terms and an inverted index. BM25 combines term frequency, inverse document frequency, and document-length normalization. It works well for identifiers and distinctive terms when tokenization preserves them; arbitrary exact identifiers may need keyword fields or explicit exact-match lookup.

**Dense retrieval** searches embeddings and can match paraphrases such as “who maintains the sales pipeline?” against “sales workflow owner.” It may struggle with rare codes, nearly identical names, negation, or exact constraints.

**Hybrid search** combines complementary candidate sets. In the catalog assistant, lexical search can find `customer_events_v2`, while dense search can find documentation describing “the latest customer activity feed.” Neither should override tenant, classification, or current-version constraints.

### Combining rankings

Raw BM25 and cosine scores have different scales. Adding them without calibration gives an arbitrary weighting.

**Reciprocal rank fusion**, or RRF, combines ranks rather than raw scores:

```text
RRF(document) = sum over result lists containing document:
                    1 / (c + rank_in_list)
```

Ranks start at 1; a missing document contributes zero. The rank constant `c` controls how strongly top positions dominate. Candidate windows and optional weighting are still design choices. Deduplicate using stable document or chunk identity. [Elasticsearch RRF documentation](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/reciprocal-rank-fusion).

For `c = 60`, a document ranked second in lexical search and fourth in dense search scores `1/62 + 1/64`, approximately 0.03175. One ranked first in only one list scores `1/61`, approximately 0.01639. Agreement across lists can beat a single first-place result.

### Reranking and context selection

A **bi-encoder** embeds queries and documents independently, making large-scale candidate generation efficient. A **cross-encoder** jointly processes a query and candidate to score relevance. It can model interactions that independent vectors miss, but requires work for each candidate pair. [Sentence Transformers retrieve and rerank](https://sbert.net/examples/sentence_transformer/applications/retrieve_rerank/README.html).

An illustrative pipeline retrieves 100 candidates from each search method, fuses and deduplicates them, reranks 50, and places up to eight useful passages into the context budget. These are separate limits to tune, not one shared `top_k` value.

Reranking cannot recover a document absent from all candidate sets. It also does not prove a claim is true. After ranking, remove redundant passages, preserve qualifiers and provenance, and select complementary evidence. For multi-part questions, ensure context covers each required part.

### Building a retrieval evaluation set

Collect realistic queries with relevance judgments tied to source versions and authorization scopes. Include exact identifiers, paraphrases, abbreviations, multilingual questions, fresh documents, rare tenants, ambiguous requests, and questions with no answer.

Label at the level you measure: document, chunk, fact, or complete evidence set. Several overlapping chunks from one document should not inflate the impression of useful coverage. A question requiring an upstream table and its downstream dashboard needs evidence for both.

Keep tuning and held-out test sets separate. Synthetic questions help expand coverage, but questions generated from chunks may share their wording and overestimate real-world performance. Include real questions and expert review.

### Retrieval metrics

| Metric | Definition | What it diagnoses |
| --- | --- | --- |
| Precision@k | Relevant retrieved items / `k`, when `k` items are returned | Noise in a fixed-size result list |
| Recall@k | Relevant retrieved items / all labeled relevant items | Missing known relevant evidence |
| Hit rate@k | Fraction of queries with at least one relevant result in top `k` | Whether any useful result appears |
| MRR | Mean reciprocal rank of the first relevant result; zero if none | How quickly a user encounters useful evidence |
| nDCG@k | Discounted graded relevance, normalized by ideal ordering | Quality of ordering when relevance has levels |
| ANN recall@k | Overlap with exact vector top `k`, divided by `k` | Loss caused by approximate search |

For nDCG, a common gain is `2^relevance - 1` and the discount at rank `i` is `log2(i + 1)`. Declare conventions for unjudged results, queries with no relevant items, ties, short result lists, and empty eligible corpora; they affect comparisons.

**Example:** A query has four relevant documents. The top five contain two, at ranks 1 and 4. Precision@5 is `2/5 = 0.4`, recall@5 is `2/4 = 0.5`, reciprocal rank is `1`, and the query is a hit. Strong MRR alone hides missing evidence.

Compare ANN results against exact vector neighbors using the same filters, metric, and corpus snapshot. Separately compare retrieved results against human relevance labels. High ANN recall does not establish that an embedding model captures the task.

### Diagnosing retrieval failures

| Observation | First checks |
| --- | --- |
| Relevant source never appears | Ingestion, freshness, extraction, metadata, filters, and candidate recall |
| Relevant source appears but ranks poorly | Lexical analysis, embedding fit, fusion, and reranker behavior |
| Correct chunk is removed before generation | Deduplication, context truncation, token budget, and evidence selection |
| Correct evidence is supplied but answer is wrong | Instructions, model behavior, evidence conflict, and answer evaluation |

**Interview question:** Retrieval recall improved, but answer accuracy fell. More noisy, duplicate, conflicting, or outdated evidence may have entered the context. Evaluate the final context and generated claims, not just the retriever's candidate set.

## 5. Knowledge Graphs, GraphRAG, and Metadata Discovery

### Knowledge graph fundamentals

A **knowledge graph** represents entities and relationships. A property graph attaches properties to nodes and edges; RDF represents statements as subject-predicate-object triples, with additional mechanisms for richer context.

For a data platform:

```text
Dataset: raw.orders -> transformed_by -> Job: build_orders
Job: build_orders -> produces -> Dataset: analytics.orders
Dataset: analytics.orders -> used_by -> Dashboard: revenue
Dataset: analytics.orders -> owned_by -> Team: finance_data
Column: analytics.orders.email -> classified_as -> Classification: personal_data
```

A graph provides explicit connectivity, while an embedding provides similarity. HNSW's internal neighbor graph is a search structure, not a business knowledge graph.

### Building trustworthy relationships

Prefer authoritative catalog APIs, parsed transformation definitions, and observed execution metadata for relationships those sources can establish. LLM extraction can propose entities and links from prose, but proposals need validation and provenance.

Resolve aliases to stable IDs and include environment, namespace, and version where identity requires them. Two tables named `orders` in different environments are not automatically the same entity. An owner change must preserve its effective time when historical questions matter.

Record source evidence, extraction method, timestamps or validity intervals, and verification status. A model-generated confidence score is not automatically a calibrated probability. Contradictory claims should be reconciled using source authority and time, rather than silently overwriting each other.

### GraphRAG architectures

**GraphRAG** is a broad pattern that augments generation with graph-derived evidence. A system can link a query to catalog entities, traverse dependencies, fetch associated documents, and generate an answer with the resulting evidence. It need not extract its entire graph with an LLM.

Microsoft's GraphRAG implementation also uses extracted entities, relationships, and community summaries. Its **local search** combines entity-related structured information and source text; **global search** uses community reports for corpus-level questions. Its documented methods also include DRIFT and basic vector search. [GraphRAG query overview](https://microsoft.github.io/graphrag/query/overview/).

| Question | Useful retrieval approach | Main concern |
| --- | --- | --- |
| “What does this metric mean?” | Lexical/dense retrieval of the authoritative definition | Correct version and business context |
| “Which dashboards depend on this column?” | Typed lineage traversal | Complete edges and column-level resolution |
| “Which team owns those downstream pipelines?” | Entity resolution, graph traversal, and ownership lookup | Identity, freshness, and permission checks |
| “What themes recur across incident reports?” | Community summaries or another corpus summarization design | Summary accuracy, coverage, and build cost |

Graphs help when the question depends on explicit relationships or several hops. They introduce entity resolution, edge maintenance, schema design, and graph access controls. Compare them against simpler retrieval or SQL for the actual task.

### Metadata discovery and catalog assistants

**Metadata discovery** helps users find suitable datasets and understand their meaning, quality, ownership, freshness, and usage constraints. Search catalog descriptions and glossary terms semantically, but use structured filters for environment, certification, classification, and domain.

A discovery response should explain why a dataset matches and provide stable IDs, ownership, documentation, and relevant quality signals. Do not infer that the first result is the authoritative dataset or that popularity proves correctness.

Separate the question “What data might answer this?” from “What is the actual answer in the data?” The first is a catalog retrieval task; the second may need approved SQL, business definitions, and time-sensitive execution results.

### Graph security, freshness, and evaluation

Graph results can expose restricted information through edges, counts, entity names, and summaries even if source document bodies are hidden. Authorize nodes, traversals, and derived summaries according to policy. Removing citations after generation does not remove leaked evidence from a summary.

Keep lineage events and document-derived relationships independently traceable. Bound traversal depth and fan-out, detect cycles, and define how missing edges affect answers. A missing recorded dependency is not proof of no dependency.

Evaluate entity linking, relationship correctness, path completeness, answer support, freshness, and permissions separately. A fluent explanation can hide an incorrect starting entity or one fabricated edge.

**Interview question:** How would you answer “What breaks if I rename this column?” Resolve the exact column, traverse known downstream dependencies, inspect SQL and runtime evidence, and report coverage limits. Dynamic SQL, external consumers, and stale lineage prevent a universal guarantee.

## 6. LangChain, LangGraph, LlamaIndex, and MCP

### What problem each tool solves

| Tool or protocol | Main role | Responsibility it does not remove |
| --- | --- | --- |
| LangChain | Model/tool integrations and higher-level agent construction | Data quality, permissions, and business validation |
| LangGraph | Stateful workflow and agent orchestration | Idempotent external actions and correct state design |
| LlamaIndex | Ingestion, indexing, retrieval, and data-connected application abstractions | Reliable source data and suitable retrieval evaluation |
| MCP | Standardized communication between AI applications and external capabilities | Authorization policy and safe use of tools |

These overlap and can be combined. A small RAG application can also use direct model and database calls. Choose an abstraction when it removes real integration or orchestration work.

### LangChain

LangChain provides model and tool abstractions and higher-level agent APIs. Its documented agent architecture builds on LangGraph, gaining runtime features such as persistence and human interaction. [LangChain overview](https://docs.langchain.com/oss/python/langchain/overview).

For an interview, explain the actual flow: receive a question, construct model context, allow selected tool calls, validate tool arguments, execute authorized operations, and return results. Naming a framework does not explain the control flow or failure handling.

Avoid assuming older examples use current APIs. Pin package versions and inspect the installed integration's behavior when implementing an application.

### LangGraph

LangGraph models execution with state, nodes, and transitions. Nodes perform work; transitions can route to another step, repeat, pause, or finish. Checkpointing supports persisted progress and human review in long-running workflows. [LangGraph overview](https://docs.langchain.com/oss/python/langgraph/overview).

For a migration assistant, states might be “inventory collected,” “candidate translation generated,” “validation failed,” “review pending,” and “approved artifact.” Store relevant source versions and artifact hashes so approval applies to a particular result.

Durable execution can replay work from checkpoints. Put side effects behind appropriate task boundaries and design external mutations with idempotency keys or transactions. A crash after a database commit but before workflow acknowledgment can otherwise repeat an action. Framework persistence alone does not guarantee exactly-once effects in external systems. [LangGraph durable execution](https://docs.langchain.com/oss/python/langgraph/durable-execution).

### LlamaIndex

LlamaIndex provides data ingestion and RAG abstractions. A **Document** represents ingested content; **Nodes** represent smaller retrieval units with metadata and relationships. An ingestion pipeline applies transformations such as parsing, chunking, and embedding; an index organizes access to the resulting data. [LlamaIndex ingestion and nodes](https://developers.llamaindex.ai/python/framework/module_guides/indexing/vector_store_index/).

A **retriever** returns relevant nodes, while a **query engine** combines retrieval and response generation. Response synthesis turns selected evidence into an answer. These boundaries are useful for testing retrieval independently of generation. [LlamaIndex query engines](https://developers.llamaindex.ai/python/framework/module_guides/deploying/query_engine/usage_pattern/).

Using an ingestion abstraction does not ensure deletes, permission changes, or metadata filters propagate correctly through every configured storage backend. Verify those behaviors for the integration in use.

### MCP architecture and primitives

The **Model Context Protocol**, or MCP, standardizes exchanges between an AI application's host, its clients, and external servers. A host manages clients and policy; each client communicates with a particular server. The protocol uses JSON-RPC. [MCP architecture](https://modelcontextprotocol.io/specification/2026-07-28/architecture).

Important primitives include:

- **Tools:** Callable capabilities, such as searching a catalog or validating a SQL statement.
- **Resources:** Addressable context, such as a schema document or job definition.
- **Prompts:** Reusable prompt templates exposed by a server.

A server advertises supported capabilities; support varies by protocol revision and implementation. MCP transports and lifecycle details are version-sensitive. It is an integration protocol, not a replacement for the model, retriever, or workflow engine. [MCP specification](https://modelcontextprotocol.io/specification/2026-07-28).

### Safe tool integration

For a catalog search tool, validate input length, allowed filters, and tenant scope. Obtain identity from trusted authentication context rather than a model-supplied username. Validate returned objects and enforce authorization on the server that accesses data.

For remote MCP authorization, validate tokens for the intended server and required scope; do not treat an arbitrary bearer token as authority or pass it through to unrelated services. Keep credentials outside model context. Tool descriptions and results remain untrusted input. [MCP authorization](https://modelcontextprotocol.io/specification/2026-07-28/basic/authorization).

Use narrowly scoped tools instead of a generic shell or unrestricted SQL executor when the task does not need them. Read-only tools still need row, column, tenant, timeout, and resource controls.

**Interview question:** Do you need all four technologies for RAG? No. Describe the required capabilities first. LangChain or LlamaIndex may simplify integrations, LangGraph helps with stateful control flow, and MCP helps when standardized external tool connectivity is needed.

## 7. AI Agents for Migration, Data Quality, and Lineage

### Agent versus deterministic workflow

An **AI agent** uses model decisions to select actions, tools, or subsequent steps toward a task. A deterministic workflow has predefined control flow. Many reliable systems combine a fixed outer workflow with model-driven steps inside bounded stages.

Define the objective, available tools, allowed data, stopping conditions, and success checks. Bound iterations, elapsed time, token spend, queries, and tool calls. A model's statement that the job is complete is not a success criterion.

```text
Read authorized evidence -> propose artifact -> validate independently
 -> revise within limits -> review concrete artifact -> execute if authorized
 -> reconcile outcome and record evidence
```

### Migration agents

Migration work includes SQL dialect translation, schema mapping, pipeline adaptation, documentation, and test generation. A model can propose changes, while parsers, target engines, and reconciliation establish whether the proposal works.

A controlled migration sequence:

1. Inventory source objects, callers, schedules, dependencies, and representative workloads.
2. Record behavior that must be preserved: grain, keys, nulls, timestamps, types, and transaction assumptions.
3. Generate a candidate translation tied to source and target versions.
4. Parse or compile it and run it against bounded test data in an isolated target.
5. Compare schemas, rows, business aggregates, and edge cases.
6. Review the exact artifact and deployment plan, including rollback and recovery.
7. Reconcile a shadow or staged run before cutover.

Compilation does not prove semantic equivalence. Date arithmetic, time zones, integer division, decimal precision, null ordering, regular expressions, and merge behavior can change silently.

**Example:** A daily revenue query can return the right row count but assign late-night transactions to the wrong business date. Compare totals by business date and currency, and test daylight-saving transitions and rounding.

Row hashes help comparison only after canonicalization accounts for types, nulls, ordering, and representation. Do not remove duplicates merely to make source and target results match.

### Data quality agents

An agent can propose tests from contracts, profile anomalies, summarize failed checks, and collect evidence for likely causes. Keep deterministic enforcement for properties such as non-null keys, uniqueness, referential integrity, freshness, and accepted value domains.

| Task | Useful AI contribution | Independent evidence |
| --- | --- | --- |
| Suggest quality checks | Draft rules from documentation and schema | Owner-reviewed contract and executable checks |
| Explain a volume drop | Correlate deployments, source lag, and failures | Partition counts, event rates, and run history |
| Investigate distribution drift | Summarize changed segments and possible causes | Statistical analysis and upstream change evidence |
| Propose a repair | Produce a scoped correction plan | Previewed impact, validation, authorization, and rollback |

Distinguish a genuine business change from a defect. A drop in weekend orders may be expected; a missing partition is a different failure. Sending raw sensitive records to an LLM for diagnosis may be unnecessary when aggregates and schema metadata suffice.

Never let an unverified model repair delete outliers, impute values, or rewrite history merely to pass a check. Preserve original data and define which owner can authorize a correction.

### Lineage agents

Lineage connects data sources, transformations, jobs, and consumers. **Table-level lineage** tracks dataset dependencies; **column-level lineage** tracks how specific outputs derive from input columns and expressions.

Use SQL parsers, query plans, transformation manifests, and runtime events where available. Models can help interpret documentation or explain existing paths, but a plausible narrative does not establish a dependency.

Track whether a link is declared, statically parsed, observed at runtime, or inferred. Dynamic SQL, stored procedures, aliases, temporary objects, and external consumers create coverage gaps. Keep those gaps visible when answering impact-analysis questions.

### Recovery, approval, and auditability

Agents need the same operational controls as other workers: durable progress, retries with backoff, concurrency limits, deadlines, and cancellation. Separate retryable transport failures from invalid input or denied access; repeated denied calls should not become a privilege search.

Persist artifact versions, tool arguments, outcomes, and approval evidence with appropriate redaction. Approval for one SQL diff must not authorize a changed diff after the next model iteration. Recheck current permissions and preconditions at execution time.

For mutating tools, use transaction boundaries or idempotency keys where supported, verify postconditions, and plan recovery from partial success. A completed agent trace is not proof that all external actions committed.

**Interview question:** An agent retries after a timeout. How do you prevent duplicate backfills? Use a stable operation ID, a persisted job record or uniqueness constraint, and a tool that can return the existing job status. Resolve an unknown outcome before submitting another backfill.

## 8. LLM Evaluation, Observability, Security, and Cost Optimization

### Evaluate components and end-to-end behavior

Separate retrieval, evidence selection, generation, and tool execution. This makes failures actionable.

| Layer | What to evaluate | Example failure |
| --- | --- | --- |
| Ingestion | Completeness, extraction quality, freshness, and deletions | An updated runbook never reaches the index |
| Retrieval | Relevance, coverage, ordering, and permissions | Correct definition is absent from candidates |
| Context assembly | Evidence coverage, redundancy, and token fit | The key qualifier is truncated |
| Generation | Correctness, support, relevance, citations, and abstention | Answer contradicts supplied evidence |
| Agent actions | Tool choice, argument validity, task outcome, and side effects | Correct explanation accompanies an incorrect mutation |

**Faithfulness** asks whether claims are supported by supplied evidence. **Correctness** asks whether the answer agrees with an authoritative reference or outcome. An answer can be faithful to an obsolete document and still be wrong. Citation presence is insufficient; check that the cited passage supports the associated claim. [Ragas metric categories](https://docs.ragas.io/en/latest/concepts/metrics/available_metrics/).

### Offline and online evaluation

Maintain versioned test cases containing the question, identity or access scope, source snapshot, expected facts or outcome, and evaluation rubric. Cover answerable questions, ambiguous questions, unanswerable questions, conflicts, denied requests, stale sources, and prompt injection.

Compare prompt, model, embedding, chunking, and retrieval changes on the same held-out set. Report slices by task, language, domain, tenant, and difficulty; one average can hide a severe regression. Include sample counts and uncertainty rather than treating small score differences as definitive.

LLM judges can scale rubric-based review but have position, style, and model biases. Calibrate against human labels, blind model identity when possible, test reordered answer pairs, and retain expert review for consequential outcomes. Keep evaluation data out of training and tuning inputs.

In production, use controlled rollout or shadow evaluation, user feedback, sampled review, and explicit operational thresholds. A thumbs-up rate alone does not establish factual accuracy or absence of data exposure.

### Observability and reproducibility

Trace a request across authentication, query rewriting, retrieval, reranking, generation, and tool calls. Link ingestion runs and index generations to the serving trace so a bad answer can be traced back to source content.

Useful signals include:

- **Reliability:** Error and timeout rates, retries, provider throttling, and tool failures.
- **Latency:** Queue time, retrieval and reranking duration, time to first token, total duration, and p95/p99.
- **Quality:** Empty retrieval, stale evidence, citation failures, unsupported claims, and reviewed outcomes.
- **Data pipeline health:** Ingestion lag, extraction failures, reconciliation gaps, and deletion backlog.
- **Usage:** Tokens, model calls, tool calls, cache hits, and cost per successful task.

Record model and prompt versions, retrieval configuration, source and chunk IDs, index generation, and relevant tool outcomes. Use structured spans and follow the applicable OpenTelemetry GenAI conventions; convention maturity and attribute names can change. [OpenTelemetry GenAI conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/).

Do not log full prompts, retrieved passages, credentials, or sensitive query results by default. Redact, restrict access, sample deliberately, and apply retention policies. Reproducibility usually means preserving enough authorized evidence and configuration to investigate a run, not promising identical tokens on every replay.

### Security boundaries

Treat user messages, retrieved documents, tool descriptions, and tool outputs as untrusted data. **Indirect prompt injection** occurs when external content attempts to redirect the model, for example a runbook telling it to reveal another tenant's schema.

Security controls belong outside the model:

- Authenticate callers and enforce row, column, document, tenant, and tool permissions at the data boundary.
- Keep secrets out of prompts; use scoped server-side credentials.
- Validate model-produced tool arguments and outputs before use.
- Restrict generated SQL to permitted operations, objects, and resource budgets using database controls and validation; string matching alone is insufficient.
- Scope caches and derived summaries to the correct authorization context and invalidate them when policy changes.
- Limit tool access, destinations, iterations, and payload sizes to contain exfiltration and resource abuse.

Prompt wording and content filters can help, but do not replace authorization or isolation. Review poisoning, sensitive information disclosure, improper output handling, and excessive agency as separate failure modes. [OWASP GenAI security risks](https://genai.owasp.org/llm-top-10/).

Generated Markdown or HTML also needs safe rendering; treat links and active content according to the application's output policy. Structured JSON is still untrusted input to downstream code.

### Cost models and optimization

Estimate the full task cost, not just the final model call:

```text
request cost = query embedding + retrieval infrastructure + reranking
             + sum(model calls and tool execution) + allocated operations

model call cost = input_tokens * input_rate + output_tokens * output_rate

indexing cost = parsing/OCR + chunk embedding + index construction
              + storage/replication + updates and rebuilds
```

Rates above are per token; divide token counts appropriately when prices are quoted per million. Include other billable categories for the provider in use. Separate fixed infrastructure from variable request cost.

**Example:** At hypothetical rates of $0.50 per million input tokens and $2.00 per million output tokens, an 8,000-input-token, 1,000-output-token call costs `$0.004 + $0.002 = $0.006`. At 100,000 such calls, that is $600 before retrieval, other model calls, tools, and operations.

| Optimization | Expected benefit | What to verify |
| --- | --- | --- |
| Skip unchanged documents and reuse valid embeddings | Less indexing work | Content and preprocessing/model versions match |
| Improve evidence selection | Smaller context and less noise | Answer support and coverage remain adequate |
| Use smaller models for simpler stages | Lower latency and inference cost | Held-out quality for that stage and the complete task |
| Batch ingestion work | Better embedding throughput | Rate limits, retry isolation, and freshness targets |
| Cache suitable results | Avoid repeated work | Identity, permissions, source versions, and staleness rules |
| Bound agent loops and retries | Predictable worst-case spend | Failure handling and task completion rates |
| Compress vectors or tune ANN effort | Lower memory or serving cost | Filtered recall and tail latency |

Provider prompt caching and application answer caching solve different problems. An answer cache must not return an old answer after evidence or authorization changes. Semantic caches can incorrectly equate questions that differ by date, tenant, negation, or entity.

Measure **cost per successful authorized task**. A cheaper call that triggers retries, misses evidence, or requires manual correction can increase total cost. Keep explicit quality and latency targets while tuning.

### Interview scenarios to practice

1. **Design the catalog assistant.** Describe sources, stable IDs, chunking, lexical/vector search, authorization, citations, freshness targets, and failure recovery. Explain when SQL or graph traversal is preferable to document retrieval.
2. **Recall falls after an embedding migration.** Check query/document compatibility, preprocessing, dimensions, metric, publication routing, and the held-out comparison. Preserve a rollback path to a consistent model/index pair.
3. **A user sees a restricted dataset name.** Trace retrieval, graph edges, rerankers, summaries, caches, logs, and citations. Fix the authorization boundary and invalidate affected derived artifacts.
4. **RAG is accurate but too slow.** Profile queueing, retrieval, reranking, context size, model prefill, generation, and repeated tool calls. Remove the measured bottleneck while reevaluating quality.
5. **A migration compiles but changes totals.** Reconcile types, nulls, duplicates, time zones, rounding, joins, and business grain. Use representative execution results rather than a second model's approval.
6. **The assistant cannot find enough evidence.** Distinguish denied access, empty retrieval, incomplete ingestion, and an unanswerable question. Return an appropriate abstention or clarification without inventing facts or exposing restricted information.

For each scenario, state assumptions, identify the authoritative data, describe a concrete failure, and name the measurement or validation that would demonstrate success.
