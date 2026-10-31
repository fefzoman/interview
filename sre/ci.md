# Continuous Integration (CI) — SRE Expert Checklist

A green pipeline is useful only if it tested the intended change, produced a traceable artifact, and ran within a trustworthy environment. When CI slows down or fails, engineers lose feedback and incident fixes wait in a queue. An SRE must understand both the developer workflow and the distributed system that executes it.

This guide follows the supplied checklist and assumes familiarity with Git, containers, Linux, and Kubernetes. Each section explains mechanisms, trade-offs, and operational consequences. The interview scenarios turn those ideas into diagnostic and design answers. Platform syntax and capabilities vary by version, hosting model, and subscription; verify them against the deployed release. Linked official documentation supports details that change over time.

## Contents

1. [Fundamentals & Architecture](#1-fundamentals--architecture)
2. [CI Platforms & Runners](#2-ci-platforms--runners)
3. [Pipeline Design & Execution](#3-pipeline-design--execution)
4. [Build Systems & Artifact Management](#4-build-systems--artifact-management)
5. [Testing & Quality Gates](#5-testing--quality-gates)
6. [CI Security & Supply Chain](#6-ci-security--supply-chain)
7. [Performance & Scalability](#7-performance--scalability)
8. [Reliability & High Availability](#8-reliability--high-availability)
9. [Kubernetes-Based CI](#9-kubernetes-based-ci)
10. [Observability & Troubleshooting](#10-observability--troubleshooting)
11. [Expert Interview Scenarios](#11-expert-interview-scenarios)

## 1. Fundamentals & Architecture

### CI principles, goals, benefits, and limitations

Continuous Integration means integrating small changes frequently into a shared mainline and validating the integrated result automatically. A pipeline attached to a branch that lives for months provides automation, but still delays integration risk. Good CI keeps the mainline usable, makes failures visible, and gives developers feedback quickly enough to act before switching context.

The benefits are earlier defect discovery, smaller debugging scopes, consistent checks, and repeatable artifact production. The limits follow from what the checks can observe: passing tests cannot prove correctness, vulnerability scans cannot detect every threat, and an isolated test environment cannot reproduce all production behavior. CI also becomes infrastructure that needs ownership, capacity, security, and recovery procedures.

### CI, Continuous Delivery, and Continuous Deployment

| Practice | Result | Production decision |
| --- | --- | --- |
| Continuous Integration | Changes integrate frequently and receive automated validation | Production deployment is outside the definition |
| Continuous Delivery | Validated changes remain ready for release through an automated delivery path | A person or policy can choose when to deploy |
| Continuous Deployment | Every qualifying change automatically reaches production | Automated checks and rollout policy make the decision |

Deployment and release can also differ: software may be deployed behind a disabled feature flag and released to users later. CI quality supports all three practices, but a passing build alone does not establish safe delivery.

### Pipeline lifecycle

The common path is **trigger → checkout → build → test → scan → publish**. The trigger identifies an event and revision; checkout materializes that exact source and required submodules or large files. Build resolves inputs and compiles or packages them. Tests and scans evaluate the source and resulting artifact. Publishing stores approved outputs with identifiers and metadata.

This is a logical lifecycle, not a requirement to serialize everything. Linting and source scans can start before compilation; image scans need the built image. Publication to a private staging location can precede scanning, while release promotion must wait for required checks. Retain logs and reports even when a job fails, and distinguish producing an artifact from approving it for release.

### Pipeline terminology

| Term | Meaning and important boundary |
| --- | --- |
| Pipeline / workflow | A run of an automation graph for a revision and event |
| Stage | A logical grouping, sometimes also a scheduling barrier |
| Job | A scheduling unit assigned an execution environment |
| Step | An operation within a job; steps usually share its workspace |
| Task / action | A reusable operation or packaged script; terminology varies |
| Artifact | An output intentionally retained or passed to consumers |
| Runner / agent | Software or a machine that receives and executes jobs |
| Executor | The mechanism used to execute work, such as shell, Docker, or Kubernetes |

Jobs generally require explicit artifact transfer rather than assuming a shared filesystem. Azure's task and agent terminology is documented in its [pipeline concepts](https://learn.microsoft.com/en-us/azure/devops/pipelines/get-started/key-pipelines-concepts?view=azure-devops).

### Event-driven, scheduled, and manual pipelines

Event-driven pipelines react to pushes, pull requests, tags, or upstream completions and provide fast feedback. Scheduled pipelines catch newly disclosed vulnerabilities, dependency drift, and failures in rarely changed components. Manual pipelines support controlled reruns, releases, and diagnostics; record their actor, parameters, and source revision.

Events can be duplicated, delayed, or superseded. Use immutable revisions, deduplicate where appropriate, and make side effects idempotent. A nightly scan of an existing release answers a different question from rebuilding that release with today's dependencies.

### Centralized and distributed CI architectures

A centralized service controls policy, scheduling, identity, and metadata. Distributed runners execute near source mirrors, registries, or specialized hardware. Most large installations combine centralized orchestration with distributed execution; these are separate design axes rather than mutually exclusive products.

Distribution adds capacity and isolation but introduces network failures, inconsistent environments, and more credentials. Split workloads across failure domains without making every job depend synchronously on a single remote cache or proxy. Keep execution off the control plane.

### Push-based and pull-based workflows

In a push integration workflow, developers commit directly to the shared branch, typically with strong local checks and rapid repair. A pull-request or merge-request workflow proposes a branch change for review and validation before merging. These terms are also used for infrastructure: a push deployment sends commands outward, while a pull reconciler fetches desired state. Clarify which meaning an interviewer intends.

Runner job polling is another independent mechanism. A platform can receive a push webhook, validate a pull request, and have a runner pull the resulting job from a queue.

### Trunk-based development, feature branches, merge queues, and GitOps

Trunk-based development keeps changes small and branches short-lived; feature flags can hide unfinished behavior. Feature branches provide review boundaries but accumulate integration risk as they diverge. A merge queue tests the candidate together with current mainline and, where applicable, earlier queued changes, preventing several individually green branches from breaking their combined result.

Required checks must also run for queue candidates. GitHub Actions uses the `merge_group` event for this purpose; PR checks alone are insufficient. See [merge queue event handling](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows).

GitOps stores declarative desired state in version control and uses a reconciler to apply it. CI can publish an immutable image and propose an update to its digest in a configuration repository. The reconciler handles deployment and drift correction. Avoid making the build identity a production cluster administrator.

### CI in delivery and supply-chain security

CI connects source, dependencies, build tools, tests, registries, and deployment identities. Compromising that connection can turn a small repository change into a trusted malicious release. Protect pipeline definitions like application code, record exactly what produced each artifact, and require downstream verification before promotion. Supply-chain trust depends on the builder and its inputs as well as the final binary.

## 2. CI Platforms & Runners

### Platform comparison

| Platform | Execution model | SRE considerations |
| --- | --- | --- |
| GitLab CI/CD | Repository YAML, GitLab coordination, GitLab Runner executors | Runner matching, artifact storage, GitLab service dependencies |
| GitHub Actions | Event-triggered workflows with hosted or self-hosted runners | Token permissions, action dependencies, runner access policies |
| Jenkins | Controllers schedule agents; Jenkinsfiles define pipelines | Controller state, plugin compatibility, credentials, operational ownership |
| Azure Pipelines | YAML or other supported definitions, agent pools, tasks | Agent pool access, service connections, approvals, parallel job entitlement |

Choose based on identity integration, trust requirements, workloads, operational capacity, and existing investments. A flexible plugin ecosystem also expands the patching and compatibility surface.

### GitLab Runner architecture, registration, and tags

GitLab coordinates jobs; the runner manager requests eligible work and delegates execution to its configured executor. Runner tags match capabilities or pools: a runner must satisfy the job's requested tags. Scope, protected-runner settings, paused state, and permission to run untagged jobs also affect eligibility. Tags route work; they do not replace authorization.

Create a runner through the supported UI or API workflow, then register its manager using the runner authentication token. Protect and rotate that token because it authenticates the runner to GitLab. Legacy registration-token examples can be obsolete; use the [runner creation workflow](https://docs.gitlab.com/ci/runners/new_creation_workflow/) for the installed release.

### GitHub hosted and self-hosted runners, runner groups

Hosted runners reduce machine maintenance and suit standard workloads; available images, networking, quotas, and hardware depend on the offering. Self-hosted runners provide custom networking, hardware, and images, with responsibility for patching, isolation, cleanup, and capacity.

Runner groups restrict which repositories or workflows can use a pool. Labels express capabilities; group policies express access. A job label alone is not proof of trust. Restrict privileged pools to approved callers using [runner group access controls](https://docs.github.com/en/actions/how-tos/manage-runners/self-hosted-runners/manage-access).

### Jenkins controller, agents, Pipeline DSL, and plugins

The controller stores configuration and scheduling state; agents run build work. A `Jenkinsfile` expresses a Pipeline using Groovy-based DSL. Declarative Pipeline provides a structured model; Scripted Pipeline permits more imperative control. Both can execute dangerous operations, so syntax choice is not a sandbox guarantee.

Keep build executors off the controller, restrict credentials and script approvals, and maintain a tested plugin set. Shared libraries are executable dependencies. Jenkins scales execution through agents and can divide ownership across controllers, but multiple controllers do not automatically form an active-active service for one job namespace. See [Jenkins architecture and scaling](https://www.jenkins.io/doc/book/scaling/architecting-for-scale/).

### Shell, Docker, Kubernetes, and VM execution

| Environment | Advantage | Main constraint |
| --- | --- | --- |
| Shell on host | Simple and fast access to installed tools | Jobs can affect host state and other jobs |
| Docker container | Repeatable user space and quick startup | Shared kernel; mounts and privileges can defeat isolation |
| Kubernetes pod | Scheduling, quotas, service containers, disposable workspaces | API, networking, storage, and node behavior affect execution |
| Dedicated VM | Separate guest kernel and broad workload compatibility | Provisioning time, image maintenance, and cost |

Isolation comes from the entire configuration. A container with the host Docker socket can control the host daemon, while a dedicated VM still needs restricted credentials and network access.

### Ephemeral and persistent runners

Ephemeral runners handle a bounded workload, ideally one job, then are destroyed. They reduce residue and simplify recovery from contamination but add cold starts and image distribution costs. Persistent runners keep warm dependencies and specialized tools but accumulate disk usage, drift, and cross-job exposure.

Deleting a job pod does not clear a reused persistent volume, revoke a stolen credential, or repair a compromised host. Define what is disposable: process, container, workspace, node, or VM. Match that boundary to the threat model.

### Autoscaling, shared pools, and quotas

Kubernetes can schedule additional job pods, while node autoscaling supplies missing machines. GitHub's [Actions Runner Controller](https://docs.github.com/en/actions/concepts/runners/actions-runner-controller) manages runner scale sets. KEDA offers event-driven scaling, including a [GitHub runner scaler](https://keda.sh/docs/latest/scalers); its suitability depends on runner lifecycle and queue integration. Cloud VM pools offer another isolation and capacity boundary. Avoid multiple autoscalers independently controlling the same replica count.

Shared pools improve utilization but require tenant quotas, fair scheduling, and separate trust domains. Dedicated pools serve sensitive releases or unusual hardware at the cost of idle capacity. Bound concurrency at platform, pool, manager, and tenant levels; a large runner limit cannot override cloud quotas or platform entitlements.

### Runner lifecycle and compatibility

Provision from reviewed images, register securely, execute work, upload results, clean up, and deregister or recycle. During upgrades, stop new assignments, allow running work to finish within a drain deadline, and terminate remaining jobs with a recorded reason. Upgrade a canary pool first and test artifact upload, cancellation, caches, and service containers. Check platform, runner, helper image, runtime, plugin, and Kubernetes compatibility together.

## 3. Pipeline Design & Execution

### Pipeline-as-code and declarative versus scripted design

Versioned pipeline definitions allow review, rollback, and attribution. Declarative configuration makes dependencies and policy visible; scripts handle operations that are awkward to express in YAML. Put substantive build logic in versioned repository scripts that can also run locally, and keep orchestration readable. Protect changes to scripts, templates, and included definitions as well as the top-level file.

### DAGs, stages, dependencies, and conditions

A directed acyclic graph (DAG) lets a job start once its prerequisites finish. Stage barriers are easier to visualize but may force unrelated jobs to wait. Dependencies express both control flow and data flow: waiting for a build does not necessarily download its artifact.

GitLab `needs` can bypass stage barriers; GitHub uses job `needs`, while Azure uses `dependsOn`. Their conditions and artifact behavior differ. If a conditional job can be absent, align downstream rules or use the platform's optional-dependency mechanism when omission is valid. Never make a security gate optional merely to avoid graph errors. See [GitLab DAG behavior](https://docs.gitlab.com/ci/yaml/needs/).

```text
                  +--> unit tests --------+
checkout -> build +--> integration tests -+--> quality gate -> publish
         \-------> lint / source scan ---+
```

### Triggers and duplicate runs

Use branch events for integration, PR/MR events for proposed changes, tags for controlled releases, path filters for relevant components, schedules for periodic verification, and explicit dispatch for operator actions. Validate the exact revision being tested: a branch head and a synthetic merge commit answer different questions.

Design event rules together so one push does not accidentally create both an unnecessary branch pipeline and an MR pipeline. Ensure required checks report a deliberate result when path selection skips work; otherwise merge protection may wait indefinitely. Validate release authorization even when the trigger is a tag.

### Matrix builds and dynamic or child pipelines

A matrix expands dimensions such as operating system, language version, and CPU architecture. The Cartesian product grows quickly: three operating systems × four versions × two architectures creates 24 jobs before sharding tests. Exclude unsupported combinations and identify which combinations are required for a release.

Dynamic pipelines generate a graph from repository structure or affected components. Child pipelines divide a large configuration into bounded units. Validate generated configuration, limit its size, and ensure the parent receives the required child's final status. A successful trigger job does not necessarily mean its child succeeded.

### Reusable workflows, templates, includes, and libraries

Reuse reduces drift in authentication, scanning, and publishing. Define explicit inputs, outputs, permissions, and ownership. Pin external reusable workflows, includes, and libraries to reviewed immutable revisions; changing a shared template can affect many repositories at once. Roll out updates through representative consumers and retain a rollback path. Reuse should expose policy clearly rather than hide a large implicit script behind a simple name.

### Parameters, variables, precedence, and scope

Parameters generally describe declared inputs; variables provide configuration or runtime values. Some expressions resolve while the graph is compiled, others when a job executes, and shell expansion happens later. A value created by one step cannot retroactively decide a graph that was already constructed.

Precedence is platform-specific. For example, GitLab project variables can override YAML job variables; assume neither that the closest-looking value wins nor that GitHub's rules apply elsewhere. Inspect the [GitLab precedence reference](https://docs.gitlab.com/ci/variables/). Use typed or validated inputs where supported, avoid overriding built-in names, and scope environment credentials to jobs that need them. Do not log entire environments while debugging.

### Manual approvals and protected environments

An approval gate is meaningful only when an unauthorized actor cannot bypass it by editing the pipeline or selecting another environment name. Combine reviewer policies with protected branches or tags, restricted deployment identities, and environment access control. Present the immutable artifact digest, source revision, test results, and target to the reviewer. An approval for one artifact must not authorize a rebuilt replacement with the same tag.

### Cancellation, retries, timeouts, and failure propagation

Set job and operation timeouts so a stuck network call cannot occupy a runner forever. Retry transient infrastructure failures with bounded attempts and backoff; repeated assertion failures need diagnosis. Record each attempt so retries do not hide flaky behavior or inflate success rates.

Cancel superseded verification work when safe. Publishing, migrations, and external side effects need explicit interruption behavior. Cleanup hooks are best effort because the host may disappear; pair them with expiring resources and a reconciler. Preserve required failures through aggregation, and use allowed failures only for clearly advisory checks.

### Concurrency controls and races

Limit simultaneous access to mutable targets using mechanisms such as GitLab `resource_group` or GitHub concurrency groups. Define whether new work should wait, replace pending work, or cancel running work. Cancellation is dangerous during a non-atomic release step.

A platform lock may cover only one project or workflow scope. If several repositories publish the same release, enforce immutability or conditional writes at the registry too. Locks reduce collisions; the target storage system must still reject unintended overwrites.

## 4. Build Systems & Artifact Management

### Reproducible and deterministic builds

A deterministic build produces the same output from the same complete inputs. Reproducibility means another execution can recreate those outputs under the documented conditions. Source code alone is not the input set: compiler, base image, dependencies, locale, time, filesystem ordering, generated files, and architecture can all matter.

Pin toolchains and dependencies, normalize timestamps and paths, isolate undeclared network access, and compare artifact digests across clean builds. A hermetic build restricts inputs to declared dependencies; it helps reproducibility but does not prove the build logic is benign.

### Make, Gradle, Maven, npm, and Bazel

| Tool | Useful model | CI issue to understand |
| --- | --- | --- |
| Make | Targets and prerequisites | Missing dependencies or timestamp assumptions can reuse stale results |
| Gradle | Tasks, incremental execution, build cache | Cache correctness depends on accurately declared inputs and outputs |
| Maven | Lifecycle phases and dependency resolution | Mutable snapshots and repository changes weaken reproducibility |
| npm | Package scripts and lockfile-based installation | `npm ci` enforces lockfile consistency; lifecycle scripts still execute code |
| Bazel | Explicit build graph and action caching | Undeclared inputs and toolchain configuration affect correctness and cache reuse |

Use repository wrappers and pinned tool versions where available. A lockfile constrains resolution but does not establish trust in every package it references.

### BuildKit, Buildx, Kaniko, and Buildah

BuildKit executes container builds with a dependency graph, caching, and dedicated secret mounts. Buildx is a Docker CLI extension that exposes builder instances and features such as multi-platform builds. Buildah builds OCI images without requiring a Docker daemon and supports different privilege models depending on the environment.

Kaniko was designed to build Dockerfiles inside containers without a Docker daemon. The original GoogleContainerTools repository was archived in June 2025; identify the maintained implementation and its support model before adopting or retaining it. See the [original repository status](https://github.com/GoogleContainerTools/kaniko). Daemonless does not automatically mean rootless or safely isolated.

### Multi-stage builds, layers, and invalidation

Multi-stage Dockerfiles separate compilation tools from the runtime image, reducing runtime size and exposed tooling. Copy dependency manifests before frequently changing source so dependency installation can reuse a layer. Keep the build context narrow and exclude credentials and irrelevant files.

Cache reuse depends on the build instruction and its relevant inputs. A command that downloads mutable remote content may reuse old content through cache or fetch new content after invalidation. Pin that content and refresh it deliberately. Pass secrets through dedicated mechanisms rather than `ARG`, `ENV`, or copied files that can persist in layers or metadata.

### Dependency caches and remote build caches

A dependency cache stores downloaded packages; a build cache stores computed outputs keyed by inputs. Include the relevant operating system, architecture, toolchain, dependency lock hash, and configuration in keys. A coarse key causes incorrect reuse or contamination; a key containing every commit SHA may prevent useful reuse.

Restore prefixes cautiously and validate retrieved content. BuildKit supports [external cache backends](https://docs.docker.com/build/cache/backends/), useful when ephemeral runners cannot keep local state. Measure bytes transferred and time saved: a remote cache can cost more than recomputation.

### Artifacts versus caches, retention, and immutability

Artifacts are intentional outputs needed by another job, a release consumer, or an audit. Caches are disposable acceleration data. A cache miss should trigger correct recomputation; losing a release artifact can prevent recovery. Never make a cache the sole copy of a release or test evidence.

Set artifact retention by purpose: temporary test reports, incident evidence, release binaries, and provenance have different lifetimes. Protect release objects from overwrite and premature garbage collection, including untagged digests referenced by running deployments. A content digest identifies bytes but does not itself prevent deletion.

### Registries and version identifiers

OCI registries store images and other supported artifacts; Maven, npm, and PyPI registries distribute ecosystem-specific packages. Use repository-level permissions, authenticated transport, checksum verification, and explicit internal package routing. Registry retention and replication are part of CI reliability.

Semantic versions communicate compatibility intent. Git SHAs identify source commits. Immutable content digests identify artifact bytes. Associate all three where useful: `v2.4.1` points to source revision X and image digest Y. A tag containing a Git SHA can still be overwritten unless registry policy prevents it.

### Multi-architecture builds and cross-compilation

Cross-compilation runs a compiler on one architecture to produce code for another. Multi-architecture image builds produce platform-specific images and an OCI index or manifest list that selects among them. Emulation can run foreign-architecture commands but may be slow or differ from native behavior.

Test native dependencies and runtime behavior on supported targets. Keep architecture-specific caches separate. Publish the aggregate image index only after every required platform image is present and validated; record both index and child digests when diagnosing platform-specific failures.

### Provenance, traceability, and promotion

Record artifact digest, source revision, builder identity, workflow revision, dependency information, and verification results. Promote the tested bytes between environments by digest instead of rebuilding for production. Supply environment-specific configuration separately. Rebuilding from the same source can change dependencies and invalidates the claim that production runs the tested artifact.

## 5. Testing & Quality Gates

### Test layers

| Test type | Main question | Typical trade-off |
| --- | --- | --- |
| Unit | Does a small unit behave correctly in isolation? | Fast feedback with limited integration realism |
| Component | Does a bounded service or component work through its interface? | Dependencies may be simulated or local |
| Integration | Do real components interact correctly? | More setup, state, and failure sources |
| End-to-end | Does a complete user or system path work? | High realism, slower and harder diagnosis |
| Acceptance | Does behavior satisfy agreed requirements? | A purpose, potentially implemented at several test layers |

Terminology varies by team; state the boundaries and dependencies instead of arguing over labels. Choose tests around failure modes and keep expensive end-to-end checks focused on critical paths.

### Static analysis, linting, formatting, and type checking

Formatting removes style disputes; linting finds suspicious patterns; type checking detects incompatible assumptions; static analysis inspects code without running the application. Run inexpensive checks early and make local execution easy. Pin tool versions so a formatting update does not unexpectedly fail unrelated changes. These checks complement runtime tests rather than establish runtime correctness.

### Contracts, API compatibility, and schemas

Contract tests verify the expectations between a consumer and provider. Compatibility checks compare API or schema changes against supported clients: removing a field or changing its meaning may break callers despite a valid schema. Database changes should support deployment overlap, often through expand-and-contract migrations. Validate wire format, backward compatibility, and semantic expectations separately.

### Infrastructure tests

For Terraform, use formatting, validation, policy checks, reviewed plans, and targeted tests of module behavior. Validation alone does not prove cloud permissions or provider operations will succeed. Planning can access state and credentials, so untrusted contributions require a separate trust path.

For Helm, lint and render representative values, then validate manifests against target Kubernetes schemas and admission policy. Test rendered resources and upgrade behavior in disposable environments where warranted. YAML parsing cannot catch an unsupported API version, missing CRD, or unsafe security context by itself.

### Vulnerability scans and gate policy

Scan application dependencies, container OS packages, and relevant infrastructure definitions. Distinguish source dependency scans from scans of what was actually shipped. Findings depend on advisory database freshness and package identification; severity alone does not establish exploitability.

Define blocking criteria using severity, exploitability, exposure, fix availability, and organizational policy. Exceptions need an owner, reason, and expiry. Rescan retained release images as advisories change, and define how scanner outages affect trusted releases rather than silently treating an unavailable scan as a pass.

### Coverage and quality thresholds

Line and branch coverage show executed code, not whether assertions prove useful behavior. Use thresholds to detect regressions and identify missing tests for important paths. Combine coverage with behavioral review, critical-path tests, and risk-based checks. Aggregating reports from shards requires consistent source paths and tooling; an incorrect merge can produce reassuring but false numbers.

### Parallel tests, sharding, and aggregation

Parallelize independent suites and shard by historical duration to balance work. Give each shard isolated databases, ports, identifiers, and artifact paths. Merge JUnit-style results and coverage only after accounting for every expected shard. The final gate must fail when a required shard is missing, timed out, or canceled, even if all received reports passed.

### Flaky tests and quarantine

A flaky test changes outcome without a relevant change in inputs. Investigate ordering dependencies, clocks, asynchronous waits, shared state, resource pressure, and external services. Preserve first-failure logs, random seeds, and attempt history. A successful retry is evidence of instability, not proof that the first failure was harmless.

Quarantine can restore feedback while an owner repairs the test, but quarantined tests should continue running visibly with an expiry and tracked defect. Measure flake frequency and time lost; permanent silent exclusion weakens the gate.

### Ephemeral environments, service containers, and fixtures

Use disposable environments when integration needs real dependencies or realistic deployment behavior. Service containers provide local databases or queues; fixtures provide controlled initial data. Wait for readiness rather than fixed sleeps, seed predictable data, and use unique names per job.

Set TTLs, cost limits, restricted network access, and a cleanup reconciler. Avoid copying sensitive production data into test environments. Shared environments save setup cost but create ordering and contention problems that are easily mistaken for application defects.

### Fail-fast versus comprehensive execution

Fail-fast saves resources when an expensive matrix is already invalid. Comprehensive execution collects all failures and helps during broad refactoring or release qualification. Use fast checks for early feedback, while deciding explicitly whether independent diagnostics continue after failure. Always retain enough evidence to explain why the required gate failed.

## 6. CI Security & Supply Chain

### Threat model and trust boundaries

Assume untrusted repository code can execute arbitrary commands, including through dependency installation, test configuration, and build plugins. Identify boundaries between forks and trusted branches, jobs and hosts, repositories and shared storage, and build identities and production identities. Review who can edit the workflow, provide inputs, modify an included template, or publish a consumed artifact.

Enforce sensitive policy outside configuration that the contributor can freely change. A YAML condition is insufficient if the same actor can remove it or request a more privileged runner.

### Secrets management, masking, and rotation

Fetch secrets only for authorized jobs, preferably just in time from an external secret manager with short leases. Masking reduces accidental log exposure but cannot stop a malicious process from encoding or transmitting the secret. Avoid putting secrets in command arguments, caches, artifacts, shell traces, or container layers.

Scope access to the environment and operation, audit retrieval, rotate credentials after suspected exposure, and ensure expiration does not strand partial operations. A secret available to a build is available to code executed by that build.

### OIDC federation and least privilege

OIDC federation exchanges a CI-issued identity token for short-lived cloud credentials, removing the need to store a long-lived cloud key. The cloud trust policy must validate issuer, audience, and authorized subject claims such as repository, branch, environment, and workflow identity where supported. Trusting every token from a shared issuer gives other projects a path to the role.

Grant only required actions on required resources, separate build, publish, and deploy roles, and restrict which jobs may request tokens. OIDC reduces credential lifetime; it does not make malicious code safe while authorized credentials remain usable. GitHub documents this model in [OIDC hardening](https://docs.github.com/en/actions/concepts/security/openid-connect).

### Fork PR and merge-request security

Run untrusted contributions without release secrets, production reachability, or access to privileged persistent runners. Restrict their token permissions and cache writes. Maintainer approval to start a run is an execution decision, not proof that the code is safe.

GitHub `pull_request_target` executes in an elevated base-repository context. Checking out and running the contributor's code there can expose secrets and write permissions. Prefer a low-trust validation path and a separately authorized release path. Treat artifacts from untrusted runs as attacker-controlled input too. See [GitHub's security guidance](https://docs.github.com/en/actions/reference/security/securely-using-pull_request_target).

### Runner isolation and privileged execution

Privileged containers and host Docker socket mounts can grant extensive control over the node and adjacent workloads. Place unavoidable privileged builds in dedicated pools without production credentials or access to trusted jobs. Prefer a compatible rootless builder or stronger VM isolation when the workload permits it. Apply egress controls, avoid cloud metadata credential exposure, and keep host mounts and capabilities minimal.

### Dependency confusion, malicious packages, and typosquatting

Dependency confusion occurs when resolution selects an attacker's package, often from a public registry, for an intended internal name. Typosquatting exploits misspelled package names. A correctly named dependency can also be compromised upstream.

Use explicit namespace routing, controlled registries, reviewed lockfile changes, integrity verification, and restrictions on package lifecycle scripts where practical. A registry proxy improves availability and governance but can still faithfully cache a malicious package.

### Pipeline and command injection

Pipeline injection changes the interpreted workflow or generated configuration. Command injection occurs when untrusted values become shell syntax. PR titles, branch names, commit messages, filenames, and manual parameters are inputs, not trusted program fragments.

Avoid interpolating event values directly into inline shell code. Pass them through environment variables or argument APIs, quote expansions, validate expected formats, and never use `eval` on contributor input. Environment passing avoids one interpolation boundary but does not help if the script later evaluates the value. In privileged contexts, prefer fixed operations over caller-supplied command strings.

### Pinning actions, containers, and dependencies

Pin actions and reusable workflows to reviewed full commit IDs, containers to immutable digests, and language dependencies with lockfiles and integrity data. Floating tags can move. Pinning makes changes reviewable but also requires a timely update process for security fixes. Inspect transitive scripts and downloads: a pinned action that fetches an unverified executable still has a mutable input. See [GitHub secure-use guidance](https://docs.github.com/en/actions/reference/security/secure-use).

### SBOM, SLSA, signing, and attestations

An SBOM inventories components, commonly using SPDX or CycloneDX. It supports vulnerability investigation and license review; it is not proof that a package is safe. Provenance describes how an artifact was built and binds that information to its digest. An attestation is a verifiable statement about a subject, such as provenance or scan results.

SLSA describes increasing supply-chain guarantees. In the v1.2 build track, levels progress from provenance to authenticated hosted-build provenance and stronger hardened-build guarantees. Name the track and specification version when discussing a level. Claiming a level requires meeting its requirements, not merely installing a signing tool. See [SLSA build-track basics](https://slsa.dev/spec/v1.2/build-track-basics) and [tracks](https://slsa.dev/spec/v1.2/tracks).

Sigstore/Cosign can sign artifacts and attestations, including keyless signing tied to OIDC identity. Consumers must verify the digest, trusted issuer, expected signer identity, and relevant attestation policy. A valid signature from an unauthorized workflow must be rejected. Signing a malicious build accurately records an authorized signature; it does not establish benign behavior. See [Cosign verification](https://docs.sigstore.dev/cosign/verifying/verify/).

### Cache poisoning, artifact tampering, and isolation

Treat restored cache content as potentially executable: compiled objects, package hooks, and tool binaries can affect later builds. Separate write permissions and namespaces by trust domain; untrusted PRs must not seed caches later trusted by a release. Protect artifact storage, verify content digests, and authenticate provenance before consumption. Checksums supplied alongside an attacker's artifact prove consistency, not authenticity.

## 7. Performance & Scalability

### Critical path and parallelism

Pipeline duration is bounded by the longest dependency path, including queue and setup delays. Speeding a noncritical job may save compute without changing developer wait time. Measure per-job queue, provisioning, checkout, dependency resolution, execution, and upload times before changing the graph.

Parallelism helps independent work until shared CPU, storage, databases, or API limits become bottlenecks. Ten shards contending on one saturated database can be slower and less reliable than four balanced shards.

### Cache hit and miss analysis

Separate a cache lookup hit from a useful hit: restored data may still require almost all computation. Record restore time, upload time, hit rate, key distribution, bytes, and build work avoided. Compare cold and warm runs. Unexpected misses often follow toolchain changes, architecture mismatch, volatile keys, or a new runner pool without external cache access.

### Distributed caches: S3, MinIO, and shared storage

Object storage such as S3 or MinIO can share cache archives across disposable runners. Use scoped credentials, encryption, lifecycle expiry, capacity limits, and nearby storage. Shared filesystems offer low-latency access for some workloads but require correct locking, permissions, and failure isolation.

Prefer immutable cache entries or controlled writers; concurrent archive replacement can lose useful content. Treat missing or corrupt cache entries as misses and rebuild. Keep release artifacts in a separately managed durability and retention path.

### Autoscaling, cold starts, and scale-to-zero

Provisioning time includes VM boot, node registration, image pull, runner registration, and job setup. Scale-to-zero reduces idle cost but places all those delays on the first job. Maintain a small warm pool for latency-sensitive work and allow batch jobs to tolerate cold starts. Use measured queue demand, provisioning latency, quotas, and burst limits rather than unlimited scaling from a single queue spike.

### Concurrency, fairness, and capacity planning

Track queue latency by tenant and workload class, not only globally. Per-project limits prevent a large matrix from starving urgent incident builds. Reserve capacity for release or recovery workflows with explicit access policy, while preventing priority traffic from consuming the entire platform indefinitely.

Use Little's Law carefully: average jobs in a stable system equal arrival rate × average time in the system. For a hypothetical 10 jobs/second with 60 seconds of execution, average busy execution capacity is about 600 jobs, before burst and failure headroom. Include queue time when estimating total jobs in the system; use workload distributions and load tests for sizing actual machines.

### Resource requests, CPU throttling, and OOM

Requests influence Kubernetes scheduling; limits constrain runtime use. CPU limits can throttle compilation even on a seemingly quiet node. Memory limits can terminate a process with OOM, while node memory pressure can evict a pod. Include helper containers, service containers, ephemeral storage, and build subprocesses in sizing. Choose workload-specific classes rather than applying one large limit to every job.

### Monorepos and affected-project detection

Map changed files to owned components, then traverse reverse dependencies to find affected consumers. Changes to shared libraries, lockfiles, generators, or CI tooling may require much broader validation than a service-directory filter suggests. Use the correct merge base, obtain sufficient Git history, and fall back to broad validation when dependency information is incomplete.

### Cost, transfer overhead, and redundant work

Measure cost per useful verified change alongside latency and reliability. Right-size workers, remove duplicate pipelines, cancel obsolete verification jobs, and avoid oversized matrices. Incremental compilation and content-addressed caches eliminate repeated work when their input models are correct.

Artifact compression, transfer, and registry access can dominate small jobs. Keep runners near registries, use trusted mirrors or dependency proxies, and transfer only necessary outputs. Test sharding should balance historical runtime without paying excessive setup cost for each shard. Optimize the measured bottleneck rather than maximizing cache hit rate or runner utilization in isolation.

## 8. Reliability & High Availability

### Control plane and execution failure domains

The control plane manages scheduling, identity, configuration, and state. The runner/data plane executes jobs and moves logs, caches, and artifacts. Their failures differ: a failed runner affects assigned work; a failed coordinator may stop dispatch and result reporting across the fleet. Registry, object storage, DNS, identity, and network services can affect both.

Separate pools by trust and workload, spread capacity across failure domains, and avoid coupling artifact durability to disposable runners. Document what existing jobs can continue doing during a control-plane outage; continuation depends on the platform and remaining dependencies.

### Highly available services

For platforms that support it, replicate stateless frontends and coordinate durable databases, queues, repositories, and object storage through supported HA designs. Account for replication lag, failover behavior, backups, and consistency. A redundant frontend cannot compensate for a single unavailable database.

For Jenkins, agent scaling does not remove controller statefulness. Use supported recovery or commercial HA capabilities where appropriate, and divide controllers to contain blast radius. Do not assume increasing a controller Deployment's replica count makes shared state safe.

### Runner failures, retries, and duplicate attempts

When a runner disappears, the platform eventually detects loss and marks or retries the job according to its semantics and configuration. Do not assume transparent rescheduling or exactly-once execution. An earlier attempt may have completed a remote write before losing its acknowledgment.

Make computation repeatable and side effects idempotent using immutable destinations, request IDs, conditional creation, or transactions. Record attempt IDs separately from the logical build ID. Retry infrastructure failures within a budget, avoiding storms during a shared outage.

### Partial publishing and cancellation

Upload to a unique staging location, verify completeness and digests, then expose the release through an atomic or conditional final operation supported by the registry. After a timeout, query whether that final operation succeeded before repeating it. Do not delete an existing artifact simply because the client did not receive confirmation.

Handle cancellation with traps or platform cleanup hooks, but rely on TTLs and reconcilers for abandoned environments and workspaces. A canceled job can leave remote operations running; reconcile their actual state.

### Storage, network, DNS, and external dependency outages

Use bounded timeouts, backoff with jitter, and controlled retries. Treat a cache outage as a performance incident if safe recomputation is possible; treat a required registry or verification outage according to release policy. Do not bypass authentication or signature checks to restore throughput.

Use approved dependency mirrors and retained immutable packages where appropriate. Avoid retrying every job simultaneously after recovery. Observe DNS errors, TLS failures, connection pools, bandwidth, and service quotas separately so a network outage is not misdiagnosed as broken tests.

### Disaster recovery

Define recovery point objectives (acceptable data loss) and recovery time objectives (acceptable downtime) for configuration, identity, metadata, artifacts, and logs. Back up platform configuration and database state consistently, along with encryption keys and a secure recovery path for secrets. Source repositories do not necessarily include UI settings, job history, artifacts, or externally stored credentials.

Retain critical artifacts in durable independent storage with tested access during recovery. Replication handles some infrastructure failures but can replicate deletion or corruption; keep recoverable backups and test restoration into an isolated environment. Reconcile metadata with stored objects before reopening release operations.

### Draining, upgrades, and overload protection

Stop assigning new jobs before terminating a runner; allow enough time for upload and cleanup as well as script execution. Roll upgrades through canary pools and avoid replacing all warm capacity simultaneously. Define an upper drain deadline so a stuck build cannot block maintenance forever.

Bound queues and expensive admission operations, apply tenant limits, and expose rejection or delay clearly. Use circuit breakers or reduced dispatch during downstream outages. Overload protection should preserve the control plane and critical workflows while allowing the system to recover without a retry avalanche.

## 9. Kubernetes-Based CI

### Executor architecture and pod lifecycle

With GitLab's Kubernetes executor, the runner manager requests a job from GitLab, prepares its execution specification, and asks the Kubernetes API to create a pod. The scheduler places the pod; the kubelet pulls images and starts containers. The pod usually includes a build container, a runner helper, and requested service containers. The manager coordinates execution and reporting.

Source preparation, cache restore, script execution, artifact/cache upload, and cleanup happen through the executor's lifecycle. Exact ordering, containers, and attach/exec behavior depend on runner configuration and version. The manager is not itself the build pod, and a healthy manager does not prove that new job pods can schedule. See the [Kubernetes executor documentation](https://docs.gitlab.com/runner/executors/kubernetes/).

### Pod templates, identity, RBAC, and security contexts

Use controlled pod templates to define images, resources, mounts, networking, and security settings. Separate the manager's Kubernetes permissions from the job pod's identity. The manager may need to create and observe job resources; most builds do not need API access at all. Disable service-account token mounting for jobs that do not need it, and restrict permissions when they do.

Apply non-root execution where compatible, minimal capabilities, seccomp, and restrictions on privilege escalation and host mounts. Restrict contributor overrides of namespace, service account, images, and pod fields; otherwise a safe default can be bypassed through job input.

### Quotas, placement, and workload isolation

ResourceQuotas bound aggregate namespace consumption; LimitRanges provide defaults or constraints for individual containers. Node selectors and affinity constrain placement; taints repel pods unless a matching toleration allows them. A toleration permits placement but does not reserve a node or force the pod there.

Use dedicated CI node pools and placement rules for costly or privileged builds. A namespace is a useful administrative boundary, but isolation also depends on RBAC, network policy, storage, admission controls, and host security. Do not place untrusted privileged builds beside production services merely because namespaces differ.

### DinD, host socket, rootless BuildKit, and Buildah

| Approach | Mechanism | Operational/security concern |
| --- | --- | --- |
| Docker-in-Docker (DinD) | A Docker daemon runs inside the job environment | Common configurations require privilege; nested storage and networking add complexity |
| Host Docker socket | Job sends commands to the node's daemon | Job can often create host mounts or privileged containers and gain host control |
| Rootless BuildKit | Build processes run without host root privileges | Requires compatible user namespaces, filesystem, and security configuration |
| Buildah | Builds OCI images without a Docker daemon | Rootless support and required kernel/storage settings must be verified |

Evaluate the actual privileges, kernel features, and storage driver rather than selecting by tool name. Keep registry credentials narrow and use build secret mechanisms that do not persist credentials in outputs.

### Workspaces and cache volumes

An ephemeral workspace gives each job a clean starting point. Persistent volumes can accelerate large checkouts or builds but require ownership, cleanup, access-mode compatibility, and trust separation. Stale locks and permissions on a reused volume can make failures appear intermittent. Prefer external caches with scoped access when persistence would otherwise couple unrelated jobs.

### Eviction, disruption, and autoscaling

A node drain, spot interruption, memory pressure, or disk pressure can terminate work. PodDisruptionBudgets apply to supported voluntary disruptions and are not a guarantee that a job will finish. Autoscaler decisions depend on requests, placement constraints, node pool limits, and provisionable capacity.

Tune termination grace periods, maintain reserve capacity for important jobs, and make retries safe. Preserve logs and attempt metadata outside the pod. A pod may be pending because no available or provisionable node satisfies its constraints even when aggregate cluster CPU appears free.

### API throttling, concurrency, and cleanup

Each job can create pods, secrets, watches, log streams, and cleanup calls. Raising runner concurrency may overload the Kubernetes API or admission webhooks before nodes are full. Observe API request latency, 429 responses, admission latency, and manager resource use; tune concurrency and request rates together.

For ephemeral runners, remove both execution resources and platform registration. Use constrained cleanup automation with ownership labels and age thresholds so it cannot delete active jobs. A separate reconciler should find orphaned pods, volumes, and test environments after manager crashes.

## 10. Observability & Troubleshooting

### Pipeline and runner metrics

Measure queue time, provisioning time, execution duration, upload duration, throughput, and success rate with explicit start/end events. Separate application failures, infrastructure failures, cancellations, and retries. Report user-visible first-attempt reliability alongside eventual success so retries cannot conceal platform problems.

Runner metrics include active jobs, available slots, CPU/memory/disk use, registration failures, dispatch failures, and pool saturation. Break down stage latency, critical-path time, cache effectiveness, and flaky-test frequency. Use distributions such as p50/p95/p99: an average can hide starvation in a smaller pool.

### DORA metrics and CI's contribution

Change lead time measures commit to successful production deployment; CI duration is one component. Deployment frequency measures production deployments, not successful builds. Change failure rate concerns deployments that cause production failures. Failed deployment recovery time measures recovery from a failed deployment and differs from repairing any arbitrary incident.

The traditional four metrics in the checklist remain useful interview topics. Current DORA guidance also includes deployment rework rate. Join CI records to deployment and incident data using revision, artifact digest, service, environment, and deployment ID; CI alone cannot produce all of these measures. See [DORA's metric definitions](https://dora.dev/guides/dora-metrics/).

### Metrics, logs, dashboards, and tracing

Use platform exporters and runner telemetry with Prometheus and Grafana or equivalent systems. Correlate logs by pipeline, job, attempt, runner, pod, revision, and artifact digest. Keep high-cardinality identifiers in logs or traces rather than unbounded Prometheus labels. Redact credentials without destroying the error context.

Tracing can connect webhook reception, queueing, runner provisioning, registry requests, and storage upload when those systems propagate context. Build a timeline from logs where propagation is unavailable. Sampling should preserve enough slow and failed executions to diagnose tail latency.

### Troubleshooting decision path

Start by identifying the last successful transition. A platform-pending job with no assigned runner has a different owner from a Kubernetes Pending pod or a running pod with a stalled script.

| Symptom | First evidence | Likely directions |
| --- | --- | --- |
| Job remains queued; no pod exists | Eligibility, runner heartbeat, tags/labels, pool limits | Protected scope mismatch, offline manager, dispatch or entitlement limits |
| Pod exists but is Pending | Pod events and scheduling conditions | Requests, taints, affinity, quotas, PVC binding, admission, pool limits |
| Pod starts but build does not | Container state, helper logs, readiness, startup timing | Image pulls, missing shell, service startup, attach/exec permissions |
| Script hangs | Last output, process tree, network timeouts | Deadlock, unavailable dependency, interactive prompt, exhausted connection pool |
| Failures occur intermittently | Attempt history and runner/node correlation | Flaky tests, resource contention, DNS, shared mutable state |
| Same source produces different output | Toolchain, dependency, environment and artifact diffs | Mutable inputs, timestamps, architecture, undeclared input, cache contamination |
| Upload fails after tests pass | Storage response, token validity, payload size | Permission, expiry, proxy timeout, disk or object-store outage |

For a known Kubernetes job pod, these commands collect a bounded starting point. Replace the uppercase placeholders and use an authorized context; logs can contain sensitive data.

```bash
kubectl -n CI_NAMESPACE get pod JOB_POD -o wide
kubectl -n CI_NAMESPACE describe pod JOB_POD
kubectl -n CI_NAMESPACE logs JOB_POD -c build --tail=100
kubectl -n CI_NAMESPACE logs JOB_POD -c helper --tail=100
kubectl -n CI_NAMESPACE top pod JOB_POD --containers
```

`top` requires metrics support. Container names vary by executor. Capture previous container logs with `--previous` when a restart occurred, and retain evidence before the pod is cleaned up.

### Cache, dependency, OOM, disk, and network failures

For cache suspicion, compare a clean rebuild against a cached run, preserve the problematic key and metadata, then invalidate the affected scope. Broad cache deletion destroys diagnostic evidence and can cause a dependency-download surge. Check lockfile, registry, proxy, and checksum behavior before attributing all inconsistent builds to cache corruption.

For memory failures, inspect container termination reason and node events. Exit code 137 means SIGKILL and does not alone prove an OOM. For disk failures, check bytes, inodes, container layers, workspace volume, and node ephemeral storage. For network bottlenecks, distinguish DNS, connection setup, TLS, proxy behavior, registry throttling, packet loss, and bandwidth limits. Compare failing and successful pools using the same artifact and dependency endpoint.

### CI SLIs, SLOs, and alerting

Define an availability SLI around a useful outcome, such as eligible verification jobs reaching a trustworthy terminal result without infrastructure failure within a deadline. Define a latency SLI such as the fraction of eligible jobs that start within 60 seconds. State how application failures, cancellations, maintenance, and retries enter the numerator and denominator; exclusions should remain visible in separate metrics.

Hypothetical targets might be 99.9% infrastructure reliability and 95% of standard jobs starting within 60 seconds over 28 days. These are examples, not universal standards. At 100,000 eligible jobs, a 99.9% success target allows 100 bad jobs in that window. Alert on fast and sustained error-budget burn, then use queue, storage, and saturation signals for diagnosis. Detect missing telemetry so an unavailable coordinator cannot appear perfectly healthy because it stopped emitting failures.

## 11. Expert Interview Scenarios

For design questions, establish workload, trust levels, latency targets, quotas, and recovery requirements before choosing components. For incidents, build a timeline and preserve evidence before changing state. A strong answer explains how the proposed fix will be verified.

### 1. Design a highly available CI platform for 1,000+ concurrent jobs

Separate orchestration, execution, artifact storage, and cache storage. Use the platform's supported HA topology for its stateful dependencies, then distribute runner pools across zones and trust domains. Bound tenant concurrency and reserve capacity for urgent workflows. Make artifact durability independent of runner lifetime.

Size by workload distributions. If each job hypothetically requests 2 CPU and 4 GiB memory, 1,000 concurrent jobs request 2,000 CPU and about 3.9 TiB memory before helper/service overhead and headroom. Validate Kubernetes API load, registry throughput, database/queue capacity, network egress, and cloud quotas. Test burst admission, a zone loss, artifact-store degradation, and recovery; measure tail queue time and infrastructure failure rate.

### 2. Explain a GitLab CI job's complete Kubernetes lifecycle

GitLab evaluates the pipeline and makes a job eligible. A matching runner manager requests it, receives execution details, and creates the configured pod resources through the Kubernetes API. Admission and scheduling run, the node starts build/helper/service containers, and execution prepares source, caches, and required artifacts before the script runs.

The runner streams status/logs, executes configured post-script behavior, uploads reports/artifacts and caches as applicable, reports the final status, and cleans up resources. Explain where failures occur: runner matching, API authorization, scheduling, image pull, source preparation, script, upload, or cleanup. Exact executor behavior must match the installed runner version.

### 3. Jobs are pending despite available cluster capacity

First determine whether the job has reached Kubernetes. If no pod exists, inspect runner eligibility, tags, protected settings, paused/offline state, manager concurrency, and coordinator connectivity. If a pod exists, read scheduling events and check requests, quota, LimitRange defaults, selectors, affinity, taints, architecture, PVC binding, admission, and image-pull status.

Free aggregate CPU does not imply that any one permitted node can fit the pod. Confirm whether the autoscaler can provision a matching node under its pool limits and cloud quota. Fix the specific constraint and verify queue-to-start latency; do not remove isolation rules simply to make the pod schedule.

### 4. Reduce a 30-minute pipeline to five minutes

Measure the critical path first, including queue/setup/upload time. In a hypothetical pipeline with two minutes of overhead, eight minutes of serial build, and twenty minutes of tests, test sharding alone cannot meet five minutes: the unchanged build and overhead already take ten.

Reduce the build through correct incremental/remote caching, remove stage barriers, balance test shards, reuse one built artifact, and optimize provisioning and transfers. Apply affected-project selection only with reliable dependency analysis. Benchmark cold-cache and warm-cache p95, cost, and first-attempt reliability while preserving required coverage. State when the target requires a deeper build redesign rather than more runners.

### 5. Secure isolated runners for untrusted pull requests

Use a separate low-trust pool with disposable VMs or a suitably hardened execution boundary. Provide no production secrets, deployment permissions, shared writable release cache, or access to trusted workspaces. Restrict network access and cloud metadata exposure; keep tokens minimal and short-lived.

Authorize the trusted release workflow independently and build from a reviewed protected revision. Treat all PR-produced artifacts as untrusted. Verify cross-job isolation by testing attempts to read prior workspaces, write trusted caches, select privileged pools, and reach restricted services.

### 6. Identical source produces nondeterministic builds

Compare the full inputs: toolchain and base-image digests, transitive dependency resolution, package registry responses, locale/timezone, timestamps, paths, architecture, random seeds, filesystem order, and network-derived generated data. Compare clean outputs and unpack artifacts to locate the first differing bytes.

Check for mutable tags, Maven snapshots, unpinned downloads, stale generated files, or an incomplete cache key. Pin and normalize inputs, then rebuild on separate clean workers. If outputs still differ, instrument the build to discover undeclared reads and environmental influence.

### 7. Distributed caching for autoscaled ephemeral runners

Use nearby object storage or a compatible remote action cache with trust-scoped access. Derive keys from relevant declared inputs, allow only authorized producers to write release-consumed entries, and use immutable entries or controlled writers. Bound size and retention, encrypt data, and keep secrets out.

Treat cache failure as a miss, measure total time saved after transfer, and avoid making cache availability a release dependency. Test cold starts, concurrent writes, poisoned or corrupt archives, and a cache-store outage. Keep release artifacts outside the cache retention policy.

### 8. Runner dies halfway through artifact publishing

Identify the logical release and attempt IDs, then query storage for staged objects and the final publication record. Verify sizes and digests before retrying; a lost acknowledgment can mean the release already exists. Resume or repeat upload to a unique attempt location, then conditionally finalize if the release is still absent.

If a conflicting digest exists for that version, stop and investigate instead of overwriting it. Clean incomplete staging objects later through retention rules. Demonstrate recovery by killing a test publisher before upload completion, after upload, and after finalization but before acknowledgment.

### 9. Concurrent pipelines overwrite the same release artifact

Publish candidates under unique immutable identifiers and enable registry rules that reject release-version overwrite. Serialize the logical release operation where the platform's lock scope is sufficient, and use conditional storage writes to protect against other publishers outside that scope.

If two candidates request the same version, the second must fail or confirm that the existing bytes are identical under an explicit idempotency policy. An older queued pipeline must not replace a newer approved release merely because it finishes later. Validate with deliberately overlapping runs.

### 10. Supply-chain model with OIDC, SBOM, signing, and provenance

Build a reviewed revision on an isolated builder with pinned inputs. Generate an SBOM for the actual artifact and provenance bound to its digest. Obtain a short-lived publish/signing identity through OIDC with restrictive claims, then sign artifacts and attestations using the authorized workflow identity.

At promotion, verify digest, signature identity and issuer, provenance builder/source policy, and required scan results. Separate untrusted validation from privileged publishing. Test rejection of the wrong repository, wrong workflow identity, modified artifact, missing attestation, and expired policy exceptions. Signing without enforcement leaves the trust decision incomplete.

### 11. CI failures caused by cluster autoscaling

Correlate job attempts with node creation, scale-down, drain, eviction, and spot-interruption events. Look for unschedulable requests, pool maximums, zone constraints, cloud quota, slow image pulls, and manager timeouts shorter than node provisioning time.

Distinguish scale-up delay from interruption of running work. Tune capacity buffers, supported disruption controls, job timeouts, and bounded infrastructure retries. Verify with controlled node replacement and a burst from minimal capacity, confirming that duplicate attempts cannot corrupt releases.

### 12. GitLab control-plane outage without losing critical artifacts

Store release artifacts durably outside disposable runner disks before the outage occurs, using supported object storage and registry arrangements with independent recovery. Back up the metadata and keys needed to map and access those objects. Independent object availability does not guarantee the normal GitLab-authenticated download path remains available.

During the outage, preserve existing runner workspaces and pending uploads where possible, and use only a preapproved authenticated recovery path. In-flight jobs may lose coordination or fail final reporting; do not promise that they all finish. Restore consistent control-plane state, reconcile stored digests and metadata, and retry safely. Exercise this recovery path before relying on it.

### 13. Monorepo selective builds and dependency-aware testing

Maintain a component dependency graph and compute changed files against the correct integration base. Select directly changed components plus affected reverse dependencies, accounting for shared tools, root lockfiles, schemas, generators, and deployment configuration. Generate bounded child pipelines or matrices from that result.

Use broad validation for unknown mappings and run periodic full builds to detect graph omissions. Keep gate names and result aggregation predictable for merge protection. Validate selection against historical changes and ensure each selected consumer is tested with the candidate dependency version.

### 14. Measure and improve CI reliability through SLIs/SLOs

Define which jobs are eligible, what counts as infrastructure failure, and the deadline for an acceptable result. Measure latency separately from availability and report first-attempt success, eventual success, cancellation, and retry rates. Segment by pool and workload to expose uneven service.

Set targets from developer needs and recovery workflows, allocate an error budget, and alert on burn rate. Use incident data to prioritize the largest causes, such as runner loss or artifact-upload failures. Verify improvement over representative load and include outage periods when telemetry was missing.

### 15. Compromised runner with production credentials

Contain the runner's access and stop new assignments; preserve disks, logs, process information, and cloud audit evidence through the incident procedure. Revoke exposed credentials and active sessions where supported, restrict affected federation trust, and investigate access to production. Coordinate containment so evidence collection does not unnecessarily extend attacker access.

Trace every repository, secret, cache, artifact, and deployment the runner could access. Quarantine suspect outputs, identify downstream consumers, and rebuild releases on clean trusted infrastructure after verifying source and dependencies. Replace the runner image and credentials; wiping the workspace alone is insufficient. Close the original trust-boundary failure and verify that low-trust jobs can no longer acquire production authority.
