# Kubernetes — SRE Expert Checklist

A Pod can be running while receiving no traffic, remain Pending while nodes look idle, or lose its process while its persistent data survives. An SRE needs to explain which component owns each transition, what evidence distinguishes the failure modes, and how to restore service without corrupting state. This guide connects Kubernetes internals to those operational decisions and follows the sixteen sections of the interview checklist.

The examples assume Linux worker nodes and familiarity with containers, TCP/IP, and basic Linux administration. Replace uppercase placeholders such as `NAMESPACE`, `POD`, and `NODE` before running commands. Start with the correct kubeconfig context and namespace; node-level commands require authorized host access. Debug containers need an approved image and may expose sensitive process or network data.

Kubernetes, cloud providers, CNI/CSI drivers, and installed controllers have separate release cycles. Use `kubectl version`, `kubectl api-resources`, `kubectl explain`, and the documentation for the deployed versions to confirm fields, feature gates, defaults, and compatibility. The guide emphasizes durable mechanisms and calls out features whose availability depends on those versions.

## Contents

1. [Fundamentals & Architecture](#1-fundamentals--architecture)
2. [Control Plane Internals](#2-control-plane-internals)
3. [Nodes & Container Runtime](#3-nodes--container-runtime)
4. [Workloads & Controllers](#4-workloads--controllers)
5. [Scheduling & Resource Management](#5-scheduling--resource-management)
6. [Kubernetes Networking](#6-kubernetes-networking)
7. [Storage & Persistent Volumes](#7-storage--persistent-volumes)
8. [Configuration & Secrets](#8-configuration--secrets)
9. [Autoscaling & Capacity Management](#9-autoscaling--capacity-management)
10. [Security & Multi-Tenancy](#10-security--multi-tenancy)
11. [Operators, CRDs & Extensibility](#11-operators-crds--extensibility)
12. [High Availability & Disaster Recovery](#12-high-availability--disaster-recovery)
13. [Observability & Troubleshooting](#13-observability--troubleshooting)
14. [Performance & Scalability](#14-performance--scalability)
15. [Kubernetes Operations & GitOps](#15-kubernetes-operations--gitops)
16. [Expert Interview Scenarios](#16-expert-interview-scenarios)

## 1. Fundamentals & Architecture

### Control plane and workers

The control plane stores cluster intent and coordinates changes. The API Server exposes that state, etcd persists it, the scheduler assigns unscheduled Pods to nodes, and controllers continually repair differences between desired and observed state. Worker nodes execute workloads through kubelet and a container runtime. Networking and storage plugins connect that execution to the surrounding infrastructure.

```text
kubectl / GitOps / controllers
             |
        API Server <----> etcd
          ^     ^
          |     +-------- scheduler: assigns Pods to nodes
          +-------------- controllers: reconcile API objects
             |
           kubelet ---- CRI runtime ---- OCI runtime ---- processes
             |                |
          CSI mounts       Pod network through CNI
```

Components communicate through the API rather than having the scheduler directly start a container. Losing the API can stop scheduling and reconciliation while existing processes and programmed network paths continue working. That survival is conditional: applications may still depend on API access, expiring credentials, DNS, or other failing services.

### Declarative state and reconciliation

A Deployment specifying three replicas expresses an ongoing requirement. Its controller creates or adjusts a ReplicaSet, which creates Pods. Kubelets then reconcile assigned Pods with local runtime state. Each loop observes current state, compares it with intent, performs a bounded action, and tries again after events or retries. These loops are asynchronous; accepting an API request does not mean the application is ready.

`spec` generally describes intent, while `status` reports observations and conditions. `metadata.generation` tracks changes to desired configuration, and a controller may expose `status.observedGeneration` to show which generation it processed. Reconciliation must tolerate stale reads, duplicate notifications, restarts, and partial success. Successful controllers are idempotent and converge without relying on a single uninterrupted workflow.

### API resources, groups, and versions

Kubernetes exposes REST resources such as `/api/v1/namespaces/team-a/pods` and `/apis/apps/v1/namespaces/team-a/deployments`. The core group uses `/api`; named groups use `/apis`. A resource is the REST collection, a kind identifies an object's schema, and a subresource such as `status` or `scale` exposes a narrower operation. Namespaced and cluster-scoped resources have different authorization and ownership implications.

API versions describe contracts, not separate independent copies of each object. The API Server can serve multiple versions and convert to a storage version. Removing a served version breaks clients and manifests that still use it even if the underlying object remains. Discover available APIs instead of assuming a manifest accepted by one cluster works everywhere.

### Cluster lifecycle and ownership

Bootstrap establishes PKI and trust, etcd, API endpoints, node credentials, runtime configuration, CNI, DNS, and storage integration. kubeadm assists with initializing and joining a cluster, but provisioning machines, operating add-ons, renewing trust, and testing recovery remain operational responsibilities. Cluster API represents infrastructure and cluster lifecycle declaratively through provider controllers.

Upgrades require a compatibility plan for control-plane components, nodes, APIs, admission, drivers, and applications. Disaster recovery must recover both Kubernetes state and application data; an etcd backup alone cannot restore database files on external disks.

| Option | What it provides | Responsibility that remains |
| --- | --- | --- |
| AKS, EKS, GKE | Provider-operated Kubernetes control plane and integrated lifecycle features | Workloads, identities, policy, data recovery, capacity, and the responsibilities defined by the selected service mode |
| Self-managed with kubeadm | Bootstrap and upgrade tooling for Kubernetes components | Infrastructure, control-plane HA, etcd, certificates, add-ons, patching, and recovery |
| Docker Swarm | Services, scheduling, and integrated orchestration around Docker | A different API and ecosystem; assess workload and operations requirements rather than expecting Kubernetes extensions |
| Nomad | Scheduling for containerized and other task types | Service discovery, secrets, networking, and policy architecture depend on the chosen integrations |

The interview tradeoff is operational ownership versus flexibility. A managed cluster reduces some infrastructure work; it does not automatically make application deployment, multi-tenancy, or regional recovery reliable.

### Namespaces and metadata

Namespaces scope names and many policies, quotas, and RBAC bindings. They share cluster infrastructure and are not by themselves a strong tenant boundary. Labels are structured identifiers used by selectors for routing, grouping, and placement. Annotations hold non-selectable metadata such as tool configuration. Avoid secrets in either because they commonly appear in inventory and logs.

Selectors connect independent objects: a Service label selector identifies backends, while a controller's selector identifies managed Pods. An accidental broad selector can send traffic to the wrong workload. Owner references identify an owner's UID for garbage collection; labels alone do not establish ownership. Namespaced owner references cannot be used to create arbitrary cross-namespace ownership.

## 2. Control Plane Internals

### API Server request lifecycle

For a typical object write, TLS establishes the connection, authentication identifies the caller, and authorization evaluates the requested verb, resource, namespace, and subresource. Admission and object processing then apply defaults, mutations, schema and semantic checks, and validating policy before persistence. Exact internal conversion and defaulting stages depend on the resource and request; the key boundary is that admission follows authorization and successful admission precedes durable storage. Reads do not generally pass through object admission.

Mutating admission can add or alter fields, such as injecting a proxy. Validating admission accepts or rejects the resulting object without modifying it. Mutating webhooks can be reinvoked, so they must tolerate repeated execution; validating webhooks must evaluate the final object rather than assume a particular mutation order. A successful write returns an API representation, not confirmation that downstream controllers completed their work. See the official [admission control description](https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/).

Webhook network dependencies become API availability dependencies. Use narrow resource matching, bounded timeouts, redundant endpoints, valid certificates, and a deliberate `failurePolicy`. `Fail` can block writes during an outage; `Ignore` can permit writes without that check. Neither choice substitutes for understanding the protected resource and failure consequences.

### API Priority and Fairness and throttling

API Priority and Fairness classifies requests using FlowSchemas and PriorityLevelConfigurations, allocating concurrency and queueing capacity so one source is less likely to starve others. It manages estimated execution seats, not simply a fixed number of HTTP requests per second. Long-running watches have special handling; do not equate the number of open watches with ordinary inflight mutations.

Separate client-side QPS/burst throttling, server-side queueing or rejection, and downstream storage/admission latency. A controller log saying it waited for its client rate limiter differs from an HTTP 429 from the server. Raising client QPS can intensify an already overloaded API. Compare request rate, rejected requests, APF queue wait, execution time, webhook latency, and etcd latency before changing limits.

### etcd: consensus and versioned state

etcd uses Raft to replicate committed state through a leader and a majority of members. Three members tolerate one member failure; five tolerate two. Two members still require both for quorum. Adding members increases replication cost, so member count is a failure-tolerance choice rather than a way to linearly increase write throughput.

MVCC retains key revisions so transactions and watches can reason about changes. Kubernetes uses versioned reads and optimistic concurrency to avoid silently overwriting concurrent changes. Watch history is finite: compaction removes old revisions, forcing sufficiently stale clients to obtain fresh state. Compaction and defragmentation differ: the first discards historical versions, while the second reclaims backend file space and can affect availability during execution.

A snapshot captures the persisted keyspace for recovery. Slow fsync, high disk latency, packet loss, oversized objects, or rapid write churn can delay consensus and make the entire API feel slow. Protect etcd with low-latency durable storage, peer/client TLS, restricted access, verified snapshots, and measured capacity.

### Scheduler and controllers

The scheduler considers unscheduled Pods, filters infeasible nodes, scores remaining candidates, and binds a selected node through the API. Plugins also participate in stages such as reserve, permit, and pre-bind, including storage coordination. Scheduler caches and assumptions reduce API round trips but introduce transient states that diagnostics must interpret.

Preemption may remove lower-priority Pods when doing so makes placement possible. It cannot solve every constraint: a wrong zone, unsupported device, or untolerated taint remains wrong after victims leave. A selected preemption target also does not mean the new Pod is already running.

Controller Manager runs loops such as Deployment, ReplicaSet, node, and endpoint reconciliation. Work queues coalesce keys, retry failures with rate limiting, and distribute processing among workers. Leader election usually uses Lease objects so one active controller-manager instance coordinates a given leader-elected set of loops. API Server replicas serve requests concurrently and do not share that same single-leader model.

The Cloud Controller Manager bridges node metadata/lifecycle, optional route management, and cloud load balancers. Modern volume provisioning and attachment generally use CSI and its controllers rather than assuming the CCM manages volumes. Provider architecture determines the exact split; see [Cloud Controller Manager responsibilities](https://kubernetes.io/docs/concepts/architecture/cloud-controller/).

### Informers, caches, and resourceVersion

An informer obtains a collection's current state and follows changes through watch, exposing a shared local cache and event handlers. Shared informers avoid every controller independently listing the same objects. Newer streaming mechanisms can combine initial state delivery with watch semantics; the operational objective remains synchronized state without repeated full polling.

Treat `resourceVersion` as an opaque API concurrency/watch token. Do not parse it as a timestamp or assume client arithmetic works. A stale update can receive a conflict and must re-read and retry safely. An expired watch history can return HTTP 410, requiring fresh synchronization. A cache read can lag a successful write, so reconciliation must tolerate temporarily missing or outdated objects.

HA therefore requires replicated API Servers behind a healthy load balancer, quorum-preserving etcd placement, and redundant schedulers/controllers with working leader election. Recovery includes client reconnects and cache synchronization, not just restarting processes.

## 3. Nodes & Container Runtime

### kubelet, CRI, and OCI

kubelet observes Pods assigned to its node, prepares volumes and credentials, asks the CRI runtime to create sandboxes and containers, runs probes, and reports status. It also reconciles static Pods from local configuration. It does not choose the node for a normal unscheduled Pod, and an application controller does not normally invoke kubelet directly.

CRI is the kubelet-to-runtime API implemented by containerd and CRI-O. OCI defines lower-level image and runtime standards used by implementations such as runc. Docker-built OCI-compatible images still work without Docker Engine being the node runtime. Check the actual CRI socket and runtime configuration before choosing diagnostic tools.

Linux namespaces isolate views such as networking, processes, mounts, and users. Cgroups account for and constrain resources. Cgroup v1 separates controllers across hierarchies; v2 uses a unified hierarchy with different accounting and control interfaces. kubelet and runtime cgroup-driver alignment matters, and kernel behavior must be interpreted for the installed cgroup mode. CNI configures workload networking, while CSI supplies the storage interface; neither is a replacement for CRI.

### Health signals, static Pods, and allocatable resources

Nodes publish conditions such as Ready, MemoryPressure, DiskPressure, and PIDPressure. Node Lease renewal provides a lightweight heartbeat in addition to status updates. The control plane can mark a node unhealthy or unreachable and apply taints, after which eviction depends on tolerations and controller behavior. A missed heartbeat does not prove the old application process stopped.

Static Pods are managed from kubelet's local manifest source, commonly for self-managed control-plane components. Mirror Pods expose their status through the API, but deleting a mirror object does not remove the underlying static Pod. Recovery and configuration changes must target the actual manifest and kubelet configuration.

Node capacity is raw hardware; allocatable is what Kubernetes makes available to Pods after configured reservations. `systemReserved`, `kubeReserved`, and eviction reservations protect host services and Kubernetes agents. Reserve real headroom for kernel memory, runtime, logging, and networking. A node filled to its physical capacity can lose the very processes needed to recover it.

### Shutdown, hooks, and node maintenance

On ordinary graceful Pod termination, kubelet runs a configured `preStop` hook and asks the runtime to stop containers with the applicable stop signal, commonly SIGTERM. The termination grace countdown includes hook execution. Processes still alive after the deadline are forcefully killed; applications need correct signal forwarding and bounded request draining. A shell wrapper that fails to `exec` the application can interfere with signal delivery. Native sidecars have additional shutdown ordering; ordinary application containers do not have a general dependency order.

`cordon` prevents ordinary new scheduling but leaves existing workloads running. `drain` cordons and requests eviction of eligible Pods, normally observing PDBs, while handling DaemonSets, unmanaged Pods, and local data requires deliberate options. It cannot migrate memory or local disk content. Graceful node shutdown support lets kubelet respond to an announced host shutdown when enabled and configured; sudden power loss cannot run hooks.

```bash
kubectl get node NODE -o wide
kubectl describe node NODE
kubectl get lease -n kube-node-lease NODE -o yaml
kubectl get pods -A --field-selector spec.nodeName=NODE -o wide
```

For maintenance, first establish spare placement capacity, workload quorum, storage mobility, and acceptable disruption. Then cordon and drain a single canary node with a bounded timeout, verify replacements and application health, and proceed gradually. Avoid routinely adding force or local-data deletion flags to make a blocked drain finish.

## 4. Workloads & Controllers

### Pods and workload choices

A Pod is a scheduling and lifecycle unit for tightly coupled containers. Its containers share a network namespace, IP address, and port space, and can share explicitly mounted volumes. They do not automatically share root filesystems or a PID namespace. A runtime sandbox anchors Pod-level execution context; the familiar pause container is an implementation detail of common Linux runtimes.

| Workload | Behavior | Interview consideration |
| --- | --- | --- |
| Deployment | Manages ReplicaSets for replaceable application replicas | Pod identity and node placement can change |
| ReplicaSet | Maintains a replica count for a selected Pod template | Usually owned by a Deployment rather than operated directly |
| DaemonSet | Places node-local agents on eligible nodes | Taints, selectors, and rollout behavior still affect coverage |
| StatefulSet | Maintains ordinal identity and per-replica storage associations | Does not implement database replication or leader election |
| Job | Drives finite work toward a completion criterion | Retried execution requires idempotent work or deduplication |
| CronJob | Creates Jobs on a schedule | Scheduling delays or duplicates must not corrupt the business operation |

Jobs separate target `completions` from maximum active `parallelism`. Indexed Jobs assign completion indexes so workers can process deterministic partitions, but duplicate execution remains possible. `backoffLimit`, per-index policies where supported, `activeDeadlineSeconds`, and Pod failure policies bound retries. A success policy can let supported indexed Jobs finish after specified successful indexes rather than requiring every index. Confirm field availability against the deployed API; see [Job semantics](https://kubernetes.io/docs/concepts/workloads/controllers/job/).

CronJobs add schedule/time-zone handling, missed-start deadlines, history retention, and `Allow`, `Forbid`, or `Replace` concurrency policies. Those policies govern Jobs created by that CronJob and do not provide an exactly-once distributed lock. Make scheduled operations safe to retry and observable when skipped.

### Init, sidecar, and ephemeral containers

Ordinary init containers run to completion in sequence before application containers start. They suit setup work with bounded retries, such as preparing a configuration file. Native sidecars are restartable entries in `initContainers` with container-level `restartPolicy: Always`; they can keep running while later init steps and application containers execute. Kubernetes gives these sidecars defined startup/shutdown behavior, and they do not prevent a Job from completing once its main work finishes. See [native sidecar lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/).

A traditional sidecar placed under `containers` shares the application's lifecycle without all native sidecar ordering semantics. Ephemeral containers add diagnostic tools to an existing Pod when permitted; they are not a mechanism to update the application or add permanent resource reservations. Their process visibility depends on runtime support and namespace targeting.

### Rollouts and StatefulSet ordering

Deployment rolling updates balance `maxSurge` and `maxUnavailable`; a Recreate strategy removes old replicas before bringing up replacements. Readiness, minimum ready time, scheduling headroom, and startup time determine actual availability. A progress deadline reports a stalled Deployment but does not by itself automatically roll it back.

Blue-green runs separate versions and switches traffic after validation. Canary exposes a controlled fraction of traffic or users to a new version. These require explicit routing or progressive-delivery tooling; changing Deployment replica counts alone does not guarantee a precise traffic percentage. Rollback also requires compatible schemas and dependencies, not just an old image.

StatefulSet ordinals give stable names and, with a governing headless Service, predictable DNS identities. `volumeClaimTemplates` create claims per replica. OrderedReady startup and reverse-order scale-down protect some sequencing assumptions; Parallel pod management changes scaling behavior but is not an application consistency protocol. Rolling update partitions can stage changes. Claim retention policies and storage reclaim policies must be understood before scale-down or deletion.

### Lifecycle, probes, and restarts

Pod phases are Pending, Running, Succeeded, Failed, and Unknown. Container states are Waiting, Running, and Terminated. Conditions such as Ready report additional facts. `CrashLoopBackOff`, `ImagePullBackOff`, and `Terminating` are useful displayed reasons/statuses, not additional Pod phases. A Running Pod can contain an unready or repeatedly restarting container.

| Probe | Failed-probe consequence | Appropriate question |
| --- | --- | --- |
| Startup | Eventually restarts the container after its failure threshold; gates liveness/readiness until successful | Has this slow-starting process initialized? |
| Readiness | Marks the container unready and affects normal Service endpoint eligibility | Can this instance accept traffic now? |
| Liveness | Eventually restarts the container after its failure threshold | Is the process stuck in a state a restart can repair? |

Do not make liveness depend on every remote dependency: a database outage can otherwise cause a fleet-wide restart storm. Readiness also needs care, because making all replicas unready during a shared failure can eliminate useful degraded service. Choose thresholds from startup and recovery behavior, and measure probe cost.

Pod-level restart policies include Always, OnFailure, and Never; controllers restrict appropriate values, and newer per-container rules require compatible versions. CrashLoopBackOff is kubelet's delay between repeated restart attempts. Diagnose the prior termination reason, previous logs, configuration, and probes rather than deleting the Pod to hide the restart history. Exact backoff settings can vary; see the [Pod lifecycle reference](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/).

### Disruptions, deletion, and ownership

A PodDisruptionBudget limits supported voluntary evictions, commonly during drains. It does not stop hardware failures, node-pressure eviction, or direct Pod deletion, and it does not replace workload rollout settings. `minAvailable` and `maxUnavailable` express different budgets; a single replica with `minAvailable: 1` can block maintenance until the availability design changes.

Deletion sets a deletion timestamp, after which finalizers can keep an object present while responsible controllers complete cleanup. Garbage collection follows owner references, with foreground, background, and orphaning choices. A finalizer is a cleanup obligation, not proof that cleanup succeeded. Removing it manually can leak infrastructure or bypass data protection.

## 5. Scheduling & Resource Management

### Requests, limits, and QoS

Requests inform scheduling and resource allocation; limits constrain consumption through runtime/kernel mechanisms. CPU is a compressible resource, so a limit usually produces bandwidth throttling. Memory is not compressible in the same way, and exceeding a cgroup ceiling can cause an OOM kill. Low node utilization does not override a container limit.

Under conventional per-container resource definitions, Guaranteed QoS requires equal nonzero CPU and memory requests/limits for every relevant container. BestEffort has no CPU or memory requests/limits, and Burstable covers the middle. Newer Pod-level resource features require checking the deployed rules. QoS influences behavior under contention and OOM selection but is not an absolute protection against eviction. See [resource management](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/).

Effective Pod requests account for containers that run concurrently, init phases, restartable sidecars, and Pod overhead. They are not always a simple sum of application containers. A missing request can also be defaulted by admission, including from a limit, so inspect the admitted object rather than only its source YAML.

CPU quota operates over short enforcement periods. Several runnable threads can exhaust a small quota quickly, causing latency spikes even when a long-window CPU average looks low. Compare throttled-period/time counters with request latency, workload concurrency, and cgroup ancestor limits. Memory analysis must include native allocations, page cache, tmpfs volumes, and peak bursts as well as the application heap.

### Placement constraints

The scheduler filters before scoring. A node with free CPU can still be infeasible because of memory requests, taints, affinity, volume topology, host ports, device resources, Pod count limits, or policy. Priority influences queue order and preemption, but does not allow a Pod to ignore required placement constraints.

| Mechanism | Effect | Common trap |
| --- | --- | --- |
| `nodeSelector` | Requires matching node labels | A label rename can strand a workload |
| Node affinity | Expresses required or preferred node selection | Preferred placement is not an availability guarantee |
| Pod affinity/anti-affinity | Places Pods relative to selected Pods and topology domains | Broad selectors add cost; required anti-affinity can exhaust eligible domains |
| Taints and tolerations | Repels Pods unless tolerated | A toleration permits placement but does not attract a Pod |
| Topology spread | Bounds skew across eligible domains | Hard constraints can block recovery when a zone is unavailable |

`NoSchedule` blocks new untolerating placements; `PreferNoSchedule` is a soft preference. `NoExecute` can also evict existing untolerating Pods, with `tolerationSeconds` allowing a bounded stay. A node cordon is a separate scheduling control. For failure domains, compare `topologyKey`, `maxSkew`, `whenUnsatisfiable`, `minDomains`, selectors, and eligibility rules with the actual zone labels and available capacity.

PriorityClasses express relative importance. Preemption can reclaim lower-priority capacity but may still disrupt a PDB on a best-effort basis. A non-preempting priority class avoids evicting others while retaining queue priority. Protect critical system capacity and constrain who can assign high priorities; otherwise important tenant workloads can starve the rest of the cluster.

### Governance and specialized resources

ResourceQuota limits aggregate namespace consumption or object counts. LimitRange can default requests/limits and enforce allowed ranges. These act during admission; they do not create physical capacity or guarantee that admitted Pods fit. Quota errors can prevent Pod creation altogether, leaving a Deployment short of replicas without new Pending Pods.

RuntimeClass selects a runtime handler and can declare Pod overhead and scheduling requirements, for example for a sandboxed runtime. Extended resources such as GPUs are advertised by device plugins and have distinct allocation semantics. They cannot be inferred from free CPU or freely overcommitted like ordinary CPU requests.

CPU Manager's static policy can assign exclusive CPUs to qualifying containers. Topology Manager coordinates NUMA alignment among CPU and device allocations; Memory Manager can participate in appropriate configurations. Policies trade placement flexibility for locality and latency. An apparently idle server may reject a Pod because no single allowed NUMA arrangement satisfies its request.

### Pressure and Pending diagnostics

kubelet reacts to memory, filesystem space/inodes, and PID pressure. It may reclaim node resources before evicting Pods; victim ranking considers priority and usage relative to requests, with resource-specific details. Node-pressure eviction bypasses PDB protection, and hard thresholds do not provide ordinary application shutdown guarantees. Inspect configured thresholds, grace periods, and actual pressured filesystem; see [node-pressure eviction](https://kubernetes.io/docs/concepts/scheduling-eviction/node-pressure-eviction/).

```bash
kubectl describe pod POD -n NAMESPACE
kubectl get events -n NAMESPACE --field-selector involvedObject.name=POD --sort-by=.metadata.creationTimestamp
kubectl get resourcequota,limitrange -n NAMESPACE
kubectl describe node NODE
```

For FailedScheduling, use the reported filter reasons to build a set of feasible nodes. Compare requests with allocatable and already allocated requests, then check topology, devices, storage, taints, and Pod limits. `kubectl top` reports observed usage and cannot prove scheduling feasibility.

## 6. Kubernetes Networking

### Addressing and packet paths

The Kubernetes network model gives Pods their own addresses and expects cross-node Pod connectivity without requiring applications to manage address translation. The implementation may use encapsulation, direct routing, cloud networking, or eBPF. Containers in one Pod use localhost to communicate; a Pod on another node needs a working route through both nodes and the underlying network.

Pod CIDRs allocate workload addresses. Service CIDRs allocate virtual Service addresses, which are usually implemented by forwarding rules rather than interfaces that accept connections. Prevent overlap with node networks, VPNs, peered networks, and external destinations. Some CNIs use provider subnets or their own IPAM rather than a simple per-node Pod-CIDR model; IP exhaustion can stop Pod startup while CPU and memory remain free.

A typical routed path is application socket → Pod interface → host veth → host route/policy → source node interface → network → destination node → destination veth → application socket. An overlay inserts encapsulation/decapsulation, such as VXLAN, around the cross-node leg. A Service path adds backend selection and often DNAT before reaching a Pod. eBPF or socket-level implementations can move these steps, so draw the deployed path rather than assume every packet traverses iptables.

### CNI implementations and routing

| Implementation | Typical capabilities | What to verify |
| --- | --- | --- |
| Calico | Network policy and routed or encapsulated networking, with dataplane choices | BGP sessions/routes, encapsulation mode, policy engine, MTU |
| Cilium | eBPF networking and policy, optional kube-proxy replacement | Kernel compatibility, agent health, BPF maps, routing and encryption mode |
| Flannel | Pod connectivity using selectable network backends | Overlay or direct-route reachability; separate policy enforcement if required |
| Azure CNI | Integration with Azure networking, including multiple address-allocation modes | Selected mode, subnet/IP capacity, routes, security controls, and provider limits |

An overlay reduces requirements for routing every Pod prefix through the physical network but adds headers and MTU constraints. An underlay/direct-routing design avoids that encapsulation but requires valid routes and infrastructure support. BGP distributes routes; it does not itself implement network policy or encrypt traffic. A plugin name alone does not reveal its configured mode.

### Services and backend selection

| Service type | Behavior | Operational implication |
| --- | --- | --- |
| ClusterIP | Stable cluster virtual address for a backend set | Tests DNS and forwarding separately |
| NodePort | Exposes a port on eligible node addresses as well as usual Service access | Node firewalls, address selection, and traffic policy matter |
| LoadBalancer | Requests an external implementation through a controller/provider | Provisioning, health checks, and direct-to-Pod versus node targets vary |
| ExternalName | Returns a DNS alias to an external name | Does not proxy connections or automatically rewrite HTTP Host/TLS identity |
| Headless, `clusterIP: None` | Publishes backend discovery without a Service VIP | Clients choose and reconnect to individual endpoints |

EndpointSlices partition backend address/port information and readiness/serving/termination conditions. A selector-based Service normally gets slices from a controller. Services without selectors can use explicitly managed slices. CoreDNS publishes Service and eligible Pod records according to configuration; a name such as `api.team-a.svc.cluster.local` uses the configured cluster domain, which need not be `cluster.local`.

Normal Service routing excludes unready endpoints, but options such as `publishNotReadyAddresses`, terminating-endpoint handling, and controller-specific routing affect the result. Finding an address in an EndpointSlice proves discovery state, not reachability, a listening process, or an accepted request. See [Service behavior](https://kubernetes.io/docs/concepts/services-networking/service/).

kube-proxy watches Service and endpoint state and programs the host dataplane. Linux modes include iptables, IPVS, and nftables, with support and defaults varying by release; IPVS is deprecated in recent Kubernetes releases. eBPF implementations can replace kube-proxy's Service handling. Inspect the actual mode and compatible driver/kernel support before applying backend-specific advice; see [virtual IPs and Service proxies](https://kubernetes.io/docs/reference/networking/virtual-ips/).

DNAT changes the destination to a backend. SNAT/masquerading can change the source so replies follow the correct path. Conntrack maintains flow/NAT state; a full table or stale entries can cause failures despite correct current rules. ClientIP session affinity influences backend selection but is not a guarantee that application sessions survive Pod replacement.

### Traffic policies, ingress, and meshes

`externalTrafficPolicy: Local` restricts external traffic to node-local eligible endpoints and avoids the usual cross-node forwarding/SNAT path, supporting source IP preservation at that layer. The external load balancer may still alter the source. Nodes without suitable local endpoints need correct health checks or traffic can be dropped. `Cluster` allows broader backend selection. `internalTrafficPolicy: Local` similarly restricts internal Service traffic to local endpoints, which can deliberately produce no usable backend on some nodes.

Ingress defines HTTP/HTTPS routing and requires an implementation. Gateway API models gateway infrastructure and routes with clearer role separation and attachment rules; supported route kinds and features depend on the controller. Neither API alone creates a working dataplane. Check listeners, route acceptance, backend references, certificates, and external load balancer health.

Istio and Linkerd provide service-mesh capabilities such as workload identity, mTLS, telemetry, and traffic policy. Envoy is a proxy used by many systems; Linkerd uses its own data proxy. Mesh architecture may use sidecars or other traffic interception models. mTLS protects authenticated transport between participating peers but does not automatically authorize every application action. Retries need budgets and idempotency or can amplify overload.

NetworkPolicies isolate selected Pods for ingress and/or egress when the network implementation enforces them. Policies are additive allow rules; once isolated in a direction, a Pod needs allowed traffic in that direction. For traffic between isolated Pods, source egress and destination ingress must both permit it. Namespace selectors and Pod selectors in the same peer entry combine, while separate entries provide alternatives. Include UDP and TCP DNS where required. Policy behavior around NAT, host traffic, and unsupported protocols needs implementation-specific verification; see [NetworkPolicy semantics](https://kubernetes.io/docs/concepts/services-networking/network-policies/).

### Network investigation

```bash
kubectl get svc SERVICE -n NAMESPACE -o yaml
kubectl get endpointslices -n NAMESPACE -l kubernetes.io/service-name=SERVICE -o yaml
kubectl get networkpolicy -n NAMESPACE
kubectl get pods -n kube-system -o wide
```

Test progressively from an affected client: DNS name, Service IP/port, each backend Pod IP/port, and same-node versus cross-node destinations. Then inspect listeners and routes with `ss` and `iproute2`, capture narrowly with `tcpdump`, and check conntrack and the installed dataplane's state. `nsenter -t PID -n` can inspect the relevant host process network namespace when authorized. A successful node-to-backend connection does not prove the Pod's policies, namespace, or return route work.

## 7. Storage & Persistent Volumes

### Volumes and provisioning

A volume makes storage available to containers in a Pod. `emptyDir` lasts with the Pod on its node; a container restart alone does not remove it. A PersistentVolume represents provisioned storage, and a PersistentVolumeClaim requests storage with particular capacity, access modes, and class. The claim is namespaced while the PV is cluster-scoped. The lifetime of the backing data depends on the storage and reclamation policy, not simply the lifetime of the application container.

A StorageClass selects a provisioner and parameters for dynamic provisioning. `reclaimPolicy: Delete` permits deletion of the backing volume when the released PV is reclaimed; `Retain` leaves it for explicit recovery and reuse procedures. Inspect the resulting PV policy because changing a StorageClass does not retroactively rewrite existing PVs. A retained disk also still needs access controls and an owner.

`Immediate` binding can allocate storage before knowing where a Pod will run. `WaitForFirstConsumer` delays binding/provisioning so scheduling topology can influence placement. This is important for zonal disks and local PVs. A PVC waiting for its first consumer can be healthy, while a consumer with impossible constraints can prevent binding indefinitely.

### Access modes and CSI

| Mode | Intended access scope | Important qualification |
| --- | --- | --- |
| RWO / ReadWriteOnce | Read-write from one node | Multiple Pods on that node may still use it |
| RWX / ReadWriteMany | Read-write from multiple nodes | Application locking/consistency remains necessary |
| ROX / ReadOnlyMany | Read-only from multiple nodes | Access-mode matching alone is not a universal write-protection control |
| RWOP / ReadWriteOncePod | Read-write by one Pod across the cluster | Requires compatible CSI support and sidecar versions |

Choose modes supported by the driver and backing store. The API declaration does not add shared-filesystem semantics to a block disk or establish database fencing. The official [PV access-mode reference](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) explains these distinctions.

The CSI controller plugin supports operations such as provisioning and, where applicable, attachment. Kubernetes sidecars translate API objects into CSI calls. The node plugin stages and publishes storage into the node/Pod mount path. A successful provision does not prove attachment or mounting succeeded. Inspect controller and node components separately, along with `VolumeAttachment`, PVC, PV, and Pod events.

Expansion needs a StorageClass that permits it plus compatible driver/backend and filesystem behavior. Increasing a claim requests growth; it does not ensure immediate filesystem growth, and shrinking is generally unsupported. CSI VolumeSnapshots require snapshot CRDs/controllers and driver support. A snapshot can be crash-consistent without being application-consistent; quiescing or database-native backup may still be required.

### Topology and data recovery

StatefulSet `volumeClaimTemplates` bind each ordinal to its own claim. Deleting and recreating a Pod normally reuses that claim, but scale/delete retention policies must be checked explicitly. A StatefulSet cannot turn a single-zone disk into a multi-zone volume.

Local PVs provide locality and performance but tie data to a node and its recovery plan. NFS and Ceph offer networked storage with different consistency, performance, and failure characteristics. AWS EBS and Azure Disk are block-storage examples whose zone and multi-attachment capabilities depend on the selected product and driver. Shared attachment does not automatically make an ordinary filesystem safe for concurrent writers.

For a stuck attachment, establish whether the original node is still writing before detaching or forcing replacement. Fencing prevents two instances from writing state intended for one owner. Recover to a separate volume when possible, validate consistency at the application level, and preserve the original until recovery is confirmed. Back up storage data, encryption keys, restore permissions, and the Kubernetes objects needed to connect the data to workloads.

## 8. Configuration & Secrets

### How configuration reaches a process

ConfigMaps carry nonsecret configuration; Secrets carry sensitive values but base64 encoding is not encryption. Both can supply environment variables or projected files. Environment variables are captured at process start, so changing an API object does not update the running process environment.

Mounted ConfigMap/Secret volumes are refreshed asynchronously by kubelet under its configured change-detection strategy. Applications must notice changes and reopen files as needed; atomic file replacement and open file descriptors matter. A `subPath` mount does not receive the normal automatic updates. Rollouts driven by a configuration checksum or versioned object name make activation explicit. See [ConfigMap update behavior](https://kubernetes.io/docs/concepts/configuration/configmap/).

Immutable ConfigMaps and Secrets prevent data edits and can reduce watch overhead. Updating configuration then means creating a new object and rolling consumers. They do not make a value confidential or eliminate cleanup and rollback obligations.

### Encryption, external stores, and projection

API Server encryption providers can encrypt selected API resources before storing them in etcd. Verify the active configuration and rewrite previously stored objects when enabling or rotating encryption; a new provider does not automatically re-encrypt all existing data. KMS integrations separate key protection from the API Server's local configuration, but KMS reachability, permissions, and recovery become control-plane dependencies. Protect snapshots and preserve the keys needed to decrypt them.

Secrets Store CSI Driver mounts values from external secret services through provider integrations. Rotation and optional synchronization into Kubernetes Secrets depend on configuration. External Secrets Operator instead reconciles external values into Kubernetes Secret objects, adding etcd and Kubernetes RBAC to the secret's storage/access path. Neither approach makes an application automatically reload a credential; design and test the full rotation sequence.

Projected volumes combine sources such as ConfigMaps, Secrets, Downward API data, and service account tokens. TokenRequest-based projected tokens can be time-limited, audience-bound, and associated with a Pod; kubelet refreshes them, and clients must reread rather than cache the original forever. Disable token automount where no API identity is needed. See [service account credential handling](https://kubernetes.io/docs/concepts/security/service-accounts/).

The Downward API exposes selected Pod metadata and resource information to the workload through fields or files. It avoids giving every application general API read access just to discover its name or namespace. Availability and update behavior differ between fields and delivery mechanisms; do not treat it as an unrestricted API query mechanism.

### Packaging and configuration ownership

Helm renders parameterized templates from charts and values. Chart dependencies, lockfiles, and pinned artifacts make rendering more repeatable. Release history supports application release operations, while hooks run lifecycle jobs with separate cleanup and failure behavior. Hooks and CRDs require deliberate upgrade planning; a Helm rollback does not reverse a database migration or restore persistent data. Review rendered manifests and keep secret values out of logs and committed values files.

Kustomize composes bases and overlays and applies transformations/patches without a template language. Small environment overlays reduce duplication, but patch targets and generated resource names must stay predictable. Review the output with `kubectl kustomize PATH` or a matching build tool version.

GitOps controllers repeatedly compare a declared source with live resources and reconcile drift. Choose one owner per field or responsibility: a human emergency change, Helm action, operator, and GitOps controller can otherwise overwrite one another. Commit the intended durable fix and document any temporary suspension so reconciliation can resume safely.

## 9. Autoscaling & Capacity Management

### Horizontal Pod Autoscaler

HPA changes a workload's replica count using observed metrics. Resource metrics commonly flow from kubelet to Metrics Server and the `metrics.k8s.io` API; custom/external metrics use adapters or other metric API implementations. Metrics Server is a short-term resource metrics service, not a durable monitoring database.

For a simple proportional case:

```text
desiredReplicas = ceil(currentReplicas × currentMetric / targetMetric)
```

For example, four replicas at an average 90% CPU utilization with a 60% target suggest six replicas before constraints and dampening. CPU utilization is measured relative to requests, so changing requests changes this signal. Absolute resource targets, application metrics, and external queue metrics answer different capacity questions.

Missing metrics, unready Pods, tolerance, replica bounds, and scaling policies modify the simple calculation. With multiple metrics, HPA generally takes the largest replica recommendation; an unavailable metric can prevent a proposed downscale. Stabilization windows reduce reversals, while policies bound how quickly replicas change. Check metric freshness and conditions before tuning. See the [HPA algorithm](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/).

### Vertical and node autoscaling

VPA recommends CPU/memory requests using workload history and configured policy. Recommendation-only mode is useful before enabling automatic changes. `Initial` applies changes at creation; `Recreate` can replace Pods to apply recommendations. Versions supporting `InPlaceOrRecreate` can attempt in-place resizing before fallback. Do not assume all changes are disruption-free: VPA version, Kubernetes resize support, admission, and policy constrain what happens. See [VPA update modes](https://kubernetes.io/docs/concepts/workloads/autoscaling/vertical-pod-autoscale/).

HPA and VPA can interact badly when VPA changes requests used as the denominator of HPA's utilization target. Assign complementary responsibilities, such as recommendation-only VPA, different resources, or an HPA application-demand metric, and validate the combined loop under load.

Cluster Autoscaler commonly grows configured node groups after simulating whether unschedulable Pods would fit new nodes, and consolidates/removes nodes subject to constraints. Karpenter provisions capacity from permitted instance and topology choices and manages disruption/consolidation through its own policies. Provider node autoscalers have provider-specific inputs and limits. Avoid giving independent autoscalers conflicting ownership of the same capacity pool.

Node scaling follows scheduling demand, so wrong requests or impossible constraints produce wrong scaling behavior. Growth can fail because of group limits, cloud quota, unavailable instance types, IP exhaustion, incompatible labels/taints, or no allowed zone satisfying a volume. CPU utilization alone does not explain an autoscaler decision.

### KEDA and scaling from zero

KEDA ScaledObjects define a target and event triggers, commonly exposing external metrics and managing an HPA. KEDA handles activation from zero and return-to-zero behavior, while HPA usually manages scaling in the active replica range. Polling, metric caching, and cooldown behavior depend on KEDA version and state; return-to-zero cooldown is distinct from HPA's ordinary downscale stabilization. See the versioned [ScaledObject specification](https://keda.sh/docs/2.21/reference/scaledobject-spec/).

A zero-replica workload cannot emit its own resource metrics. Scale-from-zero therefore needs an observable external demand signal and a working activation path, plus available infrastructure to run the monitoring/scaling components. For synchronous requests, buffering, routing, and timeouts must tolerate node provisioning, image pulls, and application warmup. A minimum replica floor may be necessary for an interactive latency SLO.

### Headroom, fragmentation, and oscillation

Scaling latency is the sum of metric detection, decision time, provisioning, node initialization, scheduling, image pull, storage setup, and readiness. Measure it end to end. Low-priority placeholder Pods can reserve headroom that useful work preempts, but the extra capacity has a cost and needs appropriate priority policy.

Bin packing reduces cost but can leave small unusable resource fragments. Four nodes with 1 GiB free each cannot host a Pod requesting 3 GiB. Spread requirements, GPUs, topology, and oversized requests introduce additional fragmentation. Forecast demand by resource and failure domain rather than aggregate CPU alone.

Avoid oscillation with sensible metrics, realistic requests, bounded scaling rates, stabilization, and compatible node consolidation policies. Include zonal failures, cloud quotas, IP budgets, and expected provisioning failures in capacity planning. A cluster that survives a zone outage only if the cloud instantly supplies new nodes has an untested availability assumption.

## 10. Security & Multi-Tenancy

### Authentication and RBAC

Authentication establishes identity through configured mechanisms such as client certificates, OIDC-backed identity, and service account tokens. Authorization then grants actions. Certificate expiry, OIDC issuer/audience configuration, time synchronization, and token rotation are common failure points. Successful authentication does not imply permission to read a Secret or execute in a Pod.

Roles grant namespaced permissions; ClusterRoles can describe cluster-scoped permissions or reusable namespaced rules. A RoleBinding grants in one namespace, including when referencing a ClusterRole. A ClusterRoleBinding grants its referenced permissions across the cluster. RBAC permissions are additive and have no general deny rule. See [RBAC authorization](https://kubernetes.io/docs/reference/access-authn-authz/rbac/).

Use specific resources, verbs, and identities. Treat permission to create workloads, exec into them, approve certificates, impersonate identities, or modify bindings as potential escalation paths. A user who can create a Pod in a namespace may be able to mount its Secrets or use its service accounts even without direct Secret-read permission. Test effective access with `kubectl auth can-i` and audit the indirect paths.

### Workload restrictions

Pod Security Standards define Privileged, Baseline, and Restricted profiles. Pod Security Admission can enforce, warn, and audit at namespace scope, with version pinning for policy evolution. The standards constrain Pod settings; they do not replace RBAC or network isolation. See the [profile definitions](https://kubernetes.io/docs/concepts/security/pod-security-standards/).

A hardened SecurityContext uses an appropriate nonroot UID/GID, controlled group memberships, dropped capabilities, no unnecessary privilege escalation, an appropriate seccomp profile, and a read-only root filesystem with explicit writable mounts. Application compatibility and image permissions must be checked; a numeric nonroot user alone does not prove a container lacks dangerous capabilities.

AppArmor and SELinux impose additional mandatory access controls when supported and configured on nodes. Rootless runtime operation and user namespaces address different privilege boundaries: mapping container root to a nonroot host UID does not mean the entire runtime is rootless. Shared-kernel containers still require a threat model; sandboxed runtimes, dedicated nodes, or separate clusters may be appropriate for mutually untrusted tenants.

### Admission, supply chain, and runtime defense

ValidatingAdmissionPolicy evaluates declarative CEL-based checks inside the admission system, avoiding an external webhook for supported policy needs. Kyverno and Gatekeeper provide other policy engines and workflows. Scope rules carefully, test upgrades, and measure availability effects. Granting tenants control of cluster-wide admission or conversion webhooks can expose other tenants and the control plane.

Pin images by digest for reproducibility, scan for known vulnerabilities, and verify signatures and expected build identities at admission where required. A valid signature authenticates an artifact or statement; it does not prove the software is harmless. Define trust roots, acceptable provenance, remediation policy, and a controlled response when verification services are unavailable.

API auditing records security-relevant actions according to policy. Avoid recording Secret bodies or tokens in logs, and protect audit destinations and retention. etcd encryption protects stored resource content against some storage compromise paths; it does not prevent authorized API readers or a compromised API Server from accessing it.

Cloud workload identity federation exchanges a workload identity for narrowly scoped provider credentials, reducing static key distribution. Bind issuer, subject, audience, and cloud permissions deliberately; distinguish node identity from workload identity. Runtime tools such as Falco and eBPF-based detection can identify suspicious execution, file access, or network activity, but require tuned signals and an incident response path.

### Tenant boundaries

Namespaces need RBAC, enforced network policy, quotas, admission restrictions, scoped storage access, and protected platform services to provide useful shared-cluster isolation. Start with default-deny traffic rules and explicitly allow necessary dependencies, including DNS. Prevent tenant-controlled privileged Pods, host mounts, host namespaces, and broad node credentials.

Evaluate noisy-neighbor effects on nodes, API, DNS, admission, and storage as well as direct access. Use separate node pools or stronger isolation when shared resources are unacceptable. For hostile tenants or regulatory boundaries, a separate cluster/account may simplify the security model. The official [multi-tenancy guidance](https://kubernetes.io/docs/concepts/security/multi-tenancy/) explains why a namespace alone is insufficient.

## 11. Operators, CRDs & Extensibility

### Extending the API

A CustomResourceDefinition adds an API resource with a schema, scope, and served/storage versions. A custom resource is an instance of that type. Creating a CRD makes data available through Kubernetes APIs; a controller is still needed to turn its intent into infrastructure or application behavior. An operator combines controllers with operational knowledge such as backup, failover, and upgrade sequencing. See [custom resources and API extensions](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/).

Schemas validate structure, default supported fields, and control unknown-field pruning and merge behavior. CEL rules can express additional validation. Conversion webhooks translate among served versions; they do not replace a plan to migrate stored objects. Before removing an old version, migrate stored data, update clients, and account for `status.storedVersions` and conversion availability. Deleting a CRD can delete its custom resources, making it a particularly dangerous uninstall or rollback shortcut.

### Controller correctness

A typical reconcile reads the desired resource, observes dependent objects, computes the next small change, applies it, and records status. Informers and shared caches make reads efficient; work queues retry keys with rate limiting. The controller must tolerate a cache that has not yet observed its own write and a retry after an external action succeeded but its status update failed.

Use stable external identifiers and idempotency keys to avoid duplicate cloud resources. Keep status truthful, surface conditions and observed generation, and separate transient retryable failures from invalid desired configuration. Bound external calls and avoid requeue storms. controller-runtime provides common controller machinery; Kubebuilder and Operator SDK scaffold projects and workflows, but application-specific recovery correctness remains the operator author's responsibility.

Finalizers arrange cleanup before deletion completes. A controller should handle deletion as an explicit branch, complete required external cleanup, then remove its own finalizer. If cleanup cannot finish, publish the reason and retry safely. An unavailable operator can leave resources deleting indefinitely; force-removing its finalizer may leak disks, credentials, or billable infrastructure.

### Availability and upgrade contracts

Operator upgrades must preserve CRD compatibility, permissions, conversion, status meaning, and external data formats. Roll out compatible readers before writers that emit a new representation. An older controller may not understand fields or state created by a newer one, so reverting its image is not always a valid rollback.

Admission webhooks need redundant serving, stable certificates, narrow matching, and a failure policy appropriate to the resource. Avoid dependencies that require the same webhook to recover: blocking creation of the webhook's own replacement Pods can deadlock the cluster. Conversion webhooks are also read/list availability dependencies for affected resource versions.

Custom scheduler profiles/plugins customize scheduling stages within the scheduler framework. A separate scheduler handles Pods selecting its `schedulerName`; provide leader election, observability, and a clear ownership boundary. API aggregation registers extension servers through APIServices, used by systems such as metrics APIs. An unavailable aggregated API can break discovery or consumers even while the core API works; distinguish this from a CRD served directly by kube-apiserver.

## 12. High Availability & Disaster Recovery

### Availability across failure domains

For a self-managed three-AZ design, place three voting etcd members across the zones, use multiple API Servers behind a redundant endpoint, and run redundant schedulers/controllers with leader election. Stacked control-plane/etcd placement reduces infrastructure but couples their failures; external etcd separates those lifecycles at additional operating cost. See [HA topology options](https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/ha-topology/).

Distribute application replicas across zones and hosts using topology spread or anti-affinity, and provision enough surviving-domain capacity to meet the SLO. Ensure ingress, DNS, storage, identity, and monitoring also survive the intended failure. Three API Servers in one zone or three application replicas sharing one zonal dependency do not provide three-zone service availability.

Node failure detection takes time because heartbeats, condition transitions, taints, tolerations, controller decisions, and storage fencing are separate steps. A disconnected node can continue running its old Pods. Rescheduling stateful workloads must establish safe ownership of data before replacement writers start. Maintenance should use PDB-aware drains and application quorum checks, then verify recovery before the next node.

### etcd backups and restoration

A useful backup includes a verified etcd snapshot, required certificates/configuration and encryption keys, and an accessible restore procedure. Keep copies outside the cluster's failure domain and test restoration with the actual compatible tooling. A snapshot file existing is weaker evidence than a completed restore with validated API objects.

If quorum remains, repair or replace failed members using supported membership operations. If quorum is permanently lost, preserve surviving data, fence old members, select a verified snapshot, and restore a new logical cluster with explicit membership and data directories. Coordinate API Server access and restart/reconnect dependent components according to the recovery procedure. Do not attempt to cure quorum loss by independently forcing conflicting survivors into service.

Restoring older state can move revisions backward relative to controller caches. For etcd versions supporting the documented mechanism, plan a revision bump and mark the bumped history compacted so watchers invalidate old state. This is part of recovery correctness, not merely a process startup detail. Use the version-specific [etcd disaster recovery guide](https://etcd.io/docs/v3.6/op-guide/recovery/) and [Kubernetes etcd operations guidance](https://kubernetes.io/docs/tasks/administer-cluster/configure-upgrade-etcd/).

### Workload data and regional recovery

Velero can back up Kubernetes resources and integrate with supported snapshots or data movement, depending on installation and provider support. It does not make every database backup transaction-consistent. Pair infrastructure backups with database-native backups, logs, or replication and capture dependency ordering, encryption keys, external services, and restore credentials.

RPO is the acceptable loss of recent data; RTO is the acceptable recovery duration. Derive replication, backup frequency, restore automation, and warm capacity from those targets. Test application queries and writes after restore, including cross-service consistency and credentials. A successful restore Job is not the same as a usable service.

Regional recovery commonly uses separate clusters, replicated artifacts/configuration, off-region backups or data replication, traffic failover, and a policy deciding which region may write. Active-active designs add conflict and consistency problems. Active-passive designs still need enough quota and capacity in the recovery region. Prefer independent regional control planes over stretching one etcd cluster across unreliable, high-latency regional links.

### Upgrades and rollback limits

Before an upgrade, inventory deprecated APIs and stored versions, test admission/CRD conversions, verify driver and controller compatibility, and confirm backups and recovery access. Upgrade control-plane components and nodes in the order allowed by the distribution and [version skew policy](https://kubernetes.io/releases/version-skew-policy/). Use canary workloads and node pools, protect failure-domain capacity, and watch application SLOs throughout.

Control-plane downgrade is not a universal supported rollback. Storage formats, APIs, and managed-provider constraints can prevent it. Keep application rollback separate from cluster recovery, and plan replacement nodes or a separate cluster where appropriate. An old etcd snapshot also reverts cluster state and can conflict with newer external data, so restoring it is not a casual undo operation.

## 13. Observability & Troubleshooting

### Which source answers which question?

| Source | What it explains | Limitation |
| --- | --- | --- |
| kube-state-metrics | API object state: desired/available replicas, conditions, restarts, claims | Does not measure application latency or raw resource use |
| kubelet/cAdvisor and runtime metrics | Node/container resource accounting and runtime behavior | Availability and labels depend on version and configuration |
| Metrics Server | Recent resource metrics for HPA and `kubectl top` | Not long-term monitoring or a complete diagnosis |
| Prometheus and application instrumentation | Time-series history, SLOs, request behavior, dependency health | Scrape failures and cardinality need monitoring too |
| Events | Recent scheduling, mount, probe, and lifecycle explanations | Retention is limited and events can be aggregated |
| Audit logs | API actions and identities according to audit policy | Do not describe every kernel/runtime event |
| Component and workload logs | Local errors, reconciliation decisions, application traces | Need timestamps, correlation, retention, and redaction |

Prometheus Operator reconciles Prometheus-related resources. ServiceMonitor and PodMonitor describe scrape discovery; the Prometheus object's selectors must include them, and the target must expose the correct endpoint with reachable networking and credentials. Creating a monitor object alone does not prove the target is scraped.

Monitor API request rate/errors/latency, APF queueing, webhook duration, etcd fsync/commit latency, leader stability, scheduler queues/latency, and controller queue depth/retries. For nodes, correlate utilization with memory/disk/PID pressure, filesystem inodes, I/O latency, packet loss, and runtime health. For Pods, track readiness, restarts, termination reasons, throttling, startup time, and eviction alongside application errors. The [observability overview](https://kubernetes.io/docs/concepts/cluster-administration/observability/) distinguishes infrastructure signals from application health.

### A bounded diagnostic workflow

Start with the affected request or workload, establish when it changed, and compare affected versus healthy nodes, zones, namespaces, and versions. Preserve previous logs and event evidence before recreating resources. Then test hypotheses at the layer that owns the failed transition.

```bash
kubectl config current-context
kubectl get pod POD -n NAMESPACE -o wide
kubectl describe pod POD -n NAMESPACE
kubectl logs POD -n NAMESPACE -c CONTAINER --tail=200 --timestamps
kubectl logs POD -n NAMESPACE -c CONTAINER --previous --tail=200 --timestamps
kubectl get events -n NAMESPACE --sort-by=.metadata.creationTimestamp
kubectl top pod POD -n NAMESPACE --containers
kubectl get --raw='/readyz?verbose'
```

`--previous` reads the previous container instance when its logs remain available; it cannot recover arbitrarily old restarts. `kubectl debug` can add an ephemeral container or create a node debug session with an approved image and appropriate permissions. Keep diagnostics scoped because a node debug session can expose host data.

On nodes, `crictl` inspects the configured CRI runtime and its Pod/container state. `ctr` is a lower-level containerd tool whose namespace and abstractions may differ from Kubernetes; avoid making runtime mutations behind kubelet's back. `journalctl -u kubelet` and the runtime's unit logs reveal local failures when the API view is incomplete. Use `nsenter`, `tcpdump`, `ss`, and `ip` to inspect the actual process/network namespace.

### Common symptoms and discriminating evidence

| Symptom | First evidence | Follow-up question |
| --- | --- | --- |
| Pending, no node assigned | Scheduler events and admitted resource requests | Which filter eliminates each possible node? |
| Pending, node assigned | kubelet/runtime/CNI/CSI events | Is sandbox creation, image pull, init, or mount blocked? |
| CrashLoopBackOff | Last termination reason, previous logs, probe events | Is it application exit, OOM, probe restart, or startup configuration? |
| ImagePullBackOff | Pull event error, image reference, credentials, node connectivity | Is the cause authentication, missing digest/platform, registry rate limit, or network/TLS? |
| Terminating | Deletion timestamp, finalizers, kubelet reachability, shutdown logs | Is cleanup blocked, the node absent, or the application still draining? |
| PVC Pending | Claim events, StorageClass, consumer and topology | Is it intentionally waiting, quota, no provisioner, or impossible placement? |
| Attach/mount failure | VolumeAttachment, CSI logs, node/cloud state | Is there an old writer, permission problem, topology mismatch, or filesystem issue? |
| Service/DNS failure | Client lookup, direct Pod test, EndpointSlices, node scope | Does failure start at discovery, dataplane, policy, or application listener? |
| Slow/unavailable API | Load balancer/TLS, readiness, component and webhook/etcd metrics | Is the bottleneck entry, admission, execution seats, or storage? |

Distinguish timeout from refusal, NXDOMAIN from SERVFAIL, and HTTP 403 from TLS/authentication failure. For API outages, retain out-of-band node and provider access; the tool needed to inspect the API must not be your only recovery path. After mitigation, verify the original failing path and sustained SLO recovery, not just a green component status.

## 14. Performance & Scalability

### API and etcd load

API cost depends on object size, list frequency, watches, write churn, authorization/admission work, and downstream latency. Tune client QPS/burst together with concurrency, APF, and server capacity. Adding API Server replicas cannot fix an etcd write bottleneck or a synchronous webhook bottleneck by itself.

Monitor etcd backend size against quota, fragmentation, committed write rate, fsync/commit latency, and leader changes. Retain only necessary object history and completed resources. Configure compaction and plan defragmentation with quorum and operational impact in mind; neither action replaces diagnosing slow disks or pathological writes.

Shared informer caches reduce repeated API reads but consume controller memory and watch bandwidth. Scope watches where possible, use indexed lookups, avoid listing the whole cluster on each event, and bound worker concurrency. A controller that rapidly updates unchanged status can generate a self-sustaining event/write storm.

### Scheduling and startup budgets

Measure scheduling queue time separately from plugin execution, binding, sandbox creation, image pulls, storage setup, init work, and readiness. More scheduler capacity will not accelerate a blocked volume attachment. Expensive inter-Pod affinity, too many infeasible Pods, and incompatible constraints can dominate scheduling cost.

Large-cluster design must consider nodes, total Pods/containers, Pods per node, Services/endpoints, API object sizes, and churn together. EndpointSlices avoid putting every backend in one huge object, but consumers still process changes. Published [large-cluster guidance](https://kubernetes.io/docs/setup/best-practices/cluster-large/) is a tested envelope under stated assumptions, not a promise that any workload mix performs equally well.

Reduce startup time with smaller compatible images, accessible registries, controlled parallel pulls, and caching or pre-pulling where measurements justify them. A registry mirror/cache needs integrity, credential isolation, and failure handling. Pre-pulling every large image on every node can itself saturate disk and network.

### DNS, network, and capacity performance

CoreDNS capacity depends on query rate, upstream latency, cache hit rate, retries, and expensive lookup patterns. Search domains and high `ndots` can multiply queries. Scale DNS across failure domains and test TCP fallback as well as UDP. NodeLocal DNSCache can reduce remote queries and some conntrack load, but adds node-local configuration and failure modes.

Investigate CNI encapsulation/encryption overhead, MTU, packet drops, conntrack occupancy, socket backlog, and endpoint rule/map synchronization before attributing all network latency to the application. Kernel and eBPF maps also have capacity limits. Compare throughput and tail latency under realistic concurrent traffic and endpoint churn.

Capacity forecasting should use resource distributions and growth by pool/zone, including network IPs, volume limits, image storage, and provider quotas. kube-burner can generate Kubernetes object/workload load and collect measurements; use an isolated representative environment and an explicit object rate, duration, success criteria, and cleanup plan. A static cluster with thousands of nodes may pass a test that misses the dangerous case of thousands of simultaneous replacements.

## 15. Kubernetes Operations & GitOps

### Provisioning and release operations

kubeadm handles important bootstrap and component upgrade tasks; Cluster API reconciles cluster/machine lifecycles through providers; managed services expose their own control-plane and node lifecycle contracts. Keep bootstrap configuration, add-on versions, identity setup, and out-of-band recovery instructions reproducible. Cluster objects alone do not document cloud networking or external IAM dependencies.

Package Helm charts with explicit versions, pinned dependencies, reviewed values, and predictable rendering. Distinguish a chart version from application image versions. Validate hook and CRD handling separately, and retain release artifacts needed for rollback. Successful installation means API operations completed according to tool behavior; application SLO verification remains necessary.

### Argo CD and Flux reconciliation

Argo CD uses components for source rendering, application reconciliation, and API/UI access to compare declared and live state. Flux separates responsibilities among controllers for sources, Kustomize, Helm, and optional image automation. Both need source credentials, reconciliation health, dependency ordering, and clear resource ownership.

Automatic sync, pruning, and self-healing are policy choices. Pruning can delete resources removed from source, so protect stateful lifecycles and review large deletion diffs. A forceful replacement may cause downtime even when the rendered YAML is valid. Monitor reconciliation errors, stale source revisions, and application health rather than only controller uptime.

### Server-side apply and field ownership

Server-side apply lets the API Server merge declarative intent while recording field managers in `metadata.managedFields`. Applying a different value to another manager's field can raise a conflict. Ownership is field-granular and depends on schema merge semantics; it is not a cluster lock and does not prevent every ordinary update or patch.

Inspect the conflicting field and managers, decide which controller should own it, and remove or coordinate competing declarations. For example, GitOps should not continually reset a replica count that HPA owns. `--force-conflicts` transfers ownership and can be appropriate for an intentional handoff, but repeated forcing can conceal a continuing controller fight. See [server-side apply ownership](https://kubernetes.io/docs/reference/using-api/server-side-apply/).

### Progressive delivery and safe change

Deployment rollbacks restore a prior Pod template, not database state or all referenced external configuration. Argo Rollouts adds progressive-delivery strategies and analysis around rollout resources and supported traffic routers. Define representative success/error/latency checks, evaluation windows, traffic routing, and an abort policy. Low error counts from a canary receiving no meaningful traffic do not establish safety.

For CRD upgrades, preserve served versions and conversion until stored data and clients have migrated. For node-pool upgrades, reserve surge capacity, verify PDBs and stateful placement, drain a canary, and expand only after application recovery. Keep backups of configuration, sensitive recovery material, and workload data in appropriate protected stores; Git cannot reconstruct an absent encryption key or database volume.

Fleet management needs inventory of cluster/add-on versions, policy, identity, and source revisions. Promote a verified artifact through environments with deliberate configuration changes instead of rebuilding it for each environment. Limit the blast radius of fleet-wide changes with staged rollout groups and independent rollback/recovery paths. When emergency changes are necessary, coordinate source updates or bounded reconciliation suspension to prevent automated reversal.

## 16. Expert Interview Scenarios

Structure an answer around the owning component, competing hypotheses, evidence that separates them, a bounded mitigation, and a recovery check. Explain data ownership and failure-domain consequences before suggesting deletion, detachment, or forced recovery.

### 1. Explain the complete lifecycle from kubectl apply to a running Pod

kubectl resolves the target context and sends API requests; apply mode determines client-side versus server-side merge behavior. The API authenticates, authorizes, admits, validates, and persists the Deployment. Its controller reconciles a ReplicaSet, which creates Pods through the API. The scheduler filters/scores nodes and binds each Pod. kubelet observes the assignment, coordinates volumes and credentials, and asks the runtime to establish a sandbox/network and create/start containers. Init work, probes, status reporting, and endpoint reconciliation then determine readiness and traffic. These are asynchronous transitions: successful apply, Running, Ready, and successful user requests are separate milestones.

### 2. Explain the internal Kubernetes reconciliation mechanism

Use a ReplicaSet as an example: a watch event queues its key, a worker reads current desired/observed state, calculates the replica difference, and creates or deletes a bounded number of Pods. Failures are retried with backoff. Repeated events and stale cache reads are normal, so the loop must be idempotent and converge after interruptions. Owner references, status, optimistic concurrency, and leader election support coordination; no single transaction spans all controllers and runtime actions.

### 3. Diagnose Pending Pods despite apparently available resources

First check whether `spec.nodeName` is set. If not, read FailedScheduling events and compare admitted requests with per-node allocatable minus existing requests. Then inspect taints, affinity, topology spread, host ports, devices, Pod/IP limits, PVC topology, and scheduler selection. Aggregate free resources or `kubectl top` averages can hide per-node fragmentation. If already assigned, investigate image pull, init, CNI, runtime, and CSI instead of scaling nodes blindly. Verify that the chosen fix makes an actual node feasible and the application becomes ready.

### 4. Explain why a Pod is OOMKilled despite low node memory usage

Inspect which container terminated, its admitted memory limit, applicable cgroup ancestors, kernel/runtime OOM evidence, and peak memory around the event. A container can exceed its own ceiling while the node has spare RAM. Include nonheap memory, tmpfs, page cache, concurrency, and short bursts that monitoring missed. Distinguish a container cgroup OOM from global node OOM and kubelet eviction. Repair leaks/concurrency or adjust limits with capacity evidence, then verify peak-load behavior.

### 5. Diagnose CPU throttling despite low average CPU utilization

Compare quota/period and throttled-time/period deltas with latency and runnable threads over short intervals. Parallel bursts can consume a small CPU budget early in each period while long-window averages remain low. Check parent cgroups, CPU affinity, and contention as well. Adjust concurrency or CPU policy with an SLO-based load test; adding nodes alone does not remove a container's quota.

### 6. Explain the packet path between Pods on different nodes

Identify source/destination addresses, the CNI mode, policy enforcement points, and whether a Service is involved. Trace the source network namespace through veth/host routing or eBPF, optional encapsulation, the underlay, destination-node processing, and destination namespace. Explain reverse routing, MTU, and filtering. For a Service, add backend selection and DNAT/SNAT where applicable. Verify with routes and captures at both ends rather than asserting one universal Kubernetes packet path.

### 7. Troubleshoot a Service that has endpoints but cannot route traffic

Test direct backend IP/port from the failing client and confirm the application listens on the Pod interface. Inspect Service ports/targetPorts, address family, EndpointSlice conditions, traffic policies, and network policies. Compare failing and healthy nodes for kube-proxy/CNI state, conntrack, routes, and MTU. If backend access works but the VIP fails, focus on Service dataplane programming. If both fail, follow policy, transport, or listener evidence. Recheck the original Service path after mitigation.

### 8. Diagnose CoreDNS failures affecting only certain nodes

Compare affected Pods' resolver configuration and dnsPolicy, node-local cache health, and DNS Service/backend reachability. Test the same query against the configured resolver, Service IP, individual CoreDNS endpoints, and upstreams as appropriate, using both UDP and TCP. Inspect node-specific conntrack, policies, CNI routes, packet loss, MTU, and kubelet cluster-DNS configuration. Healthy CoreDNS Pods do not rule out broken client-to-DNS routing. Validate from Pods on each previously affected node.

### 9. Recover an etcd cluster after losing quorum

Establish whether lost members can return safely before declaring permanent quorum loss. Preserve data and logs, fence stale members, and select a verified snapshot with an accepted recovery point. Restore a new logical cluster using version-matched tools, explicit membership, and the documented revision/watch-cache handling. Restore keys and API connectivity, then validate object state and controllers. Reconcile external resources and workload data separately because those may have advanced beyond the snapshot.

### 10. Design HA Kubernetes infrastructure across three availability zones

Place quorum members so one zone loss preserves a majority, provide redundant API entry points and elected control-plane components, and spread worker pools and application replicas. Include DNS, ingress, identity, storage, monitoring, and provider dependencies. Reserve enough surviving-zone capacity and compatible placement to maintain the chosen SLO. Test a zone-loss scenario including blocked provisioning and zonal disks; a topology diagram alone does not establish recovery performance.

### 11. Diagnose Cluster Autoscaler refusing to scale up

Inspect autoscaler decisions/events and the precise scheduler reason for the Pod. Determine whether any configured node-group template could satisfy its requests, taints, labels, topology, devices, and storage. Then check group maximums, cloud quota/capacity, IP space, permissions, and provisioning backoff. Quota or admission errors may mean no Pending Pod exists to trigger scaling. Fix the limiting condition and observe node creation, readiness, scheduling, and workload readiness end to end.

### 12. Explain HPA vs VPA vs KEDA vs Cluster Autoscaler interactions

HPA changes replicas, VPA changes/recommends resource requests, KEDA supplies event-based activation and HPA integration, and node autoscaling supplies placement capacity. Growing replicas can create Pending Pods that trigger node growth. VPA can change both packing and HPA utilization denominators. KEDA return-to-zero policy and HPA stabilization are separate controls. Give each loop clear ownership, preserve metric availability, and test detection-to-readiness latency plus downscale disruption under realistic bursts.

### 13. Troubleshoot a StatefulSet Pod stuck during volume attachment

Read Pod/PVC/PV and VolumeAttachment events, inspect CSI controller/node logs, and compare allowed zone and node attachment limits. Determine whether the prior instance/node is still using the disk. An RWO volume may need detachment before use on another node, and force-detaching without fencing risks corruption. Restore driver/provider permissions or capacity as indicated, establish safe single-writer ownership, and verify mount plus application consistency before removing evidence or old resources.

### 14. Explain why a Deployment rollout causes temporary 502/503 errors

Correlate gateway errors with readiness transitions, EndpointSlice changes, application shutdown, endpoint propagation, and connection reuse. A 503 may indicate no usable upstream; a 502 can indicate failed/reset upstream connections, depending on the proxy. Check probe accuracy, surge/unavailable settings, startup headroom, termination grace, `preStop`, external LB health/deregistration, and long-lived requests. Coordinate readiness and draining with measured propagation time, then validate a rollout under traffic with tail latency and error monitoring.

### 15. Design zero-downtime node upgrades for stateful workloads

Start with the application's quorum, failover, replication lag, and storage topology requirements. Provision compatible spare capacity, test backups, and use a canary node with a PDB-aware drain. Move one failure unit at a time, transfer leadership if supported, fence old writers, and wait for data replication and traffic recovery before proceeding. A single-writer service on one nonportable disk cannot gain zero downtime from a drain command alone; the application/storage architecture must support continuity.

### 16. Diagnose API Server latency caused by overloaded admission webhooks

Separate APF queue time, webhook duration/errors, and storage latency. Identify matched operations and correlate webhook saturation with writes, endpoint health, DNS/network dependencies, TLS, and external calls. Reduce unnecessary matching/calls, restore capacity or a compatible known-good webhook, and enforce bounded timeouts. Any temporary failure-policy change must account for the protection being bypassed and be tightly scoped. Verify write latency, rejection rates, and policy enforcement after recovery.

### 17. Recover a namespace stuck in Terminating due to finalizers

Inspect namespace conditions, discovery failures including unavailable APIServices, and remaining namespaced resources with their finalizers. Restore the responsible controller or external dependency and let cleanup finish. If manual cleanup is required, verify external resources and data ownership before removing only the relevant finalizer through a reviewed recovery procedure. Forcing namespace finalization can orphan objects and infrastructure. Confirm both namespace removal and external cleanup rather than treating disappearance as proof.

### 18. Design secure multi-tenant Kubernetes infrastructure

Define whether tenants are trusted teams or hostile users and identify shared failure/security boundaries. Combine scoped RBAC and service accounts, admission restrictions, default-deny networking, quotas, controlled storage, workload identity, and auditing. Protect cluster-scoped resources and prevent access to host privileges or platform credentials. Choose dedicated nodes, sandboxed runtimes, virtual control planes, or separate clusters where the threat model requires them. Test indirect escalation and noisy-neighbor paths in addition to direct API denials.

### 19. Explain controller conflicts caused by server-side apply field ownership

Inspect the reported field path and managedFields managers, then trace the controllers and source declarations that write it. A common case is GitOps applying replicas while HPA changes the scale subresource. Establish the intended owner, coordinate ownership transfer, and remove competing intent. Forced apply can resolve a deliberate transfer but does not resolve continuing contradictory loops. Confirm stable live values and clean reconciliations over several cycles.

### 20. Design multi-region disaster recovery for critical Kubernetes workloads

Begin with RPO/RTO and consistency requirements, then choose active-passive or active-active data ownership. Use independent regional clusters, replicated configuration/artifacts and secrets recovery, tested data replication/backups, prevalidated permissions, and recovery-region quota/capacity. Define traffic cutover, write fencing, dependency restoration, validation, and failback. Exercise a regional loss with realistic data volumes and unavailable source-region services; record measured recovery and potential data loss.

### 21. Scale Kubernetes to thousands of nodes while maintaining control-plane performance

Model object count, object size, and churn alongside node count. Establish API/APF, admission, etcd disk/consensus, scheduler, controller-cache, DNS, and network capacity baselines. Use shared/scoped watches, avoid hot-loop writes and repeated global lists, distribute system components, and control replacement rates. Load-test steady state and mass rescheduling with representative objects before expanding. Choose cluster sharding when a single failure domain, API workload, or operational envelope no longer meets the service requirements.

### 22. Investigate cascading failures during a large node-pool outage

Build a timeline of node loss, endpoint removal, retries, replacement Pods, autoscaler decisions, image pulls, storage operations, and API load. Check whether surviving dependencies, DNS, admission, registries, quotas, and capacity are now overloaded. Stabilize user traffic with bounded retries, load shedding, priority-aware capacity, and controlled recovery concurrency. Avoid simultaneous restarts or evictions that remove healthy capacity. Verify application SLOs and control-plane queues while bringing nodes back in stages, then address the shared dependency or missing headroom that amplified the outage.
