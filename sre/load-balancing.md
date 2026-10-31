# Load Balancing — SRE Expert Checklist

A service can have healthy replicas and still fail because traffic reaches them unevenly, connections outlive their destinations, or retries consume the remaining capacity. Load balancing determines where work goes, but reliability depends on the entire path: discovery, routing, admission, transport, application behavior, and recovery.

This guide follows the supplied checklist. It assumes basic networking and Linux knowledge and emphasizes mechanisms, operational trade-offs, and interview reasoning. Product capabilities vary by version, edition, cloud SKU, and configuration; verify those details for the system you operate. Numerical examples are hypothetical, not sizing recommendations.

## Contents

1. [Fundamentals & Architecture](#1-fundamentals--architecture)
2. [Load Balancing Algorithms](#2-load-balancing-algorithms)
3. [Network & Protocol Internals](#3-network--protocol-internals)
4. [Health Checks & Traffic Routing](#4-health-checks--traffic-routing)
5. [High Availability & Fault Tolerance](#5-high-availability--fault-tolerance)
6. [Performance & Capacity Planning](#6-performance--capacity-planning)
7. [Load Balancer Technologies](#7-load-balancer-technologies)
8. [Kubernetes & Cloud-Native Load Balancing](#8-kubernetes--cloud-native-load-balancing)
9. [Security](#9-security)
10. [Observability & Troubleshooting](#10-observability--troubleshooting)
11. [Expert Interview Scenarios](#11-expert-interview-scenarios)

## 1. Fundamentals & Architecture

### Distribution, availability, and scalability

A load balancer selects an eligible destination for incoming work. Distribution can improve utilization and horizontal scalability; health filtering and redundancy can improve availability. Neither creates backend capacity nor makes application state resilient. A balancer that sends requests fairly to an overloaded database-backed fleet still produces an outage.

Selection usually happens in stages: match a listener and route, choose a pool or locality, filter eligible endpoints, apply affinity or a balancing algorithm, then acquire a connection and dispatch. When investigating imbalance, inspect every stage. An algorithm cannot select endpoints excluded by topology, readiness, or a stale discovery snapshot.

### Layer 4 versus Layer 7

| Dimension | L4: TCP/UDP | L7: HTTP/HTTPS |
| --- | --- | --- |
| Selection input | Addresses, ports, flow metadata | Host, path, headers, cookies, request metadata |
| Typical decision unit | TCP connection or UDP flow | HTTP request or RPC, subject to implementation |
| Encryption | Can carry encrypted payloads without terminating TLS | Usually terminates TLS to inspect HTTP |
| Routing strengths | General transport support, low processing overhead | Content routing, HTTP observability, application policy |
| Operational costs | Flow state, NAT ports, limited application visibility | Parsing, TLS, buffering, pools, more policy complexity |

These are capabilities, not universal performance rankings. An L4 full proxy still has sockets and may terminate TLS. A TLS-aware router can inspect an available ClientHello SNI without decrypting application traffic. An L7 proxy does not usually redistribute messages inside an established WebSocket or streaming RPC.

### Reverse proxy, forward proxy, load balancer, and API gateway

A **reverse proxy** receives traffic on behalf of servers and can provide TLS termination, caching, authentication, or balancing. A **forward proxy** acts on behalf of clients to reach other destinations, commonly for egress control. The difference is whose access boundary it represents.

A **load balancer** distributes work; it may be a reverse proxy or a packet-forwarding system. An **API gateway** adds API-facing policy such as authentication, quotas, transformations, and API routing. Products overlap: one component may perform all reverse-proxy, gateway, and balancing roles. Explain the required behavior before choosing a product label.

### Hardware, software, and managed services

Hardware appliances can offer dedicated acceleration and predictable interfaces, with appliance lifecycle and capacity constraints. Software proxies run on ordinary hosts or containers and expose flexible configuration, but the operator owns upgrades, resource limits, and redundancy. Cloud-managed services move much of that operation to a provider while retaining customer responsibility for health checks, policies, quotas, networking, and sufficient backend capacity.

Compare failure domains, throughput and connection limits, protocol support, observability, change safety, and cost. A managed endpoint can still fail through a bad certificate, incorrect security group, exhausted quota, or a shared regional dependency.

### Centralized versus distributed balancing

A centralized tier gives clients a stable entry point and consistent policy, but concentrates traffic and needs its own scaling and availability design. Distributed balancing moves endpoint selection into clients, node agents, sidecars, or local proxies. It can remove an extra network hop while creating many independent views of health and load.

With distributed selection, each client usually sees only its own outstanding requests. A globally idle-looking endpoint may already be busy with other clients. Discovery freshness, randomized selection, and bounded control-plane behavior become important.

### Traffic directions and the two planes

**North–south** traffic crosses a system boundary, such as the internet entering a cluster. **East–west** traffic moves between services within the system. These are architectural directions, not security guarantees: internal traffic also requires authentication and policy.

The **data plane** accepts connections, forwards packets, terminates TLS, and processes requests. The **control plane** supplies routes, endpoints, certificates, and policy. A robust data plane can often continue with its last accepted configuration when the control plane fails. Define how long that remains safe when certificates expire or endpoints change; a stale configuration is not indefinitely correct.

### Connections, requests, discovery, and mesh

Connection-level balancing chooses a backend when a transport flow begins. Every request on a persistent connection can therefore reach the same backend. Request-level balancing can distribute successive requests independently, including separate HTTP/2 streams when the proxy implements that behavior. It cannot move an in-flight request to another backend without replay or application support.

**Service discovery** answers which endpoints exist. **Load balancing** selects one for work. A **service mesh** combines distributed proxies or node-level mechanisms with identity, policy, telemetry, and traffic management. DNS returning multiple addresses provides discovery; whether those addresses receive balanced traffic depends on client behavior.

## 2. Load Balancing Algorithms

### Match the signal to the work

| Algorithm | Mechanism | Best fit and limitation |
| --- | --- | --- |
| Round Robin | Cycles through eligible destinations | Simple for similar work and capacity; equal selections do not imply equal CPU |
| Weighted Round Robin | Allocates selections in proportion to configured weights | Models unequal capacity; static weights age as conditions change |
| Least Connections | Prefers the fewest active connections | Useful when connection count tracks work; misleading with idle or multiplexed connections |
| Weighted Least Connections | Normalizes active connections by capacity weight | Handles unequal servers, but still depends on connection count being meaningful |
| Least Outstanding Requests | Prefers fewer in-flight requests | Better for multiplexed HTTP and variable request duration; request cost still varies |
| Least Response Time | Uses observed latency, often with active work | Avoids slow endpoints; stale observations and fast failures can mislead it |
| Random | Samples an eligible endpoint | Cheap, decentralized, and avoids synchronized round-robin phases; short windows are noisy |
| Power of Two Choices (P2C) | Samples two endpoints, chooses the less loaded | Reduces hotspots with small selection cost; quality depends on the load signal |

Weighted algorithms differ in their exact scoring and tie-breaking. Ask whether counters are per worker, per proxy, or shared, and whether idle connections or active requests count. Algorithm names alone do not establish behavior.

### IP hash, consistent hashing, and rendezvous hashing

**IP hash** maps a source address to a backend. It offers simple affinity, but many users behind carrier NAT can become one hot key. Address changes also break affinity. A simple modulo hash changes many mappings when the number of endpoints changes.

**Consistent hashing** places nodes and keys into a mapping designed to limit movement when membership changes. Ring implementations often use virtual nodes to improve distribution. **Rendezvous hashing** scores each candidate for a key and selects the highest score; adding or removing a node moves only keys whose winner changes. Straightforward rendezvous selection evaluates all candidates, though optimized variants exist.

All participants need compatible membership, keys, weights, and hashing behavior. Stable placement reduces cache churn and aids affinity; it does not replicate state or eliminate hot keys. Never treat possession of a routing cookie or hash key as authorization.

### Resource-aware, latency-aware, and adaptive choices

Resource-aware balancing can use CPU, queue length, concurrency, or reported capacity. Latency-aware balancing can use a smoothed response-time estimate. Both introduce a feedback loop: delay, noise, and aggressive reactions can cause oscillation or synchronized traffic shifts. A fast error must not be mistaken for useful service capacity.

Static weights encode an operator estimate. Dynamic weights respond to measurements, but should have bounded changes, smoothing, and fallback behavior when telemetry is missing. Measure completed useful work and saturation together rather than optimizing one number.

**Slow start** gradually raises a newly eligible endpoint’s traffic share while caches, runtime compilation, and connection pools warm up. Least-connections can otherwise overwhelm a new server because it starts with no active work. Slow start does not create spare capacity when the entire fleet starts cold. Envoy documents its implementation and its low-traffic and locality limitations in [slow start mode](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/load_balancing/slow_start).

### Long-lived connections and uneven workloads

Ten idle keep-alive connections can cost less than one CPU-heavy request. One HTTP/2 connection may carry hundreds of concurrent RPCs. A WebSocket remains assigned for its lifetime, so adding replicas may help only new sessions. Rebalancing existing connections requires reconnecting clients or application-level migration, which has user-visible consequences.

For interviews, identify the scheduling unit first, then compare the distribution of connections, requests, in-flight work, bytes, and CPU. An algorithm that equalizes one of these cannot necessarily equalize the others.

## 3. Network & Protocol Internals

### TCP handshake, lifecycle, FIN, RST, and TIME_WAIT

A TCP connection normally starts with SYN, SYN-ACK, ACK. Endpoints negotiate transport options and maintain sequencing, retransmission, congestion control, and receive windows. Packet loss or a full accept queue can cause delays before HTTP exists.

FIN closes one direction gracefully; the peer can still send until it closes its direction. RST aborts the connection and may indicate a closed port, application abort, stale state, or an intermediary decision. Its meaning depends on where it originated and what happened immediately before it.

The active closer normally enters TIME_WAIT to handle delayed segments and retransmitted final acknowledgments. Simultaneous close can put both endpoints there. A high TIME_WAIT count can reflect healthy connection churn; investigate allocation failures and tuple pressure before changing kernel behavior. A terminating proxy maintains separate client-side and backend-side TCP lifecycles.

### TCP proxying, packet forwarding, SNAT, DNAT, and DSR

A **full TCP proxy** accepts the client connection and creates an independent backend connection. It controls buffering and timeouts on both legs. **Packet forwarding** selects a destination and forwards packets, preserving transport endpoint behavior rather than implementing two independent TCP sessions.

**DNAT** changes the destination address or port. **SNAT** changes the source, often ensuring replies return through the translator. Connection tracking records mappings so reverse packets receive the corresponding translation. NAT changes addressing; it does not itself imply TLS termination or application parsing.

With **Direct Server Return (DSR)**, requests traverse the balancer while backend responses return directly to clients. This can remove the response-bandwidth bottleneck and preserve client addresses. It requires appropriate VIP handling, routing, and network support. The backend must respond with the expected service address, and stateful firewalls or reverse-path checks must permit the asymmetric path.

### HTTP/1.1, HTTP/2, and HTTP/3

HTTP/1.1 keep-alive reuses connections across requests. Without pipelining, one connection usually has one outstanding request at a time. Client and backend reuse are separate choices: a proxy can reuse an upstream connection for requests arriving on different client connections.

HTTP/2 multiplexes streams over one TCP connection. Independent streams remove HTTP/1.1 application-level serialization, but packet loss can stall all streams sharing that TCP transport. Backend selection at the HTTP request layer can spread streams; an L4 balancer sees a single TCP flow.

HTTP/3 runs HTTP over QUIC, using UDP and stream-level transport behavior. Loss affecting one stream need not block delivery on unrelated streams, though congestion and resource limits remain shared. QUIC supports connection migration using connection IDs, so a five-tuple-only balancer may need additional routing support to preserve the correct destination. UDP support alone does not establish correct HTTP/3 behavior.

### TLS termination, passthrough, re-encryption, and mTLS

| Mode | Where TLS ends | Operational consequence |
| --- | --- | --- |
| Termination | At the balancer; backend may receive plaintext | Enables HTTP policy; protect the internal plaintext leg if used |
| Passthrough | At the backend | Backend owns certificates and TLS; encrypted HTTP is unavailable to the balancer |
| Re-encryption | Client TLS ends at balancer; separate TLS goes upstream | Encrypts both legs; requires independent backend identity validation |
| mTLS | Both peers present certificates on a TLS leg | Authenticates both sides; authorization still requires policy |

**SNI** identifies the requested server name in a TLS ClientHello when available to the receiving component. It helps choose a certificate or passthrough route. **ALPN** negotiates application protocols such as HTTP/2. The certificate must match the intended hostname; sending SNI does not replace validation. Encrypted ClientHello can limit what intermediaries can inspect.

A client certificate authenticated at an edge proxy is not automatically an authenticated backend connection. Propagate verified identity through an explicitly trusted mechanism, and prevent untrusted clients from supplying that identity metadata.

### Client IP preservation and trust

A backend behind a full proxy commonly sees the proxy’s source address. HTTP proxies can provide `X-Forwarded-For` or standardized `Forwarded` metadata. A layer that first receives untrusted traffic should sanitize incoming forwarding metadata and construct an authoritative value. Later layers should accept only the expected trusted chain.

Do not blindly take the leftmost address. Define trusted proxies and walk the chain according to the chosen library’s semantics, accounting for every intermediary. An attacker can supply forwarding headers directly if the backend is exposed.

**PROXY protocol** carries connection metadata before application traffic, including for protocols without HTTP headers. Both ends must agree to use it, and the receiver must restrict senders. It provides metadata, not cryptographic proof of identity. Enabling it on only one end commonly breaks TLS or application parsing.

### Pools, idle timeouts, draining, and streams

Connection pools reduce handshakes and port churn but consume sockets and can concentrate work. Limit both idle and active connections; an idle-cache setting is not necessarily an active-connection cap. Align proxy reuse lifetimes with backend idle behavior so a connection already closed upstream is not repeatedly reused.

Distinguish connect timeout, TLS handshake timeout, request/header timeout, response/read-idle timeout, and total deadline. A read-idle timer may reset whenever bytes arrive; it is not necessarily a maximum response duration. Order timeouts so an inner operation can fail and return a useful error before the outer deadline expires.

Draining stops new assignments and allows existing work to finish within a bound. HTTP/2 GOAWAY can signal clients to establish replacement connections, while existing streams complete. WebSockets, server-sent events, gRPC streams, and long downloads need explicit lifetime and reconnection policies. gRPC keepalive pings are not application deadlines, and a stream timeout must match its intended semantics.

UDP has no transport handshake or FIN. Balancers often use pseudo-flow state with idle expiry; request and response routing, packet size, fragmentation, and protocol-specific retry behavior still matter.

### Ephemeral ports and conntrack

Outbound proxy and SNAT connections require usable address/port tuples. A common pressure point is many connections from a limited set of source IPs to the same destination IP and port. Tuple reuse rules, active connections, TIME_WAIT, and NAT implementation determine effective capacity; there is no universal 64K-connection limit for an entire host.

Conntrack is a separate resource: entries consume memory and persist until timeout or cleanup. Exhaustion can drop new flows even when application CPU is low. Diagnose source-port allocation failures and conntrack drops separately; adding file descriptors does not fix either one.

## 4. Health Checks & Traffic Routing

### Active, passive, and application-level health

**Active checks** generate probes. **Passive checks** infer health from real traffic, including connection failures, resets, and selected application errors. Active checks can find failures without user traffic; passive checks cover paths that a synthetic probe may miss. Both need care when a local network failure is mistaken for a globally unhealthy backend.

A TCP check proves that something accepts a connection. An HTTP check can validate a status, body, host, or route. An application-level check should represent the minimum capability required to accept useful work. A lightweight health endpoint can remain green while expensive requests fail, so compare probe success with real request outcomes.

**Liveness** asks whether restarting can recover the process. **Readiness** asks whether it should receive new work. **Startup** allows initialization before normal failure decisions apply. Including a shared database outage in every liveness probe can restart the whole fleet without repairing the database. Dependency-aware readiness can also remove all capacity at once; define degraded behavior deliberately.

### Intervals, thresholds, hysteresis, and ejection

Check intervals determine observation frequency; timeouts bound probe duration; failure and recovery thresholds control transitions. **Hysteresis** uses different conditions or sustained evidence for entering and leaving unhealthy state to reduce flapping. Jitter avoids synchronized probing.

Detection time depends on the failure’s position in the probe schedule, probe duration, threshold, and routing propagation. Do not promise failover in exactly interval × threshold. Recovery may also require warm-up and capacity checks.

A **circuit breaker** bounds admission to a dependency through concurrency, pending requests, connection limits, or an open/half-open state machine. **Outlier detection** identifies endpoints whose observed behavior differs and may temporarily eject them. These are related but separate mechanisms. Bound ejection so removing endpoints does not overload survivors, and understand whether an all-unhealthy policy fails closed or sends traffic anyway.

### Routing and affinity

Host routing selects by hostname; path routing selects by URL; header routing can select a tenant, API version, or experiment. Define precedence and normalization explicitly so overlapping rules and rewrites do not route sensitive paths incorrectly. Weighted routing splits traffic among pools, while a separate algorithm distributes within each pool.

**Cookie affinity** tracks a browser or session more accurately than a source IP, but must have expiry and safe handling when a target disappears. **Source-IP affinity** is simple but suffers from NAT concentration and address changes. **Consistent-hash affinity** limits remapping during membership changes but still moves some sessions. Affinity reduces scheduling freedom and can trap a heavy tenant on one replica.

Connection draining and deregistration delay protect in-flight work during removal. Endpoint removal does not necessarily terminate established connections, and a delay alone does not prove that a stale proxy has stopped assigning new requests. Measure convergence and completion.

### Retries, budgets, deadlines, and hedging

Retry only failures and operations that are safe to replay. A request can commit a payment and lose its response; retrying it needs application-level idempotency with a durable result, not merely a new connection. Request bodies must also be replayable, and a partially delivered response usually cannot be transparently retried.

Use an end-to-end deadline, a maximum attempt count, jittered backoff, and a **retry budget** that caps extra attempts relative to original traffic. Coordinate layers: if three layers each permit three total attempts, one original request can produce up to 27 attempts at the deepest layer. Retrying an overloaded backend immediately consumes the capacity needed for recovery.

**Hedged requests** issue a second attempt after a delay to reduce tail latency. They increase load and are suitable only for safe operations with spare capacity. Cancel redundant work when possible; cancellation does not guarantee that a backend stopped processing or avoided a side effect.

### Canary, blue-green, shadow traffic, and progressive delivery

A canary sends a limited share to a new version and advances based on errors, latency, saturation, and business outcomes. A weighted split is a statistical target, not an exact request count; affinity and long-lived streams can distort it.

Blue-green deployment switches between two environments, requiring compatible state and a tested rollback. Shadowing copies traffic to a candidate without using its response for the user. Prevent shadow requests from triggering real writes, messages, payments, or sensitive-data exposure, and bound the additional work. Progressive delivery combines gradual routing changes, observation periods, and automatic or deliberate rollback criteria.

## 5. High Availability & Fault Tolerance

### Active-active and active-passive

Active-active balancers serve traffic concurrently and can provide capacity as well as redundancy. Active-passive keeps a standby ready to take over an address or route. Either design must retain enough capacity after a failure. Configuration, certificates, and dependency health must be correct on every instance before traffic moves.

Failover may preserve a VIP while losing connection state. Existing TCP sessions can reset even though new sessions succeed immediately. State synchronization helps only for the state the product actually replicates; application sessions and TLS connections may have different recovery behavior.

### VRRP, Keepalived, and a floating VIP

VRRP elects ownership of a virtual address among participating routers or hosts. Keepalived commonly implements VRRP and health-driven priority changes on Linux. On takeover, address advertisements help neighboring devices learn the new owner. Network filtering, L2 adjacency, and provider restrictions determine whether the design works.

A partition can leave multiple nodes believing they own the VIP: **split-brain**. Use appropriate network design and fencing where required; a priority number alone cannot guarantee exclusive ownership under every partition. Validate both failure and recovery paths, including stale neighbor caches and gratuitous ARP or NDP behavior.

### Anycast, BGP, and ECMP

Anycast advertises the same service address from multiple locations. BGP influences which location receives traffic according to routing policy and topology; this is not a guarantee of the lowest application latency. Route withdrawal can redirect future packets, but a stateful connection may fail if it lands at a site without its state.

ECMP distributes flows across equal-cost next hops, commonly using a flow hash. It is usually connection-level distribution, not request-level application scheduling. Changes in next-hop membership can remap flows; resilient hashing can reduce movement. BGP convergence and application health detection are separate delays.

### DNS balancing, GSLB, and geography

DNS-based balancing returns different addresses based on weights, health, location, or policy. TTL controls how long compliant caches may reuse an answer, but resolver behavior, client caches, stale answers, and persistent connections extend practical failover time. Updating DNS does not migrate established sessions.

Global Server Load Balancing (GSLB) chooses among sites, often through DNS or a global proxy. Geo-routing estimates a suitable site from resolver or client information; geography is an imperfect substitute for measured network latency. Combine routing with capacity, data residency, and dependency readiness rather than selecting the nearest site unconditionally.

### Availability zones, regions, and failure domains

Multi-AZ balancing protects against a local failure only if the remaining zones can carry the shifted load. Cross-zone selection can smooth uneven endpoint placement but introduces network cost and potentially a larger dependency surface. Region failover additionally requires usable data, authentication, secrets, and dependencies in the destination.

Health-based traffic shifting should incorporate destination capacity and avoid moving the entire load repeatedly between unhealthy regions. Define recovery hysteresis and a controlled failback. A healthy HTTP endpoint does not prove acceptable replication lag or write safety.

Asymmetric routing occurs when request and response paths differ. It may be intentional with DSR, but it can break NAT and stateful firewalls. Trace both directions before declaring the service healthy.

A redundant balancer fleet can still share a single control plane, bad configuration, DNS zone, certificate process, or power domain. Reduce blast radius through independent cells, bounded route changes, and staged configuration rollout. Test what continues working when each shared dependency disappears.

## 6. Performance & Capacity Planning

### Throughput, concurrency, connection rate, and tail latency

RPS measures requests per second; Mbps/Gbps measure bandwidth; CPS measures new connections per second. Concurrency measures simultaneous active requests or connections. A balancer handling large downloads can saturate bandwidth at low RPS, while short TLS connections can saturate CPU at low concurrent request count.

p50 represents typical latency; p95 and p99 reveal slower tails. State whether latency includes queueing, connection setup, retries, and response transfer. A percentile averaged across replicas is not the fleet percentile: combine compatible histogram counts or retain an appropriate distribution.

As utilization approaches capacity, queueing and variance can make tail latency rise sharply before throughput visibly collapses. A growing queue is an early capacity signal, not evidence that the queue should be made larger.

### Little’s Law and sizing

For a stable system measured over a consistent boundary:

```text
L = λ × W
L: average work in the system
λ: average arrival/completion rate
W: average time spent in that system

100,000 requests/s × 0.050 s = 5,000 average concurrent requests
100,000 requests/s × 0.500 s = 50,000 average concurrent requests
```

The second case needs ten times the in-flight capacity at the same RPS. Use means in Little’s Law; multiplying RPS by p99 is not the law. Model connections separately: keep-alive and multiplexing mean request concurrency is not socket count. In an unstable, growing queue, steady-state averages do not predict a bounded system.

Suppose a representative test finds that one backend sustains 4,000 RPS within its latency target. Operating at 60% of that tested rate gives 2,400 RPS per backend; 100,000 RPS then needs at least 42 healthy backends. If three equally populated zones must tolerate losing one, 63 total provides 42 survivors. Validate this estimate against skew, dependencies, deployment surge, and actual failure tests. Size the balancer fleet independently.

### Host resources and kernel limits

Track CPU by worker and core, memory, file descriptors, socket buffers, NIC bandwidth, packet rate, drops, and conntrack pressure. A full proxy typically needs sockets on both legs, plus listeners, logs, and control connections. TLS state and buffered bodies can dominate memory before descriptor limits are reached.

On Linux, application listen backlog and `net.core.somaxconn` affect the completed-connection accept queue. SYN queue behavior has separate controls such as `net.ipv4.tcp_max_syn_backlog`. TCP keepalive detects some dead idle peers after configured delays; it is not an HTTP deadline or a readiness check. The ephemeral-port range, NAT allocation, and reserved ports affect outbound connection capacity.

Tune only after identifying a measured limit and checking the running kernel’s semantics. Larger queues can hide overload and consume deadlines. Reducing connection lifetimes can worsen CPS, TLS cost, and port churn.

### TLS, multiplexing, buffering, and backpressure

TLS handshakes consume CPU and network round trips. Connection reuse and session resumption reduce repeated work. Hardware acceleration may help a specific cipher or workload, but measure the full path. TLS 1.3 early data has replay considerations, so application replay safety matters before enabling it for sensitive operations.

HTTP/2 saves connections but concentrates work if selection occurs below the stream layer. A few persistent clients can pin most work to a few backends. Track streams and outstanding requests rather than assuming connection counts represent load.

Buffering can isolate a backend from slow clients and support replay, but costs memory or disk and adds delay. Streaming reduces full-body buffering and supports long responses, while tying up resources for longer. Backpressure should limit producers when consumers slow; otherwise queues merely move from sockets into application memory. Bound request size, pending work, and concurrency, and reject overload before expensive processing.

### Representative load tests and autoscaling

Use k6 for scripted user journeys and arrival-rate scenarios; wrk and hey for focused HTTP tests; iperf3 for transport bandwidth baselines. Verify protocol support in the chosen tool: a fast HTTP/1.1 benchmark does not validate production gRPC or HTTP/3 behavior.

Include realistic payloads, TLS, keep-alive, request mixes, streams, slow clients, skew, and backend failures. A closed-loop generator sends less work as responses slow and can hide the full impact of overload, often discussed as coordinated omission. Use an appropriate arrival model and verify that generators themselves are not saturated. Record offered, admitted, completed, rejected, and timed-out work.

Autoscaling reacts after metric collection, decision, provisioning, discovery, and warm-up delays. Maintain headroom for that interval and for the chosen failure domain. A 10× instantaneous spike needs admission control or pre-existing capacity; autoscaling alone cannot absorb it. Managed balancers also have scaling behavior and quotas to validate with representative tests.

## 7. Load Balancer Technologies

### NGINX

NGINX uses event-driven workers to proxy requests and serve content. An upstream group defines destinations; proxy configuration connects routes to that group. Important operational choices include request and response buffering, upstream keepalive, resolver behavior, retry policy, and per-worker or shared upstream state.

A graceful reload starts workers with a new configuration and lets old workers finish existing work. Long-lived sessions can keep old workers and their configuration alive, so test retirement and resource overlap. Validate configuration before reload and confirm that all processes adopted the intended version.

Upstream keepalive is an idle-connection cache, not a general active-connection limit. Feature availability and defaults vary by release and edition; use the installed-version documentation rather than assuming every online example applies. See the [NGINX upstream reference](https://nginx.org/en/docs/http/ngx_http_upstream_module.html).

### HAProxy

HAProxy organizes traffic through frontends, backend pools, and ACLs that match request or connection properties. It provides detailed connection and HTTP controls suited to dedicated proxy tiers. Stick tables retain keyed observations such as rates or affinity; their sizing, expiry, and replication affect behavior.

The runtime API can change supported live state such as server availability, weights, and maps. Live changes are not automatically durable across restart; reconcile them with the source configuration. Protect administrative sockets and test graceful reload behavior for long-lived connections. See the [HAProxy configuration manual](https://www.haproxy.com/documentation/haproxy-configuration-manual/latest/) and select the deployed version and edition.

### Envoy

Envoy listeners accept downstream connections, filters process transport or HTTP behavior, routes choose destinations, and clusters define upstream pools and connection policy. It commonly appears as a service proxy, gateway data plane, or mesh component.

The xDS API family distributes resources such as listeners, routes, clusters, endpoints, and secrets. Configuration validation and acceptance are separate from end-to-end correctness. A rejected update can leave the previous version serving; expose accepted versions and rejection reasons. Independent resources also create ordering and dependency concerns during updates. See the [Envoy architecture overview](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/arch_overview).

### Traefik

Traefik uses entry points, routers, services, and middleware to accept traffic, match routes, select backends, and apply policy. Providers watch sources such as Kubernetes or container metadata to build dynamic configuration. This reduces manual endpoint management while making provider permissions, label ownership, and reconciliation visibility operational concerns. See [Traefik dynamic configuration](https://doc.traefik.io/traefik/reference/routing-configuration/dynamic-configuration-methods/).

### Cloud-managed options

| Platform | Main distinction | What an SRE must verify |
| --- | --- | --- |
| AWS | ALB provides HTTP-aware routing; NLB provides transport-oriented balancing, including TLS listener options | Listener/target protocols, target type, address preservation, health behavior, zone policy, quotas |
| Azure | Application Gateway provides HTTP-aware routing; Azure Load Balancer handles L4 traffic | SKU and frontend scope, probes, outbound SNAT, backend connectivity, TLS behavior |
| Google Cloud | Application Load Balancers handle HTTP; Network Load Balancers include proxy and passthrough families | Regional/global scope, internal/external mode, proxy versus passthrough, supported protocols |

Cloud product names do not imply identical semantics. For example, AWS ALB enables cross-zone balancing at the load-balancer level, with target-group controls, while NLB defaults differ. Google Cloud separates proxy and passthrough network products, which changes the packet path and address visibility. Consult the [AWS architecture guide](https://docs.aws.amazon.com/elasticloadbalancing/latest/userguide/how-elastic-load-balancing-works.html), [Azure Application Gateway overview](https://learn.microsoft.com/en-us/azure/application-gateway/overview), [Azure Load Balancer overview](https://learn.microsoft.com/en-us/azure/load-balancer/load-balancer-overview), and [Google Cloud overview](https://docs.cloud.google.com/load-balancing/docs/load-balancing-overview).

### IPVS, nftables, eBPF, and XDP

IPVS implements kernel transport load balancing with multiple schedulers. nftables programs kernel packet filtering and translation rules. eBPF allows verified programs at networking and other hooks; XDP runs at an early receive hook and can make efficient forwarding or dropping decisions. These are building blocks, not interchangeable products.

Their speed and visibility depend on the chosen hook, state management, NIC support, and implementation. Kernel forwarding avoids some full-proxy work but does not automatically provide HTTP parsing, TLS termination, or request-level retries. Preserve a usable debugging path when packets bypass familiar capture points.

### HAProxy versus NGINX versus Envoy

All three use asynchronous networking and can perform well. HAProxy is often a good fit for focused connection and HTTP traffic control; NGINX combines proxying with content-serving and buffering behavior; Envoy offers a rich extensible proxy model and dynamic control-plane integration. These are tendencies, not exclusive capabilities.

Choose based on protocol correctness, operational familiarity, configuration lifecycle, observability, and measured workload. Compare tail latency, CPU per useful request, memory per connection, reload overlap, and behavior during failures. A benchmark with different TLS, logging, buffering, or retry settings is not an architecture comparison.

## 8. Kubernetes & Cloud-Native Load Balancing

### Service types and the packet path

A **ClusterIP** Service provides a stable virtual address for eligible endpoints. **NodePort** exposes a service port on nodes, subject to proxy mode and address configuration. **LoadBalancer** requests an external load-balancing implementation; the actual path depends on the controller and target mode. It may send traffic through NodePorts or directly to Pod addresses.

A headless Service publishes endpoints without a normal ClusterIP forwarding path. Clients then need suitable discovery and selection behavior. A Service object itself is not a running proxy process. See [Kubernetes Services](https://kubernetes.io/docs/concepts/services-networking/service/).

```text
Possible node-target path:
client -> cloud LB -> node:NodePort -> Service forwarding -> Pod

Possible Pod-target path:
client -> cloud LB -> Pod IP

Possible HTTP ingress path:
client -> cloud LB -> ingress/gateway proxy -> application Pod
```

The last hop may use discovered Pod endpoints directly or traverse a Service address. Trace the controller’s implementation rather than assuming every diagram contains kube-proxy.

### kube-proxy, EndpointSlices, and eBPF replacements

kube-proxy watches Service and EndpointSlice state and programs node forwarding. In iptables mode, rules select endpoints for new flows; conntrack normally preserves the selection. IPVS mode uses the kernel IPVS mechanism. nftables mode uses newer packet-rule infrastructure. eBPF-based dataplanes can replace kube-proxy functionality, but policy, source-address handling, and observability depend on the implementation.

Kubernetes documents nftables mode as stable since v1.33 and IPVS mode as deprecated since v1.35. Check the cluster release, kernel, and CNI compatibility before changing modes. See [Virtual IPs and Service Proxies](https://kubernetes.io/docs/reference/networking/virtual-ips/).

EndpointSlices describe endpoint addresses and conditions, including readiness and termination information. Their changes reach consumers asynchronously. Readiness filtering governs eligibility for new work; it does not move existing TCP connections or guarantee every proxy has observed an update.

### Ingress controllers and Gateway API

Ingress defines HTTP routing intent; an ingress controller implements it. Gateway API separates infrastructure ownership from route ownership through resources such as GatewayClass, Gateway, and HTTPRoute. It offers a broader model for routing and cross-team delegation, with support depending on the controller and installed API versions.

Neither API automatically creates correct certificates, safe reference permissions, or healthy routes. Inspect controller status and route acceptance as well as the desired YAML. See [Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/) and [Gateway API concepts](https://gateway-api.sigs.k8s.io/docs/concepts/api-overview/).

### Traffic policies, topology, and source addresses

`externalTrafficPolicy: Cluster` allows eligible endpoints across the cluster, potentially adding a node hop and source translation. `Local` restricts external Service forwarding to node-local endpoints and can preserve the source address through that hop. Nodes without eligible local endpoints need appropriate external health filtering; unequal Pod placement can produce unequal backend load.

`internalTrafficPolicy: Local` restricts internal Service traffic to local eligible endpoints. If none exist, the node has no eligible destination under that policy. Locality preferences and topology-aware routing are softer mechanisms where supported; distinguish a preference from a strict local-only requirement.

Even with Local, an external proxy may already have replaced the network source address. After an HTTP ingress proxy, use trusted forwarding metadata for application-level attribution. See [Service traffic policies](https://kubernetes.io/docs/reference/networking/virtual-ips/#traffic-policies).

Cross-zone cloud balancing and Kubernetes topology selection are separate decisions. A local-first policy saves hops only when local capacity is sufficient; it can concentrate traffic during uneven placement or partial failure.

### Controllers, MetalLB, and service mesh

Cloud load balancer controllers reconcile Kubernetes resources into provider objects. Permissions, quotas, API failures, and eventual consistency can leave declared state different from actual routing. Monitor reconciliation errors and provider target registration, not just Service existence.

MetalLB allocates addresses and advertises reachability. In L2 mode, one selected node advertises a service IP using ARP or NDP; that node becomes an ingress point for that IP. In BGP mode, nodes advertise reachability to routers, which may spread flows with ECMP. MetalLB advertisement is not itself per-request HTTP balancing. See [MetalLB concepts](https://metallb.io/concepts/).

Istio commonly uses Envoy proxies for mesh traffic management. Linkerd uses its own proxy implementation. Both can influence service-to-service selection, identity, and policy; exact retry, routing, and protocol behavior differs. Avoid duplicating retries at application, ingress, and mesh layers without one shared deadline and load budget. See [Istio traffic management](https://istio.io/latest/docs/concepts/traffic-management/) and [Linkerd architecture](https://linkerd.io/2/reference/architecture/).

### Pod termination and zero-downtime rollouts

Termination changes endpoint conditions while kubelet also runs the shutdown sequence. Routing propagation and application shutdown are not a globally ordered transaction. A terminating Pod can still see traffic from stale consumers or existing connections.

Allow time for routing convergence while the application can still respond, then stop accepting new work and drain in-flight requests. A preStop hook consumes part of the termination grace period; it does not add extra time. Size the grace period for propagation, hook execution, application drain, and sidecar behavior. Fixed sleep alone is not proof of safe draining.

Keep sufficient ready capacity through rolling-update settings, readiness behavior, and warm-up. Disruption budgets constrain supported voluntary evictions; they do not prevent all failures or replace rollout configuration. Test actual HTTP, gRPC, and streaming connections during termination. See [Pod termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination).

## 9. Security

### Certificates, backend identity, and rotation

Rotate certificates and private keys before expiry and verify that every serving instance loaded them. Include intermediate chains, hostname validation, trust stores, and clock correctness. Monitor the certificate actually presented on each path, rather than just the file or secret stored in the control plane.

Use appropriate TLS versions and cipher policy for required clients. With upstream TLS, verify the backend certificate chain and intended identity, and send the correct SNI where needed. Encryption without peer validation does not establish which backend received the request. mTLS adds peer authentication; map that identity to explicit authorization.

### WAF, DDoS protection, and limits

A WAF evaluates application traffic and may detect malicious request patterns. It does not by itself protect a saturated internet link or all non-HTTP traffic. Volumetric DDoS defense may need provider or upstream filtering before traffic reaches the origin.

Rate limits bound work per time unit; connection limits bound sockets; concurrency limits bound simultaneous work. Choose keys carefully: source-IP limits penalize shared NAT users, while unauthenticated tenant headers are spoofable. Distributed enforcement can be approximate, and a remote rate-limit service becomes another dependency. Define overload behavior when it is unavailable.

### Request smuggling and malformed input

HTTP request smuggling arises when intermediaries disagree about message boundaries or parsing. Conflicting length and transfer-encoding information, duplicate headers, protocol downgrades, and ambiguous normalization can cause one request to be interpreted as part of another. Reject malformed or ambiguous input and use patched, compatible parsers across the chain.

Validate headers and routing fields before forwarding. Prevent CR/LF injection, unsafe rewrites, and trust in client-supplied internal identity headers. Do not expose origins directly if gateway policy assumes all traffic passed through the gateway. See the HTTP message-framing requirements in [RFC 9112](https://www.rfc-editor.org/rfc/rfc9112.html#section-6.3).

### Network boundaries and management access

Limit frontend and backend access with appropriate network ACLs, firewalls, security groups, and private endpoints. Account for health checker sources and return traffic. A firewall rule that permits a proxy’s address is insufficient if that proxy forwards an attacker-controlled destination without validation.

Restrict configuration, runtime APIs, dashboards, and secret access using strong authentication, authorization, and least privilege. Audit route, weight, certificate, and policy changes. Logs should support investigations without recording credentials, session cookies, or sensitive request bodies unnecessarily.

## 10. Observability & Troubleshooting

### RED metrics and capacity signals

Use RED metrics—request rate, errors, duration—at both listener and upstream boundaries. Separate original requests from retry attempts and local rejections from backend responses. Track active and idle connections, CPS, outstanding requests, pending queue depth, ejections, and healthy endpoint counts.

Correlate these with backend CPU, memory, thread or event-loop saturation, downstream pool usage, and network drops. Low LB CPU does not rule out a saturated backend connection pool or a per-worker hot spot. Limit metric label cardinality: raw paths, request IDs, and arbitrary user identities belong in controlled logs or traces rather than unbounded labels.

### Status codes and failure ownership

| Code | Common meaning | What to investigate |
| --- | --- | --- |
| 408 | Request timeout | Which server timed out receiving the request; slow client or header/body delivery |
| 429 | Too many requests | Which quota or rate limiter acted; retry guidance and client behavior |
| 499 | Nonstandard code commonly logged by NGINX for client disconnect | Caller deadline, cancellation, or an earlier proxy closing the connection |
| 502 | Bad gateway response | Upstream connect/reset/TLS/protocol failures or invalid response; product mapping varies |
| 503 | Service unavailable | No eligible destination, overload, maintenance, local rejection, or application response |
| 504 | Gateway timeout | Upstream connect/read/response deadline, depending on implementation |

A code alone cannot locate the fault. Backends may generate the same codes as proxies, and different products map transport failures differently. Record response origin, upstream address, failure reason, and timings before choosing a fix. Standard HTTP semantics are described in [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html).

### Logs, timings, tracing, and dashboards

Access logs should identify listener, route, upstream, status origin, attempts, bytes, and relevant timings. Connect time, TLS time, queue time, time to response headers, and total duration expose different bottlenecks. Confirm whether reported timers overlap before adding them together.

Propagate trace context across trusted boundaries and represent retries as distinct attempts. Trace sampling may miss rare failures, so retain aggregate counters and targeted error logs. Check whether reset or TLS failures occur before HTTP logging begins.

Prometheus exporters and native metrics can feed Grafana dashboards for traffic, latency distributions, health, resource pressure, and configuration convergence. Alert primarily on user-visible SLO burn, with supporting alerts for capacity exhaustion, certificate expiry, and failing configuration rollout. A dashboard with only average latency and healthy target count cannot explain tail failures.

### Focused diagnostic tools

Run probes from the affected network location and compare equivalent requests. The following commands use example addresses and assume the tools support the shown flags. Packet capture normally requires elevated privileges; capture only approved traffic and protect the resulting data.

```bash
curl -sv --connect-timeout 3 --max-time 10 https://api.example.com/health
curl -sv --resolve api.example.com:443:192.0.2.10 https://api.example.com/health
openssl s_client -connect api.example.com:443 -servername api.example.com -alpn h2 -verify_hostname api.example.com -verify_return_error </dev/null
ss -s
ss -tan state time-wait
dig api.example.com A
dig api.example.com AAAA
mtr -rw -c 20 api.example.com
tcpdump -ni any -c 100 'host 192.0.2.10 and tcp port 443'
```

`curl --resolve` changes destination resolution while preserving the URL hostname for Host and TLS. It is useful for testing one LB address without changing public DNS. Do not disable certificate validation when validating the production trust path.

`ss` shows socket state; tcpdump and Wireshark reveal handshakes, retransmissions, FIN/RST, and advertised windows. Encrypted payloads remain encrypted. `dig` tests a particular resolver view, which can differ from an application cache. `mtr` helps compare routes and loss, but an intermediate router deprioritizing probes is not proof of end-to-end packet loss.

### A repeatable troubleshooting sequence

1. Establish scope: clients, protocols, regions, zones, routes, timestamps, and recent changes. Preserve evidence before restarting components.
2. Resolve DNS from the affected environment and compare the actual chosen address, address family, and cached state.
3. Check transport establishment and TLS identity separately from HTTP. Locate retransmissions, resets, handshake errors, and listener failures.
4. Inspect accepted routing configuration, eligible endpoints, policy, queues, connection pools, and timeout reasons on each hop.
5. Compare LB-path requests with controlled direct-backend probes that preserve Host, SNI, authentication, and request shape. A successful local health request is not an equivalent test.
6. Correlate failures with application traces and resource limits. Apply the smallest reversible mitigation and verify user outcomes.

For uneven distribution, compare per-endpoint requests, bytes, active streams, CPU, weights, affinity keys, and locality. For intermittent errors, group by endpoint, zone, connection age, deployment generation, and IP family. For latency spikes, separate queueing, DNS, connect, TLS, upstream execution, and transfer. This identifies whether the next investigation belongs in the LB, network, resolver, or application.

## 11. Expert Interview Scenarios

### 1. Explain the complete L4 versus L7 path

Start with DNS, destination address, route, and the frontend listener. In an L4 NAT design, the first packet selects a backend, translation and flow state preserve the choice, and replies traverse the required return path. TLS and HTTP remain between client and backend. In DSR, the reply path bypasses the balancer.

In an L4 full proxy, the LB terminates one TCP connection and opens another, even if it forwards encrypted TLS bytes. In an L7 terminating proxy, it also handles client TLS, parses HTTP, matches a route, chooses an upstream, acquires a backend connection, and forwards the request. Backend TLS, if used, is a separate session. Trace response buffering, logging, timeout ownership, and source identity on the return path.

### 2. Design an architecture for 100K+ RPS

First ask about payload size, latency SLO, request cost, TLS and connection churn, streaming, geography, state, and required failure tolerance. RPS alone is insufficient: 100,000 responses/s at 10 KB each already implies roughly 1 GB/s, or 8 Gbps, of response payload before protocol overhead.

Use redundant entry points and LB instances across failure domains, sufficient healthy backend capacity after a chosen failure, and independently scalable application tiers. Apply the concurrency and survivor-capacity calculation from section 6 to measured performance. Bound queues, retries, connections, and expensive routes. Include certificate and configuration rollout, SLO telemetry, and overload rejection.

Validate steady state, zone loss, rolling deployment, control-plane interruption, and recovery with realistic traffic. Explain what existing connections lose during failover and how clients recover. Do not choose the instance count from a generic proxy benchmark.

### 3. Healthy backends, intermittent 502/504

Health checks sample a narrow path. Group errors by upstream, zone, protocol, and connection age, then inspect whether failure happened during connect, TLS, request send, response headers, or body transfer. Compare the LB-generated status with any application-generated error.

A backend closing idle sockets earlier than the proxy expects can cause resets on reused connections. Slow downstream queries or exhausted worker pools can cause response timeouts while health checks stay fast. Also inspect stale endpoints during rollout, SNAT pressure, packet loss, and mismatched protocol settings. Change the measured failing limit or lifecycle behavior, then verify the same failure cohort disappears; simply extending every timeout can increase concurrency and deepen the outage.

### 4. Why does Round Robin produce uneven CPU?

It equalizes selections at a particular layer. Requests can have different CPU cost, duration, cache behavior, or downstream work. Servers may have unequal capacity, and connection-level round robin can pin very different HTTP/2 workloads to each endpoint. Affinity, locality, and multiple independent schedulers further change the aggregate result.

Compare useful work and saturation, then choose a scheduling signal that better tracks cost: outstanding requests, bounded resource feedback, separate expensive-work pools, or capacity weights. No simple counter perfectly predicts unknown request cost.

### 5. Troubleshoot ephemeral port exhaustion behind SNAT

Confirm allocation errors and identify which host or NAT device owns the scarce mappings. Count active and TIME_WAIT connections by source and destination tuple; check connection churn, keepalive reuse, NAT metrics, port range, and conntrack counters. Distinguish an exhausted source-port pool from a full tracking table or descriptor limit.

Reduce churn through safe connection reuse or multiplexing, distribute connections over additional supported source addresses or NAT capacity, and check per-destination limits. Adjust ranges or timeouts only when supported by the measured failure and platform semantics. Validate that retries decline and new connections succeed; a kernel tweak on a client does not expand a managed NAT device’s independent limits.

### 6. Design zero-downtime LB configuration updates

Version and validate configuration, stage referenced secrets and dependencies, then apply it to a small serving subset. Confirm acceptance, effective routes, certificate identity, and synthetic and real traffic outcomes before expanding. Keep the last working configuration available for rollback.

Use supported graceful reload or dynamic-update mechanisms. Account for overlapping worker memory, connections that retain old routes, and finite drain time for streams. An acknowledged control-plane update proves acceptance, not that every traffic path works. State the connection-lifetime assumptions behind the zero-downtime claim.

### 7. Explain retry amplification during an outage

If a dependency can handle 10,000 attempts/s and original traffic is already 9,000/s, even one extra attempt on 20% of requests adds 1,800/s and exceeds capacity. Failures trigger retries, retries increase queueing, and longer waits trigger more failures.

Assign one overall deadline, cap attempts across layers, enforce a retry budget, add jitter, and reject work beyond capacity. Retry only replay-safe operations and stop retrying errors that indicate persistent overload. Measure attempts per original request and recovery latency, not only the final success rate.

### 8. Diagnose imbalanced gRPC over HTTP/2

Determine whether the client resolves endpoints directly or connects through an L4 or L7 balancer. One long-lived HTTP/2 connection through an L4 balancer sends all its RPC streams to one backend. Adding backend replicas does not redistribute that existing connection.

Use suitable client-side RPC balancing with endpoint discovery or an HTTP/2-aware L7 proxy that selects per RPC. Where appropriate, multiple channels or controlled connection rotation can spread new work, but verify client policy and avoid synchronized reconnect storms. Streaming RPCs remain attached for their lifetime; balancing individual messages requires application-level design.

### 9. Design multi-region failover using DNS and Anycast

Define recovery-time and recovery-point objectives, active-active or standby behavior, write ownership, and standby capacity first. Use health signals that reflect user paths and required regional dependencies. DNS can shift new resolutions; Anycast can shift packet reachability after route changes. Neither moves application state or guarantees uninterrupted sessions.

Combine these mechanisms only where their roles are clear—for example, DNS choosing among regional services while Anycast fronts resilient global entry points. Bound health-triggered shifts, test partitions and resolver caching, and prevent two regions from accepting conflicting writes when the application cannot reconcile them. Fail back gradually after verifying data and capacity.

### 10. Explain client IP loss through LoadBalancer and Ingress

Draw every source-changing hop: external LB, node Service forwarding, ingress proxy, and application. Determine whether the external LB forwards or proxies, whether node traversal introduces SNAT, and whether the ingress establishes a new backend connection.

`externalTrafficPolicy: Local` can preserve the source through eligible node-local Service forwarding, but cannot restore an address lost at an earlier proxy. At the HTTP boundary, configure trusted forwarding metadata; use PROXY protocol only where both ends and the controller support it. Verify the socket peer and trusted logical client address separately, including spoofed-header tests from an untrusted client.

### 11. Design session affinity for a stateful application

Identify the actual state: disposable cache, login session, transaction state, or durable user data. Prefer external or replicated session storage when appropriate so another instance can serve after failure. Affinity can improve cache locality but should not be the only protection for durable state.

For local session state, use a controlled cookie or authenticated identity key, consistent placement, bounded session lifetime, and an explicit policy when a target fails. Source-IP affinity is weak for mobile users and shared NAT populations. Drain nodes during planned changes; expect membership changes to move some keys.

If state cannot be recovered elsewhere, state the resulting limitation: failover may require reauthentication or lose in-progress session work. Test a hot user, lost cookie, endpoint removal, and failed state store. Affinity is routing convenience; it provides neither authorization nor replication.

### 12. Diagnose cascading failure during a 10× spike

Look for the first saturated resource: LB CPS, TLS CPU, backend concurrency, database pool, queue, bandwidth, or a shared dependency. Preserve basic observability and identify original traffic versus retries. A CPU graph alone may miss sockets, ports, or queue limits.

Shed excess work early using bounded admission, concurrency limits, rate limits, and priority for critical operations. Reduce retries and optional fan-out, cap queues, and serve cached or degraded responses where correct. Add capacity where it can become useful without overwhelming the next dependency.

Avoid ejecting the entire fleet because overloaded instances fail probes together, and avoid flooding recovering instances before they warm up. Recover progressively, verify useful completion rate and tail latency, and then review headroom, autoscaling delay, retry policy, and failure-domain capacity against the observed spike.
