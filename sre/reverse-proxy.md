# Reverse Proxy — SRE Expert Checklist

A backend is healthy, yet users see intermittent gateway errors, streaming responses arrive in bursts, and a deployment drops established sessions. A reverse proxy can cause all three problems because it owns connections, protocol translation, routing, and queues on the path to the application. An expert interview answer should explain what happens at each hop, which measurements distinguish competing causes, and how a proposed fix affects reliability.

This guide follows the supplied checklist. It assumes familiarity with Linux, HTTP, TLS, and basic Kubernetes. Configuration fragments use illustrative names and addresses; they are not complete production configurations. Check the installed version, compiled modules, controller, and edition before applying directives. The Kubernetes community ingress-nginx controller reached retirement in March 2026; that project is distinct from the NGINX server and other NGINX controllers. See the [Kubernetes retirement announcement](https://kubernetes.io/blog/2025/11/11/ingress-nginx-retirement/).

## Contents

1. [Fundamentals & Architecture](#1-fundamentals--architecture)
2. [HTTP & Protocol Internals](#2-http--protocol-internals)
3. [Routing & Traffic Management](#3-routing--traffic-management)
4. [Buffering, Caching & Compression](#4-buffering-caching--compression)
5. [Performance & Scalability](#5-performance--scalability)
6. [Security & Hardening](#6-security--hardening)
7. [Reverse Proxy Technologies](#7-reverse-proxy-technologies)
8. [Kubernetes & Cloud-Native Integration](#8-kubernetes--cloud-native-integration)
9. [Reliability & High Availability](#9-reliability--high-availability)
10. [Observability & Troubleshooting](#10-observability--troubleshooting)
11. [Expert Interview Scenarios](#11-expert-interview-scenarios)

## 1. Fundamentals & Architecture

### Reverse proxy, forward proxy, load balancer, and API gateway

A **reverse proxy** accepts traffic on behalf of servers. The client addresses the public service; the proxy selects and contacts a backend. A **forward proxy** acts on behalf of clients that need controlled access to destinations, often using explicit proxy settings or an HTTP CONNECT tunnel. Both can enforce policy, but they sit on different trust boundaries.

A **load balancer** distributes traffic among targets. It may forward packets or terminate connections at L4, or act as an HTTP reverse proxy at L7. A reverse proxy can have only one backend, so proxying does not imply balancing. An **API gateway** adds API-oriented policy such as authentication, quotas, request transformations, and API lifecycle management. These roles overlap; product names alone do not describe the actual traffic path.

### L4, L7, transparent, and explicit proxying

An L4 proxy selects destinations using transport information such as addresses and ports. With TLS passthrough, it generally cannot route on HTTP paths or inspect request bodies. Some implementations inspect an unencrypted TLS ClientHello for SNI without decrypting application traffic; encrypted ClientHello can limit that visibility.

An L7 proxy parses an application protocol, allowing host, path, method, and header routing. It can translate protocols independently on its two sides: HTTP/2 from clients and HTTP/1.1 to a backend is a common example. A downstream connection is not necessarily paired one-to-one with an upstream connection.

**Explicit** proxying means the client deliberately uses a proxy, for example through an HTTP proxy setting. **Transparent/intercepting** proxying redirects traffic without that configuration. Transparent source-address preservation is another related usage of the term; state which meaning you intend. Ordinary reverse proxying preserves the public service interface but usually creates an upstream connection whose source is the proxy.

### Request lifecycle and placement

A useful lifecycle is **accept → parse → route → forward → respond**. For HTTPS over TCP, accepting includes TCP establishment and a TLS handshake. Parsing validates framing and request metadata. Routing applies virtual-host, path, and policy rules. Forwarding acquires an upstream connection, sends data, and handles failures. Responding may include caching, compression, buffering, logging, and connection reuse. Streaming makes some of these stages overlap.

An **edge proxy** faces external clients and handles public certificates, abusive traffic, and external protocol compatibility. An **internal proxy** fronts private services and often uses service discovery and workload identity. A **sidecar** sits beside a workload and intercepts or receives its inbound/outbound traffic. More proxy hops provide more policy points, but also more queues, handshakes, and failure modes.

Mesh **ingress gateways** bring external traffic into a mesh; **egress gateways** provide controlled exits. Sidecars commonly handle east–west service traffic. A reverse proxy becomes part of a mesh architecture through its discovery, identity, and policy integration; the forwarding function itself is not unique to meshes. An egress policy is enforceable only if workloads cannot bypass it through another network path.

### Data plane, control plane, and TLS boundaries

The **data plane** handles live traffic. The **control plane** supplies routes, endpoints, certificates, and policy. Static files are simple to review but need a distribution/reload process. Dynamic APIs react faster to endpoint changes but need authentication, validation, rollout control, and visibility into accepted configuration. A control-plane outage may leave established proxies serving their last accepted configuration while new instances fail to initialize.

| TLS mode | Where TLS ends | Operational consequence |
| --- | --- | --- |
| Termination | Client TLS ends at the proxy; upstream can be plaintext | L7 inspection is available; the internal hop needs an explicit security decision |
| Passthrough | Application terminates client TLS | Proxy cannot normally inspect HTTP; application manages certificates |
| Re-encryption | Separate client–proxy and proxy–backend TLS sessions | Proxy can inspect traffic and protect the internal hop; verify upstream identity |
| mTLS | Either or both hops authenticate both peers | Requires trusted issuers, identity authorization, rotation, and expiry monitoring |

Re-encryption alone does not authenticate a backend if certificate verification is disabled. Client mTLS identity also does not automatically survive termination: the next hop sees the proxy certificate unless a deliberately protected identity propagation mechanism exists.

## 2. HTTP & Protocol Internals

### HTTP/1.1 reuse and HTTP/2 multiplexing

HTTP/1.1 normally keeps connections persistent, avoiding repeated TCP/TLS setup. A connection carries sequential exchanges; pipelining permits multiple outstanding requests but requires responses in order, so one slow response blocks later responses. Many clients avoid pipelining. Connection reuse requires both endpoints to agree on message boundaries and consume or safely discard remaining data. See [HTTP/1.1 framing and persistence](https://www.rfc-editor.org/rfc/rfc9112.html).

HTTP/2 interleaves frames from multiple **streams** on one TCP connection. Stream and connection flow-control windows regulate DATA; stalled consumption can restrict progress. It removes HTTP/1.1 response-order blocking, but TCP still delivers bytes in order, so packet loss can delay every stream on that connection. Stream limits, shared congestion control, and uneven long-lived connections still affect capacity and balancing. HTTP/2 forbids connection-specific fields such as `Connection` and `Upgrade`; the special `TE: trailers` case is allowed. See [RFC 9113](https://www.rfc-editor.org/rfc/rfc9113.html).

HTTP/3 maps HTTP onto QUIC over UDP. QUIC provides reliable independent streams, congestion control, and TLS 1.3 integration; UDP does not make HTTP delivery unreliable. Loss affecting one stream need not block delivery on another, although congestion and some protocol dependencies remain shared. Networks must permit UDP, and operators need QUIC-aware visibility and a fallback strategy. The client-facing protocol does not dictate the upstream protocol. See [RFC 9114](https://www.rfc-editor.org/rfc/rfc9114.html).

### TCP lifecycle and half-close behavior

TCP establishes a connection with SYN, SYN-ACK, ACK. An orderly FIN closes one sending direction; the peer can still transmit, so a **half-closed connection** is not necessarily broken. A reset terminates the connection abruptly and can surface as a failed request or truncated response. Investigate which endpoint sent FIN/RST and when, rather than assuming the backend crashed.

TIME_WAIT commonly remains on the active closer to protect against old segments. Many short upstream connections can consume ports and connection-tracking capacity. CLOSE_WAIT usually means the peer closed its sending direction and the local application has not closed its socket; persistent growth can indicate a lifecycle bug. HTTP reuse, TCP keepalive probes, and application heartbeat messages solve different problems.

### Headers and proxy trust

End-to-end headers describe the request or representation across intermediaries. Hop-by-hop fields apply to one connection and must be handled at that hop. An HTTP/1.1 proxy must also remove fields named by `Connection`; forwarding a fixed list alone is insufficient. See [HTTP intermediary semantics](https://www.rfc-editor.org/rfc/rfc9110.html).

| Field | Meaning and interview trap |
| --- | --- |
| `Host` | HTTP/1.1 target authority; validate allowed public hosts before routing or constructing redirects |
| `:authority` | HTTP/2 and HTTP/3 pseudo-header for authority; protocol translation must avoid conflicting authority information |
| `Connection` | HTTP/1.x connection options and names of connection-specific headers |
| `Upgrade` | HTTP/1.1 protocol-switch request; it requires explicit proxy handling |
| `Via` | Records intermediary protocol information; useful for proxy-chain diagnosis |
| `Forwarded` | Standardized `for`, `by`, `host`, and `proto` information; not intrinsically trustworthy |
| `X-Forwarded-For` | Common client/proxy address chain; an attacker can supply its initial contents |
| `X-Forwarded-Proto` | Common original scheme indication; incorrect trust can cause redirect loops or insecure URL generation |
| `X-Real-IP` | Common single-address convention; it has no universal chain semantics |

At an internet edge, discard untrusted identity headers and construct authoritative values from the connection. Behind trusted intermediaries, verify the immediate peer and parse the chain according to a documented append/overwrite policy. For an append-based X-Forwarded-For chain, walk from the trusted right-hand side until the first untrusted address. Never assume the leftmost address is authentic. `Forwarded` has quoting and IPv6 syntax that simple comma/colon splitting can mishandle; see [RFC 7239](https://www.rfc-editor.org/rfc/rfc7239.html).

### WebSocket, SSE, gRPC, and framing

WebSocket over HTTP/1.1 starts with an upgrade exchange and a successful `101 Switching Protocols` response. Afterward, the proxy handles a long-lived bidirectional channel. Forward the required upgrade information deliberately and coordinate idle timeouts with heartbeat intervals. WebSocket mechanisms over HTTP/2 or HTTP/3 differ; support is implementation-specific. See [NGINX WebSocket proxying](https://nginx.org/en/docs/http/websocket.html).

**Server-sent events (SSE)** are a long-lived HTTP response, usually `text/event-stream`. Buffering or compression that waits for more bytes can turn prompt events into delayed batches. Disable inappropriate buffering, flush at the application, and send periodic heartbeat data where needed. Long stream duration is expected and should not trigger ordinary request-latency alerts.

Native **gRPC** uses HTTP/2, including trailers such as `grpc-status`. HTTP 200 alone does not prove RPC success. Bidirectional streaming needs flow control and cancellation to work across every hop; gRPC-Web uses a different browser-oriented protocol. Set RPC deadlines and propagate remaining time to dependencies; cancellation does not automatically stop application work. See the [gRPC deadline guide](https://grpc.io/docs/guides/deadlines/).

In HTTP/1.1, `Content-Length` gives the body length in bytes, while chunked transfer coding carries body chunks and can end with trailers. HTTP/2 and HTTP/3 use their own framing rather than HTTP/1.1 chunk markers. Proxies must translate framing consistently, preserve supported trailers, and reject ambiguity. A streamed body is not necessarily chunked, and chunked transport does not guarantee prompt application delivery.

### SNI, ALPN, resumption, and validation

**SNI** helps select a TLS virtual host/certificate before the HTTP request exists. **ALPN** negotiates application protocols such as HTTP/2. The HTTP authority can differ from SNI, so certificate selection is not a substitute for host validation.

Certificate validation checks the chain to a trusted issuer, validity, and the intended server identity. Upstream connections to an IP may still need a configured DNS identity for SNI and hostname verification. Session resumption reduces handshake work but requires safe ticket-key management and rotation. TLS 1.3 early data can be replayed; do not allow it to trigger unsafe application operations merely because the TLS session is resumed. See the [NGINX TLS module reference](https://nginx.org/en/docs/http/ngx_http_ssl_module.html).

## 3. Routing & Traffic Management

### Matching, rewriting, and canonicalization

Host-based routing separates virtual services; path-based routing separates resources within one host. Header and method rules can select API versions, tenants, or specific operations. Define precedence and fallbacks so an unmatched request cannot silently reach an internal administrative backend. Security decisions must use authenticated tenant identity rather than a caller-controlled routing header.

A **rewrite** changes the request forwarded internally. A **redirect** tells the client to issue another request. For redirects, 307/308 preserve the method; 301/302 can cause clients to change POST to GET. Validate redirect destinations and avoid constructing them from an arbitrary Host header.

Canonicalization is a trust boundary. Decide how repeated slashes, percent encoding, encoded slash, dot segments, case, query parameters, and trailing slashes behave. If authorization sees `/public/../admin` differently from the application, a routing rule can become an access-control bypass. Test the actual parser chain with deliberately chosen cases rather than normalizing repeatedly at different layers.

### Service discovery and DNS freshness

Static endpoints are predictable but require updates when instances move. DNS discovery delegates endpoint naming to DNS; dynamic discovery can provide endpoints, health, weight, and locality directly. For each mechanism, identify who refreshes the state and what happens during an outage.

A DNS TTL does not force every application to re-resolve at that interval. Some proxies resolve names at startup/reload; others use configured runtime resolvers or discovery APIs. Negative caches, resolver failure, stale records, and long-lived pools can all extend the use of obsolete targets. Existing connections can remain usable after a DNS change. Plan separate policies for refreshing addresses, retiring connections, and retaining last-known-good discovery state.

For NGINX, inspect the installed release and the actual upstream/variable/resolver configuration; declaring `resolver` alone is not proof that every upstream name refreshes. The [upstream module reference](https://nginx.org/en/docs/http/ngx_http_upstream_module.html) documents runtime resolution and version qualifications.

### Pools, retries, and timeout budgets

Connection pooling avoids setup latency and reduces port churn. Bound active connections, idle connections, pending acquisitions, and per-origin pools. HTTP/2 stream capacity is a separate limit from TCP connection count. Too few connections can cause queues; too many can overwhelm a backend or consume descriptors. A pool retaining sockets longer than the backend allows can produce failures when it tries to reuse a connection just closed by the peer.

Retry only when the operation and failure stage make it safe. A connect failure before transmission differs from a timeout after a payment may have committed. HTTP method idempotency helps, but application semantics, replayable bodies, and a server-enforced idempotency key determine safety. Streaming bodies or responses already delivered to the client can make replay impossible.

Use a retry budget, bounded attempts, backoff with jitter, and a remaining-time check. If three layers each make three total attempts, one user request can generate up to 27 deepest-layer attempts. Retries should consume a shared request budget rather than reset the deadline at each hop.

For a hypothetical 2-second client deadline, reserve time for edge processing and returning a response, then constrain internal work and any retry to the remaining budget. A connect timeout, an inactivity timeout, and a total request deadline are different controls. A peer sending one byte periodically can defeat an inactivity timeout without completing useful work.

### Rate limits, concurrency, and rollout control

A rate limit caps arrivals per interval; a concurrency limit caps work currently in progress. Slow requests can exhaust a backend even at a modest RPS. A circuit breaker rejects or limits work when failure or capacity signals cross a threshold; define its scope, recovery probes, and behavior during partial failure. Per-instance rate limits multiply as the proxy fleet scales unless coordinated or divided intentionally.

**Canary routing** sends a measured share to a new version; compare errors, latency, saturation, and business outcomes before increasing traffic. Weighted connections or requests do not necessarily produce the same share of users or CPU, especially with HTTP/2 and long-lived streams. **Blue-green deployment** switches between complete environments and still needs data compatibility and a rollback plan.

Session affinity may use a cookie, source address, or consistent hash. A cookie can keep a user on one backend; source IP affinity groups many users behind NAT. Consistent hashing reduces remapping when membership changes but does not prevent hotspots or preserve in-memory sessions after a backend dies. Prefer external session state when possible; validate affinity cookies and provide a safe fallback.

### Draining and configuration changes

Draining stops assigning new work while allowing existing work to finish. Removing an endpoint from discovery is not sufficient if proxies still use cached state or persistent connections. HTTP/1.1 connection closure, HTTP/2 GOAWAY, and application-level reconnect behavior can help retire connections without abandoning active requests.

Validate configuration, roll it out gradually, observe the accepted version, and retain a rollback artifact. Graceful reloads keep old workers serving existing traffic while replacements take over; long-lived streams require an explicit drain deadline. Zero downtime is a tested outcome under realistic traffic, including failed reloads and concurrent backend changes.

## 4. Buffering, Caching & Compression

### Request and response buffering

Request buffering reads some or all of a body before passing it upstream. It can protect application workers from slow uploads and allow replay when otherwise safe. Large bodies may spill to temporary files, so the proxy becomes responsible for disk space, I/O, quotas, permissions, and cleanup. Streaming begins forwarding earlier and reduces storage demand, but exposes upstream capacity to slow senders and restricts retries once data has been sent.

Response buffering lets a proxy read promptly from an application while serving a slower client. This can release backend resources earlier, at the cost of proxy memory or disk. Once buffers fill, backpressure must propagate or a bounded policy must terminate/reject work; unbounded buffering turns client slowness into proxy failure. See the [NGINX reverse proxy guide](https://docs.nginx.com/nginx/admin-guide/web-server/reverse-proxy/).

For large uploads, coordinate client body limits, backend limits, upload deadlines, temporary storage, and available bandwidth. If 100 simultaneous buffered uploads each consume 1 GiB, storage demand can approach 100 GiB before overhead and responses. Prefer direct-to-object-storage upload designs when that fits the application, with authorization and size validation at the storage boundary.

### Cache identity and HTTP semantics

A shared cache must distinguish every input that changes the selected representation: typically scheme, validated authority, path, relevant query parameters, and selected request headers. Query normalization is safe only when the application considers those forms equivalent. Cache keys that omit tenant, authorization, language, or content-encoding distinctions can expose one user’s response to another.

Freshness and storage are separate. `max-age` defines freshness, `s-maxage` targets shared caches, `private` excludes shared-cache storage, `no-store` prohibits storage, and `no-cache` requires validation before reuse. ETag and Last-Modified enable conditional revalidation, often yielding 304. `Vary` adds request-header dimensions to representation selection. Eviction removes stored entries under capacity policy; expiration makes an entry stale; invalidation deliberately removes or supersedes it. See [HTTP caching semantics](https://www.rfc-editor.org/rfc/rfc9111.html).

**Bypass** means a request does not consume a cached response; it does not necessarily prevent storing the new response. Configure both read and write behavior for sensitive paths. Do not globally cache authenticated requests or responses setting session cookies unless there is a reviewed isolation design. A cache TTL directive is not a sufficient privacy policy.

Revalidation asks the origin whether a stored object remains valid. `stale-while-revalidate` permits bounded stale serving while refresh occurs; stale-on-error is a separate policy. Neither is suitable for every resource: an old image may be acceptable, while a revoked authorization decision may not be. Define the maximum permitted staleness explicitly. See the [stale response extensions](https://www.rfc-editor.org/rfc/rfc5861.html).

### Stampedes, poisoning, and compression

A cache stampede occurs when many requests refresh the same expired object together. Request coalescing/cache locks, TTL jitter, prewarming, and bounded stale serving can reduce the surge. Keep wait limits and origin failure behavior bounded so the protection does not become another long queue.

Cache poisoning makes a cache store an attacker-influenced response under a key later used by other clients. Unkeyed forwarded headers, unsafe Host handling, parser inconsistencies, and query collisions are common causes. Test whether every response-affecting input is included in the key, ignored safely, or makes the response uncacheable. Compare body, headers, and tenant context on both miss and hit.

Gzip and Brotli trade CPU for fewer bytes. Compression thresholds avoid wasting work on tiny payloads; already compressed images and archives often gain little. Precompressed static assets can shift work out of request handling. Verify the build/module support for Brotli, and account for content negotiation and `Vary: Accept-Encoding`. Compression of responses combining secrets with attacker-controlled content can create a length side channel.

SSE needs timely flushing through the application, proxy, and compression layer. gRPC requires a protocol-aware forwarding path and trailer preservation; treating it as a buffered downloadable object breaks stream semantics. Measure event/message delivery latency, not merely time to the first response byte.

## 5. Performance & Scalability

### Event loops and NGINX workers

A process or thread per connection is straightforward but can incur significant stack and scheduling costs at high concurrency. Event-driven designs multiplex many sockets through readiness notification and nonblocking I/O. They still need bounded work: blocking disk access, expensive filters, CPU-heavy compression, or a synchronous extension can stall progress on a worker.

NGINX uses a master to manage configuration and workers; workers handle connections using event loops. Reloading creates a new worker generation while old workers drain. Worker count does not remove application or kernel bottlenecks. See the [NGINX process model](https://nginx.org/en/docs/beginners_guide.html).

`worker_connections` includes upstream and other worker connections, not only client connections. With one active HTTP/1.1 upstream socket per client, the usable proxied-client count can be substantially below workers × worker_connections. HTTP/2 multiplexing, idle pools, listening sockets, and files change that estimate. File descriptor limits must agree across the process, service manager/container, and host. See [NGINX connection limits](https://nginx.org/en/docs/ngx_core_module.html#worker_connections).

### Find the bottleneck before tuning

| Resource | Evidence to collect | Typical cause |
| --- | --- | --- |
| CPU | Per-worker usage, throttling, profiles, softirq load | TLS, compression, parsing, filters, packet processing |
| Memory | RSS, allocator behavior, socket/buffer memory, OOM events | Many streams, large headers/buffers, queues, cache metadata |
| Network | Bytes/s, packets/s, loss, retransmits, interface drops | Bandwidth or packet-rate ceiling, poor path, MTU issues |
| Disk | Temp-file volume, I/O latency, free bytes/inodes | Buffered uploads/responses, cache writes, synchronous logging |
| Connections | Active/idle sockets, pool wait, descriptors, conntrack | Excess idle retention, connection churn, backend limits |

Connection reuse saves handshakes and RTTs but retains resources while idle. Short keepalive timeouts increase churn; very long ones retain sockets and obsolete endpoint connections. Tune against traffic shape, peer timeouts, NAT behavior, and memory rather than a single universal value.

On Linux, the listen backlog is bounded by application settings and kernel limits such as `net.core.somaxconn`; SYN backlog behavior is a separate part of the connection path. Listen drops suggest accept-path pressure. Raising a backlog can absorb a burst but cannot create sustained processing capacity.

Ephemeral port exhaustion affects outbound connection creation and can appear at a proxy, node SNAT, or shared NAT gateway. Count usable source address/port combinations for each destination, connection duration, and TIME_WAIT; do not infer capacity only from total socket count. Reuse, more source addresses, and distributing destinations can help. Blind TCP sysctl changes can conceal the bottleneck or violate platform assumptions.

### TLS cost, tail latency, and capacity

Measure TLS handshakes/sec separately from requests/sec. A fleet serving 100K RPS over reused connections has a different CPU profile from one establishing 100K fresh TLS connections/sec. Session reuse and supported cryptographic acceleration can help, but protocol, key type, library, hardware, and traffic mix determine the benefit. Benchmark the deployed build and inspect negotiation/resumption rates.

Little’s Law is **L = λW**: average in-system concurrency equals throughput times average time in the same system boundary, assuming stable conditions. At 100K requests/sec and 50 ms average residence time, average concurrency is about 5,000 requests. This says nothing directly about p99; multiplying by p99 is not a valid average-concurrency calculation.

Queueing increases sharply near saturation, and averages can look healthy while p95/p99 deteriorate. Measure queue wait separately from service time, use realistic arrival patterns, and avoid load generators that omit waiting time when overloaded. A short bounded queue with explicit rejection often preserves more successful work than a large queue full of requests whose callers have already timed out.

A hypothetical 100K RPS × 10 KiB response body is about 1.024 GB/s, or 8.2 Gb/s of body bytes alone. Add request traffic, headers, TLS overhead, retransmissions, and traffic on the upstream side. Plan RPS, bytes/sec, packets/sec, active connections/streams, TLS handshakes/sec, memory, and failed-zone capacity independently. Measure streaming and large-upload workloads separately from tiny cached GETs.

## 6. Security & Hardening

### TLS and identity

Prefer supported TLS 1.2/1.3 configurations compatible with required clients; review policy against the actual TLS library. Supply the correct certificate chain, protect private keys, monitor expiry, and rehearse rotation and rollback. OCSP stapling depends on the certificate issuer, responder support, and client ecosystem; it is not universally available or a substitute for renewal monitoring.

For upstream TLS, explicitly enable certificate verification, configure the trusted issuer, and validate the intended backend name. For mTLS, also provide the proxy’s client certificate/key and ensure the backend authorizes that identity. A certificate signed by an accepted CA proves membership in a trust domain, not permission to call every service. Restrict direct backend access so callers cannot bypass edge authentication. See [securing NGINX upstream HTTP traffic](https://docs.nginx.com/nginx/admin-guide/security-controls/securing-http-traffic-upstream/).

### Browser headers and authentication gateways

HSTS instructs browsers to use HTTPS for future requests; enable it only after the named scope, including any subdomains, supports HTTPS. CSP constrains browser content execution and sources; it needs application-specific design and testing. `X-Content-Type-Options: nosniff` reduces MIME-sniffing ambiguity. These headers do not authenticate API clients or stop server-side vulnerabilities.

OAuth2 delegates authorization; OIDC adds an identity layer. A gateway validating JWTs must check signature, an explicit algorithm policy, issuer, audience, expiry, and applicable time claims. Do not accept an arbitrary token-provided key URL as trusted. Handle signing-key rotation with bounded caching and controlled refresh. Cookie-based login flows also need CSRF/session protections.

If the proxy injects identity headers, remove caller-supplied copies and make the backend accept them only over a protected trusted path. Define behavior when the authentication service is unavailable. Accidentally failing open can turn an identity dependency outage into unauthorized access.

### Filtering, resource protection, and parser agreement

IP allowlists are useful for known administrative networks but do not replace workload/user authentication. WAF rules can catch known patterns and virtual patches, yet can also block valid traffic or miss parser differences. Local rate limits cannot protect an already saturated external link; volumetric DDoS protection usually belongs upstream with the provider or edge network.

Request smuggling occurs when adjacent parsers disagree about where one request ends and another begins. Content-Length/Transfer-Encoding disagreement is a classic case; HTTP/2-to-HTTP/1 translation and malformed headers can expose other discrepancies. Reject ambiguous framing, use maintained strict parsers, and avoid forwarding malformed requests. If both framing fields are present and forwarding is chosen, HTTP/1.1 requires removing Content-Length; reject-and-close is often the clearer boundary policy. See [RFC 9112 security considerations](https://www.rfc-editor.org/rfc/rfc9112.html#section-11.2).

Header/CRLF injection occurs when untrusted data becomes raw protocol syntax. Use structured header APIs, reject invalid field characters, and avoid reflecting arbitrary input into redirects or log formats. Host-header attacks can affect password-reset links, cache identity, and routing; use allowed authorities and a configured public origin for security-sensitive URL generation.

Normalize paths consistently before routing and authorization, and test encoded slashes, double encoding, and dot segments through every hop. Do not turn a caller-controlled URL or Host into an unrestricted upstream destination: that creates SSRF or an open proxy. Use destination allowlists, controlled DNS/egress policy, and protection for internal/metadata endpoints. Restrict redirect targets to intended origins.

Limit header count/size, body size, connection counts, and concurrent streams. Slowloris protection needs header/body progress controls and resource limits; large total deadlines alone are insufficient. Apply limits per trusted client identity where feasible, and check NAT/shared-client effects. Stream endpoints need a deliberate policy distinct from finite API calls.

## 7. Reverse Proxy Technologies

### NGINX configuration and URI behavior

An `http` context contains `server` virtual hosts and `upstream` groups; a `server` contains `location` handlers. `proxy_pass` selects an HTTP upstream, and `proxy_set_header` controls forwarded metadata. `grpc_pass` is used for native gRPC proxying. This illustrative fragment belongs inside `http`; it assumes this NGINX instance is the public edge:

```nginx
upstream app_pool {
    server 192.0.2.10:8080;
    server 192.0.2.11:8080;
    keepalive 32;
}

server {
    listen 8080;
    server_name api.example.test;

    location /api/ {
        proxy_pass http://app_pool;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

The `keepalive` value controls cached idle upstream connections per worker, not a total active connection limit. Explicit HTTP/1.1 and removal of the default close behavior make the pooling intent clear across versions. Pair connection reuse with backend capacity controls. See [upstream keepalive](https://nginx.org/en/docs/http/ngx_http_upstream_module.html#keepalive).

For a simple prefix location with no rewrite or variable-based target, the URI distinction is:

```nginx
location /api/ {
    proxy_pass http://app_pool;
}
```

`/api/users?active=1` reaches the backend as `/api/users?active=1`.

```nginx
location /api/ {
    proxy_pass http://app_pool/;
}
```

Here the configured URI `/` replaces the matching normalized `/api/` prefix, producing `/users?active=1`. This is URI replacement, not a general rule to remove any trailing slash. Regex/named locations, rewrites, and variables need separate reasoning. See [`proxy_pass` URI rules](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_pass).

For ordinary same-level locations, an exact `=` match wins; otherwise NGINX finds the longest prefix, then checks regex locations in order unless the selected prefix uses `^~`. The first matching regex wins, or the remembered prefix is used. Nested locations and internal redirects add stages, so inspect the effective configuration. A proxy location ending in `/` can also trigger an automatic redirect for its slashless form. See [location matching](https://nginx.org/en/docs/http/ngx_http_core_module.html#location).

Inheritance is directive-specific. For example, defining any `proxy_set_header` at a child level replaces inheritance of the parent set, so a new header can unintentionally remove required settings. Includes are textual configuration, not an extra inheritance layer. Review effective behavior after includes and controller-generated configuration.

| NGINX directive | Meaning |
| --- | --- |
| `proxy_connect_timeout` | Deadline for establishing the upstream connection |
| `proxy_read_timeout` | Maximum interval between successive upstream reads, not total response duration |
| `proxy_send_timeout` | Maximum interval between successive upstream writes, not total upload duration |
| `proxy_buffering` | Controls buffering of upstream responses |
| `proxy_request_buffering` | Controls buffering of the client request body before forwarding |
| `proxy_cache` | Selects a configured cache zone; key, eligibility, storage, and freshness need separate configuration |

These controls affect different stages; raising all of them can increase retained work during an outage. See the [proxy directive reference](https://nginx.org/en/docs/http/ngx_http_proxy_module.html).

### HAProxy

HAProxy separates client-facing `frontend` rules from upstream `backend` pools. In HTTP mode, ACLs can select a backend by authority, path, or other parsed properties. A `defaults` section can provide shared modes and timeouts. This is an illustrative plaintext fragment:

```haproxy
defaults
    mode http
    timeout connect 2s
    timeout client 30s
    timeout server 30s

frontend public_http
    bind :8080
    acl api_path path_beg /api/
    use_backend api_nodes if api_path
    default_backend web_nodes

backend api_nodes
    balance roundrobin
    server api1 192.0.2.20:8080 check

backend web_nodes
    server web1 192.0.2.21:8080 check
```

The path is preserved here. A plain `check` does not by itself establish application readiness; configure an appropriate HTTP check when needed. Production virtual-host validation, TLS, authentication, and streaming timeouts remain separate decisions. See [HAProxy frontend routing](https://www.haproxy.com/documentation/haproxy-configuration-tutorials/proxying-essentials/configuration-basics/frontends/).

### Envoy and xDS

Envoy **listeners** accept connections. **Filter chains** select transport and network processing, including TLS. The **HTTP Connection Manager** handles HTTP parsing, streams, and HTTP filters. **Routes** select upstream **clusters**, whose endpoint sets and balancing policy determine the target. See [Envoy HTTP connection management](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/http/http_connection_management).

xDS distributes dynamic resources: **LDS** listeners, **RDS** routes, **CDS** clusters, and **EDS** endpoints; **SDS** can supply secrets. Resources have versions, subscriptions, ACK/NACK behavior, and dependencies. A rejected update can leave the previous accepted configuration active. Inspect each proxy’s accepted state rather than assuming a control-plane push succeeded. Ordering and aggregated discovery help manage dependencies, but do not imply a fleet-wide atomic transaction. See the [Envoy xDS protocol](https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol).

### Traefik and operational trade-offs

Traefik **routers** match requests entering configured entry points; **middlewares** apply behavior such as redirects, authentication integration, or header changes; **services** describe forwarding and balancing. **Providers** supply dynamic configuration from sources such as Kubernetes or files. Provider credentials and discovery permissions are security-sensitive because discovery can publish routes. See [Traefik dynamic configuration](https://doc.traefik.io/traefik/reference/routing-configuration/dynamic-configuration-methods/).

| Technology | Common strength | Operational question |
| --- | --- | --- |
| NGINX | Reverse proxying, static serving, caching; established file-based configuration | How are endpoint updates, reloads, module support, and edition differences managed? |
| HAProxy | L4/L7 balancing, explicit traffic controls, connection visibility | How are runtime changes reconciled with persistent configuration and reloads? |
| Envoy | Protocol-aware filtering, telemetry, dynamic discovery | Who operates the control plane, manages resource dependencies, and debugs rejected updates? |
| Traefik | Provider-driven discovery and integrated routing configuration | Who may publish routes, and how are conflicting/provider-specific rules controlled? |

Select by protocol requirements, change rate, team experience, failure behavior, and measured workload. An implementation’s feature list does not prove a particular controller or packaged edition exposes that feature safely.

## 8. Kubernetes & Cloud-Native Integration

### Ingress, Gateway API, and implementations

An **Ingress** describes HTTP(S) routing, but a controller must translate it into a functioning data plane. Core Ingress features are limited; annotations often expose controller-specific behavior. `Exact`, `Prefix`, and `ImplementationSpecific` path types have different contracts; Kubernetes Prefix matching is element-based, so `/foo` does not mean every path beginning with those characters. Confirm the selected IngressClass and controller interpretation. See [Kubernetes Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/).

**Gateway API** separates infrastructure ownership through GatewayClass, listener configuration through Gateway, and application routing through resources such as HTTPRoute. It adds explicit attachment and cross-namespace reference controls. Resources still require an implementation; feature support and conformance depend on controller and API version. Inspect status conditions such as Accepted, Programmed, and ResolvedRefs as applicable. See the [Gateway API overview](https://kubernetes.io/docs/concepts/services-networking/gateway/).

The community **ingress-nginx** controller is retired and receives no continuing upstream maintenance after March 2026. Treat it as a migration/legacy subject in an interview. **Envoy Gateway** and **Traefik** are examples of other controller choices, with different supported APIs and extensions. F5’s NGINX Ingress Controller is a separate project; the similar names do not imply the same lifecycle. Verify migration behavior for rewrites, authentication, rate limits, certificates, and annotations instead of translating YAML mechanically.

### Traffic path and endpoint discovery

A common logical path is:

```text
Client → external load balancer → ingress/gateway proxy → backend Pod
                                      │
                         Service + EndpointSlice discovery
```

Sometimes the actual forwarding path is proxy → Service ClusterIP → Pod. Other controllers watch EndpointSlices and connect directly to Pod IPs, bypassing the Service virtual IP. A Service in the route definition therefore does not prove a Service VIP is a packet hop. The external load balancer may preserve source addresses, SNAT them, send PROXY protocol, or terminate HTTP and add forwarding headers; configure the next hop accordingly.

Services give stable service identity while EndpointSlices describe backend addresses, ports, and conditions such as ready, serving, and terminating. Discovery and data-plane updates are asynchronous. Inspect both the API state and the endpoints the proxy currently uses. See [EndpointSlice semantics](https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/).

### TLS placement, readiness, and termination

TLS at the ingress centralizes public certificate operations and enables edge policy. TLS at the application preserves termination there but limits ingress HTTP inspection if traffic passes through encrypted. Mesh mTLS can protect a separate internal hop; it does not automatically cover traffic between an external load balancer and ingress or enforce application authorization. Document each TLS session and identity separately.

Readiness determines whether a workload should receive new service traffic. It is distinct from liveness, which can restart the container, and startup checks, which protect slow initialization. Make readiness reflect the ability to accept useful work without turning every shared dependency failure into removal of all replicas.

During deletion, endpoint updates and kubelet shutdown progress concurrently. A preStop hook consumes the termination grace period; it is not extra time. Stop new work, allow discovery changes to propagate, and let the application finish active requests within the remaining grace period. Persistent upstream connections and long streams require explicit draining behavior. A fixed sleep alone is not a guarantee. See [Pod termination behavior](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination).

### Mesh and scaling

Sidecars introduce per-workload connection pools, retry policies, and telemetry. Diagnose both application and sidecar saturation; a healthy application can still be unreachable through an exhausted proxy. Avoid independently enabled retries in the client, gateway, sidecar, and SDK.

Scale proxy replicas using measured saturation signals: CPU, active streams, connection counts, network throughput, and queue delay. CPU alone can miss idle long-lived connection load. New replicas help only after they are configured, ready, and reached by upstream balancing; existing HTTP/2/WebSocket sessions may stay on old replicas. Spread instances across zones/nodes, reserve failure headroom, and account for topology policies that can concentrate traffic when an endpoint or zone disappears.

## 9. Reliability & High Availability

### Redundancy and health checks

Active-active deployments share traffic across proxies and can retain capacity after one fails. Active-passive uses a standby, often through VIP or routing failover, but must test detection, takeover time, and standby readiness. Stateful sessions, local caches, and rate counters do not automatically transfer. Existing connections commonly fail when their owning instance disappears; plan client reconnection and safe request replay.

Active health checks probe on a schedule; passive checks infer health from real traffic. A TCP handshake proves reachability, not application correctness. HTTP checks should use the intended port, path, Host, and TLS identity. Distinguish process liveness from readiness, and use thresholds/hysteresis to prevent flapping. Outlier detection should avoid ejecting every endpoint for a common upstream dependency problem.

### Cascades, load shedding, and discovery outages

If a backend slows, request concurrency grows, queues expand, caller deadlines expire, and retries create more traffic. Circuit breaking, concurrency limits, bounded queues, and early load shedding can interrupt this feedback loop. Rejecting excess work promptly preserves resources for requests that can still finish. Define priority and tenant fairness so a single workload cannot consume the entire pool.

Discovery outages need a bounded stale-state policy: retaining known endpoints may preserve service, but those addresses can later be reassigned or revoked. Monitor discovery age and errors separately from data-plane success. Keep enough independent resolvers/control-plane replicas, verify recovery, and secure bootstrap configuration. Last-known-good state does not help a new proxy that has never received valid configuration.

In a proxy chain, track who owns retries and the total deadline. A backend’s 503 may pass through unchanged, while a gateway may create its own 503 before contacting any backend. Once a response body has started, the proxy often cannot replace it with a clean error; truncation/reset is a different failure from a complete 502 response.

### Reloads and backend deployments

A safe NGINX file rollout validates the intended configuration, sends a reload, and verifies new workers and traffic. NGINX keeps the previous configuration if it cannot apply the replacement, but success of a command alone does not prove all traffic has moved to new workers. Graceful shutdown uses QUIT; fast termination can interrupt work. See [NGINX control signals and reload behavior](https://nginx.org/en/docs/control.html).

```bash
sudo nginx -t
sudo nginx -s reload
```

Run reload only after validation succeeds, then inspect logs, accepted configuration, and request results. Keep the previous artifact ready to restore. Set an intentional policy for long-lived connections and worker shutdown deadlines; repeated reloads can retain several old generations if streams never end.

For backends, provision healthy replacement capacity before withdrawing old targets. Keep old and new application versions compatible with routes, cookies, schemas, and sessions during overlap. Remove old targets from new-work selection, wait for propagation and draining, then terminate. Roll out the proxy and application independently when possible so a failure can be attributed and reversed.

## 10. Observability & Troubleshooting

### Logs, timings, metrics, and traces

Use structured access logs for method, validated host, route, final status, upstream address/status, bytes, timings, request ID, and selected retry/cache fields. Error logs explain connection refusal, TLS verification, parser, DNS, or timeout failures. Escape log values and exclude authorization tokens, session cookies, and sensitive query/body data.

For NGINX, `$request_time` covers the request from first client bytes through completion of response sending. `$upstream_connect_time` measures upstream connection establishment, including TLS where applicable; `$upstream_header_time` measures time to the upstream response header; `$upstream_response_time` measures upstream response handling. These are not independent stages to sum or subtract blindly. Multiple attempts can produce multiple values, and buffering or streaming changes how client and upstream time overlap. See [NGINX upstream timing variables](https://nginx.org/en/docs/http/ngx_http_upstream_module.html#variables).

Track request rate, error rate by route and origin, latency distributions, active/idle connections, streams, pool/queue wait, retries, handshake failures, DNS/discovery age, cache hit ratio, temp disk usage, and worker saturation. For gRPC, include RPC status; for streams, include disconnect reasons and message delay. Keep metric labels bounded: raw paths, user IDs, and request IDs belong in logs/traces, not unbounded Prometheus labels.

Generate or validate request IDs at the trust boundary and propagate tracing context with a documented policy. Trace client, proxy, and application spans to expose queueing and retries. Treat caller trace headers as untrusted input and avoid placing credentials in baggage. Sampled traces explain examples; aggregate metrics determine whether the problem is widespread.

### Status codes and likely investigation paths

| Status | Meaning at the observed hop | First evidence to collect |
| --- | --- | --- |
| 400 | Invalid request, authority, headers, or framing; possibly an application error | Generating component, parser error, request size and protocol |
| 408 | Request timeout, often while receiving client data | Client progress, header/body timeout, upstream presence |
| 413 | Request content exceeds a limit | Proxy and app limits, buffering stage, declared/actual body size |
| 429 | Rate/concurrency policy rejected traffic | Limiter identity, scope, counters, retry behavior |
| 499 | NGINX log convention for client closing before a response completes | Caller deadline, downstream proxy cancellation, backend latency |
| 502 | Gateway received an invalid/unusable upstream response or connection failure | Error log, upstream address, TLS, refusal/reset, malformed/truncated headers |
| 503 | Service unavailable or policy/overload rejection | No healthy targets, breaker/queue rejection, maintenance, backend-generated status |
| 504 | Gateway timed out waiting for upstream work | Which timer expired, DNS/connect/read stage, backend trace and saturation |

Codes alone do not identify the root cause. Products map failures differently; NGINX also has configurable status mappings. A 499 is usually visible in logs rather than a response the client received. Identify the generating hop from correlated logs/upstream status and traces; response headers or a branded error page are clues, not proof.

### Practical tools

These commands are examples for Linux/network troubleshooting; use the relevant namespace/container and replace hosts and ports. Packet captures and verbose output can contain credentials, so collect narrowly and store securely.

```bash
curl -v --connect-timeout 2 --max-time 5 https://api.example.test/health
curl --resolve api.example.test:443:192.0.2.30 https://api.example.test/health
curl -N https://api.example.test/events
curl -sS -o /dev/null -w 'dns=%{time_namelookup} connect=%{time_connect} tls=%{time_appconnect} first_byte=%{time_starttransfer} total=%{time_total}\n' https://api.example.test/health
openssl s_client -connect api.example.test:443 -servername api.example.test -alpn h2,http/1.1 -verify_hostname api.example.test -verify_return_error </dev/null
ss -s
ss -tan state close-wait
sudo tcpdump -ni any -c 100 'host 192.0.2.10 and tcp port 8080'
sudo nginx -t
sudo nginx -T
```

`--resolve` bypasses DNS for the chosen authority while preserving Host/SNI and certificate validation. Curl timings are cumulative milestones, not independent durations; they also describe the client’s connection, not every internal hop. Do not use `-k` as a fix for TLS failures. `openssl s_client` needs the appropriate trust store, especially for private CAs. NGINX `-T` dumps configuration and can reveal embedded credentials; `-t` validates configuration but does not prove backend reachability.

Prometheus exporters expose implementation-specific metrics; confirm what each counter measures and whether optional status modules are required. Grafana dashboards should correlate traffic, latency, errors, saturation, deployments, discovery freshness, and certificate expiry. Alert on user-visible SLO burn and approaching resource limits rather than every transient individual failure.

### A repeatable diagnostic sequence

1. Bound the incident: affected host/route, zone, proxy instance, protocol, body size, and time window. Check recent changes.
2. Identify the failing hop from access/error logs and traces. Distinguish downstream cancellation from an upstream failure.
3. Compare request time with upstream timing/attempts, queue wait, cache behavior, and application service time.
4. Check resolver/discovery state, selected endpoints, TCP/TLS connection results, and pool capacity.
5. Inspect buffering and temporary disk, memory, descriptors, connection limits, and backend saturation.
6. Reproduce with the same Host, SNI, protocol, and route. Change one hypothesis at a time and confirm recovery under representative traffic.

High request time with fast upstream completion can indicate slow client delivery or body upload; high first-upstream-header time can include application queueing or buffering before response headers. A healthy direct Pod request does not exercise ingress policy, discovery, upstream TLS, or pooled connections. Preserve those differences when comparing paths.

## 11. Expert Interview Scenarios

### 1. Explain an HTTPS request through NGINX

Start with DNS and any external load balancer, then TCP acceptance and TLS selection using SNI/ALPN. NGINX parses HTTP, validates framing/authority, selects the server/location, and applies access, authentication, rewrite, limit, or cache policy. A cache hit may avoid the application entirely.

On a miss, NGINX selects an upstream, obtains a connection, establishes TCP/TLS if needed, forwards headers/body according to buffering policy, and reads the response. It may buffer, compress, cache eligible data, and send downstream. Finally it records outcome/timings and reuses or closes each connection independently. Mention that retries, streaming, internal redirects, and client cancellation alter this path.

### 2. Diagnose 502 versus 504 and locate the generator

A 502 suggests an unusable upstream exchange, such as refusal, failed TLS verification, premature closure, or invalid headers. A 504 suggests an upstream timeout. Confirm the implementation’s mapping and the exact error rather than treating these as exhaustive definitions.

Correlate the outer proxy’s final status with upstream status/address and request ID. If the outer proxy received 504 from the next hop, inspect that hop’s logs to find its timer. Reproduce against the same target with correct SNI/Host, inspect backend resource pressure, and compare failure timing to configured timeouts. Raising a timer before identifying the stage can retain more failing requests.

### 3. Explain `proxy_pass` with and without the trailing slash

Use `/api/users?active=1`: in a simple `location /api/`, a target without a URI preserves `/api/users`; a target ending in `/` replaces the matched prefix and sends `/users`, retaining the query. Explain that the slash introduces a URI component in this example. State the assumptions, then test encoded paths, the slashless `/api` case, rewrites, and variable/regex configurations separately.

### 4. Diagnose incorrect client IPs behind several proxies

Draw each hop and record the TCP peer, incoming forwarding fields, and emitted values. Determine whether the external load balancer preserves source IP, uses PROXY protocol, or sends HTTP forwarding headers. PROXY protocol is separate from HTTP headers and should be accepted only from trusted peers.

For NGINX with realip support, an illustrative fragment is:

```nginx
set_real_ip_from 192.0.2.0/24;
real_ip_header X-Forwarded-For;
real_ip_recursive on;
```

Use the actual proxy address ranges, never all internet addresses. Ensure those peers sanitize/append correctly and cannot be bypassed. Log both the effective client and original peer (`$realip_remote_addr`) while verifying the chain. See [NGINX real-IP behavior](https://nginx.org/en/docs/http/ngx_http_realip_module.html). Test a forged incoming X-Forwarded-For value from an untrusted client; it must not become an authoritative identity.

### 5. Explain unexpected WebSocket or SSE disconnects

First establish whether the handshake/initial response succeeds. A failed WebSocket handshake points to upgrade forwarding, routing, authentication, or protocol compatibility. Successful streams that end after a repeatable idle interval suggest a proxy, load balancer, firewall/NAT, or application timer. Capture who sends close/FIN/RST and correlate deployments/reloads.

For SSE, inspect whether bytes are flushed promptly or arrive in bursts. Coordinate buffering/compression and heartbeat behavior at every hop. A WebSocket fragment can use this `http`-level map and server-level location:

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    '' close;
}

server {
    listen 8081;
    server_name stream.example.test;

    location /socket/ {
        proxy_pass http://app_pool;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
        proxy_read_timeout 90s;
    }
}
```

The 90-second interval is illustrative; design heartbeat frequency and all-hop timeouts from the product’s reconnect requirements. Do not assume this HTTP/1.1 upgrade fragment configures SSE or native gRPC.

### 6. Separate buffering, DNS, and pooling latency

Compare cold and reused connections, cache hits/misses, upload sizes, response sizes, and protocol versions. DNS delays may appear on first resolution, refresh, or a discovery/control-plane path rather than every request. Pool waits may grow while actual backend execution remains fast. Request buffering delays upstream dispatch; response buffering can delay event delivery while backend completion is fast.

Correlate client milestones, proxy timings, resolver logs, pool queues, temp disk I/O, and application spans. Use a small controlled experiment for each cause: pin an endpoint with preserved authority, compare a warm pool, or disable response buffering only on a test streaming route. Confirm the improvement without shifting overload onto the backend.

### 7. Design public TLS termination with upstream mTLS

Use redundant edge proxies with public certificates, protected private keys, authority validation, authentication, and bounded traffic limits. Restrict backend access to trusted proxy identities/network paths. The proxy validates backend certificates and presents its own client identity; the backend validates and authorizes that identity. Automate separate public/internal certificate rotation and monitor both.

Inside a TLS-enabled NGINX server, an illustrative upstream fragment is:

```nginx
location / {
    proxy_pass https://secure_app;
    proxy_ssl_server_name on;
    proxy_ssl_name app.internal.example;
    proxy_ssl_trusted_certificate /etc/nginx/pki/backend-ca.pem;
    proxy_ssl_verify on;
    proxy_ssl_verify_depth 3;
    proxy_ssl_certificate /etc/nginx/pki/proxy-client.pem;
    proxy_ssl_certificate_key /etc/nginx/pki/proxy-client.key;
}
```

Define `secure_app`, public listener/certificate configuration, filesystem permissions, and the correct chain depth for the real deployment. Test expired, wrong-name, untrusted, and unauthorized client certificates, not just the successful handshake. Protect forwarded user identity separately from the proxy’s TLS identity.

### 8. Explain request smuggling between different parsers

Suppose one hop delimits a request using Content-Length while another accepts a conflicting transfer-coding interpretation. They consume different byte counts, allowing bytes intended as one body to become another request on a reused connection. The consequence can be authorization bypass, misassociated responses, or cache contamination.

Explain the framing disagreement and shared-connection consequence without assuming all attacks require the same two headers. Prevent ambiguity at ingress, preserve strict framing during protocol conversion, patch all hops, and close connections after invalid framing. Validate the complete chain with an authorized test environment; testing one proxy in isolation cannot establish parser agreement.

### 9. Implement graceful reloads and backend deployment

Validate and stage a concrete configuration artifact, reload one proxy cohort, confirm worker replacement and representative requests, then expand. Keep rollback available and watch long-lived old workers. Do not restart the entire fleet to apply a routine routing change.

For backend replacement, add ready capacity, verify it through the proxy, stop new assignment to old targets, and drain while discovery converges. Keep the application running long enough for existing requests, with an explicit maximum for streams. Test ordinary traffic, uploads, HTTP/2, and persistent sessions during rollout; exercise failure and rollback paths too.

### 10. Diagnose stale DNS and closed keepalive sockets

Correlate failed upstream IPs with current DNS/EndpointSlice membership and the proxy’s actual accepted state. If failures continue to old addresses, distinguish lack of refresh from old connections surviving a successful refresh. Check negative caching and discovery errors before reducing TTL.

If failures cluster on the first request after an idle period, compare upstream idle timeout, proxy pool retention, NAT timers, and FIN/RST traces. Shorter proxy idle retention or adjusted backend policy may reduce the race. A safe bounded retry can mask some stale-connection failures, but it cannot guarantee that an already transmitted non-idempotent request did not execute.

### 11. Design a highly available 100K+ RPS deployment

Begin with the workload: request/response sizes, cache hit ratio, TLS connection rate, HTTP versions, stream duration, authentication/compression cost, regions, and SLO. Benchmark one instance under realistic open-arrival load, including logs, filters, and upstream latency, until the tail-latency or resource boundary appears.

For a hypothetical tested safe capacity of 20K RPS per instance, six instances across three zones can carry 100K normally but only 80K after losing one zone. Nine instances, three per zone, leave six instances or 120K safe capacity after one zone fails. That arithmetic still requires validation of connection skew, load-balancer convergence, network capacity, and shared dependencies; it is not a hardware guarantee.

Use active-active zone distribution, upstream connection reuse, bounded queues/concurrency, discovery resilience, certificate automation, and tested overload shedding. Capacity-test the authentication service, backend fleet, DNS, NAT/conntrack, and external balancer. Add canary configuration rollout, SLO alerts, and zone-failure exercises. Scale on the first constrained resource rather than RPS alone.

### 12. Diagnose retries and timeout cascades

A backend slowdown followed by rising concurrency, retry attempts, and outer-proxy cancellations suggests positive feedback. Compare user request rate with attempts at every hop; calculate worst-case amplification and identify work still running after callers time out. Long read timeouts and large queues can make recovery slower even after the original trigger ends.

Mitigate by shedding excess work, reducing unnecessary retries, constraining concurrency, and propagating cancellation/deadlines. Preserve a small budget for safe retries with jitter and remaining-time checks. Then address the underlying bottleneck and verify that dependency recovery reduces queues, orphaned work, and tail latency. A strong answer distinguishes immediate stabilization from the permanent change and states what evidence would confirm both.
