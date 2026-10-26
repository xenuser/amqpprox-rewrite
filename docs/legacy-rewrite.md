# Legacy application migration: vhosts and quorum queues

This fork can translate a client's AMQP 0.9.1 `Connection.Open` vhost and
compatible `Queue.Declare` arguments. Both features are opt-in. Defaults retain
upstream behavior. This is a compatibility adapter, not a message migration tool.

## What is rewritten

An application configured with `amqp://server-url1:5672/VHOST1` must first reach
the proxy. Change its endpoint to the proxy, or deliberately direct
`server-url1` to the proxy using DNS/network configuration. The proxy connects
to `server-url2` via its backend configuration and changes `VHOST1` to `VHOST2`.
There is no complete URI string on the AMQP wire to replace. The scheme/host/
port select transport; the vhost is a separate connection method.

```text
application -> proxy: VHOST1, x-queue-type=classic
proxy -> new broker:  VHOST2, x-queue-type=quorum
```

Use the actual decoded vhost in flags: `/`, `VHOST1`, or `customer-a/app`.
Do not put `amqp://`, a hostname, or URI percent-encoding in these flags. In an
AMQP URI, `/` as the vhost is normally encoded as `%2F` in its path.

| Option | Meaning |
| --- | --- |
| `--rewriteVhostFrom /` | Exact, case-sensitive source vhost; defaults to `/` |
| `--rewriteVhostTo customer-a/app` | Destination; enabling vhost rewriting |
| `--rewriteClassicQueuesToQuorum` | Also rewrite queue declarations on matching connections |
| `--passThroughUnconvertibleQueues` | Forward declarations that cannot become quorum queues unchanged instead of closing the connection (requires the quorum option) |

A source vhost or quorum option without a nonempty destination is rejected.
Vhost values must be nonempty, at most 255 bytes, and contain no NUL.
Configuration is immutable for the lifetime of the process. Restart to change
it. There is **one mapping per process**, shared by all its listeners. Use
separate processes/configurations for multiple independent customer mappings.
Unmatched vhosts continue through unchanged; this is not an access-control rule.

The destination name is used for backend routing, auth-service requests,
vhost limits, pause controls and statistics. `MAP FARM`/`MAP BACKEND` must
therefore refer to the destination vhost. Credentials are unchanged; the broker
checks them against that destination. The auth interceptor's existing credential
replacement capability is separate from this feature.

## Plain AMQP quick start

```sh
amqpprox --listenPort 5673 --destinationDNS server-url2 --destinationPort 5672 \
  --rewriteVhostFrom VHOST1 --rewriteVhostTo VHOST2 \
  --rewriteClassicQueuesToQuorum --consoleVerbosity 3
```

The app uses the proxy address/port and still requests `VHOST1`. Simple mode
routes all accepted vhosts to that backend. For a shared service, prefer explicit
mappings and restricted broker users. Source-to-target rewriting can also be
used without the quorum flag; in that case every queue declaration is unchanged.

## Three-node backend and AMQPS

AMQPS works on ingress and egress independently. TLS must terminate at the proxy
so it can read AMQP frames; it opens a separate TLS connection to the broker.
TLS passthrough cannot perform these rewrites. The existing implementation uses
TLS 1.2 contexts. A legacy client must negotiate a protocol/cipher suite accepted
by the deployed OpenSSL policy. This feature does not add obsolete TLS support.

Start the proxy **without** the simple-mode listener:

```sh
amqpprox --controlSocket /run/amqpprox/control --logDirectory /var/log/amqpprox \
  --rewriteVhostFrom VHOST1 --rewriteVhostTo VHOST2 \
  --rewriteClassicQueuesToQuorum --consoleVerbosity 3
```

In a second terminal (directories and permissions must already exist):

```sh
ctl=amqpprox_ctl
socket=/run/amqpprox/control
$ctl "$socket" TLS INGRESS CERT_CHAIN_FILE /etc/amqpprox/proxy-chain.pem
$ctl "$socket" TLS INGRESS KEY_FILE /etc/amqpprox/proxy-key.pem
$ctl "$socket" TLS EGRESS CA_CERT_FILE /etc/amqpprox/broker-ca.pem
$ctl "$socket" TLS EGRESS VERIFY_MODE PEER
$ctl "$socket" BACKEND ADD_DNS rabbit1 local rabbit1.example.com 5671 TLS
$ctl "$socket" BACKEND ADD_DNS rabbit2 local rabbit2.example.com 5671 TLS
$ctl "$socket" BACKEND ADD_DNS rabbit3 local rabbit3.example.com 5671 TLS
$ctl "$socket" FARM ADD cluster round-robin rabbit1 rabbit2 rabbit3
$ctl "$socket" MAP FARM VHOST2 cluster
$ctl "$socket" LISTEN START_SECURE 5673
```

The app connects to `amqps://proxy-name:5673/VHOST1`. Its trusted CA must validate
the proxy certificate, whose SAN must cover the name used by the app. If DNS
redirects an unchanged legacy name, the certificate must cover that name.
For mutual TLS, also configure ingress `CA_CERT_FILE` and
`VERIFY_MODE PEER FAIL_IF_NO_PEER_CERT`, plus egress `CERT_CHAIN_FILE` and
`KEY_FILE` if the broker requires a client certificate. Client certificate
identity is not transparently forwarded; the broker sees the proxy certificate.
AMQP PLAIN credentials still authenticate the application. SASL mechanisms
requiring Connection.Secure/SecureOK are not supported by the upstream handshake.

**Existing TLS limitation:** upstream egress `VERIFY_MODE PEER` verifies the CA
chain but does not configure hostname verification or SNI. This change does not
add them. Do not interpret that option as full broker hostname authentication;
review this with your security team and use a tightly scoped trusted broker CA
and controlled routing. Backends requiring SNI need an additional TLS change.
The provided test application's ingress TLS connection does verify hostnames.

For plain AMQP in this full setup, use backend port 5672 without `TLS` and
`LISTEN START 5673`. Mixed transports (AMQP ingress, AMQPS egress, or the reverse)
are possible. Keep credentials protected on any plaintext network leg.

## Queue declaration behavior

| Incoming declaration on a matching connection | Result with quorum rewriting enabled |
| --- | --- |
| Active, no `x-queue-type`, convertible | Inject `x-queue-type=quorum` |
| Active, explicit `classic`, convertible | Replace with `quorum` |
| Active, missing/`classic`, unconvertible | Close the connection (default), or forward unchanged with `--passThroughUnconvertibleQueues` |
| Active, explicit `quorum`, `stream` or another type | Pass through unchanged for the broker to validate |
| Passive declaration | Pass through unchanged |
| Nonmatching connection or feature disabled | Pass through unchanged |

A declaration is **convertible** when the queue is named, durable,
non-exclusive and non-auto-delete, and has no `x-max-priority` and no
`x-overflow=reject-publish-dlx` argument. Everything else is unconvertible:
quorum queues cannot be exclusive, transient or server-named, and converting
priority queues or auto-delete queues would silently change application
behavior.

Classic-only tuning arguments are dropped from converted declarations:
`x-queue-mode` (for example `lazy`), `x-queue-version` and
`x-queue-master-locator`. They control classic storage and mirroring-era
placement, have no quorum meaning, and would otherwise make RabbitMQ refuse the
quorum declaration. All other arguments are kept byte-exact. If the application
later redeclares the queue with these arguments, the proxy drops them again, so
the redeclaration stays equivalent.

### Unconvertible declarations

By default the proxy refuses an unconvertible declaration with an AMQP
`Connection.Close` to the application: reply code **540** (`NOT_IMPLEMENTED`),
the failing method `Queue.Declare` (class 50, method 10), and a reason such as
`amqpprox: cannot convert to quorum, queue 'reply' is exclusive; quorum queues
cannot be exclusive`. Client libraries surface this as a connection shutdown
with that code and text (for example `OperationInterruptedException` in the
.NET client), and the proxy logs a warning with the connection summary.
Frames the application sent before the rejected declaration are still
forwarded; the declaration and anything after it are not. The proxy then waits
for the application's `Connection.CloseOk` and closes the broker side normally.
Malformed declarations (reply code 502) and rewritten frames exceeding the
negotiated frame-max (reply code 501) are refused the same way.

The whole connection closes, because the proxy cannot close a single channel
without the broker's agreement. Configure client retry/backoff to avoid
reconnect loops, and fix the application's declaration or use pass-through.

With `--passThroughUnconvertibleQueues` the declaration is forwarded unchanged
and the target vhost's rules decide: a server-named exclusive reply queue, for
example, is created with the vhost's default type or as classic, as RabbitMQ
decides for exclusive queues. Use it when the application relies on temporary
or RPC reply queues and the cluster permits those queues. It does not weaken
conversion: convertible declarations on the same connection are still
converted. Raise `--consoleVerbosity` to 4 (debug) to log each forwarded
unconvertible declaration and each dropped argument. If the cluster forbids
classic queues entirely, temporary reply queues need a different design, such
as RabbitMQ Direct Reply-to.

Queue names, bindings, exchanges, flags, unrelated argument bytes, channel IDs,
message bodies, headers, confirms and acknowledgements are preserved. TCP
fragmentation/coalescing is handled at frame boundaries. Rewritten buffers are
kept alive until asynchronous writes finish. Added bytes count toward the
negotiated frame limit. Traffic counters continue to measure input wire bytes.

If the application omits queue type, you can instead set the target vhost's
RabbitMQ default queue type to quorum and omit the quorum rewrite option. An
explicit classic declaration overrides that default; the rewrite option handles
that case. RabbitMQ's relaxed queue-type redeclaration setting is not required
for this implementation and should not be enabled just for this adapter.

## Migration considerations and checklist

1. **Inventory every declaration.** Include startup/recovery, dynamically
   created queues, reply queues, all services sharing a connection and code
   paths triggered only under failure. Temporary/exclusive/non-durable queues
   cannot be transparently converted by this adapter: decide between the
   default rejection and `--passThroughUnconvertibleQueues` from this
   inventory. Direct Reply-to is a separate broker capability; test the app's
   actual RPC behavior.
2. **Create the destination topology and permissions deliberately.** Grant the
   app's user only the destination vhost permissions it needs. No source `/`
   permission is needed after rewriting. Do not map different tenants' `/`
   requests through one shared fixed mapping. Keep the control socket restricted.
3. **Plan existing data migration.** Existing classic queues do not become quorum
   queues. Redeclaring one with rewritten type raises `406 PRECONDITION_FAILED`.
   Drain/move messages and recreate topology using a separate approved migration
   plan. The proxy neither copies messages nor reconciles bindings/policies.
4. **Check quorum semantics beyond declaration.** Global QoS prefetch and
   exclusive consumers are incompatible. TTL, retry/requeue behavior, delivery
   limits, dead-lettering, priorities, overflow rules and consumer timeouts may
   differ from the old broker. This proxy does not rewrite those operations;
   of the declaration arguments it only drops the classic-only tuning listed
   above. Rewriting is not a universal old-client compatibility layer.
5. **Test reliability at the application level.** Publisher confirms, mandatory
   returns, manual ACK/NACK, redelivery, duplicate handling and reconnect recovery
   matter. Replication does not give exactly-once processing. A cluster or proxy
   disconnect requires the application's normal reconnection/topology recovery.
6. **Verify replication and failure behavior.** Check queue members and online
   replicas. A three-member quorum queue needs two members available. Backend
   load balancing selects a node for a connection; it does not move an existing
   TCP session seamlessly. A proxy is an additional availability dependency.
7. **Review TLS and authentication.** Distinguish the two TLS legs, certificate
   SANs/trust, mutual TLS and broker credentials. See the egress hostname/SNI
   limitation above. Never use TRACE logging with production credentials: the
   inherited handshake logger can include SASL data. Keep normal verbosity at 3
   or below and restrict log access.
8. **Validate performance.** Rewriting adds declaration parsing/allocation. Test
   bursts of queue creation, reconnect storms, large messages and sustained
   throughput, plus disk/network capacity for quorum replication. It does not
   add client-side publisher confirms if the old app never used them.
9. **Keep rollback explicit.** Preserve the old endpoint/configuration until
   acceptance. Stop writers before switching. Messages already accepted by the
   new cluster do not follow a DNS rollback; account for them to avoid loss or
   duplicate processing. Keep old/new schemas and permissions consistent with
   the chosen direction.
10. **Record evidence before cutover.** Use the existing-server script, inspect
    JSON results and skipped cases, then test the real Windows application in an
    isolated vhost. Include one-node outage/recovery and both TLS legs. Treat a
    successful synthetic test as evidence for the adapter, not certification of
    every application workflow.

References: [RabbitMQ vhosts/default queue type](https://www.rabbitmq.com/docs/vhosts),
[quorum queue compatibility](https://www.rabbitmq.com/docs/quorum-queues),
[TLS](https://www.rabbitmq.com/docs/ssl).

## Build, tests, packaging

- [Existing RabbitMQ 4.3 server test instructions](../tests/rabbitmq43/README.md)
- [RHEL 9 RPM build and systemd deployment](../packaging/rhel9/README.md)
- Native distro dependencies: CMake >=3.27, C++17 compiler, Boost, OpenSSL,
  protobuf compiler/development libraries, GoogleTest and GoogleMock.

```sh
make native-test
```

The `system` build flavor uses installed libraries, without Conan or containers.
The existing Conan build remains available. See the test runbook for the exact
verification performed and the live RabbitMQ/RHEL checks still needed.
