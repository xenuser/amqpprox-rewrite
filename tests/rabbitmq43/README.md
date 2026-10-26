# Test against an existing RabbitMQ 4.3 server (no containers)

Use an isolated test environment/vhosts on your existing server. The proxy runs
on Linux; the Python test client runs on Windows or Linux. These tests require
RabbitMQ's management HTTP API as well as AMQP access through the proxy.

## Verification status

The C++ build and local regression suite are exercised in Ubuntu 24.04 WSL.
Real proxy socket smoke tests passed for plain AMQP and AMQPS on both legs
against a scripted broker, including fragmentation, graceful rejection,
pass-through mode and the packaged startup configuration helper. This is not a
RabbitMQ server test.

Live RabbitMQ 4.3 and AMQPS against RabbitMQ have **not** been run: Docker failed to start, and no enterprise broker endpoint or test
credentials were supplied. RHEL RPM tooling is supplied but has not been built
on RHEL. Run this procedure before deployment and retain the JSON report.

## 1. Prepare isolated test resources

On the RabbitMQ admin host, choose two **new** vhosts and two dedicated users.
The examples below use names expected by the client defaults. Do not reuse
production users/vhosts. Verify absence before running these commands; RabbitMQ
commands can otherwise modify existing resources. Use your approved credential
handling method in place of the example password placeholders.

```sh
rabbitmqctl add_vhost rewrite-test-target --default-queue-type classic
rabbitmqctl add_vhost rewrite-test-control --default-queue-type classic
rabbitmqctl add_user rewrite_test 'REPLACE_WITH_TEST_PASSWORD'
rabbitmqctl add_user rewrite_control 'REPLACE_WITH_CONTROL_PASSWORD'
rabbitmqctl set_permissions -p rewrite-test-target rewrite_test '.*' '.*' '.*'
rabbitmqctl set_permissions -p rewrite-test-control rewrite_control '.*' '.*' '.*'
```

The main test user must have permissions **only** on the target, with no `/`
permission. A successful connection through the proxy therefore proves the
broker receives the rewritten vhost. Test-vhost defaults are deliberately
**classic**, so quorum queues cannot be attributed to a broker default. Do not
enable relaxed queue-type equivalence for this test. Supply a management API
account authorized to read nodes, vhosts, user permissions and queues, and to
create/delete test queues and exchanges. Management credentials are separate
from the application's credentials and are never sent over AMQP by this script.

A three-node cluster must have three running nodes; newly declared quorum queues
must have three online members. `--min-members 1` permits a single-node smoke
test but is not evidence of three-node replication.

## 2. Start the proxy

Build on a supported Linux host (`make native-test` or the existing Conan build).
On RHEL 9 use [the RPM tooling](../../packaging/rhel9/README.md). Binaries from
a local native build are in `build/system/bin/`.

A simple plaintext test uses:

```sh
build/system/bin/amqpprox --listenPort 5673 --destinationDNS rabbit1.example.com \
  --destinationPort 5672 --rewriteVhostFrom / --rewriteVhostTo rewrite-test-target \
  --rewriteClassicQueuesToQuorum --consoleVerbosity 3
```

This routes both the rewritten connection and the nonmatching control vhost to
the same cluster. For a multi-backend setup use the control commands in
[the migration guide](../../docs/legacy-rewrite.md). Map **both**
`rewrite-test-target` and `rewrite-test-control` to the farm for this test.
A farm does not move established TCP connections between nodes.

For AMQPS, configure certificates and the backend TLS flag before listening,
as described in the migration guide. Do not use simple mode for TLS. Run the
same tests with `--tls` and the client CA/name options below. If the source vhost
is `VHOST1`, replace the proxy source flag and pass `--source-vhost VHOST1` to
the script. Never grant the main test user access to that source vhost.

## 3. Install the test client

Python 3.9+ is sufficient. Use a virtual environment:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r tests/rabbitmq43/requirements.txt
```

On Windows PowerShell:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r tests/rabbitmq43/requirements.txt
```

For an offline enterprise machine, download the pinned wheel on an approved
connected workstation, transfer the directory, then install without an index:

```sh
python -m pip download -r tests/rabbitmq43/requirements.txt -d wheelhouse
python -m pip install --no-index --find-links wheelhouse -r tests/rabbitmq43/requirements.txt
```

## 4. Supply credentials and run

Set these environment variables using your secret-management process:
`RMQ_ADMIN_USER`, `RMQ_ADMIN_PASSWORD`, `RMQ_TEST_USER`, `RMQ_TEST_PASSWORD`,
`RMQ_CONTROL_USER`, `RMQ_CONTROL_PASSWORD`. For the examples, test usernames are
`rewrite_test` and `rewrite_control`. In PowerShell use `$env:NAME = 'value'`;
in a POSIX shell use `export NAME='value'`. Avoid putting real passwords in
saved scripts or shell history.

Linux invocation:

```sh
python tests/rabbitmq43/test_legacy.py \
  --proxy-host proxy.example.com --proxy-port 5673 \
  --management-url https://rabbit-admin.example.com:15671 \
  --management-ca /path/to/management-ca.pem --report rabbitmq43-results.json
```

PowerShell invocation (single line):

```powershell
.\.venv\Scripts\python.exe tests/rabbitmq43/test_legacy.py --proxy-host proxy.example.com --proxy-port 5673 --management-url https://rabbit-admin.example.com:15671 --management-ca C:\certs\management-ca.pem --report rabbitmq43-results.json
```

For AMQPS append:

```text
--tls --ca-file /path/to/proxy-ca.pem --server-name proxy.example.com
```

For mutual TLS append `--client-cert ... --client-key ...`. Both HTTPS and
AMQPS use certificate verification; there is no insecure skip-verification flag.
HTTP management URLs are available for a suitably isolated plaintext lab.
Use HTTPS when credentials traverse a shared network.

The client emulates an old AMQP 0.9.1 application: it connects to the source
vhost, actively declares explicit classic queues, publishes with confirms,
consumes and acknowledges messages, and reconnects. It uses modern Pika for
portability; this does not replace testing the actual legacy .NET/client library.

## Assertions and expected results

| Scenario | Expected result |
| --- | --- |
| Server preflight | Version starts with `4.3.`, sufficient running nodes, main user restricted to target, test defaults classic |
| Explicit classic declaration/redeclaration/reconnect | Broker reports quorum in target, no matching queue in source |
| Missing type, multiple channels, passive check | Quorum queues with requested membership and successful passive checks |
| Exchange/binding, confirmed persistent publish, large body | Exact body and headers, including a body spanning multiple frames |
| Reject/requeue and ACK | Redelivery and successful final acknowledgement |
| Explicit quorum and stream | Types retained |
| Nonmatching control vhost | Classic remains classic in control vhost |
| Non-durable, exclusive, auto-delete, server-named | Proxy sends Connection.Close 540 naming the reason; no queue created |
| `x-max-priority` | Proxy sends Connection.Close 540 naming the argument; no queue created |
| `x-queue-mode`, `x-queue-version`, `x-queue-master-locator` | Dropped; quorum queue created, other arguments kept |
| Existing classic queue | Broker returns 406; queue remains classic |
| Optional disabled proxy | Explicit classic remains classic |
| Optional vhost-only proxy | Vhost changes but explicit classic remains classic |
| Optional pass-through proxy | Exclusive reply queue forwarded unchanged (classic); durable queue still quorum |

Exit code 0 means all executed assertions passed; 1 means test failure; 2 means
configuration failure. The JSON report records the server version, online nodes,
quorum members and test/skip counts without credentials. Three optional regression
cases are skipped unless you provide the ports below. Review skipped cases;
a partial suite is not a complete production acceptance.

Each run uses UUID-prefixed queue/exchange names and deletes only those exact
resources in cleanup. It never deletes users or vhosts. A killed process or
lost management access can leave test resources behind: use the reported run ID
to identify them and inspect before deleting. Do not run multiple test clients
while changing shared test-vhost defaults or permissions.

Optional extra proxy processes, each in its own terminal:

```sh
# No rewriting; client connects directly to target vhost through this listener.
build/system/bin/amqpprox --controlSocket /tmp/amqpprox-disabled --logDirectory logs-disabled \
  --listenPort 5674 --destinationDNS rabbit1.example.com --destinationPort 5672
# Vhost only; no declaration rewrite.
build/system/bin/amqpprox --controlSocket /tmp/amqpprox-vhost-only --logDirectory logs-vhost-only \
  --listenPort 5675 --destinationDNS rabbit1.example.com --destinationPort 5672 \
  --rewriteVhostTo rewrite-test-target
# Quorum rewriting that forwards unconvertible declarations unchanged.
build/system/bin/amqpprox --controlSocket /tmp/amqpprox-passthrough --logDirectory logs-passthrough \
  --listenPort 5676 --destinationDNS rabbit1.example.com --destinationPort 5672 \
  --rewriteVhostTo rewrite-test-target --rewriteClassicQueuesToQuorum \
  --passThroughUnconvertibleQueues
```

Append `--passthrough-port 5674 --vhost-only-port 5675
--unconvertible-passthrough-port 5676` to run all cases. For a
TLS test, configure these listeners with the same TLS credentials and use the
same client flags. Each process needs a distinct control socket and log folder.

## Additional admin/application acceptance scenarios

- Run the real Windows application's complete workflow, including recovery and
  temporary/RPC queue creation. Record every unsupported declaration.
- Stop one broker during a confirmed publish/consume workload in the isolated
  environment. Verify the app reconnects where required, the queue retains a
  majority, and confirmed messages survive. Restore the node and verify members.
  This script deliberately does not stop enterprise broker nodes automatically.
- Restart the proxy and verify application reconnection/topology recovery.
- Test certificate expiry, untrusted CA, wrong proxy hostname and required client
  certificates. Validate egress chain trust separately. The inherited egress
  hostname/SNI limitation is documented in the migration guide.
- Exercise the app's QoS, mandatory returns, retries/dead-lettering, priorities,
  long-running consumers and permissions failures. Unsupported global QoS is
  not rewritten into per-consumer QoS.
- Measure throughput, queue creation storms, latency and resource use at expected
  load. Verify rollback and message reconciliation with your operations team.

## Cleanup after the test campaign

Stop the test proxy processes. After verifying these resources belong exclusively
to this test, administrators can remove the two dedicated vhosts and users:

```sh
rabbitmqctl delete_vhost rewrite-test-target
rabbitmqctl delete_vhost rewrite-test-control
rabbitmqctl delete_user rewrite_test
rabbitmqctl delete_user rewrite_control
```

Never substitute a production vhost into these cleanup commands.

## Local wire smoke test without RabbitMQ

On a Linux builder with Python 3 and `openssl`, run:

```sh
python3 tests/local_wire_smoke.py build/system/bin/amqpprox
```

This starts ephemeral loopback listeners and the compiled proxy, then checks
unmodified AMQP, rewritten AMQP, and rewritten AMQPS on both legs. It also
checks that a rejected declaration produces Connection.Close 540 to the client
followed by a normal close of the broker side (AMQP and AMQPS), and that
`--passThroughUnconvertibleQueues` forwards an exclusive declaration unchanged.
A scripted broker validates the actual outgoing vhost and queue arguments. Temporary
certificates and processes are cleaned up. It does not implement or validate
RabbitMQ queue semantics, replication, authentication or application recovery.
