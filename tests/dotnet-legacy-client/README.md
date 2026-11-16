# .NET legacy client check

A small console program that talks to the proxy with **RabbitMQ.Client 6.8.1**,
the library used by the legacy .NET application, and prints what that client
experiences for typical legacy declarations. Use it to separate proxy behavior
from broker behavior before testing the real application.

Each scenario opens its own connection:

| Scenario | What the client does | Expected through the proxy |
| --- | --- | --- |
| `durable-named` | `QueueDeclare(name, durable: true, exclusive: false, autoDelete: false)` | Declared; the broker receives `x-queue-type=quorum` |
| `classic-lazy-args` | Explicit `classic`, `x-queue-mode=lazy`, TTL | Declared as quorum; `x-queue-mode` dropped, TTL kept |
| `library-defaults` | `QueueDeclare(name)`: the library defaults are **non-durable, exclusive, auto-delete** | Refused with 540 by default; forwarded unchanged with `--passThroughUnconvertibleQueues` |
| `server-named` | `QueueDeclare()` | As `library-defaults` |
| `global-qos-consume` | `BasicQos(0, 10, global: true)` then `BasicConsume` on a converted queue | Decided by the broker; quorum queues do not support global QoS |
| `existing-queue` | Redeclare `--existing-queue NAME` durable, as the application does at startup | Refused by the broker with 406 if the queue exists as classic |
| `recovery` | With automatic recovery (the library default), trigger a refusal and watch | Shows whether and how often the library reconnects |

The program also prints the library defaults (automatic recovery is **on** in
6.x, retry every 5 seconds) and how it maps `amqp://host` and `amqp://host/` to
a vhost (both to `/` in 6.8.1).

## Run

Requires the .NET 8 SDK or newer. NuGet must be reachable for the first restore
(or use your internal mirror). The password is read from `RMQ_PASSWORD` so it
does not end up in shell history.

```powershell
cd tests\dotnet-legacy-client
$env:RMQ_PASSWORD = 'REPLACE_WITH_TEST_PASSWORD'
dotnet run -c Release -- --host proxy.example.com --port 5673 --user rewrite_test --watch-recovery 15
```

Options: `--vhost` (default `/`), `--tls` with optional `--server-name`,
`--existing-queue NAME` to redeclare a queue that already exists, and
`--watch-recovery SECONDS` (0 skips the recovery scenario).

Use a test user and test vhost, as in the
[RabbitMQ 4.3 runbook](../rabbitmq43/README.md). The program creates durable
queues named `legacy-client-check-<run id>-...` and deletes them at the end; if
cleanup fails it prints the prefix to delete by hand. Raise the proxy's log
level while it runs (`--consoleVerbosity 4`, or `amqpprox_ctl <socket> LOG FILE 4`
for the log files) and compare with the
[diagnostics table](../../docs/legacy-rewrite.md#diagnostics).
