# Modification record

Date: 2026-10-19, updated 2026-10-26 and 2026-11-16 (five times). Modified/added by the amqpprox-rewrite fork.

This record describes the legacy RabbitMQ compatibility feature relative to
upstream commit `b14d9ed57d188207f1ea524a7feab21a37e2d70d` (before the feature branch).
Original copyright and license notices are retained. The repository's
[LICENSE](LICENSE) (Apache License 2.0) and [COPYRIGHT](COPYRIGHT) remain unchanged.
Modified upstream files also carry prominent modification notices, as required
by section 4(b); this document supplements those file-level notices. New files
are distributed under the same repository license. The RPM includes LICENSE,
COPYRIGHT and this modification record.

The changes add opt-in vhost and compatible classic-to-quorum declaration
rewriting, native builds, regression tests, an existing-server test client,
RHEL 9 packaging tooling, and operational documentation. They do not migrate
existing queues/messages, change authentication credentials, or remove inherited
TLS limitations. See [the migration guide](docs/legacy-rewrite.md).

## Changed and added files

All paths below are repository-relative. "Modified" means an upstream file was
changed; "Added" means a new file (including adapted system-build files).

| File | Status | Modification |
| --- | --- | --- |
| [.gitattributes](.gitattributes) | Added | Keep Linux shell/Python scripts, RPM specs, service files and Makefile in LF format. |
| [.gitignore](.gitignore) | Modified | Ignore local Python caches, test environments and generated test reports, and .NET test client build output. |
| [MODIFICATIONS.md](MODIFICATIONS.md) | Added | Record the modified and added files, upstream baseline and scope of this fork. |
| [Makefile](Makefile) | Modified | Add native system-package build/test targets and a RHEL 9 RPM target. |
| [README.md](README.md) | Modified | Describe fork features, opt-in flags, constraints (including per-user vhost mapping and promotion of non-durable queues) and administrator documentation. |
| [amqpprox/amqpprox.m.cpp](amqpprox/amqpprox.m.cpp) | Modified | Parse and validate startup rewrite options, including the per-user vhost map file, pass-through of unconvertible declarations and empty-vhost help text; pass immutable configuration to Server. Register the REWRITE control command for runtime reloads of the user vhost map. Add the option to promote non-durable queues. |
| [buildfiles/system/amqpprox.pre.cmake](buildfiles/system/amqpprox.pre.cmake) | Added | Adapt Conan target link settings for installed system libraries. |
| [buildfiles/system/amqpprox_ctl.pre.cmake](buildfiles/system/amqpprox_ctl.pre.cmake) | Added | Adapt control-client target link settings for installed system libraries. |
| [buildfiles/system/libamqpprox.post.cmake](buildfiles/system/libamqpprox.post.cmake) | Added | Reuse library public include-directory setup for the system flavor. |
| [buildfiles/system/libamqpprox.pre.cmake](buildfiles/system/libamqpprox.pre.cmake) | Added | Adapt library dependency linking for the system flavor. |
| [buildfiles/system/main.pre.cmake](buildfiles/system/main.pre.cmake) | Added | Discover installed Boost, OpenSSL, GoogleTest and Protobuf without Conan. |
| [buildfiles/system/tests.pre.cmake](buildfiles/system/tests.pre.cmake) | Added | Adapt test dependency linking for installed system libraries. |
| [docs/handshake.md](docs/handshake.md) | Modified | Explain the optional vhost and post-handshake declaration rewrite paths. |
| [docs/legacy-rewrite.md](docs/legacy-rewrite.md) | Added | Document transport/routing, AMQPS, semantics, per-user vhost mapping, limitations, diagnostics log lines and migration checklist. |
| [docs/tls.md](docs/tls.md) | Modified | Correct the secure listener control command to include LISTEN. |
| [libamqpprox/CMakeLists.txt](libamqpprox/CMakeLists.txt) | Modified | Compile the new rewrite component and its control command. |
| [libamqpprox/amqpprox_connectionselector.cpp](libamqpprox/amqpprox_connectionselector.cpp) | Modified | Log a missing vhost route as an error only when no default farm applies; otherwise log the default farm at info level. |
| [libamqpprox/amqpprox_connector.cpp](libamqpprox/amqpprox_connector.cpp) | Modified | Rewrite matching vhosts before routing/authentication and log them quoted; expose queue scope, pass-through policy and frame limit; start a graceful close with a custom reason. Mark connections whose vhost was rewritten. Rewrite to the vhost mapped for the client's PLAIN username and refuse unmapped users with Connection.Close 530. Relay the broker's refusal of Connection.Open (for example a missing vhost) to the client instead of ignoring it. |
| [libamqpprox/amqpprox_connector.h](libamqpprox/amqpprox_connector.h) | Modified | Store immutable rewrite configuration and per-connection rewrite state; declare the graceful close with reason. Track vhost rewriting and the close handshake for diagnostics. Expose the non-durable promotion option. |
| [libamqpprox/amqpprox_methods_open.h](libamqpprox/amqpprox_methods_open.h) | Modified | Add a setter for the outbound Connection.Open virtual host. |
| [libamqpprox/amqpprox_packetprocessor.cpp](libamqpprox/amqpprox_packetprocessor.cpp) | Modified | Rewrite eligible ingress declarations while retaining other frames and partial input; stop at a rejected declaration, keeping the frames before it. Log rejections with forwarded/discarded sizes, broker channel/connection closes on rewritten connections, and passed-through Connection.Close. Pass the non-durable promotion option to the declaration rewrite. |
| [libamqpprox/amqpprox_packetprocessor.h](libamqpprox/amqpprox_packetprocessor.h) | Modified | Expose shared ownership of rewritten output for asynchronous writes and the declaration rejection. |
| [libamqpprox/amqpprox_resourcemapper.cpp](libamqpprox/amqpprox_resourcemapper.cpp) | Modified | Leave logging of a missing vhost mapping to the connection selector, which knows about the default farm. |
| [libamqpprox/amqpprox_rewrite.cpp](libamqpprox/amqpprox_rewrite.cpp) | Added | Implement vhost matching (an empty vhost counts as `/`), bounded declaration parsing, byte-preserving type replacement, classic-only argument removal, convertibility policy and AMQP reply codes for rejections. Decode Close reasons for diagnostics; log conversions at debug level. Parse the user vhost map file, extract PLAIN usernames and choose the destination per user. List every conversion obstacle in rejections and optionally promote non-durable queues to durable quorum queues. |
| [libamqpprox/amqpprox_rewrite.h](libamqpprox/amqpprox_rewrite.h) | Added | Define rewrite configuration, the rejection error type and frame rewrite interface. Declare the Close reason decoder. Declare the user vhost map, its parser and the PLAIN username helper. Add the non-durable promotion setting. |
| [libamqpprox/amqpprox_rewritecontrolcommand.cpp](libamqpprox/amqpprox_rewritecontrolcommand.cpp) | Added | Implement `REWRITE RELOAD` (re-read the user vhost map, keep the current one on errors) and `REWRITE PRINT`. Show the promotion setting in REWRITE PRINT. |
| [libamqpprox/amqpprox_rewritecontrolcommand.h](libamqpprox/amqpprox_rewritecontrolcommand.h) | Added | Declare the REWRITE control command. |
| [libamqpprox/amqpprox_server.cpp](libamqpprox/amqpprox_server.cpp) | Modified | Copy startup rewrite configuration into each accepted session. Hand each new session the current user vhost map, which can be replaced at runtime. |
| [libamqpprox/amqpprox_server.h](libamqpprox/amqpprox_server.h) | Modified | Accept and retain startup rewrite configuration. Allow replacing the user vhost map for new sessions and reading the active rewrite configuration. |
| [libamqpprox/amqpprox_session.cpp](libamqpprox/amqpprox_session.cpp) | Modified | Pass configuration to Connector, retain rewritten bytes until async writes finish, and answer rejected declarations with Connection.Close after forwarding preceding frames. Log socket teardown after Connection.Close at info level and other connection losses as warnings naming the side. |
| [libamqpprox/amqpprox_session.h](libamqpprox/amqpprox_session.h) | Modified | Extend session construction, asynchronous output ownership and write-completion interface. |
| [packaging/rhel9/README.md](packaging/rhel9/README.md) | Added | Document RHEL 9 prerequisites, native RPM builds, service configuration (including the user vhost map) and platform validation status. |
| [packaging/rhel9/amqpprox-rewrite.service](packaging/rhel9/amqpprox-rewrite.service) | Added | Provide an unprivileged systemd service with restricted runtime/log directories. Reload the user vhost map with systemctl reload, failing on invalid files. |
| [packaging/rhel9/amqpprox-rewrite.spec](packaging/rhel9/amqpprox-rewrite.spec) | Added | Build/test/package the binaries, service, configuration files including the user vhost map, license and modification notices. |
| [packaging/rhel9/amqpprox-rewrite.sysconfig](packaging/rhel9/amqpprox-rewrite.sysconfig) | Added | Supply editable startup rewrite options and document the pass-through and user vhost map options. Document the non-durable promotion option. |
| [packaging/rhel9/apply-config.py](packaging/rhel9/apply-config.py) | Added | Apply restricted startup control commands without a shell; fail on configuration errors. |
| [packaging/rhel9/build-rpm.sh](packaging/rhel9/build-rpm.sh) | Added | Validate EL9/toolchain/clean source and invoke native RPM/SRPM builds. |
| [packaging/rhel9/control.conf](packaging/rhel9/control.conf) | Added | Supply commented backend/farm/mapping/listener configuration examples, including a default farm for per-user destinations. |
| [packaging/rhel9/vhost-users.conf](packaging/rhel9/vhost-users.conf) | Added | Supply the commented per-user destination vhost map for `--rewriteVhostUserMap`, with reload instructions. |
| [tests/CMakeLists.txt](tests/CMakeLists.txt) | Modified | Register the new rewrite regression tests. |
| [tests/dotnet-legacy-client/LegacyClientCheck.csproj](tests/dotnet-legacy-client/LegacyClientCheck.csproj) | Added | Build the .NET legacy client check against RabbitMQ.Client 6.8.1. |
| [tests/dotnet-legacy-client/Program.cs](tests/dotnet-legacy-client/Program.cs) | Added | Exercise typical legacy .NET declarations, QoS, existing queues and automatic recovery through the proxy and report what the client sees. Cover named non-durable declarations. |
| [tests/dotnet-legacy-client/README.md](tests/dotnet-legacy-client/README.md) | Added | Document the .NET legacy client check scenarios, expected results and usage. Describe the named non-durable scenario. |
| [tests/amqpprox_rewrite.t.cpp](tests/amqpprox_rewrite.t.cpp) | Added | Test complete rejection reasons, promotion of non-durable queues, broker refusal of Connection.Open, user vhost map parsing, PLAIN usernames, per-user routing and refusal, close-reason decoding, close tracking, vhost replay including empty vhosts, declaration transformations, flags, convertibility, argument removal, pass-through, rejection reply codes, graceful close, limits, malformed input, fragmentation and scope. |
| [tests/local_wire_smoke.py](tests/local_wire_smoke.py) | Added | Exercise actual proxy sockets in plaintext and TLS with a scripted broker, including rejection Close handshakes pass-through, per-user vhost mapping (AMQP and AMQPS) refusal of unmapped users, a vhost missing on the broker, and runtime reload of the user vhost map. Cover promotion of non-durable queues and assert that a default farm route logs no mapping error. |
| [tests/rabbitmq43/README.md](tests/rabbitmq43/README.md) | Added | Provide container-free existing-server setup, test, cleanup and manual acceptance instructions, including the pass-through proxy. Describe the optional promotion proxy. |
| [tests/rabbitmq43/requirements.txt](tests/rabbitmq43/requirements.txt) | Added | Pin the Pika dependency for the administrator test application. |
| [tests/rabbitmq43/test_legacy.py](tests/rabbitmq43/test_legacy.py) | Added | Provide the AMQP/AMQPS client and management-API assertions for an existing RabbitMQ 4.3 cluster, including rejection reasons, argument removal and pass-through. Add an optional live test of non-durable promotion. |
