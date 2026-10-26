# Modification record

Date: 2026-10-19, updated 2026-10-26. Modified/added by the amqpprox-rewrite fork.

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
| [.gitignore](.gitignore) | Modified | Ignore local Python caches, test environments and generated test reports. |
| [MODIFICATIONS.md](MODIFICATIONS.md) | Added | Record the modified and added files, upstream baseline and scope of this fork. |
| [Makefile](Makefile) | Modified | Add native system-package build/test targets and a RHEL 9 RPM target. |
| [README.md](README.md) | Modified | Describe fork features, opt-in flags, constraints and administrator documentation. |
| [amqpprox/amqpprox.m.cpp](amqpprox/amqpprox.m.cpp) | Modified | Parse and validate startup rewrite options, including pass-through of unconvertible declarations; pass immutable configuration to Server. |
| [buildfiles/system/amqpprox.pre.cmake](buildfiles/system/amqpprox.pre.cmake) | Added | Adapt Conan target link settings for installed system libraries. |
| [buildfiles/system/amqpprox_ctl.pre.cmake](buildfiles/system/amqpprox_ctl.pre.cmake) | Added | Adapt control-client target link settings for installed system libraries. |
| [buildfiles/system/libamqpprox.post.cmake](buildfiles/system/libamqpprox.post.cmake) | Added | Reuse library public include-directory setup for the system flavor. |
| [buildfiles/system/libamqpprox.pre.cmake](buildfiles/system/libamqpprox.pre.cmake) | Added | Adapt library dependency linking for the system flavor. |
| [buildfiles/system/main.pre.cmake](buildfiles/system/main.pre.cmake) | Added | Discover installed Boost, OpenSSL, GoogleTest and Protobuf without Conan. |
| [buildfiles/system/tests.pre.cmake](buildfiles/system/tests.pre.cmake) | Added | Adapt test dependency linking for installed system libraries. |
| [docs/handshake.md](docs/handshake.md) | Modified | Explain the optional vhost and post-handshake declaration rewrite paths. |
| [docs/legacy-rewrite.md](docs/legacy-rewrite.md) | Added | Document transport/routing, AMQPS, semantics, limitations and migration checklist. |
| [docs/tls.md](docs/tls.md) | Modified | Correct the secure listener control command to include LISTEN. |
| [libamqpprox/CMakeLists.txt](libamqpprox/CMakeLists.txt) | Modified | Compile the new rewrite component. |
| [libamqpprox/amqpprox_connector.cpp](libamqpprox/amqpprox_connector.cpp) | Modified | Rewrite matching vhosts before routing/authentication; expose queue scope, pass-through policy and frame limit; start a graceful close with a custom reason. |
| [libamqpprox/amqpprox_connector.h](libamqpprox/amqpprox_connector.h) | Modified | Store immutable rewrite configuration and per-connection rewrite state; declare the graceful close with reason. |
| [libamqpprox/amqpprox_methods_open.h](libamqpprox/amqpprox_methods_open.h) | Modified | Add a setter for the outbound Connection.Open virtual host. |
| [libamqpprox/amqpprox_packetprocessor.cpp](libamqpprox/amqpprox_packetprocessor.cpp) | Modified | Rewrite eligible ingress declarations while retaining other frames and partial input; stop at a rejected declaration, keeping the frames before it. |
| [libamqpprox/amqpprox_packetprocessor.h](libamqpprox/amqpprox_packetprocessor.h) | Modified | Expose shared ownership of rewritten output for asynchronous writes and the declaration rejection. |
| [libamqpprox/amqpprox_rewrite.cpp](libamqpprox/amqpprox_rewrite.cpp) | Added | Implement bounded declaration parsing, byte-preserving type replacement, classic-only argument removal, convertibility policy and AMQP reply codes for rejections. |
| [libamqpprox/amqpprox_rewrite.h](libamqpprox/amqpprox_rewrite.h) | Added | Define rewrite configuration, the rejection error type and frame rewrite interface. |
| [libamqpprox/amqpprox_server.cpp](libamqpprox/amqpprox_server.cpp) | Modified | Copy startup rewrite configuration into each accepted session. |
| [libamqpprox/amqpprox_server.h](libamqpprox/amqpprox_server.h) | Modified | Accept and retain startup rewrite configuration. |
| [libamqpprox/amqpprox_session.cpp](libamqpprox/amqpprox_session.cpp) | Modified | Pass configuration to Connector, retain rewritten bytes until async writes finish, and answer rejected declarations with Connection.Close after forwarding preceding frames. |
| [libamqpprox/amqpprox_session.h](libamqpprox/amqpprox_session.h) | Modified | Extend session construction, asynchronous output ownership and write-completion interface. |
| [packaging/rhel9/README.md](packaging/rhel9/README.md) | Added | Document RHEL 9 prerequisites, native RPM builds, service configuration and platform validation status. |
| [packaging/rhel9/amqpprox-rewrite.service](packaging/rhel9/amqpprox-rewrite.service) | Added | Provide an unprivileged systemd service with restricted runtime/log directories. |
| [packaging/rhel9/amqpprox-rewrite.spec](packaging/rhel9/amqpprox-rewrite.spec) | Added | Build/test/package the binaries, service, configuration, license and modification notices. |
| [packaging/rhel9/amqpprox-rewrite.sysconfig](packaging/rhel9/amqpprox-rewrite.sysconfig) | Added | Supply editable startup rewrite options and document the pass-through option. |
| [packaging/rhel9/apply-config.py](packaging/rhel9/apply-config.py) | Added | Apply restricted startup control commands without a shell; fail on configuration errors. |
| [packaging/rhel9/build-rpm.sh](packaging/rhel9/build-rpm.sh) | Added | Validate EL9/toolchain/clean source and invoke native RPM/SRPM builds. |
| [packaging/rhel9/control.conf](packaging/rhel9/control.conf) | Added | Supply commented backend/farm/mapping/listener configuration examples. |
| [tests/CMakeLists.txt](tests/CMakeLists.txt) | Modified | Register the new rewrite regression tests. |
| [tests/amqpprox_rewrite.t.cpp](tests/amqpprox_rewrite.t.cpp) | Added | Test vhost replay, declaration transformations, flags, convertibility, argument removal, pass-through, rejection reply codes, graceful close, limits, malformed input, fragmentation and scope. |
| [tests/local_wire_smoke.py](tests/local_wire_smoke.py) | Added | Exercise actual proxy sockets in plaintext and TLS with a scripted broker, including rejection Close handshakes and pass-through. |
| [tests/rabbitmq43/README.md](tests/rabbitmq43/README.md) | Added | Provide container-free existing-server setup, test, cleanup and manual acceptance instructions, including the pass-through proxy. |
| [tests/rabbitmq43/requirements.txt](tests/rabbitmq43/requirements.txt) | Added | Pin the Pika dependency for the administrator test application. |
| [tests/rabbitmq43/test_legacy.py](tests/rabbitmq43/test_legacy.py) | Added | Provide the AMQP/AMQPS client and management-API assertions for an existing RabbitMQ 4.3 cluster, including rejection reasons, argument removal and pass-through. |
