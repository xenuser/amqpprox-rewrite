#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Real proxy sockets against a tiny scripted broker, not a RabbitMQ substitute.
Runs plain AMQP and TLS on both legs. Requires Python3, openssl and Linux proxy.
"""
import argparse
import concurrent.futures
import importlib.util
import pathlib
import socket
import ssl
import struct
import subprocess
import tempfile
import time


def short(value):
    value = value.encode()
    return bytes([len(value)]) + value


def long(value):
    value = value.encode()
    return struct.pack('!I', len(value)) + value


def frame(payload, channel=0, kind=1):
    return struct.pack('!BHI', kind, channel, len(payload)) + payload + b'\xce'


def exact(connection, size):
    data = b''
    while len(data) < size:
        chunk = connection.recv(size - len(data))
        if not chunk:
            raise RuntimeError('Unexpected EOF')
        data += chunk
    return data


def receive(connection):
    header = exact(connection, 7)
    kind, channel, size = struct.unpack('!BHI', header)
    body = exact(connection, size)
    assert exact(connection, 1) == b'\xce'
    return kind, channel, body


def declare(queue_type, flags=2, name='jobs'):
    entry = short('x-queue-type') + b'S' + long(queue_type)
    return b'\x00\x32\x00\x0a\x00\x00' + short(name) + bytes([flags]) + struct.pack('!I', len(entry)) + entry


def declare_ok(name):
    return b'\x00\x32\x00\x0b' + short(name) + b'\x00'*8


START = b'\x00\x0a\x00\x0a\x00\x09' + b'\x00'*4 + long('PLAIN') + long('en_US')
TUNE = b'\x00\x0a\x00\x1e' + struct.pack('!HIH', 2047, 131072, 60)
HEADER = b'AMQP\x00\x00\x09\x01'
CLOSE = b'\x00\x0a\x00\x32'
CLOSE_OK = b'\x00\x0a\x00\x33'

# mode -> (proxy flags, client queue name/flags, declaration the broker sees)
EXCLUSIVE = ('reply', 6)
MODES = {
    'disabled': ([], ('jobs', 2), declare('classic')),
    'convert': (['--rewriteClassicQueuesToQuorum'], ('jobs', 2), declare('quorum')),
    'reject': (['--rewriteClassicQueuesToQuorum'], EXCLUSIVE, None),
    'passthrough': (['--rewriteClassicQueuesToQuorum', '--passThroughUnconvertibleQueues'],
                    EXCLUSIVE, declare('classic', 6, 'reply')),
    'usermap': (['--rewriteClassicQueuesToQuorum'], ('jobs', 2), declare('quorum')),
    'usermap-refused': (['--rewriteClassicQueuesToQuorum'], ('jobs', 2), None),
    'vhost-missing': (['--rewriteClassicQueuesToQuorum'], ('jobs', 2), None),
    'promote': (['--rewriteClassicQueuesToQuorum', '--promoteTransientQueuesToDurable'],
                ('jobs', 0), declare('quorum', 2, 'jobs')),
}
VHOST_MISSING = b'NOT_ALLOWED - vhost VHOST2 not found'

# --rewriteVhostUserMap files; the client logs in as user 'test'.
USER_MAPS = {
    'usermap': 'test VHOST3\nother VHOST4\n',
    'usermap-refused': '# test is not mapped\nother VHOST4\n',
}


def target_vhost(mode):
    return {'disabled': 'VHOST1', 'usermap': 'VHOST3'}.get(mode, 'VHOST2')


def connect(port):
    # LISTEN returns before the proxy's I/O thread has bound the port.
    deadline = time.monotonic() + 10
    while True:
        try:
            return socket.create_connection(('127.0.0.1', port), timeout=15)
        except ConnectionRefusedError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.1)


def closed(connection):
    try:
        return connection.recv(1) == b''
    except (ssl.SSLEOFError, ConnectionResetError):
        return True


def broker(listener, tls_context, target, mode):
    connection, _ = listener.accept()
    connection.settimeout(15)
    with connection:
        if tls_context:
            connection = tls_context.wrap_socket(connection, server_side=True)
        with connection:
            assert exact(connection, 8) == HEADER
            connection.sendall(frame(START))
            assert receive(connection)[2][:4] == b'\x00\x0a\x00\x0b'
            connection.sendall(frame(TUNE))
            assert receive(connection)[2][:4] == b'\x00\x0a\x00\x1f'
            assert receive(connection)[2] == b'\x00\x0a\x00\x28' + short(target) + b'\x00\x00'
            if mode == 'vhost-missing':
                # RabbitMQ refuses Connection.Open for a vhost it does not have.
                connection.sendall(frame(CLOSE + struct.pack('!H', 530) + bytes([len(VHOST_MISSING)]) +
                                         VHOST_MISSING + struct.pack('!HH', 10, 40)))
                assert closed(connection)
                return
            connection.sendall(frame(b'\x00\x0a\x00\x29\x00'))
            assert receive(connection) == (8, 0, b'')
            name = MODES[mode][1][0]
            if mode == 'reject':
                # Nothing after the rejected declaration arrives; the proxy
                # closes this side normally once the client acknowledged.
                kind, channel, payload = receive(connection)
                assert (kind, channel, payload[:6]) == (1, 0, CLOSE + b'\x00\xc8'), payload
                connection.sendall(frame(CLOSE_OK))
                assert closed(connection)
                return
            assert receive(connection) == (1, 7, MODES[mode][2])
            assert receive(connection) == (3, 7, b'body-unchanged')
            connection.sendall(frame(declare_ok(name), channel=7))
            # Stay alive until client closes, so pending proxy writes complete.
            connection.recv(1)


def smoke(binary, directory, tls=False, mode='convert'):
    rewrite = mode != 'disabled'
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0)); listener.listen(1); listener.settimeout(20)
        backend_port = listener.getsockname()[1]
        with socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0)); proxy_port = reserve.getsockname()[1]
        sock = str(directory / 'control')
        command = [binary, '--controlSocket', sock, '--logDirectory', str(directory / 'logs')]
        if mode in USER_MAPS:
            user_map = directory / 'vhost-users.conf'; user_map.write_text(USER_MAPS[mode])
            command += ['--rewriteVhostFrom', 'VHOST1', '--rewriteVhostUserMap', str(user_map)] + MODES[mode][0]
        elif rewrite:
            command += ['--rewriteVhostFrom', 'VHOST1', '--rewriteVhostTo', 'VHOST2'] + MODES[mode][0]
        cert = str(directory / 'cert.pem'); key = str(directory / 'key.pem')
        context = None
        if tls:
            subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                            '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost',
                            '-keyout', key, '-out', cert], check=True, capture_output=True)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); context.load_cert_chain(cert, key)
        with open(directory / 'proxy.log', 'w') as log, concurrent.futures.ThreadPoolExecutor(1) as pool:
            refused = mode == 'usermap-refused'
            future = None if refused else pool.submit(broker, listener, context, target_vhost(mode), mode)
            process = subprocess.Popen(command, stdout=log, stderr=log)
            try:
                spec = importlib.util.spec_from_file_location('config', pathlib.Path(__file__).resolve().parents[1] / 'packaging/rhel9/apply-config.py')
                module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
                commands = []
                if tls:
                    commands += [f'TLS INGRESS CERT_CHAIN_FILE {cert}', f'TLS INGRESS KEY_FILE {key}',
                                 f'TLS EGRESS CA_CERT_FILE {cert}', 'TLS EGRESS VERIFY_MODE PEER']
                commands += [f'BACKEND ADD backend local 127.0.0.1 {backend_port}' + (' TLS' if tls else ''),
                             'MAP BACKEND ' + target_vhost(mode) + ' backend',
                             'LISTEN ' + ('START_SECURE' if tls else 'START') + f' {proxy_port}']
                # Exercise the packaged config parser as well as its socket client.
                config = directory / 'control.conf'; config.write_text('\n'.join(commands) + '\n')
                module.apply(sock, module.load_commands(config))
                connection = connect(proxy_port)
                if tls:
                    ctx = ssl.create_default_context(cafile=cert)
                    connection = ctx.wrap_socket(connection, server_hostname='localhost')
                with connection:
                    connection.sendall(HEADER); assert receive(connection)[2][:4] == START[:4]
                    start_ok = b'\x00\x0a\x00\x0b' + b'\x00'*4 + short('PLAIN') + long('\x00test\x00test') + short('en_US')
                    connection.sendall(frame(start_ok)); assert receive(connection)[2][:4] == TUNE[:4]
                    connection.sendall(frame(b'\x00\x0a\x00\x1f' + TUNE[4:]) +
                                       frame(b'\x00\x0a\x00\x28' + short('VHOST1') + b'\x00\x00'))
                    if refused:
                        # Unmapped user: Connection.Close 530 for Connection.Open,
                        # and the proxy never connects to the broker.
                        kind, channel, payload = receive(connection)
                        assert (kind, channel, payload[:4]) == (1, 0, CLOSE), payload
                        code, = struct.unpack('!H', payload[4:6])
                        text = payload[7:7 + payload[6]]
                        assert struct.unpack('!HH', payload[7 + payload[6]:]) == (10, 40), payload
                        assert code == 530 and text == b"amqpprox: no vhost mapping for user 'test'", payload
                        assert closed(connection)
                        listener.settimeout(1)
                        try:
                            listener.accept()
                            raise AssertionError('proxy connected to the broker for an unmapped user')
                        except socket.timeout:
                            pass
                        print(f'PASS real proxy sockets: tls={tls}, mode={mode}')
                        return
                    if mode == 'vhost-missing':
                        # The broker's refusal reaches the client with its reason.
                        kind, channel, payload = receive(connection)
                        assert (kind, channel, payload[:4]) == (1, 0, CLOSE), payload
                        code, = struct.unpack('!H', payload[4:6])
                        assert code == 530 and payload[7:7 + payload[6]] == VHOST_MISSING, payload
                        assert closed(connection)
                        future.result(timeout=20)
                        print(f'PASS real proxy sockets: tls={tls}, mode={mode}')
                        return
                    assert receive(connection)[2][:4] == b'\x00\x0a\x00\x29'
                    name, flags = MODES[mode][1]
                    data = (frame(b'', kind=8) + frame(declare('classic', flags, name), channel=7) +
                            frame(b'body-unchanged', channel=7, kind=3))
                    # Split within a frame and coalesce neighboring frame types.
                    for chunk in (data[:11], data[11:19], data[19:]):
                        connection.sendall(chunk); time.sleep(0.01)
                    if mode == 'reject':
                        # Connection.Close naming the reason and Queue.Declare.
                        kind, channel, payload = receive(connection)
                        assert (kind, channel, payload[:4]) == (1, 0, CLOSE), payload
                        code, = struct.unpack('!H', payload[4:6])
                        text = payload[7:7 + payload[6]]
                        assert struct.unpack('!HH', payload[7 + payload[6]:]) == (50, 10), payload
                        assert code == 540 and b"queue 'reply' is exclusive" in text, payload
                        connection.sendall(frame(CLOSE_OK))
                        assert closed(connection)
                    else:
                        assert receive(connection) == (1, 7, declare_ok(name))
                future.result(timeout=20)
            except Exception:
                log.flush()
                print((directory / 'proxy.log').read_text())
                raise
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill(); process.wait()
        print(f'PASS real proxy sockets: tls={tls}, mode={mode}')


def control(sock, command):
    with socket.socket(socket.AF_UNIX) as c:
        c.settimeout(10); c.connect(sock); c.sendall((command + '\n').encode())
        reply = b''
        while True:
            part = c.recv(4096)
            if not part:
                return reply.decode()
            reply += part


def open_client(port):
    """Handshake as user 'test' to VHOST1; returns the socket and the reply to Open."""
    connection = connect(port)
    connection.sendall(HEADER); assert receive(connection)[2][:4] == START[:4]
    start_ok = b'\x00\x0a\x00\x0b' + b'\x00'*4 + short('PLAIN') + long('\x00test\x00test') + short('en_US')
    connection.sendall(frame(start_ok)); assert receive(connection)[2][:4] == TUNE[:4]
    connection.sendall(frame(b'\x00\x0a\x00\x1f' + TUNE[4:]) +
                       frame(b'\x00\x0a\x00\x28' + short('VHOST1') + b'\x00\x00'))
    return connection, receive(connection)


def reload_smoke(binary, directory):
    """REWRITE RELOAD changes the user map for new connections; a broken file keeps the old map."""
    user_map = directory / 'vhost-users.conf'; user_map.write_text('other VHOST4\n')
    sock = str(directory / 'control')
    with socket.socket() as listener, socket.socket() as reserve, \
            open(directory / 'proxy.log', 'w') as log, concurrent.futures.ThreadPoolExecutor(1) as pool:
        listener.bind(('127.0.0.1', 0)); listener.listen(1); listener.settimeout(20)
        reserve.bind(('127.0.0.1', 0)); proxy_port = reserve.getsockname()[1]; reserve.close()
        process = subprocess.Popen([binary, '--controlSocket', sock, '--logDirectory', str(directory / 'logs'),
                                    '--rewriteVhostFrom', 'VHOST1', '--rewriteVhostUserMap', str(user_map),
                                    '--rewriteClassicQueuesToQuorum'], stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 20
            while not pathlib.Path(sock).exists():
                assert time.monotonic() < deadline, 'control socket missing'
                time.sleep(0.1)
            # MAP DEFAULT routes every destination vhost the user map produces.
            for command in (f'BACKEND ADD backend local 127.0.0.1 {listener.getsockname()[1]}',
                            'FARM ADD farm round-robin backend', 'MAP DEFAULT farm',
                            f'LISTEN START {proxy_port}'):
                assert control(sock, command).strip() == '', command

            # 'test' is not mapped yet: refused.
            connection, (kind, _, payload) = open_client(proxy_port)
            with connection:
                assert payload[:4] == CLOSE and struct.unpack('!H', payload[4:6])[0] == 530, payload

            # Map it and reload while running: the next connection goes to VHOST3.
            user_map.write_text('other VHOST4\ntest VHOST3\n')
            reply = control(sock, 'REWRITE RELOAD')
            assert reply.startswith('OK: reloaded 2 user vhost mappings'), reply
            future = pool.submit(broker, listener, None, 'VHOST3', 'usermap')
            connection, (_, _, payload) = open_client(proxy_port)
            with connection:
                assert payload[:4] == b'\x00\x0a\x00\x29', payload
                data = (frame(b'', kind=8) + frame(declare('classic'), channel=7) +
                        frame(b'body-unchanged', channel=7, kind=3))
                connection.sendall(data)
                assert receive(connection) == (1, 7, declare_ok('jobs'))
            future.result(timeout=20)

            # A broken file is refused and the loaded map stays active.
            user_map.write_text('test VHOST3\ntest VHOST5\n')
            reply = control(sock, 'REWRITE RELOAD')
            assert reply.startswith("ERROR: User vhost map line 2: duplicate user 'test'"), reply
            assert 'test -> VHOST3' in control(sock, 'REWRITE PRINT')
        except Exception:
            log.flush()
            print((directory / 'proxy.log').read_text())
            raise
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait()
    # With MAP DEFAULT, an unmapped destination vhost is no error.
    logs = '\n'.join(p.read_text(errors='replace') for p in (directory / 'logs').glob('*') if p.is_file())
    assert 'using default farm farm' in logs, logs[-2000:]
    assert 'No mapping available' not in logs, logs[-2000:]
    print('PASS real proxy sockets: runtime user map reload')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', help='Path to Linux amqpprox executable')
    args = parser.parse_args()
    binary = str(pathlib.Path(args.binary).resolve())
    cases = ((False, 'disabled'), (False, 'convert'), (True, 'convert'),
             (False, 'reject'), (True, 'reject'), (False, 'passthrough'),
             (False, 'usermap'), (True, 'usermap'), (False, 'usermap-refused'),
             (False, 'vhost-missing'), (False, 'promote'))
    for tls, mode in cases:
        with tempfile.TemporaryDirectory(prefix='amqpprox-wire-') as directory:
            smoke(binary, pathlib.Path(directory), tls, mode)
    with tempfile.TemporaryDirectory(prefix='amqpprox-wire-') as directory:
        reload_smoke(binary, pathlib.Path(directory))
    print('WIRE SMOKE PASSED')


if __name__ == '__main__':
    main()
