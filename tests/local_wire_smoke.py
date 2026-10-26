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
}


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
        if rewrite:
            command += ['--rewriteVhostFrom', 'VHOST1', '--rewriteVhostTo', 'VHOST2'] + MODES[mode][0]
        cert = str(directory / 'cert.pem'); key = str(directory / 'key.pem')
        context = None
        if tls:
            subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                            '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost',
                            '-keyout', key, '-out', cert], check=True, capture_output=True)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); context.load_cert_chain(cert, key)
        with open(directory / 'proxy.log', 'w') as log, concurrent.futures.ThreadPoolExecutor(1) as pool:
            future = pool.submit(broker, listener, context, 'VHOST2' if rewrite else 'VHOST1', mode)
            process = subprocess.Popen(command, stdout=log, stderr=log)
            try:
                spec = importlib.util.spec_from_file_location('config', pathlib.Path(__file__).resolve().parents[1] / 'packaging/rhel9/apply-config.py')
                module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
                commands = []
                if tls:
                    commands += [f'TLS INGRESS CERT_CHAIN_FILE {cert}', f'TLS INGRESS KEY_FILE {key}',
                                 f'TLS EGRESS CA_CERT_FILE {cert}', 'TLS EGRESS VERIFY_MODE PEER']
                commands += [f'BACKEND ADD backend local 127.0.0.1 {backend_port}' + (' TLS' if tls else ''),
                             'MAP BACKEND ' + ('VHOST2' if rewrite else 'VHOST1') + ' backend',
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', help='Path to Linux amqpprox executable')
    args = parser.parse_args()
    cases = ((False, 'disabled'), (False, 'convert'), (True, 'convert'),
             (False, 'reject'), (True, 'reject'), (False, 'passthrough'))
    for tls, mode in cases:
        with tempfile.TemporaryDirectory(prefix='amqpprox-wire-') as directory:
            smoke(str(pathlib.Path(args.binary).resolve()), pathlib.Path(directory), tls, mode)
    print('WIRE SMOKE PASSED')


if __name__ == '__main__':
    main()
