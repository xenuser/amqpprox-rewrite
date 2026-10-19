#!/usr/bin/python3
"""Apply startup-only control commands. No shell execution; fail on any reply.
The allowed setters return an empty response on success in this checkout.
"""
import pathlib
import socket
import sys
import time


def load_commands(path):
    commands = []
    allowed = {('BACKEND', 'ADD'), ('BACKEND', 'ADD_DNS'), ('FARM', 'ADD'),
               ('MAP', 'FARM'), ('MAP', 'BACKEND'), ('MAP', 'DEFAULT'),
               ('LISTEN', 'START'), ('LISTEN', 'START_SECURE')}
    tls_options = {'KEY_FILE', 'CERT_CHAIN_FILE', 'RSA_KEY_FILE',
                   'TMP_DH_FILE', 'CA_CERT_FILE', 'VERIFY_MODE'}
    for number, raw in enumerate(pathlib.Path(path).read_text().splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        tokens = line.split()
        valid_tls = (len(tokens) >= 4 and tokens[0] == 'TLS' and
                     tokens[1] in ('INGRESS', 'EGRESS') and tokens[2] in tls_options)
        if tuple(tokens[:2]) not in allowed and not valid_tls:
            raise ValueError(f'Unsupported startup command on line {number}')
        commands.append(line)
    if not any(c.startswith('LISTEN ') for c in commands):
        raise ValueError('Configure control.conf with backends, mapping and LISTEN before starting')
    listening = False
    for command in commands:
        if command.startswith('LISTEN '):
            listening = True
        elif listening:
            raise ValueError('All LISTEN commands must be last')
    return commands


def apply(sock_path, commands):
    deadline = time.monotonic() + 20
    while not pathlib.Path(sock_path).exists():
        if time.monotonic() > deadline:
            raise TimeoutError('Proxy control socket did not appear')
        time.sleep(0.1)
    for command in commands:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(10)
            connection.connect(sock_path)
            connection.sendall((command + '\n').encode())
            reply = bytearray()
            while True:
                part = connection.recv(4096)
                if not part:
                    break
                reply.extend(part)
                if len(reply) > 65536:
                    raise RuntimeError('Excessive control response')
        if reply.strip():
            raise RuntimeError('Proxy rejected startup command: ' + reply.decode(errors='replace').strip())


if __name__ == '__main__':
    try:
        if len(sys.argv) != 3:
            raise ValueError('Usage: amqpprox-apply-config SOCKET CONFIG')
        apply(sys.argv[1], load_commands(sys.argv[2]))
    except (OSError, ValueError, RuntimeError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
