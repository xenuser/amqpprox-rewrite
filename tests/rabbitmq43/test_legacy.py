#!/usr/bin/env python3
"""Exercise a running amqpprox against an existing RabbitMQ 4.3 cluster.
No containers. Uses only uniquely named test queues/exchanges and removes them.
See README.md in this directory for isolated test-vhost provisioning.
"""
import argparse
import base64
import json
import os
import ssl
import sys
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
import uuid

import pika


def required_env(name):
    value = os.environ.get(name)
    if not value:
        raise ValueError("Set environment variable " + name)
    return value


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--proxy-host', required=True)
    p.add_argument('--proxy-port', type=int, default=5673)
    p.add_argument('--source-vhost', default='/')
    p.add_argument('--target-vhost', default='rewrite-test-target')
    p.add_argument('--control-vhost', default='rewrite-test-control')
    p.add_argument('--management-url', required=True, help='e.g. https://rabbit-admin.example:15671 (no /api)')
    p.add_argument('--management-ca', help='PEM CA for HTTPS management API')
    p.add_argument('--tls', action='store_true', help='Use AMQPS to proxy')
    p.add_argument('--ca-file', help='PEM CA for AMQPS; otherwise OS trust')
    p.add_argument('--server-name', help='TLS name, default proxy-host')
    p.add_argument('--client-cert')
    p.add_argument('--client-key')
    p.add_argument('--min-members', type=int, default=3)
    p.add_argument('--passthrough-port', type=int, help='Optional second proxy with rewriting disabled')
    p.add_argument('--vhost-only-port', type=int, help='Optional second proxy with only vhost rewriting')
    p.add_argument('--report', default='rabbitmq43-results.json')
    a = p.parse_args()
    if len({a.source_vhost, a.target_vhost, a.control_vhost}) != 3:
        p.error('source, target, and control vhosts must differ')
    if a.client_key and not a.client_cert:
        p.error('--client-key requires --client-cert')
    if not a.tls and any((a.ca_file, a.server_name, a.client_cert, a.client_key)):
        p.error('TLS options require --tls')
    if a.min_members < 1:
        p.error('--min-members must be positive')
    return a


class API:
    def __init__(self, args):
        self.base = args.management_url.rstrip('/') + '/api/'
        self.context = ssl.create_default_context(cafile=args.management_ca)
        self.auth = base64.b64encode((required_env('RMQ_ADMIN_USER') + ':' +
                                     required_env('RMQ_ADMIN_PASSWORD')).encode()).decode()

    def call(self, method, *parts, body=None, missing_ok=False):
        url = self.base + '/'.join(urllib.parse.quote(x, safe='') for x in parts)
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method=method,
                                     headers={'Authorization': 'Basic ' + self.auth,
                                              'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=20, context=self.context) as response:
                payload = response.read()
                return json.loads(payload) if payload else None
        except urllib.error.HTTPError as error:
            if missing_ok and error.code == 404:
                return None
            # Do not include request headers or credentials in diagnostics.
            raise RuntimeError(f'Management API {method} failed with HTTP {error.code}') from None


class LegacyTests(unittest.TestCase):
    args = None
    api = None
    run_id = 'rewrite-' + uuid.uuid4().hex
    evidence = {}

    def setUp(self):
        self.connections = []
        self.resources = []

    def tearDown(self):
        errors = []
        for conn in self.connections:
            try:
                if conn.is_open:
                    conn.close()
            except pika.exceptions.AMQPError as error:
                errors.append(error)
        # Attempt every exact resource cleanup even after a connection fails.
        for kind, vhost, name in reversed(self.resources):
            try:
                self.api.call('DELETE', kind, vhost, name, missing_ok=True)
            except (RuntimeError, OSError) as error:
                errors.append(error)
        if errors:
            raise RuntimeError('Cleanup was incomplete; inspect run ID ' + self.run_id) from errors[0]

    def name(self, suffix, vhost=None, kind='queues'):
        name = self.run_id + '-' + self._testMethodName + '-' + suffix
        self.resources.append((kind, vhost or self.args.target_vhost, name))
        return name

    def connect(self, vhost=None, control=False, port=None):
        user = required_env('RMQ_CONTROL_USER' if control else 'RMQ_TEST_USER')
        password = required_env('RMQ_CONTROL_PASSWORD' if control else 'RMQ_TEST_PASSWORD')
        options = None
        if self.args.tls:
            ctx = ssl.create_default_context(cafile=self.args.ca_file)
            if self.args.client_cert:
                ctx.load_cert_chain(self.args.client_cert, self.args.client_key)
            options = pika.SSLOptions(ctx, self.args.server_name or self.args.proxy_host)
        params = pika.ConnectionParameters(
            host=self.args.proxy_host, port=port or self.args.proxy_port,
            virtual_host=vhost or self.args.source_vhost,
            credentials=pika.PlainCredentials(user, password), ssl_options=options,
            heartbeat=30, socket_timeout=10, stack_timeout=20,
            blocked_connection_timeout=20, connection_attempts=1)
        conn = pika.BlockingConnection(params)
        self.connections.append(conn)
        return conn

    def queue_info(self, name, vhost=None):
        return self.api.call('GET', 'queues', vhost or self.args.target_vhost, name)

    def assert_quorum(self, name):
        info = self.queue_info(name)
        self.assertEqual(info['type'], 'quorum')
        self.assertTrue(info['durable'])
        self.assertFalse(info['exclusive'])
        # Poll management stats briefly while member data becomes visible.
        deadline = time.monotonic() + 20
        while len(info.get('online', [])) < self.args.min_members and time.monotonic() < deadline:
            time.sleep(0.5)
            info = self.queue_info(name)
        self.assertGreaterEqual(len(info.get('members', [])), self.args.min_members)
        self.assertGreaterEqual(len(info.get('online', [])), self.args.min_members)
        self.assertIsNone(self.api.call('GET', 'queues', self.args.source_vhost, name, missing_ok=True))
        self.evidence[name] = {'type': info['type'], 'vhost': info['vhost'],
                               'members': info.get('members'), 'online': info.get('online')}

    @classmethod
    def setUpClass(cls):
        cls("runTest").check_environment()

    def check_environment(self):
        overview = self.api.call('GET', 'overview')
        self.assertTrue(overview['rabbitmq_version'].startswith('4.3.'), overview['rabbitmq_version'])
        nodes = self.api.call('GET', 'nodes')
        self.assertGreaterEqual(sum(bool(n.get('running')) for n in nodes), self.args.min_members)
        permissions = self.api.call('GET', 'users', required_env('RMQ_TEST_USER'), 'permissions')
        self.assertEqual({p['vhost'] for p in permissions}, {self.args.target_vhost},
                         'Test user must have permission ONLY on the target vhost')
        for vhost in (self.args.target_vhost, self.args.control_vhost):
            metadata = self.api.call('GET', 'vhosts', vhost)
            self.assertEqual(metadata.get('default_queue_type'), 'classic',
                             'Set isolated test vhosts to classic to prove proxy rewriting')
        self.evidence['server'] = {'version': overview['rabbitmq_version'],
                                   'running_nodes': [n['name'] for n in nodes if n.get('running')]}

    def test_explicit_classic_publish_consume_reconnect(self):
        q = self.name('jobs')
        exchange = self.name('exchange', kind='exchanges')
        ch = self.connect().channel()
        args = {'x-queue-type': 'classic', 'x-message-ttl': 60000}
        ch.queue_declare(q, durable=True, arguments=args)
        ch.queue_declare(q, durable=True, arguments=args)
        ch.exchange_declare(exchange, exchange_type='direct', durable=True)
        ch.queue_bind(q, exchange, 'job')
        ch.confirm_delivery()
        payload = bytes(range(256)) * 1200  # multiple AMQP body frames
        ch.basic_publish(exchange, 'job', payload, mandatory=True,
                         properties=pika.BasicProperties(delivery_mode=2, headers={'marker': 'unchanged'}))
        ch.connection.close()
        ch = self.connect().channel()
        ch.queue_declare(q, durable=True, arguments=args)
        ch.basic_qos(prefetch_count=1, global_qos=False)
        method, props, body = ch.basic_get(q, auto_ack=False)
        self.assertIsNotNone(method)
        self.assertEqual(body, payload)
        self.assertEqual(props.headers['marker'], 'unchanged')
        ch.basic_reject(method.delivery_tag, requeue=True)
        method, props, body = ch.basic_get(q, auto_ack=False)
        self.assertIsNotNone(method)
        self.assertTrue(method.redelivered)
        self.assertEqual(body, payload)
        ch.basic_ack(method.delivery_tag)
        self.assert_quorum(q)
        self.assertEqual(self.queue_info(q)['arguments']['x-message-ttl'], 60000)

    def test_missing_type_and_multiple_channels(self):
        conn = self.connect()
        for n in range(3):
            q = self.name(str(n))
            ch = conn.channel()
            ch.queue_declare(q, durable=True)
            ch.queue_declare(q, passive=True)
            self.assert_quorum(q)
            ch.close()

    def test_explicit_quorum_and_stream(self):
        ch = self.connect().channel()
        q = self.name('quorum')
        ch.queue_declare(q, durable=True, arguments={'x-queue-type': 'quorum'})
        self.assert_quorum(q)
        stream = self.name('stream')
        ch.queue_declare(stream, durable=True, arguments={'x-queue-type': 'stream'})
        self.assertEqual(self.queue_info(stream)['type'], 'stream')

    def test_nonmatching_vhost_is_unchanged(self):
        q = self.name('control', self.args.control_vhost)
        ch = self.connect(self.args.control_vhost, control=True).channel()
        ch.queue_declare(q, durable=True, arguments={'x-queue-type': 'classic'})
        self.assertEqual(self.queue_info(q, self.args.control_vhost)['type'], 'classic')
        self.assertIsNone(self.api.call('GET', 'queues', self.args.target_vhost, q, missing_ok=True))

    def test_incompatible_flags_are_rejected(self):
        for flags in ({'durable': False}, {'durable': True, 'exclusive': True},
                      {'durable': True, 'auto_delete': True}):
            with self.subTest(flags=flags):
                q = self.name(str(len(self.resources)))
                ch = self.connect().channel()
                with self.assertRaises((pika.exceptions.StreamLostError, pika.exceptions.ConnectionClosed,
                                        pika.exceptions.ChannelClosedByBroker)):
                    ch.queue_declare(q, arguments={'x-queue-type': 'classic'}, **flags)
                self.assertIsNone(self.api.call('GET', 'queues', self.args.target_vhost, q, missing_ok=True))

    def test_existing_classic_is_not_converted(self):
        q = self.name('preexisting')
        self.api.call('PUT', 'queues', self.args.target_vhost, q,
                      body={'durable': True, 'auto_delete': False, 'arguments': {'x-queue-type': 'classic'}})
        ch = self.connect().channel()
        with self.assertRaises(pika.exceptions.ChannelClosedByBroker) as caught:
            ch.queue_declare(q, durable=True, arguments={'x-queue-type': 'classic'})
        self.assertEqual(caught.exception.reply_code, 406)
        self.assertEqual(self.queue_info(q)['type'], 'classic')

    def test_disabled_proxy(self):
        if not self.args.passthrough_port:
            self.skipTest('Supply --passthrough-port for live disabled-mode regression')
        q = self.name('disabled')
        ch = self.connect(self.args.target_vhost, port=self.args.passthrough_port).channel()
        ch.queue_declare(q, durable=True, arguments={'x-queue-type': 'classic'})
        self.assertEqual(self.queue_info(q)['type'], 'classic')

    def test_vhost_only_proxy(self):
        if not self.args.vhost_only_port:
            self.skipTest('Supply --vhost-only-port for live vhost-only regression')
        q = self.name('vhost-only')
        ch = self.connect(port=self.args.vhost_only_port).channel()
        ch.queue_declare(q, durable=True, arguments={'x-queue-type': 'classic'})
        self.assertEqual(self.queue_info(q)['type'], 'classic')


def main():
    args = parse_args()
    for name in ('RMQ_TEST_USER', 'RMQ_TEST_PASSWORD', 'RMQ_CONTROL_USER', 'RMQ_CONTROL_PASSWORD'):
        required_env(name)
    LegacyTests.args = args
    LegacyTests.api = API(args)
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(LegacyTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    report = {'run_id': LegacyTests.run_id, 'tests': result.testsRun,
              'failures': len(result.failures), 'errors': len(result.errors),
              'skipped': [(str(test), reason) for test, reason in result.skipped],
              'success': result.wasSuccessful(), 'evidence': LegacyTests.evidence}
    with open(args.report, 'w', encoding='utf-8') as out:
        json.dump(report, out, indent=2)
    print('Report:', args.report)
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, RuntimeError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(2)
