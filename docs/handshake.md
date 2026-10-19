# Connection establishment in `amqpprox`

The core feature of `amqpprox` is the ability to forward incoming client connections to a RabbitMQ broker based on the vhost specified as part of the initial AMQP handshake. This document explains the full handshake procedure.

## Overview
The setup of each successful connection goes through the following steps

1. Ingress connection & partial AMQP handshake 
    a. The client must send `Connection.Open` to progress to step 2

2. Egress connection establishment & replay initial AMQP handshake
    a. A connection is established with the RabbitMQ broker, and the initial ingress messages are replayed to the broker

3. Pure traffic forwarding
    a. For the rest of the socket lifetime `amqpprox` purely forwards bytes from one socket to the other.

An outgoing/egress connection is established after an ingress connetion sends the "Connection.Open" message (the first message which indicates the required vhost)

The `Connector` class (`amqpprox_connector.h`) holds all state/logic for moving through the process described in this document. The `Connector` is passed frames by the `PacketProcessor` (`amqpprox_packetprocessor.h`).

## 1. Ingress connection
The inbound connection may be over TLS depending on how amqpprox is configured, and which port the client connected to.

The AMQP handshake is performed as follows:

| Message              | Origin       | `amqpprox` state before message arrives | Notes |
|----------------------|--------------|---------------------------------------|-------|
| `AMQP 0.9.1 Header`  | 🙋‍♀️ client    | `AWAITING_PROTOCOL_HEADER`            |       |
| `Connection.Start`   | 🖥 amqpprox  |                                       |       |
| `Connection.StartOk` | 🙋‍♀️ client    | `START_SENT`                          | Stored for use when the broker connection |
| `Connection.Tune`    | 🖥 amqpprox  |                                       |       |
| `Connection.TuneOk`  | 🙋‍♀️ client    | `TUNE_SENT`                           | Stored for use when the broker connection |
| `Connection.Open`    | 🙋‍♀️ client    | `AWAITING_OPEN`                       | Stored for use when the broker connection |

Once `Connection.Open` is received, the proxy does not reply until the outbound/egress connection is successfully established.


## 2. Egress connection

The exact handshake depends on the following configuration for the relevant backend:
1. Proxy protocol (on/off)
2. TLS (on/off)

### 2.1. Proxy protocol

If enabled, the proxy protocol header is the first message sent to the RabbitMQ broker.
This is a one-way header, so doesn't increase the number of roundtrips. 

 > Notably the proxy protocol header is sent before the TLS handshake begins

## 2.2 TLS Handshake

At this point a TLS connection is setup. Depending on the TLS configuration in `amqpprox` and RabbitMQ this handshake can authenticate none/one/both parties.

This handshake requires two roundtrips

## 2.3. AMQP Handshake

The AMQP handshake performed between `amqpprox` and the RabbitMQ broker is an emulation of the original inbound connection. Certain aspects of the inbound connection are repeated on the new connection.

| Message              | Origin       | `amqpprox` state before message | Notes |
|----------------------|--------------|---------------------------------|-------|
| `AMQP 0.9.1 Header`  | 🖥 amqpprox  | `AWAITING_CONNECTION`           |       |
| `Connection.Start`   | 🐇 RabbitMQ | `AWAITING_CONNECTION`           |       |
| `Connection.StartOk` | 🖥 amqpprox  |                                 | Forwarded from earlier client handshake ***with amqpprox header fields added*** |
| `Connection.Tune`    | 🐇 RabbitMQ | `STARTOK_SENT`                  |       |
| `Connection.TuneOk`  | 🖥 amqpprox  |                                 | Directly forwarded from earlier client handshake |
| `Connection.Open`    | 🖥 amqpprox  |                                 | Directly forwarded from earlier client handshake |
| `Connection.OpenOk`  | 🐇 RabbitMQ | `OPEN_SENT`                     |       |

## 3. Fully connected

After `Connection.OpenOk` is sent by `RabbitMQ`, this frame and all further traffic is directly forwarded from one socket to the next. This passthrough happens inside of `PacketProcessor::process` and is controlled by the state of the Connector being `OPEN`.

## Notes

1. Connection.Secure/SecureOK are not supported by `amqpprox`

 > Authentication using PLAIN is passed through to the destination broker. TLS authentication (terminated at the proxy) can be configured as well. See `docs/tls.md`

## Optional rewriting in this fork

When `--rewriteVhostTo` is configured, matching incoming vhosts are replaced
before routing/authentication and outgoing Connection.Open. With
`--rewriteClassicQueuesToQuorum`, ingress Queue.Declare frames on those
connections are also inspected and selectively rewritten after connection
establishment. Other traffic keeps the forwarding behavior described above.
See [legacy-rewrite.md](legacy-rewrite.md) for scope and limitations.
