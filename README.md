# prolog-mqtt

A prototype for declaring relationships between microservices in Prolog,
using MQTT as the message transport. Service topology, routing
rules, and interaction patterns are expressed as Prolog facts and rules
rather than hardcoded in each service.

Backed by a small C bridge process that wraps the Paho MQTT C library.

> **Why not [swi-mqtt-pack](https://github.com/sprior/swi-mqtt-pack)?**
> That project is outdated and unmaintained. Rather than updating its FFI
> bindings, it turned out simpler to wrap the C library in a standalone
> process and talk to it over a plain TCP socket.

## Architecture

`mqtt_bridge` (built from `bridge/mqtt_bridge.c` against the Paho MQTT C library)
runs as a sidecar process and routes MQTT traffic over a local TCP socket.
`prolog/mqtt.pl` connects to that socket and exposes the MQTT operations as
ordinary Prolog predicates under the `mqtt:` module namespace.

## Prolog API

```prolog
:- use_module(prolog/mqtt).

mqtt:connect(+Host, -Conn)
mqtt:connect(+Host, +Port, -Conn)
mqtt:connect(+Host, +Port, +Options, -Conn)
mqtt:disconnect(+Conn)
mqtt:pub(+Conn, +Topic, +Payload)
mqtt:pub(+Conn, +Topic, +Payload, +Options)
mqtt:sub(+Conn, +Topic)
mqtt:sub(+Conn, +Topic, +Options)
mqtt:unsub(+Conn, +Topic)
```

### connect/4 options

| Option | Default | Description |
|--------|---------|-------------|
| `client_id(+Atom)` | `mqtt_client` | MQTT client identifier |
| `keepalive(+Int)` | `60` | keepalive interval in seconds |
| `tls(+Bool)` | `false` | enable TLS |
| `verify(+Bool)` | `true` | verify server certificate (when TLS) |
| `ca_cert(+Path)` | system store | CA certificate file (PEM) |
| `client_cert(+Path)` | none | client certificate file (PEM) |
| `client_key(+Path)` | none | client private key file (PEM) |

### pub/4 options

| Option | Default | Description |
|--------|---------|-------------|
| `qos(+Int)` | `0` | QoS level 0, 1, or 2 |
| `retain(+Bool)` | `false` | set the retain flag |

### sub/3 options

| Option | Default | Description |
|--------|---------|-------------|
| `qos(+Int)` | `0` | QoS level 0, 1, or 2 |

### Event hooks

Incoming events are delivered via multifile hook predicates in the `mqtt` module:

```prolog
:- multifile mqtt:on_message/2.

mqtt:on_message(Conn, Data) :-
    memberchk(topic(Topic), Data),
    memberchk(payload(Payload), Data),
    format("~w: ~w~n", [Topic, Payload]).
```

Available hooks: `mqtt:on_connect/2`, `mqtt:on_disconnect/2`,
`mqtt:on_message/2`, `mqtt:on_publish/2`, `mqtt:on_subscribe/2`,
`mqtt:on_unsubscribe/2`, `mqtt:on_error/2`.

### TLS example

```prolog
mqtt:connect('broker.example.com', 8883,
    [ client_id(myservice),
      tls(true),
      ca_cert('/etc/ssl/certs/ca.pem')
    ], Conn).
```

For mutual TLS (client certificate authentication):

```prolog
mqtt:connect('broker.example.com', 8883,
    [ tls(true),
      ca_cert('/certs/ca.pem'),
      client_cert('/certs/client.pem'),
      client_key('/certs/client.key')
    ], Conn).
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MQTT_BRIDGE_HOST` | `mqtt-bridge` | hostname of the bridge process |
| `MQTT_BRIDGE_PORT` | `7883` | TCP port the bridge listens on |

## Examples

| File | Description |
|------|-------------|
| [`app/plain.pl`](app/plain.pl) | Plain TCP connection — connect, subscribe, publish, disconnect |
| [`app/tls.pl`](app/tls.pl) | TLS with client certificate auth — reads certs from `/certs/` |
| [`app/alice.pl`](app/alice.pl) | Demo: Alice side of the ping-pong exchange |
| [`app/bob.pl`](app/bob.pl) | Demo: Bob side of the ping-pong exchange |

`tls.pl` expects PEM files at `/certs/ca.pem`, `/certs/client.pem`, and
`/certs/client.key`. The file includes `openssl` commands to generate
self-signed test certificates.

## Running the demo

```sh
docker compose up --build
```

Starts Mosquitto, Alice, and Bob. Alice sends a `ping` to Bob; they echo it
back and forth 10 times, then stop.

## Running the TLS demo

Generate test certificates once:

```sh
sh certs/gen-certs.sh
```

Then run:

```sh
docker compose -f docker-compose.tls.yml up --build
```

This starts Mosquitto with a TLS listener on port 8883 with mutual certificate
auth required, then runs [`app/tls.pl`](app/tls.pl) against it. The app
connects with its client certificate, subscribes to `example/topic`, publishes
`hello over TLS` to that topic, receives it back via the `on_message` hook, and
disconnects. Both Mosquitto and the app receive the generated certs from
`certs/` via read-only volume mounts.
