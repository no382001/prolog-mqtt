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
ordinary Prolog predicates.

## Prolog API

```prolog
:- use_module(prolog/mqtt).

mqtt_connect(+Host, -Conn)
mqtt_connect(+Host, +Port, -Conn)
mqtt_connect(+Host, +Port, +Options, -Conn)   % options: client_id/1, keepalive/1
mqtt_disconnect(+Conn)
mqtt_pub(+Conn, +Topic, +Payload)
mqtt_pub(+Conn, +Topic, +Payload, +Options)   % options: qos/1, retain/1
mqtt_sub(+Conn, +Topic)
mqtt_sub(+Conn, +Topic, +Options)             % options: qos/1
mqtt_unsub(+Conn, +Topic)
```

Incoming events are delivered via multifile hook predicates:

```prolog
:- multifile mqtt_hook_on_message/2.

mqtt_hook_on_message(Conn, Data) :-
    memberchk(topic(Topic), Data),
    memberchk(payload(Payload), Data),
    format("~w: ~w~n", [Topic, Payload]).
```

Available hooks: `mqtt_hook_on_connect/2`, `mqtt_hook_on_disconnect/2`,
`mqtt_hook_on_message/2`, `mqtt_hook_on_publish/2`, `mqtt_hook_on_subscribe/2`,
`mqtt_hook_on_unsubscribe/2`, `mqtt_hook_on_error/2`.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MQTT_BRIDGE_HOST` | `mqtt-bridge` | hostname of the bridge process |
| `MQTT_BRIDGE_PORT` | `7883` | TCP port the bridge listens on |

## Running the demo

```sh
docker compose up --build
```

Starts Mosquitto, Alice, and Bob. Alice sends a `ping` to Bob; they echo it
back and forth 10 times, then stop.
