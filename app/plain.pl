/*  plain.pl -- minimal example: connect without TLS, publish, subscribe.

    Usage:
      swipl -g main plain.pl

    Expects an MQTT broker reachable at mqtt-bridge:1883 (plain TCP).
    Set MQTT_BRIDGE_HOST / MQTT_BRIDGE_PORT to override the bridge address.
*/

:- use_module(prolog/mqtt).

:- multifile mqtt:on_message/2.

mqtt:on_message(_Conn, Data) :-
    memberchk(topic(Topic), Data),
    memberchk(payload(Payload), Data),
    format("received ~w: ~w~n", [Topic, Payload]).

main :-
    mqtt:connect(mosquitto, 1883, [client_id(plain_example)], Conn),
    format("connected (conn ~w)~n", [Conn]),

    mqtt:sub(Conn, 'example/topic'),
    format("subscribed~n"),

    mqtt:pub(Conn, 'example/topic', 'hello world'),
    format("published~n"),

    sleep(2),

    mqtt:disconnect(Conn),
    format("disconnected~n").
