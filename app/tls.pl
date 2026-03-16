:- use_module(prolog/mqtt).

:- multifile mqtt:on_message/2.

mqtt:on_message(_Conn, Data) :-
    memberchk(topic(Topic), Data),
    memberchk(payload(Payload), Data),
    format("received ~w: ~w~n", [Topic, Payload]).

ca_cert('/certs/ca.pem').
client_cert('/certs/client.pem').
client_key('/certs/client.key').

main :-
    ca_cert(CA),
    client_cert(Cert),
    client_key(Key),
    mqtt:connect(mosquitto, 8883,
        [ client_id(tls_example),
          tls(true),
          ca_cert(CA),
          client_cert(Cert),
          client_key(Key)
        ], Conn),
    format("connected with TLS (conn ~w)~n", [Conn]),

    mqtt:sub(Conn, 'example/topic'),
    format("subscribed~n"),

    mqtt:pub(Conn, 'example/topic', 'hello over TLS'),
    format("published~n"),

    sleep(2),

    mqtt:disconnect(Conn),
    format("disconnected~n").
