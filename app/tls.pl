/*  tls.pl -- example: connect with TLS and client certificate auth.

    Usage:
      swipl -g main tls.pl

    Expects an MQTT broker reachable at mqtt-bridge:8883 (TLS).
    Set MQTT_BRIDGE_HOST / MQTT_BRIDGE_PORT to override the bridge address.

    Certificate paths below assume the standard Docker volume mount at /certs/.
    Adjust CA_CERT / CLIENT_CERT / CLIENT_KEY to match your setup.

    To generate self-signed test certificates:
      openssl genrsa -out ca.key 2048
      openssl req -x509 -new -key ca.key -days 3650 -out ca.pem -subj "/CN=test-ca"
      openssl genrsa -out client.key 2048
      openssl req -new -key client.key -out client.csr -subj "/CN=test-client"
      openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key \
          -CAcreateserial -days 3650 -out client.pem
*/

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
