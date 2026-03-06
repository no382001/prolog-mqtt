:- use_module(prolog/mqtt).

:- multifile
    mqtt_hook_on_connect/2,
    mqtt_hook_on_disconnect/2,
    mqtt_hook_on_message/2,
    mqtt_hook_on_publish/2,
    mqtt_hook_on_subscribe/2,
    mqtt_hook_on_unsubscribe/2,
    mqtt_hook_on_error/2.

:- dynamic main_thread/1.

mqtt_hook_on_message(_Conn, Data) :-
    memberchk(payload(quit), Data), !,
    format("alice: got quit — stopping~n"), flush_output,
    (main_thread(T) -> thread_send_message(T, done) ; true).

mqtt_hook_on_message(Conn, Data) :-
    memberchk(topic(Topic), Data),
    memberchk(payload(Payload), Data),
    format("alice: [recv] ~w: ~w~n", [Topic, Payload]), flush_output,
    mqtt_pub(Conn, 'chat/bob', Payload).

main :-
    sleep(2),
    thread_self(Me),
    assertz(main_thread(Me)),
    mqtt_connect(mosquitto, 1883, [client_id(alice)], Conn),
    format("alice: connected~n"), flush_output,
    mqtt_sub(Conn, 'chat/alice'),
    format("alice: subscribed~n"), flush_output,
    sleep(1),
    mqtt_pub(Conn, 'chat/bob', ping),
    format("alice: sent initial ping~n"), flush_output,
    thread_get_message(done),
    format("alice: done~n"), flush_output.
