:- use_module(prolog/mqtt).

:- multifile
    mqtt_hook_on_connect/2,
    mqtt_hook_on_disconnect/2,
    mqtt_hook_on_message/2,
    mqtt_hook_on_publish/2,
    mqtt_hook_on_subscribe/2,
    mqtt_hook_on_unsubscribe/2,
    mqtt_hook_on_error/2.

:- dynamic msg_count/1, main_thread/1.
msg_count(0).

mqtt_hook_on_message(Conn, Data) :-
    memberchk(topic(Topic), Data),
    memberchk(payload(Payload), Data),
    retract(msg_count(N)),
    N1 is N + 1,
    assertz(msg_count(N1)),
    format("bob: [recv ~w/10] ~w: ~w~n", [N1, Topic, Payload]), flush_output,
    (   N1 < 10
    ->  mqtt_pub(Conn, 'chat/alice', Payload)
    ;   format("bob: limit reached — sending quit~n"), flush_output,
        mqtt_pub(Conn, 'chat/alice', quit),
        (main_thread(T) -> thread_send_message(T, done) ; true)
    ).

main :-
    sleep(2),
    thread_self(Me),
    assertz(main_thread(Me)),
    mqtt_connect(mosquitto, 1883, [client_id(bob)], Conn),
    format("bob: connected~n"), flush_output,
    mqtt_sub(Conn, 'chat/bob'),
    format("bob: subscribed~n"), flush_output,
    thread_get_message(done),
    format("bob: done~n"), flush_output.
