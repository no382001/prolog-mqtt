/*  mqtt.pl -- prolog MQTT client library

    connects to the mqtt_bridge helper process over a local TCP socket and
    multiplexes any number of MQTT connections over it.

    configuration via environment variables (with defaults):
      MQTT_BRIDGE_HOST  hostname of the bridge process   (default: mqtt-bridge)
      MQTT_BRIDGE_PORT  TCP port the bridge listens on   (default: 7883)

    to receive events, add clauses to the hook predicates in your own file and
    declare them multifile there, e.g.

      :- multifile mqtt_hook_on_message/2.

      mqtt_hook_on_message(Conn, Data) :-
          memberchk(payload(P), Data),
          format("got: ~w~n", [P]).
*/

:- module(mqtt, [
    mqtt_connect/2,
    mqtt_connect/3,
    mqtt_connect/4,
    mqtt_disconnect/1,
    mqtt_pub/3,
    mqtt_pub/4,
    mqtt_sub/2,
    mqtt_sub/3,
    mqtt_unsub/2
]).

:- use_module(library(socket)).

%! mqtt_hook_on_connect(+Conn, +Data) is semidet.
%  called when an MQTT connection is established (not waited on by mqtt_connect/4).
%  Data = [rc(RC)] where RC is 0 on success.
:- multifile mqtt_hook_on_connect/2.

%! mqtt_hook_on_disconnect(+Conn, +Data) is semidet.
%  called when an MQTT connection drops unexpectedly.
%  Data = [cause(Cause)].
:- multifile mqtt_hook_on_disconnect/2.

%! mqtt_hook_on_message(+Conn, +Data) is semidet.
%  called for every incoming MQTT PUBLISH.
%  Data = [topic(Topic), payload(Payload), qos(QoS), retain(Retain)].
:- multifile mqtt_hook_on_message/2.

%! mqtt_hook_on_publish(+Conn, +Data) is semidet.
%  called when the broker confirms delivery of a published message.
%  Data = [token(Token)].
:- multifile mqtt_hook_on_publish/2.

%! mqtt_hook_on_subscribe(+Conn, +Data) is semidet.
%  called when a subscribe request is acknowledged by the broker.
%  Data = [token(Token)].
:- multifile mqtt_hook_on_subscribe/2.

%! mqtt_hook_on_unsubscribe(+Conn, +Data) is semidet.
%  called when an unsubscribe request is acknowledged by the broker.
%  Data = [token(Token)].
:- multifile mqtt_hook_on_unsubscribe/2.

%! mqtt_hook_on_error(+Conn, +Data) is semidet.
%  called on asynchronous errors not tied to a waiting caller.
%  Data = [description(Desc)].
:- multifile mqtt_hook_on_error/2.

:- dynamic
    bridge_stream/1,   % write side of the bridge TCP socket
    bridge_in/1,       % read side, owned by the event_loop thread
    next_id/1,
    waiting/2.         % waiting(Key, Thread) -- caller blocked in mqtt_connect/disconnect

:- volatile bridge_stream/1, bridge_in/1.

next_id(1).

bridge_ensure :-
    bridge_stream(_), !.
bridge_ensure :-
    bridge_host(Host),
    bridge_port(Port),
    tcp_socket(Sock),
    tcp_connect(Sock, Host:Port),
    tcp_open_socket(Sock, In, Out),
    set_stream(In,  type(text)),
    set_stream(In,  buffer(false)),
    set_stream(Out, type(text)),
    set_stream(Out, buffer(false)),
    assertz(bridge_stream(Out)),
    assertz(bridge_in(In)),
    thread_create(event_loop(In), _, [detached(true)]).

bridge_host(Host) :-
    (getenv('MQTT_BRIDGE_HOST', H) -> Host = H ; Host = 'mqtt-bridge').

bridge_port(Port) :-
    (getenv('MQTT_BRIDGE_PORT', P)
     -> (number(P) -> Port = P ; atom_number(P, Port))
     ;  Port = 7883).

event_loop(In) :-
    catch(
        event_loop_r(In),
        _Error,
        (   retractall(bridge_stream(_)),
            retractall(bridge_in(_)),
            retractall(waiting(_, _))
        )
    ).

event_loop_r(In) :-
    read_term(In, Term, [end_of_file(eof)]),
    (   Term == eof
    ->  retractall(bridge_stream(_)),
        retractall(bridge_in(_))
    ;   catch(dispatch(Term), E, (print_message(error, E), flush_output(user_error))),
        event_loop_r(In)
    ).

dispatch(connected(Id, RC)) :-
    (   retract(waiting(Id, Caller))
    ->  thread_send_message(Caller, connected(Id, RC))
    ;   ignore(user:mqtt_hook_on_connect(Id, [rc(RC)]))
    ).

dispatch(disconnected(Id, Cause)) :-
    (   retract(waiting(disconnect(Id), Caller))
    ->  thread_send_message(Caller, disconnected(Id, Cause))
    ;   ignore(user:mqtt_hook_on_disconnect(Id, [cause(Cause)]))
    ).

dispatch(message(Id, Topic, Payload, QoS, Retain)) :-
    ignore(user:mqtt_hook_on_message(Id,
        [topic(Topic), payload(Payload), qos(QoS), retain(Retain)])).

dispatch(published(Id, Token)) :-
    ignore(user:mqtt_hook_on_publish(Id, [token(Token)])).

dispatch(subscribed(Id, Token)) :-
    ignore(user:mqtt_hook_on_subscribe(Id, [token(Token)])).

dispatch(unsubscribed(Id, Token)) :-
    ignore(user:mqtt_hook_on_unsubscribe(Id, [token(Token)])).

dispatch(error(Id, Desc)) :-
    (   retract(waiting(Id, Caller))
    ->  thread_send_message(Caller, error(Id, Desc))
    ;   ignore(user:mqtt_hook_on_error(Id, [description(Desc)]))
    ).

dispatch(_).  % ignore unknown events

send_tab(Stream, Parts) :-
    with_mutex(mqtt_write, (
        atomic_list_concat(Parts, '\t', Line),
        format(Stream, "~w\n", [Line]),
        flush_output(Stream)
    )).

send_connect(Id, Host, Port, ClientId, Keepalive) :-
    bridge_stream(Stream),
    send_tab(Stream, [connect, Id, Host, Port, ClientId, Keepalive]).

send_publish(Id, Topic, Payload, QoS, Retain) :-
    bridge_stream(Stream),
    send_tab(Stream, [publish, Id, Topic, Payload, QoS, Retain]).

send_subscribe(Id, Topic, QoS) :-
    bridge_stream(Stream),
    send_tab(Stream, [subscribe, Id, Topic, QoS]).

send_unsubscribe(Id, Topic) :-
    bridge_stream(Stream),
    send_tab(Stream, [unsubscribe, Id, Topic]).

send_disconnect(Id) :-
    bridge_stream(Stream),
    send_tab(Stream, [disconnect, Id]).

alloc_id(Id) :-
    retract(next_id(Id)),
    Next is Id + 1,
    assertz(next_id(Next)).

%! mqtt_connect(+Host, -Conn) is semidet.
%  connect to an MQTT broker at Host on port 1883.
%  blocks until the broker acknowledges the connection (up to 15 s).
mqtt_connect(Host, Conn) :-
    mqtt_connect(Host, 1883, Conn).

%! mqtt_connect(+Host, +Port, -Conn) is semidet.
%  connect to an MQTT broker at Host:Port.
mqtt_connect(Host, Port, Conn) :-
    mqtt_connect(Host, Port, [], Conn).

%! mqtt_connect(+Host, +Port, +Options, -Conn) is semidet.
%  connect to an MQTT broker with options.
%
%  Options:
%    client_id(+Atom)   MQTT client identifier  (default: mqtt_client)
%    keepalive(+Int)    keepalive interval in s  (default: 60)
%
%  Conn is an opaque integer identifying this connection for subsequent calls.
mqtt_connect(Host, Port, Options, Conn) :-
    bridge_ensure,
    alloc_id(Id),
    Conn = Id,
    (option(client_id(CId), Options) -> true ; CId = 'mqtt_client'),
    (option(keepalive(Ka),  Options) -> true ; Ka  = 60),
    thread_self(Me),
    assertz(waiting(Id, Me)),
    send_connect(Id, Host, Port, CId, Ka),
    (   thread_get_message(Me, Reply, [timeout(15)])
    ->  (   Reply = connected(Id, 0) -> true
        ;   retractall(waiting(Id, _)), fail
        )
    ;   retractall(waiting(Id, _)), fail
    ).

%! mqtt_disconnect(+Conn) is det.
%  gracefully disconnect from the broker.
%  blocks until the broker acknowledges the disconnect (up to 10 s).
mqtt_disconnect(Conn) :-
    thread_self(Me),
    assertz(waiting(disconnect(Conn), Me)),
    send_disconnect(Conn),
    (   thread_get_message(Me, _, [timeout(10)])
    ->  true
    ;   retractall(waiting(disconnect(Conn), _))
    ).

%! mqtt_pub(+Conn, +Topic, +Payload) is det.
%  publish Payload to Topic with QoS 0, no retain.
mqtt_pub(Conn, Topic, Payload) :-
    mqtt_pub(Conn, Topic, Payload, []).

%! mqtt_pub(+Conn, +Topic, +Payload, +Options) is det.
%  publish Payload to Topic.
%
%  Options:
%    qos(+Int)       QoS level 0, 1, or 2  (default: 0)
%    retain(+Bool)   set the retain flag   (default: false)
mqtt_pub(Conn, Topic, Payload, Options) :-
    (option(qos(QoS),     Options) -> true ; QoS = 0),
    (option(retain(true), Options) -> Retain = 1 ; Retain = 0),
    send_publish(Conn, Topic, Payload, QoS, Retain).

%! mqtt_sub(+Conn, +Topic) is det.
%  subscribe to Topic with QoS 0.
%  incoming messages are delivered via mqtt_hook_on_message/2.
mqtt_sub(Conn, Topic) :-
    mqtt_sub(Conn, Topic, []).

%! mqtt_sub(+Conn, +Topic, +Options) is det.
%  subscribe to Topic.
%
%  Options:
%    qos(+Int)   QoS level 0, 1, or 2  (default: 0)
mqtt_sub(Conn, Topic, Options) :-
    (option(qos(QoS), Options) -> true ; QoS = 0),
    send_subscribe(Conn, Topic, QoS).

%! mqtt_unsub(+Conn, +Topic) is det.
%  unsubscribe from Topic.
mqtt_unsub(Conn, Topic) :-
    send_unsubscribe(Conn, Topic).

option(Opt, Options) :-
    functor(Opt, Name, 1),
    functor(Pat, Name, 1),
    member(Pat, Options),
    Pat = Opt.
