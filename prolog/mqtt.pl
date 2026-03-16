:- module(mqtt, [
    connect/2,
    connect/3,
    connect/4,
    disconnect/1,
    pub/3,
    pub/4,
    sub/2,
    sub/3,
    unsub/2,
    bridge_log/1
]).

:- use_module(library(socket)).

%! mqtt:on_connect(+Conn, +Data) is semidet.
%  called when an MQTT connection is established (not waited on by connect/4).
%  Data = [rc(RC)] where RC is 0 on success.
:- multifile on_connect/2.

%! mqtt:on_disconnect(+Conn, +Data) is semidet.
%  called when an MQTT connection drops unexpectedly.
%  Data = [cause(Cause)].
:- multifile on_disconnect/2.

%! mqtt:on_message(+Conn, +Data) is semidet.
%  called for every incoming MQTT PUBLISH.
%  Data = [topic(Topic), payload(Payload), qos(QoS), retain(Retain)].
:- multifile on_message/2.

%! mqtt:on_publish(+Conn, +Data) is semidet.
%  called when the broker confirms delivery of a published message.
%  Data = [token(Token)].
:- multifile on_publish/2.

%! mqtt:on_subscribe(+Conn, +Data) is semidet.
%  called when a subscribe request is acknowledged by the broker.
%  Data = [token(Token)].
:- multifile on_subscribe/2.

%! mqtt:on_unsubscribe(+Conn, +Data) is semidet.
%  called when an unsubscribe request is acknowledged by the broker.
%  Data = [token(Token)].
:- multifile on_unsubscribe/2.

%! mqtt:on_error(+Conn, +Data) is semidet.
%  called on asynchronous errors not tied to a waiting caller.
%  Data = [description(Desc)].
:- multifile on_error/2.

:- dynamic
    bridge_stream/1,   % write side of the bridge TCP socket
    bridge_in/1,       % read side, owned by the event_loop thread
    next_id/1,
    waiting/2.         % waiting(Key, Thread) -- caller blocked in connect/disconnect

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
    ;   ignore(on_connect(Id, [rc(RC)]))
    ).

dispatch(disconnected(Id, Cause)) :-
    (   retract(waiting(disconnect(Id), Caller))
    ->  thread_send_message(Caller, disconnected(Id, Cause))
    ;   ignore(on_disconnect(Id, [cause(Cause)]))
    ).

dispatch(message(Id, Topic, Payload, QoS, Retain)) :-
    ignore(on_message(Id,
        [topic(Topic), payload(Payload), qos(QoS), retain(Retain)])).

dispatch(published(Id, Token)) :-
    ignore(on_publish(Id, [token(Token)])).

dispatch(subscribed(Id, Token)) :-
    ignore(on_subscribe(Id, [token(Token)])).

dispatch(unsubscribed(Id, Token)) :-
    ignore(on_unsubscribe(Id, [token(Token)])).

dispatch(error(Id, Desc)) :-
    (   retract(waiting(Id, Caller))
    ->  thread_send_message(Caller, error(Id, Desc))
    ;   ignore(on_error(Id, [description(Desc)]))
    ).

dispatch(_).  % ignore unknown events

send_tab(Stream, Parts) :-
    with_mutex(mqtt_write, (
        atomic_list_concat(Parts, '\t', Line),
        format(Stream, "~w\n", [Line]),
        flush_output(Stream)
    )).

send_connect(Id, Host, Port, ClientId, Keepalive, TlsMode, CaCert, ClientCert, ClientKey) :-
    bridge_stream(Stream),
    send_tab(Stream, [connect, Id, Host, Port, ClientId, Keepalive,
                      TlsMode, CaCert, ClientCert, ClientKey]).

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

%! connect(+Host, -Conn) is semidet.
%  connect to an MQTT broker at Host on port 1883.
%  blocks until the broker acknowledges the connection (up to 15 s).
connect(Host, Conn) :-
    connect(Host, 1883, Conn).

%! connect(+Host, +Port, -Conn) is semidet.
%  connect to an MQTT broker at Host:Port.
connect(Host, Port, Conn) :-
    connect(Host, Port, [], Conn).

%! connect(+Host, +Port, +Options, -Conn) is semidet.
%  connect to an MQTT broker with options.
%
%  Options:
%    client_id(+Atom)     MQTT client identifier          (default: mqtt_client)
%    keepalive(+Int)      keepalive interval in s          (default: 60)
%    tls(+Bool)           enable TLS                       (default: false)
%    verify(+Bool)        verify server certificate        (default: true when TLS)
%    ca_cert(+Path)       CA certificate file (PEM)        (default: system trust store)
%    client_cert(+Path)   client certificate file (PEM)    (default: none)
%    client_key(+Path)    client private key file (PEM)    (default: none)
%
%  Conn is an opaque integer identifying this connection for subsequent calls.
connect(Host, Port, Options, Conn) :-
    bridge_ensure,
    alloc_id(Id),
    Conn = Id,
    (option(client_id(CId), Options) -> true ; CId = 'mqtt_client'),
    (option(keepalive(Ka),  Options) -> true ; Ka  = 60),
    (option(tls(true),      Options) -> UseTls = true ; UseTls = false),
    (   UseTls = true
    ->  (option(verify(false), Options) -> TlsMode = 2 ; TlsMode = 1)
    ;   TlsMode = 0
    ),
    (option(ca_cert(CaCert),       Options) -> true ; CaCert = ''),
    (option(client_cert(CCert),    Options) -> true ; CCert  = ''),
    (option(client_key(CKey),      Options) -> true ; CKey   = ''),
    thread_self(Me),
    assertz(waiting(Id, Me)),
    send_connect(Id, Host, Port, CId, Ka, TlsMode, CaCert, CCert, CKey),
    (   thread_get_message(Me, Reply, [timeout(15)])
    ->  (   Reply = connected(Id, 0) -> true
        ;   retractall(waiting(Id, _)), fail
        )
    ;   retractall(waiting(Id, _)), fail
    ).

%! disconnect(+Conn) is det.
%  gracefully disconnect from the broker.
%  blocks until the broker acknowledges the disconnect (up to 10 s).
disconnect(Conn) :-
    thread_self(Me),
    assertz(waiting(disconnect(Conn), Me)),
    send_disconnect(Conn),
    (   thread_get_message(Me, _, [timeout(10)])
    ->  true
    ;   retractall(waiting(disconnect(Conn), _))
    ).

%! pub(+Conn, +Topic, +Payload) is det.
%  publish Payload to Topic with QoS 0, no retain.
pub(Conn, Topic, Payload) :-
    pub(Conn, Topic, Payload, []).

%! pub(+Conn, +Topic, +Payload, +Options) is det.
%  publish Payload to Topic.
%
%  Options:
%    qos(+Int)       QoS level 0, 1, or 2  (default: 0)
%    retain(+Bool)   set the retain flag   (default: false)
pub(Conn, Topic, Payload, Options) :-
    (option(qos(QoS),     Options) -> true ; QoS = 0),
    (option(retain(true), Options) -> Retain = 1 ; Retain = 0),
    send_publish(Conn, Topic, Payload, QoS, Retain).

%! sub(+Conn, +Topic) is det.
%  subscribe to Topic with QoS 0.
%  incoming messages are delivered via mqtt:on_message/2.
sub(Conn, Topic) :-
    sub(Conn, Topic, []).

%! sub(+Conn, +Topic, +Options) is det.
%  subscribe to Topic.
%
%  Options:
%    qos(+Int)   QoS level 0, 1, or 2  (default: 0)
sub(Conn, Topic, Options) :-
    (option(qos(QoS), Options) -> true ; QoS = 0),
    send_subscribe(Conn, Topic, QoS).

%! unsub(+Conn, +Topic) is det.
%  unsubscribe from Topic.
unsub(Conn, Topic) :-
    send_unsubscribe(Conn, Topic).

%! bridge_log(+Bool) is det.
%  enable (true) or disable (false) stderr logging in the bridge process.
bridge_log(Bool) :-
    bridge_ensure,
    bridge_stream(Stream),
    (Bool = true -> V = 1 ; V = 0),
    send_tab(Stream, [log, V]).

option(Opt, Options) :-
    functor(Opt, Name, 1),
    functor(Pat, Name, 1),
    member(Pat, Options),
    Pat = Opt.
