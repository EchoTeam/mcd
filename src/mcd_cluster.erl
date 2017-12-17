%%%
%%% An implementation of DHT based on memcached. It is a gen_server process.
%%%
%%% XXX: more on the behaviour?
%%%

-module(mcd_cluster).
-behaviour(gen_server).

-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    terminate/2
]).

-export([
    start_link/1,
    start_link/2,
    start_link/3,
    nodes/1,
    health/1,
    add/2,
    delete/2,
    stop/1,
    forwardQueryToMCD/5
]).

-record(state,
        {
         mod,
         dispatcher,
         down = []
        }).

-type node_params() :: [{atom(), mcd:start_params(), pos_integer()}, ...].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link(Nodes :: node_params()) -> {'ok', pid()}.
start_link(Nodes) ->
    gen_server:start_link(?MODULE, [anonymous, Nodes, dht_ring], []).

-spec start_link(Name :: atom(), Nodes :: node_params()) -> {'ok', pid()}.
start_link(Name, Nodes) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, Nodes, dht_ring], []).

-spec start_link(Name :: atom(), Nodes :: node_params(), Dispatcher :: module()) -> {'ok', pid()}.
start_link(Name, Nodes, Dispatcher) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, Nodes, Dispatcher], []).

nodes(ServerRef) ->
    gen_server:call(ServerRef, get_nodes).

health(ServerRef) ->
    PidsDown = gen_server:call(ServerRef, get_pids_down),
    [{Name, State, Info} ||
        {Name, Ref} <- ?MODULE:nodes(ServerRef),
        Info <- [gen_server:call(Ref, {version})],
        State <- [case lists:member(Ref, PidsDown) of
            false -> up;
            true -> down
        end]
    ].

add(ServerRef, {_, _, _} = Node) ->
    gen_server:call(ServerRef, {add, [Node]});

add(ServerRef, Nodes) when is_list(Nodes) ->
    gen_server:call(ServerRef, {add, Nodes}).

delete(ServerRef, Node) when not is_list(Node) ->
    gen_server:call(ServerRef, {delete, [Node]});

delete(ServerRef, Nodes) ->
    gen_server:call(ServerRef, {delete, Nodes}).

stop(ServerRef) ->
    gen_server:call(ServerRef, stop).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%

handle_call({'$constructed_query', _, _} = ReadyQuery, From,
            #state{dispatcher = Dispatcher, mod = Mod, down = PidsDown}=State) ->
    spawn(?MODULE, forwardQueryToMCD, [From, Mod, Dispatcher, mk_down_filter(PidsDown), ReadyQuery]),
    {noreply, State};
handle_call({get, _} = ReadyQuery, From,
            #state{dispatcher = Dispatcher, mod = Mod, down = PidsDown}=State) ->
    spawn(?MODULE, forwardQueryToMCD, [From, Mod, Dispatcher, mk_down_filter(PidsDown), ReadyQuery]),
    {noreply, State};

handle_call(get_nodes, _From, #state{mod = Mod, dispatcher = Dispatcher} = State) ->
    {reply, Mod:nodes(Dispatcher), State};

handle_call(get_pids_down, _From, #state{down = Down} = State) ->
    {reply, Down, State};

handle_call({lookup_nodes, Key}, _From, #state{mod = Mod, dispatcher = Dispatcher} = State) ->
    {reply, Mod:lookup(Dispatcher, Key), State};

handle_call(get_internal_state, _From, State) ->
    {reply, State, State};

handle_call({add, Nodes}, _From, #state{mod = Mod, dispatcher = Dispatcher} = State) ->
    Result = case Mod:add(Dispatcher, Nodes) of
        ok ->
            ok;
        {error, already_there, _} = Overlaps ->
            Overlaps;
        Other ->
            {error, Other}
    end,
    {reply, Result, State};

handle_call({delete, Nodes}, _From, #state{mod = Mod, dispatcher = Dispatcher} = State) ->
    Result = case Mod:delete(Dispatcher, Nodes) of
        ok ->
            ok;
        {error, unknown_nodes, _} = NotThere ->
            NotThere;
        Other ->
            {error, Other}
    end,
    {reply, Result, State};

handle_call(stop, _From, State) ->
    {stop, normal, {ok, stopped}, State};

handle_call(_Request, _From, State) ->
    {noreply, State}.

%%%

handle_cast({'$constructed_query', _, _} = ReadyQuery,
            #state{mod = Mod, dispatcher = Dispatcher, down=PidsDown}=State) ->
    spawn(?MODULE, forwardQueryToMCD, [anon, Mod, Dispatcher, mk_down_filter(PidsDown), ReadyQuery]),
    {noreply, State};

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({memcached, Pid, state, up}, #state{down = NodesDown} = State) ->
    lager:info("memcached node(~p) seems to be back up~n", [Pid]),
    {noreply, State#state{down = lists:delete(Pid, NodesDown)}};

handle_info({memcached, Pid, state, down}, #state{down = NodesDown} = State) ->
    lager:info("memcached node(~p) is disconnected~n", [Pid]),
    {noreply, State#state{down = lists:usort([Pid|NodesDown])}};

handle_info(_Request, State) ->
    {noreply, State}.

%%%

init([ClusterName, Nodes, DispatcherMod]) ->
    lager:info("starting mcd_cluster(~p): ~p ~p ~p", [self(), ClusterName, Nodes, DispatcherMod]),
    Peers = lists:map(fun ({Name, Addr, Weight}) ->
                {ok, Pid} = mcd:start_link(Addr),
                mcd:monitor(Pid, self(), [state]),
                {Name, Pid, Weight}
            end, Nodes),
    {ok, Dispatcher} = DispatcherMod:start_link(Peers),
    {ok, #state{ dispatcher = Dispatcher, mod = DispatcherMod }}.

terminate(_Reason, _State) ->
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

log_problems(Node, Command, Start, Res) ->
    Now = erlang:monotonic_time(),
    Diff = erlang:convert_time_unit(Now - Start, native, milli_seconds),
    ShouldLog = case {Diff, Res} of
        {T,_}  when T > 1000 -> true;
        {_, {exception, _}} -> true;
        _ -> false
    end,
    case ShouldLog of
        true ->
            MD5 = case Command of
                {'$constructed_query', M, _} -> M;
                _ -> Command
            end,
            lager:warning([{tag, mcd_long_response}],
                "Request took more than 1000ms or exception caught: ~n"
                "Node:~p~n"
                "MD5:~p~n"
                "Res:~p~n"
                "Time:~pms~n", [Node, MD5, Res, Diff div 1000]);
        _ -> nop
    end.

call_node({_Name, ServerRef}=Node, Command) ->
    Start = erlang:monotonic_time(),
    try gen_server:call(ServerRef, Command) of
        Res ->
            log_problems(Node, Command, Start, Res),
            Res
    catch C:R ->
        log_problems(Node, Command, Start, {exception, {C,R}}),
        erlang:raise(C, R, erlang:get_stacktrace())
    end.

%%%

call_nodes([], _, _) -> {error, nonodes};

call_nodes([Node | OtherNodes], Filter, Command) ->
    case Filter(Node) of
        true  ->
            Reply = call_node(Node, Command),
            case is_connectivity_failure(Reply) of
                true ->
                    call_nodes(OtherNodes, Filter, Command);
                false ->
                    Reply
            end;
        false ->
            call_nodes(OtherNodes, Filter, Command)
    end.

%%%

is_connectivity_failure({error, noconn}) -> true;
is_connectivity_failure(_) -> false.

%%%

forwardQueryToMCD(From, Mod, Dispatcher, Filter, {'$constructed_query', MD5Key, _} = Q) ->
    forwardQueryToMCD(From, Mod, Dispatcher, Filter, MD5Key, Q);
forwardQueryToMCD(From, Mod, Dispatcher, Filter, {get, Key} = Q) ->
    MD5Key = erlang:md5(term_to_binary(Key)),
    forwardQueryToMCD(From, Mod, Dispatcher, Filter, MD5Key, Q).

forwardQueryToMCD(From, Mod, Dispatcher, Filter, MD5Key, Q) ->
    NodeList = case MD5Key of
        <<>> -> Mod:nodes(Dispatcher);
        Bin -> Mod:lookup(Dispatcher, Bin)
    end,
    Reply = call_nodes(NodeList, Filter, Q),
    case From of
        anon -> ok;
        _ ->
            Reply2 =
                case Reply of
                    {error, nonodes} ->
                        lager:error("Node ~p doesn't see others: ~p~n", [node(), NodeList]),
                        {error, all_nodes_down};
                    _ -> Reply
                end,
            gen_server:reply(From, Reply2)
    end.

mk_down_filter(PidsDown) ->
    fun ({_, Pid}) ->
        not lists:member(Pid, PidsDown)
    end.
