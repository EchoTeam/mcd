%%% 
%%% Copyright (c) 2008 JackNyfe, Inc. <info@jacknyfe.com>
%%% All rights reserved.
%%%
%%% An implementation of DHT based on memcached. It is a gen_server process.
%%%
%%% XXX: more on the behaviour?
%%%
%%% Exports:
%%%
%%% start_link(Peers)
%%%
%%%   Start the cluster according to the peers specification Peers, which is
%%%   in the format accepted by dht_ring:start_link/1. The Opaque field of the
%%%   specification tuple has to be a ServerRef (per gen_server(3)) to a
%%%   memcached process tied to the particular node.
%%%

-module(mcd_cluster).
-behaviour(gen_server).

-export([
  start_link/1,
  start_link/2,
  nodes/1,
  health/1,

  % gen_server callbacks
  code_change/3,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  init/1,
  terminate/2,

  % internal
  forwardQueryToMCD/4
]).

-define(DEBUG(X, Y), io:format("DEBUG: " ++ X, Y)).

% How often to probe nodes marked down to see if they came back up.
-define(PROBE_PERIOD, 15000).

-record(state, { ring, down = [] }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Peers) ->
  gen_server:start_link(?MODULE, [anonymous, Peers], []).

start_link(Name, Peers) ->
  gen_server:start_link({local, Name}, ?MODULE, [Name, Peers], []).

nodes(ServerRef) ->
  gen_server:call(ServerRef, get_nodes).

health(ServerRef) ->
  NodesDown = gen_server:call(ServerRef, get_nodes_down),
  [{Name, State, Info} ||
	{Name, Ref} = Node <- ?MODULE:nodes(ServerRef),
	Info <- [gen_server:call(Ref, info)],
	State <- [case lists:member(Node, NodesDown) of
			false -> up;
			true -> down
		end]
  ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%

handle_call({'$constructed_query', _, _} = ReadyQuery, From, #state{ring=Ring}=State) ->
	spawn(?MODULE, forwardQueryToMCD, [self(), From, Ring, ReadyQuery]),
	{noreply, State};

handle_call(get_nodes, _From, #state{ring = Ring} = State) ->
	{reply, dht_ring:nodes(Ring), State};

handle_call(get_nodes_down, _From, #state{down = Down} = State) ->
	{reply, Down, State};

handle_call(_Request, _From, State) ->
  {noreply, State}.

%%%

handle_cast({down, NewDown}, #state{ down = CurDown } = State) ->
  {
    noreply,
    State#state{
      down = lists:ukeymerge(1, CurDown, lists:keysort(1, NewDown))
    }
  };

handle_cast({'$constructed_query', _, _} = ReadyQuery, #state{ring=Ring}=State) ->
	spawn(?MODULE, forwardQueryToMCD, [self(), anon, Ring, ReadyQuery]),
	{noreply, State};

handle_cast(_Request, State) ->
  {noreply, State}.

%%%

handle_info({probe_nodes}, #state{ down = [] } = State) ->
  {noreply, State};

handle_info({probe_nodes}, #state{ down = NodesDown } = State) ->
  Server = self(),

  spawn_link(
    fun() ->
      [Server ! {up, Name}
        || {Name, _} = Node <- NodesDown,
        FlushResult <- [flush_node(Node)],
        FlushResult /= {error, noconn}
      ]
    end
  ),

  {noreply, State};

handle_info({up, Name}, #state{ down = NodesDown } = State) ->

  case lists:keytake(Name, 1, NodesDown) of
    {value, _RemovedNode, StillDown} ->
      error_logger:info_msg("memcached node ~p seems to be back up~n", [Name]),
      {noreply, State#state{ down = StillDown }};

    false ->
      {noreply, State}
  end;
  
handle_info(_Request, State) ->
  {noreply, State}.

%%%

init([ClusterName, Peers]) ->
  timer:send_interval(?PROBE_PERIOD, {probe_nodes}),
  {ok, Ring} = dht_ring:start_link(Peers),

  % The peer can be specified by its registered name or its pid.
  % in case of pid, we are risking to be left without any underlying
  % memcached gen_servers if they die, without notice.
  % That is why we link them here to restart the whole shebang
  % if any underlying gen_server dies.
  [error_logger:info_msg("Linked memcached ~p (~p) to cluster '~p'~n",
		[Name, NodeRef, ClusterName])
	|| {Name, NodeRef} <- dht_ring:nodes(Ring),
		is_pid(NodeRef), link(NodeRef) == true],

  {ok, #state{ ring = Ring }}.


terminate(_Reason, _State) ->
  ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

call_node({Name, ServerRef}, Command) ->
  Reply = gen_server:call(ServerRef, Command),
  case connectivity_failure(Reply) of
    {true, Reason} ->
      error_logger:error_msg(
        "Couldn't reach memcached at ~p (~p), considering it down.~n",
        [Name, Reason]
      ),
      Reply;

    false ->
      Reply
  end.

%%%

call_nodes(Nodes, Command) ->
  call_nodes(Nodes, Command, [], not_failover).

call_nodes([], _, NodesDown, _) -> {{error, all_nodes_down}, NodesDown};

call_nodes(Nodes, Command, NodesDown, failover) ->
  [{Name, _ServerRef} = Node | OtherNodes] = Nodes,
  error_logger:info_msg("Failing over to node ~p~n", [Name]),

  case flush_node(Node) of
    {error, noconn} ->
      call_nodes(OtherNodes, Command, [Node | NodesDown], failover);

    {error, Reason} = Reply ->
      error_logger:error_msg(
        "Failed to flush node ~p, considering it's up but returning error "
        "(~p)~n",
        [Name, Reason]
      ),
      Reply;

    _ ->
      call_nodes(Nodes, Command, NodesDown, not_failover)
  end;

call_nodes([Node | OtherNodes], Command, NodesDown, not_failover) ->
  Reply = call_node(Node, Command),
  case Reply of
    {error, noconn} ->
      call_nodes(OtherNodes, Command, [Node | NodesDown], failover);
    _ ->
      {Reply, NodesDown}
  end.

%%%

flush_node({Name, _ServerRef} = Node) ->
  error_logger:info_msg("Flushing memcached node ~p~n", [Name]),
  call_node(Node, {flush_all}).

%%%

connectivity_failure({error, noconn}) -> {true, noconn};
connectivity_failure(_) -> false.

%%%

forwardQueryToMCD(ClusterRef, From, Ring, {'$constructed_query', MD5Key, _} = Q) ->
	NodeList = case MD5Key of
		<<>> -> dht_ring:nodes(Ring);
		Bin -> dht_ring:lookup(Ring, Bin)
	end,
	{Reply, NodesDown} = call_nodes(NodeList, Q),
	case NodesDown of
		[] -> ok;
		_ -> gen_server:cast(ClusterRef, {down, NodesDown})
	end,
	case From of
		anon -> ok;
		_ -> gen_server:reply(From, Reply)
	end.

