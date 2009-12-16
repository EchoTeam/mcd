%%% 
%%% Copyright (c) 2008 JackNyfe, Inc. <info@jacknyfe.com>
%%% All rights reserved.
%%%
%%% XXX: License?
%%%
%%% An implementation of consistent hashing ring for distributed hash tables as
%%% a gen_server.
%%%
%%% Here is an informal explanation of the technique:
%%%   http://www.spiteful.com/2008/03/17/programmers-toolbox-part-3-consistent-hashing/
%%%
%%% Exports:
%%%
%%% start_link(Peers) -> ServerRef
%%%   Types:
%%%     Peers = [{Node, Opaque, Weight}]
%%%     Node = term()
%%%     Opaque = term()
%%%     Weight = int(), >= 1
%%%
%%%   Create a ring according to peer configuration Peers. Opaque is an opaque
%%%   data associated with the node. Greater weight makes the node own bigger
%%%   portion of the ring.
%%%
%%%   The ring creation time is N^2 where N is the sum of the nodes' weights.
%%%
%%%
%%% add(ServerRef, Peers) -> ok | {error, already_there, [Node]}
%%%
%%%   Add Peers to the ring. If any of the nodes being added is already in the
%%%   ring (the check is done by comparing node *names*), error is returned and
%%%   the ring remains intact.
%%%
%%%
%%% add(ServerRef, {Node, Opaque, Weight})
%%%
%%%   Equivalent to add(ServerRef, [{Node, Opaque, Weight}]).
%%% 
%%%
%%% delete(ServerRef, [Node]) -> ok | {error, unknown_nodes, [Node]}
%%%
%%%   Removes nodes from the ring by their names. If any of the names doesn't
%%%   reference an existing node in the ring, an error is returned and the ring
%%%   remains intact.
%%%
%%%
%%% delete(ServerRef, Node)
%%%
%%%   Equivalent to delete(ServerRef, [Node]).
%%%
%%%
%%% get_config(ServerRef) -> Peers
%%%   
%%%   Returns the current configuration of the ring. The order of nodes within
%%%   the returned list is not specified.
%%%
%%%
%%% lookup(ServerRef, Key) -> [{Node, Opaque}]
%%%
%%%   Types:
%%%     Key = term()
%%%
%%%   Look up the Ring for a list of nodes corresponding to Key. The returned
%%%   list is guaranteed to contain no duplicates.
%%%
%%%   The lookup time is log(N), where N is the sum of the nodes' weights.
%%%
%%%
%%% nodes(ServerRef) -> [{Node, Opaque}]
%%%
%%%   Return the list of nodes in the ring. The order of nodes in the returned
%%%   list is not specified, but it is guaranteed that the list contains no
%%%   duplicates.
%%%
%%%   This operation execution time is constant.
%%%

-module(dht_ring).
-behaviour(gen_server).

-export([
  % public API
  add/2,
  delete/2,
  get_config/1,
  lookup/2,
  nodes/1,
  start_link/1,

  % gen_server callbacks
  code_change/3,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  init/1,
  terminate/2
]).

-define(DEBUG(X, Y), io:format("DEBUG: " ++ X, Y)).

-record(state, { ring, nodes }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

add(Ring, {_Node, _Opaque, _Weight} = Peer) ->
  gen_server:call(Ring, {add, [Peer]});

add(Ring, Nodes) ->
  gen_server:call(Ring, {add, Nodes}).

%%%

get_config(Ring) ->
  gen_server:call(Ring, {get_config}).

%%%

delete(Ring, Node) when not is_list(Node) ->
  gen_server:call(Ring, {delete, [Node]});

delete(Ring, Nodes) ->
  gen_server:call(Ring, {delete, Nodes}).

%%% 

lookup(Ring, Key) ->
  gen_server:call(Ring, {lookup, index(Key)}).

%%%

nodes(Ring) ->
  gen_server:call(Ring, {nodes}).

%%%

start_link(Peers) ->
  gen_server:start_link(?MODULE, Peers, []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%

handle_call({add, Nodes}, _From, #state{ nodes = OldNodes } = State) ->
  case [ N || {N, _, _} <- Nodes, lists:keymember(N, 1, OldNodes) ] of
    [] ->
      {ok, NewState} = init(OldNodes ++ Nodes),
      {reply, ok, NewState};

    Overlaps ->
      {reply, {error, already_there, Overlaps}, State}
  end;


handle_call({delete, Nodes}, _From, #state{ nodes = OldNodes } = State) ->
  case [ N || N <- Nodes, not lists:keymember(N, 1, OldNodes) ] of
    [] -> 
      {ok, NewState} = init(
        [Node || {N, _, _} = Node <- OldNodes, not lists:member(N, Nodes)]
      ),
      {reply, ok, NewState};

    NotThere -> {reply, {error, unknown_nodes, NotThere}, State}
  end;


handle_call({get_config}, _From, #state{ nodes = Nodes } = State) ->
  {reply, Nodes, State};


handle_call({lookup, KeyIndex}, _From, #state{ ring = Ring } = State) ->
  true = (KeyIndex >= 0) andalso (KeyIndex < 65536),
  case bsearch(Ring, KeyIndex) of
    empty -> {reply, [], State};
    PartIdx ->
      {_Hash, NodeList} = array:get(PartIdx, Ring),
      {reply, NodeList, State}
  end;


handle_call({nodes}, _From, #state{ nodes = Nodes } = State) ->
  {reply, [{Name, Opaque} || {Name, Opaque, _} <- Nodes], State};

  
handle_call(_Request, _From, State) ->
  {noreply, State}.

%%%

handle_cast(_Request, State) ->
  {noreply, State}.


handle_info(_Request, State) ->
  {noreply, State}.


init(Peers) ->
  RawRing = lists:keysort(1,
    [{H, {Node, Opaque}} || {Node, Opaque, Weight} <- Peers,
      N <- lists:seq(1, Weight),
      H <- [index([Node, integer_to_list(N)])]
    ]
  ),

  Length = length(RawRing),

  Ring = array:from_list([
    begin
      {L1, [{Hash, _} | _] = L2} = lists:split(X, RawRing),
      {Hash, remove_dups(L2 ++ L1)}
    end
    || X <- if Length == 0 -> []; true -> lists:seq(0, Length - 1) end
  ]),

  ?DEBUG("created a ring with ~b points in it~n", [array:sparse_size(Ring)]),
  {ok, #state{ ring = Ring, nodes = Peers }}.


terminate(_Reason, _State) ->
  ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

index(Key) ->
  <<A,B,_/bytes>> = erlang:md5(term_to_binary(Key)),
  A bsl 8 + B.


remove_dups([]) -> [];

remove_dups([{_Hash, {Node, Opaque}} | T]) ->
  [
    {Node, Opaque} | 
    remove_dups(lists:filter(fun({_, {N, _}}) -> N /= Node end, T))
  ].


%%%

% We rely on the fact that the array is kept intact after creation, e.g. no
% undefined entries exist in the middle.
bsearch(Arr, K) ->
  Size =  array:sparse_size(Arr),
  if Size == 0 -> empty; true -> bsearch(Arr, Size, 0, Size - 1, K) end.

bsearch(Arr, Size, LIdx, RIdx, K) ->
  MIdx = LIdx + (RIdx - LIdx + 1) div 2,

  true = (MIdx >= LIdx) andalso (MIdx =< RIdx),

  case key_fits(Arr, Size, MIdx - 1, MIdx, K) of
    {yes, Idx} -> Idx;
    {no, lt} -> bsearch(Arr, Size, LIdx, MIdx, K);
    {no, gt} ->
      if 
        MIdx == (Size - 1) -> Size - 1;
        true -> bsearch(Arr, Size, MIdx, RIdx, K)
      end
  end.

%%%

key_fits(_Arr, 1, -1, 0, _K) ->
  {yes, 0};

key_fits(Arr, Size, -1, 0, K) ->
  {Hash0, _} = array:get(0, Arr),
  {HashS, _} = array:get(Size - 1, Arr),

  true = K < HashS,

  if
    K < Hash0 -> {yes, Size - 1};
    true -> {no, gt}
  end;

key_fits(Arr, Size, L, R, K) ->
  {LHash, _} = array:get(L, Arr),
  {RHash, _} = array:get(R, Arr),

  if
    K < LHash -> if L == 0 -> {yes, Size - 1}; true -> {no, lt} end;
    (K >= LHash) andalso (K < RHash) -> {yes, L};
    K >= RHash -> {no, gt}
  end.
