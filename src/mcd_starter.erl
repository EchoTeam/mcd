%%% Copyright (c) 2008, 2009 JackNyfe. All rights reserved.
%%% See the accompanying LICENSE file.
%%%
%%% Supervises a set of of memcached instances and mcd_cluster attached to them.
%%% All memcached instances and mcd_cluster are linked to this supervisor,
%%% so a proper exit hierarchy is formed.
%%%
%%% USAGE
%%%
%%% Start from the command line:
%%%
%%% mcd_starter:start_link(mainMemcachedCluster,
%%% 		[ ["remoteHost1", 11211], ["remoteHost2", 11211] ]).
%%%
%%% Start from the supervisor (ChildSpec):
%%%
%%% 	{ mainCluster, { mcd_starter, start_link, [mainCluster, [
%%% 			["remoteHost1", 11211],
%%% 			["remoteHost2", 11211] ] },
%%%		permanent, 60000, worker,
%%%		[mcd_starter, mcd_cluster, dht_ring, mcd]},
%%% 
-module(mcd_starter).
-export([start_link/2]).
-behavior(supervisor).
-export([init/1]).

start_link(ClusterName, PeerAddresses) ->
    case supervisor:start_link(
	{local, list_to_atom(atom_to_list(ClusterName) ++ ".sup")},
	?MODULE, PeerAddresses) of
      {ok, SupRef} ->
	io:format("started mcd_cluster: ~p~n", [SupRef]),
	Peers = [{Name, Pid, Weight} ||
		{{Name, Weight}, Pid, worker, _}
			<- supervisor:which_children(SupRef), is_pid(Pid) ],
	{ok, _} = supervisor:start_child(SupRef, { mcd_cluster,
		{ mcd_cluster, start_link, [ClusterName, Peers] },
		permanent, 60000, worker, [mcd_cluster, dht_ring] }),
	{ok, SupRef};
      Error -> Error
    end.

init(PeerAddresses) ->
    McdChildSpecs = [begin
	{Address, Weight} = case Addr of
		[_Host] -> {Addr, 10};
		[_Host, _Port] -> {Addr, 10};
		[Host, Port, Wght] -> {[Host, Port], Wght}
	end,
	NameTag = list_to_atom(lists:flatten(io_lib:format("~p", [Address]))),
	{ { NameTag, Weight },
	  { mcd, start_link, [Address] },
	  permanent, 10000, worker, [mcd] }
    end || [Host|_] = Addr <- PeerAddresses],
    {ok, { {one_for_all, 10, 10}, McdChildSpecs }}.

