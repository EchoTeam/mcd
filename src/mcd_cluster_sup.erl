%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(mcd_cluster_sup).

-behaviour(supervisor).

-export([init/1]).

-export([
    start_link/0,
    reconfigure/0,
    children_specs/1
]).

% For selftesing
-export([
    buckets/0
]).

-define(WEIGHT, 200).

%% ==========================================================
%% API functions
%% ==========================================================
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, mcd_cluster).

children_specs({?MODULE, start_link, []}) ->
    superman:get_child_specs(?MODULE, mcd_cluster);

children_specs({supervisor, start_link, [_, _, {supervisor, Spec}]}) ->
    superman:get_child_specs(?MODULE, {supervisor, Spec}).

reconfigure() ->
    superman:reconfigure_supervisor_tree_init_args(?MODULE, mcd_cluster).

%% ==========================================================
%% Supervisor callbacks
%% ==========================================================
init(mcd_cluster) ->
    Specs = [cluster_supervisor_spec(C) || C <- clusters()],
    {ok, {{one_for_one, 10, 10}, Specs}};

init({supervisor, Cluster}) ->
    lager:info("Starting membase supervisor for ~p cluster", [Cluster]),
    {ok, {{one_for_one, 10, 10}, [bucket_spec(B) || B <- buckets_by_cluster(Cluster)]}}.

%% ==========================================================
%% Internal functions
%% ==========================================================
mcd_bucket(B) ->
    {ok, Bs} = application:get_env(mcd, mcd_buckets),
    proplists:get_value(B, Bs).

mcd_cluster(C) ->
    {ok, Cs} = application:get_env(mcd, mcd_hosts),
    proplists:get_value(C, Cs).
    
buckets() ->
    {ok, Bs} = application:get_env(mcd, mcd_buckets),
    lists:usort([B || {B, _} <- Bs]).

clusters() ->
    lists:usort([cluster_by_bucket(B) || B <- buckets()]).

cluster_by_bucket(Bucket) ->
    {Cluster, _Port} = mcd_bucket(Bucket),
    Cluster.

buckets_by_cluster(Cluster) ->
    [Bucket || Bucket <- buckets(), Cluster == cluster_by_bucket(Bucket)].

cluster_supervisor_spec(Cluster) ->
    Name = cluster_name(Cluster),
    {Cluster, {supervisor, start_link, [{local, Name}, ?MODULE, {supervisor, Cluster}]},
        permanent, infinity, supervisor, [mcd_cluster_sup]}.

bucket_spec(Bucket) ->
    {Cluster, Port} = mcd_bucket(Bucket),
    Nodes = mcd_cluster(Cluster),
    Peers = [{list_to_atom(Node), [Node, Port], ?WEIGHT} || Node <- Nodes],
    {Bucket, {mcd_cluster, start_link, [Bucket, Peers]}, permanent, 60000, worker, [mcd_cluster]}.

cluster_name(Cluster) ->
    list_to_atom("mcd_cluster_" ++ atom_to_list(Cluster) ++ "_sup").
