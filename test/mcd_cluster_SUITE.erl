%%% vim: set ts=4 sts=4 sw=4 expandtab:
-module(mcd_cluster_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    test_noconn/1,
    test_all_nodes_down/1
]).

all() -> [
    test_noconn,
    test_all_nodes_down
].

init_per_suite(Config) ->
    ok = application:start(dht_ring),

    ok = application:set_env(mcd, mcd_hosts, [{localhost, ["localhost"]}]),
    ok = application:set_env(mcd, mcd_buckets, [
        {host1, {localhost, 11211}},
        {host2, {localhost, 11211}}
    ]),
    ok = application:start(mcd),
    ok = mcd_test_helper:wait_all(),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(mcd),
    ok = application:stop(dht_ring).

% Tests

test_noconn(_Config) ->
    [begin
        [{NodeName, Pid} | _] = mcd_cluster:nodes(Bucket),
        io:format("Checking bucket: ~p, NodeName: ~p~n", [Bucket, NodeName]),
        false = lists:member(Pid, gen_server:call(Bucket, get_pids_down)),
        io:format("Ok. Node is UP~n"),
        ok = mcd:break_connection(Pid),
        try
            {error, noconn} = mcd:version(Pid),
            timer:sleep(100),
            true = lists:member(Pid, gen_server:call(Bucket, get_pids_down)),
            io:format("Ok. Node is marked as down~n")
        after
            ok = mcd:fix_connection(Pid),
            ok = mcd_test_helper:wait_connection(Pid)
        end,
        {ok, _} = mcd:version(Pid),
        false  = lists:member(Pid, gen_server:call(Bucket, get_pids_down)),
        io:format("Ok. Node is marked as up~n")
    end || Bucket <- mcd_cluster_sup:buckets()].

test_all_nodes_down(_Config) ->
    Bucket = hd(mcd_cluster_sup:buckets()),
    Pids = [Pid || {_, Pid} <- mcd_cluster:nodes(Bucket)],
    [ok = mcd:break_connection(Pid) || Pid <- Pids],
    try
        {error, all_nodes_down} = mcd:version(Bucket)
    after
        [ok = mcd:fix_connection(Pid) || Pid <- Pids],
        [ok = mcd_test_helper:wait_connection(Pid) || Pid <- Pids]
    end.
