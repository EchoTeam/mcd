%%% vim: set ts=4 sts=4 sw=4 expandtab:
-module(mcd_test_helper).

-export([
    overload_all/0,
    unload_all/0,
    wait_all/0,
    wait_connection/1,
    wait_connection/3
]).

wait_connection(Pid) ->
    wait_connection(Pid, now(), 5000).

wait_connection(Pid, Started, Timeout) ->
    case {mcd:version(Pid), Timeout - (timer:now_diff(now(), Started) div 1000)} of
        {{ok, [_ | _]}, _} ->
            ok;
        {Error, WaitMore} when WaitMore =< 0 ->
            lager:error("Unexpected mcd answer: ~p~n", [Error]),
            {error, timeout};
        {_, _} ->
            timer:sleep(10),
            ?MODULE:wait_connection(Pid, Started, Timeout)
    end.

overload_all() ->
    map_all_connections(fun mcd:overload_connection/1).

unload_all() ->
    map_all_connections(fun mcd:unload_connection/1).

wait_all() ->
    map_all_connections(fun ?MODULE:wait_connection/1).

% private

map_all_connections(Fun) when is_function(Fun, 1) ->
    ConnectionResults = [{Bucket, Pid, Fun(Pid)} || Bucket <- mcd_cluster_sup:buckets(), {_Node, Pid} <- mcd_cluster:nodes(Bucket)],
    case [ConnectionResult || {_, _, Result} = ConnectionResult <- ConnectionResults, Result =/= ok] of
        [] ->
            ok;
        Result ->
            {error, Result}
    end.
