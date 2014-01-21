%%% vim: set ts=4 sts=4 sw=4 et:
-module(mcd_parallel_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    test_simple/1,
    test_noproc/1
]).

all() -> [
    test_simple,
    test_noproc
].

init_per_suite(Config) ->
    ok = application:start(dht_ring),

    ok = application:set_env(mcd, mcd_hosts, [{localhost, ["localhost"]}]),
    ok = application:set_env(mcd, mcd_buckets, [
        {host1, {localhost, 11211}},
        {host2, {localhost, 11211}},
        {host3, {localhost, 11211}}
    ]),
    ok = application:start(mcd),
    ok = mcd_test_helper:wait_all(),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(mcd),
    ok = application:stop(dht_ring).

% Tests

test_simple(_Config) ->
    MessagesCount = 1,
    [Bucket1, Bucket2, Bucket3] = mcd_cluster_sup:buckets(),
    [Key1, Value1, Key2, Value2, Key3, Value3] = ["id_" ++ integer_to_list(Id) || Id <- lists:seq(1, 6)],
    Data = [
        {Bucket1, Key1, Value1},
        {Bucket2, Key2, Value2},
        {Bucket3, Key3, Value3}
    ],
    Satisfier = fun
        (A, B, C) when A =/= waiting, B =/= waiting, C =/= waiting ->
            {result, {A, B, C}};
        ({_, _}, waiting, waiting) ->
            continue;
        (waiting, {_, _}, waiting) ->
            continue;
        (waiting, waiting, {_, _}) ->
            continue
    end,

    Ref = make_ref(),
    Messages = lists:seq(1, MessagesCount),
    [self() ! {Ref, Message} || Message <- Messages],
    try
        test_parallel(Data, Satisfier)
    after
        {ok, Messages} = messages_receive(Ref, MessagesCount)
    end.

test_noproc(_Config) ->
    Satisfier = fun(A) ->
        {result, {A}}
    end,
    Wants = [{undefined, undefined}],
    {{error, noproc}} = mcd_parallel:get(Wants, Satisfier).

test_parallel(Data, Satisfier) ->
    Wants = [{Bucket, Key} || {Bucket, Key, _Value} <- Data],
    ErrorResult = list_to_tuple([{error, notfound} || _Item <- Data]),
    {TDelta1, ErrorResult} = timer:tc(mcd_parallel, get, [Wants, Satisfier]),

    [{ok, Value} = mcd:set(Bucket, Key, Value) || {Bucket, Key, Value} <- Data],
    try
        OkResult = list_to_tuple([{ok, Value} || {_Bucket, _Key, Value} <- Data]),
        {TDelta2, OkResult} = timer:tc(mcd_parallel, get, [Wants, Satisfier]),
        {error, {parallel, timeout}} = mcd_parallel:get(Wants, Satisfier, 0),
        {ok, (TDelta1 + TDelta2) / 2.0}
    after
        [{ok, deleted} = mcd:delete(Bucket, Key) || {Bucket, Key, _Value} <- Data]
    end.

% private functions

messages_receive(Ref, Count) ->
    messages_receive(Ref, Count, []).

messages_receive(_Ref, 0, Messages) ->
    {ok, lists:sort(Messages)};
messages_receive(Ref, Count, Messages) ->
    receive
        {Ref, Message} ->
            messages_receive(Ref, Count - 1, [Message | Messages])
    after 1000 ->
        {error, {timeout, [Count]}}
    end.
