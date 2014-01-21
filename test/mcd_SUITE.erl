%%% vim: set ts=4 sts=4 sw=4 et:
-module(mcd_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    test_local/1,
    test_do/1,
    test_api/1,
    test_common_errors/1
]).

-define(BUCKET, test).
-define(KEY, list_to_binary(?MODULE_STRING ++ "_" ++ "key")).
-define(VALUE, <<"value">>).
-define(TTL, 1).

all() -> [
    test_local,
    test_do,
    test_api,
    test_common_errors
].

init_per_suite(Config) ->
    ok = application:start(dht_ring),

    ok = application:set_env(mcd, mcd_hosts, [{localhost, ["localhost"]}]),
    ok = application:set_env(mcd, mcd_buckets, [
        {mcd:lserver(), {localhost, 11211}},
        {?BUCKET, {localhost, 11211}}
    ]),
    ok = application:start(mcd),
    ok = mcd_test_helper:wait_all(),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(mcd),
    ok = application:stop(dht_ring).

% Tests

test_do(_Config) ->
    {ok, [_ | _]} = mcd:do(?BUCKET, version),
    {ok, flushed} = mcd:do(?BUCKET, flush_all),
    {ok, flushed} = mcd:do(?BUCKET, {flush_all, 10}),
    {error, notfound} = mcd:do(?BUCKET, get, ?KEY),
    {error, notfound} = mcd:do(?BUCKET, delete, ?KEY),
    try
        {ok, ?VALUE} = mcd:do(?BUCKET, set, ?KEY, ?VALUE),
        {error, notstored} = mcd:do(?BUCKET, add, ?KEY, ?VALUE),
        {ok, ?VALUE} = mcd:do(?BUCKET, replace, ?KEY, ?VALUE),
        {ok, ?VALUE} = mcd:do(?BUCKET, {set, 0, ?TTL}, ?KEY, ?VALUE),
        {error, notstored} = mcd:do(?BUCKET, {add, 0, ?TTL}, ?KEY, ?VALUE),
        {ok, ?VALUE} = mcd:do(?BUCKET, {replace, 0, ?TTL}, ?KEY, ?VALUE),
        {ok, deleted} = mcd:do(?BUCKET, delete, ?KEY)
    after
        mcd:do(?BUCKET, delete, ?KEY)
    end.

test_api(_Config) ->
    {ok, [_ | _]} = mcd:version(?BUCKET),

    GetFun = fun() -> mcd:get(?BUCKET, ?KEY) end,
    DeleteFun = fun() -> mcd:delete(?BUCKET, ?KEY) end,

    test_set(GetFun, DeleteFun, fun() -> mcd:set(?BUCKET, ?KEY, ?VALUE) end),
    test_set_expiration(GetFun, DeleteFun, fun() -> mcd:set(?BUCKET, ?KEY, ?VALUE, ?TTL) end),
    test_set_expiration(GetFun, DeleteFun, fun() -> mcd:set(?BUCKET, ?KEY, ?VALUE, ?TTL, 0) end),
    test_set(GetFun, DeleteFun, fun() -> {ok, mcd:async_set(?BUCKET, ?KEY, ?VALUE)} end),
    test_set_expiration(GetFun, DeleteFun, fun() -> {ok, mcd:async_set(?BUCKET, ?KEY, ?VALUE, ?TTL)} end),
    test_set_expiration(GetFun, DeleteFun, fun() -> {ok, mcd:async_set(?BUCKET, ?KEY, ?VALUE, ?TTL, 0)} end).

test_common_errors(_Config) ->
    {_, Pid} = hd(mcd_cluster:nodes(?BUCKET)),

    {error, timeout} = mcd:version(self()),
    {error, noproc} = mcd:version(undefined),

    {ok, [_ | _]} = mcd:version(Pid),
    {error, not_broken} = mcd:fix_connection(Pid),
    ok = mcd:break_connection(Pid),
    try
        {error, noconn} = mcd:version(Pid)
    after
        ok = mcd:fix_connection(Pid),
        ok = mcd_test_helper:wait_connection(Pid)
    end,

    {ok, [_ | _]} = mcd:version(Pid),
    {error, not_overloaded} = mcd:unload_connection(Pid),
    ok = mcd:overload_connection(Pid),
    try
        {error, overload} = mcd:version(Pid)
    after
        ok = mcd:unload_connection(Pid)
    end,

    {ok, [_ | _]} = mcd:version(Pid).

test_local(_Config) ->
    GetFun = fun() -> mcd:lget(?KEY) end,
    DeleteFun = fun() -> mcd:ldelete(?KEY) end,

    test_set(GetFun, DeleteFun, fun() -> mcd:lset(?KEY, ?VALUE) end),
    test_set_expiration(GetFun, DeleteFun, fun() -> mcd:lset(?KEY, ?VALUE, ?TTL) end),

    {error, notfound} = mcd:lget(?KEY),
    try
        {ok, ?VALUE} = mcd:lset(?KEY, ?VALUE),
        {ok, flushed} = mcd:lflush_all(),
        {error, notfound} = mcd:lget(?KEY)
    after
        mcd:ldelete(?KEY)
    end.


% private functions

test_set(GetFun, DeleteFun, SetFun) ->
    {error, notfound} = GetFun(),
    try
        {ok, ?VALUE} = SetFun(),
        {ok, ?VALUE} = GetFun(),
        {ok, deleted} = DeleteFun(),
        {error, notfound} = GetFun()
    after
        DeleteFun()
    end.

test_set_expiration(GetFun, DeleteFun, SetFun) ->
    {error, notfound} = GetFun(),
    try
        {ok, ?VALUE} = SetFun(),
        {ok, ?VALUE} = GetFun(),
        timer:sleep((?TTL + 1) * 1000),
        {error, notfound} = GetFun()
    after
        DeleteFun()
    end.
