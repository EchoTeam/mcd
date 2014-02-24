%%% vim: set ts=4 sts=4 sw=4 expandtab:
-module(mcd_parallel).
-export([get/2, get/3]).
-export_type([
    error/0
]).

%%
%% The premise is the following. We want to get:
%% 1. Something from cache A.
%% 2. Something from cache B.
%% 3. Something from cache C.
%% And are satisfied with the following:
%% * Got something from A and B.
%% * Got something from C, while either A or B are not resolved.
%% Then we need to specify keys for A, B, C, and a satisfaction function.
%%
%% Example:
%%
%% get([{mbviews1, a}, {mbviews2, b}],
%%              fun
%%                (waiting, {ok, B}) -> {result, B};
%%                ({ok, A}, waiting) -> continue
%%              end)
%%
%% TODO: spawn separate process to avoid of skipping unexcepted mails
%%

-type server() :: mcd:server().
-type get_result() :: mcd:get_result().

-type satisfier_result() :: term().
-type satisfier_return() :: 'continue' | {'result', satisfier_result()}.
-type satisfier() :: fun().
% TODO: when dialyzer will fix this bug, uncomment this
%-type intermediate_value() :: get_result() | 'waiting'.
%-type satisfier() :: fun((intermediate_value(), ...) -> satisfier_return()).
-type wants() :: [{server(), term()}, ...].
-type error() :: {'parallel', 'timeout'}.

-spec get(Wants :: wants(), Satisfier :: satisfier(), Timeout :: timeout()) -> {'error', error()} | satisfier_result().
get(Wants, Satisfier) -> get(Wants, Satisfier, 5000).
get(Wants, Satisfier, Timeout) ->
    ProcessRef = make_ref(),
    wait(ProcessRef, now(), Timeout, [begin
        ItemRef = make_ref(),
        try
            ServerRef ! { '$gen_call', {self(), {ProcessRef, ItemRef}}, {get, Key} }
        catch
            error:badarg ->
                self() ! {{ProcessRef, ItemRef}, {error, noproc}}
        end,
        {ItemRef, waiting}
    end || {ServerRef, Key} <- Wants], Satisfier).

wait(ProcessRef, Started, Timeout, Waiting, Satisfier) ->
    WaitMore = case Timeout - (timer:now_diff(now(), Started) div 1000) of
        Time when Time > 0 ->
            Time;
        _ ->
            0
    end,
    receive
        {{ProcessRef, ItemRef}, Reply} ->
            case wait_check(ItemRef, Reply, Waiting, Satisfier) of
                {continue, NewWaiting} ->
                        wait(ProcessRef, Started, Timeout, NewWaiting, Satisfier);
                {result, Result} ->
                        Result
            end
    after WaitMore ->
        {error, {parallel, timeout}}
    end.

wait_check(ItemRef, Reply, OldWaiting, Satisfier) ->
    NewWaiting = lists:keyreplace(ItemRef, 1, OldWaiting, {ItemRef, Reply}),
    Args = [V || {_ItemRef, V} <- NewWaiting],
    try apply(Satisfier, Args) of
        continue -> {continue, NewWaiting};
        {result, _} = Result -> Result
    catch
        error:function_clause ->
            {continue, NewWaiting}
    end.
