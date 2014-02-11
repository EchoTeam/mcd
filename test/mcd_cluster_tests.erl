-module(mcd_cluster_tests).

-include_lib("eunit/include/eunit.hrl").

-define(NAME, mb_test).
-define(setup(F), {setup, fun setup/0, fun cleanup/1, F}).

% XXX: turn off 'INFO' output
% XXX: mock memcached
% XXX: add check that adding nodes preserves their order, e.g. if we add
%      a, c and then b we will get back [a, c, b], not [a, b, c]

%%% Test helpers %%%

mcd_node(NodeId) ->
    {NodeId, ["localhost", 2222], 10}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_XXX_test_() -> 
  [
    {
      foreach,

      % setup
      fun() ->
        NodeId = localhost,
        {ok, _Pid} = mcd_cluster:start_link(?NAME, [mcd_node(NodeId)]),
        NodeId
      end,

      % cleanup
      fun(_NodeId) ->
        mcd_cluster:stop(?NAME)
      end,

      % instantiators
      [
        fun(NodeId) ->
          {
            % XXX: this should really be tested outside of the fixture
            "using startup parameters",
            fun() ->
              Nodes = mcd_cluster:nodes(?NAME),
              ?assertMatch([{NodeId, _}], Nodes)
            end
          }
        end
      ]
    }
  ].

all_test_() ->
    [{"Check MCD cluster",
      ?setup(fun() -> [check_node_(),     % using_startup_args_test
                        add_node_(),
                        check_node_2_(),
                        add_node_dup_(),
                        check_node_2_(),
                        del_node_(),
                        check_node_(),
                        add_node_list_(),
                        check_node_3_(),
                        del_node_list_(),
                        check_node_(),
                        del_node_non_exists_(),
                        check_node_()]
                end)}].

setup() ->
    ?assertMatch({ok, _Pid}, mcd_cluster:start_link(?NAME, [{localhost, ["localhost", 2222], 10}])).

cleanup(_) ->
    ?assertEqual({ok, stopped}, mcd_cluster:stop(?NAME)).

check_node_() ->
    ?assertMatch([{localhost, _}], mcd_cluster:nodes(?NAME)).

check_node_2_() ->
    ?assertMatch([{localhost, _}, {localhost2, _}], mcd_cluster:nodes(?NAME)).

check_node_3_() ->
    ?assertMatch([{localhost, _}, {localhost2, _}, {localhost3, _}], mcd_cluster:nodes(?NAME)).

add_node_() ->
    ?assertEqual(ok, mcd_cluster:add(?NAME, {localhost2, ["localhost2", 2222], 10})).

add_node_dup_() ->
    ?assertEqual({error, already_there, [localhost]}, mcd_cluster:add(?NAME, {localhost, ["localhost", 2222], 10})).

add_node_list_() ->
    ?assertEqual(ok, mcd_cluster:add(?NAME, [{localhost2, ["localhost2", 2222], 10},
                                             {localhost3, ["localhost3", 2222], 10}])).

del_node_() ->
    ?assertEqual(ok, mcd_cluster:delete(?NAME, localhost2)).

del_node_list_() ->
    ?assertEqual(ok, mcd_cluster:delete(?NAME, [localhost2, localhost3])).

del_node_non_exists_() ->
    ?assertEqual({error, unknown_nodes, [localhost2]}, mcd_cluster:delete(mb_test, localhost2)).

