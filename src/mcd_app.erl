%%% 
%%% Copyright (c) 2007-2014 JackNyfe, Inc. <info@jacknyfe.com>
%%% All rights reserved.
%%%
-module(mcd_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    mcd_cluster_sup:start_link().

stop(_State) ->
    ok.
