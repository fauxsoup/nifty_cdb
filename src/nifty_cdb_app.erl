-module(nifty_cdb_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    nifty_cdb_sup:start_link().

stop(_State) ->
    ok.
