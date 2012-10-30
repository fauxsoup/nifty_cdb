
-module(nifty_cdb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, {
            {simple_one_for_one, 5, 10}, 
            [
                {
                    nifty_cdb,
                    {nifty_cdb, start_link, []},
                    transient,
                    5000,
                    worker,
                    [nifty_cdb]
                }
            ]
        }
    }.

