%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_plugin_mysql).

-include("emqx_plugin_mysql.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-define(SAVE_MESSAGE_PUBLISH, <<"INSERT INTO mqtt_msg(`mid`, `client_id`, `topic`, `payload`, `time`) VALUE(?, ?, ?, ?, ?);">>).

-export([load_hook/1, unload_hook/0, on_message_publish/2]).


load_hook(Env) ->
  emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).

unload_hook() ->
  emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2).

on_message_publish(#message{from = emqx_sys} = Message, _State) ->
  {ok, Message};
on_message_publish(#message{flags = #{retain := true}} = Message, _State) ->
  #message{id = Id, topic = Topic, payload = Payload, from = From} = Message,
  emqx_cli_mysql:query(?SAVE_MESSAGE_PUBLISH, [emqx_guid:to_hexstr(Id), binary_to_list(From), binary_to_list(Topic), binary_to_list(Payload), timestamp()]),
  {ok, Message};
on_message_publish(Message, _State) ->
  {ok, Message}.

timestamp() ->
  {A,B,_C} = os:timestamp(),
  A*1000000+B.
