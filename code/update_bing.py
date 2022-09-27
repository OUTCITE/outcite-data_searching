#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
import requests
from common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index            = sys.argv[1]; #'geocite' #'outcite_ssoar' #'ssoar_gold'
_chunk_size       = 10;
_max_scroll_tries = 2;
_scroll_size      = 10;
_requestimeout    =  60;

_great_score  = [100,50]; #TODO: Adjust
_ok_score     = [36,18]; #TODO: Adjust
_max_rel_diff = [0.4,0.33]; #TODO: Adjust

_recheck = False;

#====================================================================================
_api_address = "https://api.bing.microsoft.com/v7.0/search"; #"https://api.bing.microsoft.com/v7.0/custom/search";
_api_key     = "241948e1f88744068c3e7df046577981";#"8fd4bec3208a48319838efff6d3e08c0";
_api_tps     = 100;#150
_to_field    = 'bing_urls';
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,search(_to_field,_index,_api_address,_api_key,_api_tps,_great_score,_ok_score,_max_rel_diff,_recheck),chunk_size=_chunk_size, request_timeout=_requestimeout):
    i += 1;
    if not success:
        print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'],'\n');
    print(i,info)
    if i % _chunk_size == 0:
        print(i,'refreshing...');
        _client.indices.refresh(index=_index);
print(i,'refreshing...');
_client.indices.refresh(index=_index);
#-------------------------------------------------------------------------------------------------------------------------------------------------
