#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
import requests
from pathlib import Path
from common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index = sys.argv[1];

# LOADING THE CONFIGS CUSTOM IF AVAILABLE OTHERWISE THE DEFAULT CONFIGS FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

# MORE PARAMETERS FOR THE BULK UPDATING ELASTICSEARCH PROCESS
_chunk_size       = _configs['chunk_size'];
_max_scroll_tries = _configs['max_scroll_tries'];
_scroll_size      = _configs['scroll_size'];
_request_timeout  = _configs['requestimeout'];

# THRESHOLDS FOR WHETHER A RETRIEVED WEBSITE IS CONSIDERED A MATCH OR NOT
_great_score  = _configs['great_score']; #TODO: Adjust
_ok_score     = _configs['ok_score']; #TODO: Adjust
_max_rel_diff = _configs['max_rel_diff']; #TODO: Adjust

# IF THE WEBSEARCH HAS ALREADY BEEN PEFORMED FOR THE CURRENT DOC, WHETHER TO REDO IT
_recheck = _configs['recheck'];


#=KEY PARAMETERS FOR THE API=========================================================
_api_address = _configs['api_address'];#"https://api.bing.microsoft.com/v7.0/search"; #"https://api.bing.microsoft.com/v7.0/custom/search";
_api_key     = _configs['api_key'];#"6558d5b67a784007b0e97938952b5e49"#"241948e1f88744068c3e7df046577981";#"8fd4bec3208a48319838efff6d3e08c0";
_api_tps     = _configs['api_tps'];#3#100;#150
#====================================================================================

# FIELD TO WRITE THE MATCHED WEBSITES URLS TO
_to_field = 'bing_urls';
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

# CLIENT OF THE DOCUMENT INDEX WHERE TO UPDATE THE REFERENCES WITH THE MATCHED WEBSITES
_client = ES(['http://localhost:9200'],timeout=60);

# BULK UPDATING THE DOCUMENT INDEX WITH THE MATCHED WEBSITES
i = 0;
for success, info in bulk(_client,search(_to_field,_index,_api_address,_api_key,_api_tps,_great_score,_ok_score,_max_rel_diff,_recheck),chunk_size=_chunk_size, request_timeout=_request_timeout):
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
