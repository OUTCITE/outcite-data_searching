import numpy as np
from scipy.optimize import linear_sum_assignment as LSA
from difflib import SequenceMatcher as SM
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
import re
import time
import sys

_max_extract_time = 10; #minutes
_max_scroll_tries = 2;
_scroll_size      = 100;

_max_val_len = 512;
#'''
_refobjs = [    'anystyle_references_from_cermine_fulltext',
                'anystyle_references_from_cermine_refstrings',
                'anystyle_references_from_grobid_fulltext',
                'anystyle_references_from_grobid_refstrings',   #                'anystyle_references_from_gold_fulltext',
                'cermine_references_from_cermine_refstrings',          #                'anystyle_references_from_gold_refstrings',
                'cermine_references_from_grobid_refstrings',#,    #                'cermine_references_from_gold_refstrings',
                'grobid_references_from_grobid_xml'
                ];

_ids     = None;#['GaS_2000_0001'];#["gesis-ssoar-29359","gesis-ssoar-55603","gesis-ssoar-37157","gesis-ssoar-5917","gesis-ssoar-21970"];#None
#'''
#_refobjs = [ 'anystyle_references_from_gold_refstrings' ];

YEAR = re.compile(r'1[5-9][0-9]{2}|20(0[0-9]|1[0-9]|2[0-3])'); #1500--2023

def walk_down(pointer,match_keys):
    if len(match_keys)==0:
        yield pointer;
    elif isinstance(pointer,dict):
        if match_keys[0] in pointer:
            for el in walk_down(pointer[match_keys[0]],match_keys[1:]):
                yield el;
    elif isinstance(pointer,list):
        for el_p in pointer:
            for el in walk_down(el_p,match_keys):
                yield el;

def extract(L):
    L_ = L[0] if len(L)>0 else None;
    if isinstance(L_,list):
        return extract(L_);
    return L_;

def merge(d, u):
    for k, v in u.items():
        if v == None:                                   # discard None values
            continue;
        elif (not k in d) or d[k] == None:              # new key or old value was None
            d[k] = v;
        elif isinstance(v,dict) and v != {}:            # non-Counter dicts are merged
            d[k] = merge(d.get(k,{}),v);
        elif isinstance(v,set):                         # set are joined
            d[k] = d[k] | v;
        elif isinstance(v,list):                        # list are concatenated
            d[k] = d[k] + v;
        elif isinstance(v,int) or isinstance(v,float):  # int and float are added
            d[k] = d[k] + v;
        elif v != dict():                               # anything else is replaced
            d[k] = v;
    return d;


def transform(result,transformap):
    matchobj = dict();
    for match_keystr in transformap:
        #--------------------------------------------------------------------------------------------------------------------------------------------------------
        match_keys     = match_keystr.split('.');                                                               # The path in the matchobj
        match_pointers = list(walk_down(result,match_keys));                                                          # The information stored at the end of the path
        if len(match_pointers)==0:                                                          # If the path does not exist
            continue;
        ref_keystr,get_1st,default = transformap[match_keystr];                                                    # The information from the transformation mapping
        match_values               = [extract(match_pointers)] if get_1st and len(match_pointers)>=1 else match_pointers;  # The extracted information #TODO: this did not work
        ref_keys                   = ref_keystr.split('.');                                                        # The path in the refobj
        #--------------------------------------------------------------------------------------------------------------------------------------------------------
        if len(ref_keys) == 1:
            matchobj_ = {ref_keys[0]: match_values};
        else:
            if default == []:
                matchobj_ = {ref_keys[0]: []};
                values    = match_values if isinstance(match_values,list) else [match_values];
                for value in values:
                    matchobj_[ref_keys[0]].append(dict());
                    ref_pointer = matchobj_[ref_keys[0]][-1];
                    for i in range(1,len(ref_keys)):
                        ref_pointer[ref_keys[i]] = dict();
                        if i+1 < len(ref_keys):
                            ref_pointer = ref_pointer[ref_keys[i]];
                        elif value and len(value) > 0:
                            ref_pointer[ref_keys[i]] = value;
            else:
                matchobj_[ref_keys[0]] = dict();
                ref_pointer           = matchobj_[ref_keys[0]];
                for i in range(1,len(ref_keys)):
                    ref_pointer[ref_keys[i]] = dict();
                    if i+1 < len(ref_keys):
                        ref_pointer = ref_pointer[ref_keys[i]];
                    elif match_values and len(match_values) > 0:
                        ref_pointer[ref_keys[i]] = match_values;
        matchobj = merge(matchobj,matchobj_);
        #--------------------------------------------------------------------------------------------------------------------------------------------------------
        keys = list(matchobj.keys());
        for key in keys:
            if not matchobj[key] or len(matchobj[key]) == 0:
                del matchobj[key];
    return matchobj;

def distance(a,b):
    a,b        = a.lower(), b.lower();
    s          = SM(None,a,b);
    overlap    = sum([block.size for block in s.get_matching_blocks()]);
    return 1-(overlap / max([len(a),len(b)]));

def distance_2(a,b):
    a,b      = a.lower(), b.lower();
    s        = SM(None,a,b);
    overlap  = sum([block.size for block in s.get_matching_blocks()]);
    dist     = max([len(a),len(b)]) - overlap;
    return dist;

def flatten(d, parent_key='', sep='_'):
    items = [];
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k;
        if isinstance(v, dict):
            items.extend(flatten(v, new_key, sep=sep).items());
        else:
            items.append((new_key, v));
    return dict(items);

def pairfy(d, parent_key='', sep='_'): # To be applied after flatten!
    for key in d:
        if isinstance(d[key],list):
            for el in d[key]:
                if isinstance(el,dict):
                    for a,b in pairfy(el,key,sep):
                        yield (a,str(b),);
                else:
                    yield (parent_key+sep+key,str(el),);
        else:
            yield (parent_key+sep+key,str(d[key]),);

def dictfy(pairs):
    d = dict();
    for attr,val in pairs:
        if not attr in d:
            d[attr] = [];
        d[attr].append(val);
    return d;

def assign(A,B): # Two lists of strings
    #print(A); print(B); print('---------------------------------------------------------');
    M          = np.array([[distance_2(a,b) if isinstance(a,str) and isinstance(b,str) else a!=b for b in B] for a in A]);
    rows, cols = LSA(M);
    mapping    = [pair for pair in zip(rows,cols)];
    costs      = [M[assignment] for assignment in mapping];
    return mapping,costs;

def similar_enough(a,b,cost,threshold):
    if isinstance(a,str) and isinstance(b,str):
        if YEAR.fullmatch(a) and YEAR.fullmatch(b):
            y1, y2 = int(a), int(b);
            return abs(y1-y2) <= 1; # A one year difference between years is accepted
        return cost / max([len(a),len(b)]) < threshold;
    return a == b;

def compare_refstrings(P_strings,T_strings,threshold): # Two lists of strings
    mapping,costs = assign(P_strings,T_strings);
    pairs         = [(P_strings[i],T_strings[j],) for i,j in mapping];
    matches       = [(P_strings[mapping[i][0]],T_strings[mapping[i][1]],) for i in range(len(mapping)) if     similar_enough(P_strings[mapping[i][0]],T_strings[mapping[i][1]],costs[i],threshold)];
    mismatches    = [(P_strings[mapping[i][0]],T_strings[mapping[i][1]],) for i in range(len(mapping)) if not similar_enough(P_strings[mapping[i][0]],T_strings[mapping[i][1]],costs[i],threshold)];
    precision     = len(matches) / len(P_strings);
    recall        = len(matches) / len(T_strings);
    return precision, recall, len(matches), len(P_strings), len(T_strings), matches, mismatches, mapping, costs;

def compare_refobject(P_dict,T_dict,threshold):                       # Two dicts that have already been matched based on refstring attribute
    P_pairs     = pairfy(flatten(P_dict));                            # All attribute-value pairs from the output dict
    T_pairs     = pairfy(flatten(T_dict));                            # All attribute-value pairs from the gold   dict
    P_pair_dict = dictfy(P_pairs);                                    # Output values grouped by attributes in a dict
    T_pair_dict = dictfy(T_pairs);                                    # Gold   values grouped by attributes in a dict
    P_keys      = set(P_pair_dict.keys());                            # Output attributes
    T_keys      = set(T_pair_dict.keys());                            # Gold attributes
    TP_keys     = P_keys & T_keys;                                    # Attributes present in output and gold
    P           = sum([len(P_pair_dict[P_key]) for P_key in P_keys]); # Number of attribute-value pairs in output
    T           = sum([len(T_pair_dict[T_key]) for T_key in T_keys]); # Number of attribute-value pairs in gold object
    TP          = 0;                                                  # Number of attribute-value pairs in output and gold
    matches     = [];
    mismatches  = [];
    mapping     = [];
    costs       = [];
    for TP_key in TP_keys:
        prec, rec, TP_, P_, T_, matches_, mismatches_, mapping_, costs_ = compare_refstrings(P_pair_dict[TP_key],T_pair_dict[TP_key],threshold);
        TP                                                             += TP_;
        matches                                                        += [(TP_key,str(match_0),str(match_1),) for match_0,      match_1      in matches_    ];
        mismatches                                                     += [(TP_key,str(match_0),str(match_1),) for match_0,      match_1      in mismatches_ ];
        mapping                                                        += [(TP_key,assignment_0,assignment_1,) for assignment_0, assignment_1 in mapping_    ];
        costs                                                          += [(TP_key,cost_,)                     for cost_                      in costs_      ];
    return TP/P, TP/T, TP, P, T, matches, mismatches, mapping, costs;

def get_best_match(refobj,results,great_score,ok_score,max_rel_diff):
    TITLE = True if 'title' in refobj and refobj['title'] else False;
    query = refobj['title'] if TITLE else refobj['reference'];
    if len(results) > 0:
        print('____________________________________________________________________________________________________________\n____________________________________________________________________________________________________________\n'+query+'\n____________________________________________________________________________________________________________');#,results[0][0]['id'],'\n',results[0][0]['title'],'\n',results[0][1],'\n-------------------------------------------');
    else:
        return None;
    #TODO: Do some bing specific matching of the returned results and the refobj
    return results[0]['url']; #TODO: Check field name of output!

def bing_web_search(query): #TODO: Restrict return to top three and only relevant fields
    headers  = {"Ocp-Apim-Subscription-Key": _api_key};
    params   = { "q": query, "textDecorations": True, "textFormat": "HTML" };
    response = requests.get(_api_address, headers=headers, params=params);
    response.raise_for_status();
    return response.json(); #TODO: Is this already a dict structure?

def find(refobjects,index,api_address,api_key,field,great_score,ok_score,max_rel_diff):
    ids = [];
    for i in range(len(refobjects)):
        ID    = None;
        query = None;
        if 'title' in refobjects[i] and refobjects[i]['title']:
            query = refobjects[i]['title'][:_max_val_len];
        elif 'reference' in refobjects[i] and refobjects[i]['reference']:
            query = refobjects[i]['reference'][:_max_val_len];
        else:
            print('Neither title nor reference in refobject!');
            continue;
        results = bing_web_search(query);
        #results = [(result['_source'],result['_score'],) for result in results]; #TODO: Get pair of content and ranking score
        ID = get_best_match(refobjects[i],results,great_score,ok_score,max_rel_diff);
        if ID != None:
            refobjects[i][field[:-1]] = ID;
            ids.append(ID);
    return set(ids), refobjects;

def search(field,index,api_address,api_key,great_score,ok_score,max_rel_diff,recheck):
    #----------------------------------------------------------------------------------------------------------------------------------
    body            = { '_op_type': 'update', '_index': index, '_id': None, '_source': { 'doc': { 'has_'+field: True, field: None } } };
    scr_query       = { "ids": { "values": _ids } } if _ids else {'bool':{'must_not':{'term':{'has_'+field: True}}}} if not recheck else {'match_all':{}};
    #----------------------------------------------------------------------------------------------------------------------------------
    print('------------------->',scr_query);
    client   = ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,query=scr_query,_source=[field]+_refobjs);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    print('------------------->',page['hits']['total']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            print('---------------------------------------------------------------------------------------------\n',doc['_id'],'---------------------------------------------------------------------------------------------\n');
            body        = copy(body);
            body['_id'] = doc['_id'];
            ids         = set(doc['_source'][field]) if field in doc['_source'] and doc['_source'][field] != None else set([]);
            for refobj in _refobjs:
                previous_refobjects            = doc['_source'][refobj] if refobj in doc['_source'] and doc['_source'][refobj] else None;
                new_ids, new_refobjects        = find(previous_refobjects,index,api_address,api_key,field,great_score,ok_score,max_rel_diff) if isinstance(previous_refobjects,list) else (set([]),previous_refobjects,[]);
                ids                           |= new_ids;
                body['_source']['doc'][refobj] = new_refobjects; # The updated ones
                print('-->',refobj,'gave',['','no '][len(new_ids)==0]+'ids',', '.join(new_ids),'\n');
            print('------------------------------------------------\n-- overall ids --------------------------------\n'+', '.join(ids)+'\n------------------------------------------------');
            body['_source']['doc'][field]        = list(ids) #if len(ids) > 0 else None;
            body['_source']['doc']['has_'+field] = True      #if len(ids) > 0 else False;
            yield body;
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e, file=sys.stderr);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;
    client.clear_scroll(scroll_id=sid);
