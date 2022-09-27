import numpy as np
from scipy.optimize import linear_sum_assignment as LSA
from difflib import SequenceMatcher as SM
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
import requests
import re
import time
import sys
import sqlite3
import urllib.request
from lxml import html as lhtml
import signal
from pathlib import Path


_max_extract_time =  10; #minutes
_max_scroll_tries =   2;
_scroll_size      = 10;

_max_val_len   = 2048;
_min_title_len =   12;

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

_query_db = str((Path(__file__).parent / '../').resolve())+'/queries.db';
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

def handler(signum, frame):
    print("Took too long. Breaking...")
    raise Exception("Timeout")

def load_html(url):
    IN    = urllib.request.urlopen(url);
    bytes = IN.read();
    html  = bytes.decode("utf8");
    IN.close();
    return html;

def parse_html(string):
    html = lhtml.fromstring(string);
    return html;

def html_extract_title(html):
    for head in html.iter("head"):
        for title in head.iter("title"):
            return title.text;
    return None;

def url_complete(title,url):
    if " ..." in title:
        print(title,'seems to be abbreviated. Trying to load full title from',url,'...');
        url_title = None;
        signal.signal(signal.SIGALRM, handler);
        signal.alarm(10);
        try:
            url_title = html_extract_title(parse_html(load_html(url)));
        except Exception as e:
            print(e,'\n','Failed to load title from URL.');
        signal.alarm(0);
        if url_title:
            print('Got url title',url_title);
            if len(url_title) > len(title):
                title = url_title;
    return title;

def distance_(a,b):
    a,b        = a.lower(), b.lower();
    s          = SM(None,a,b);
    overlap    = sum([block.size for block in s.get_matching_blocks()]);
    dist       = 1-(overlap / max([len(a),len(b)]));
    print(a,'|',dist)
    return dist;

def distance__(a,b):
    if min(len(a),len(b)) < _min_title_len:
        return 1.0;
    a,b        = a.lower(), b.lower();
    s          = SM(None,a,b);
    overlap    = sum([block.size for block in s.get_matching_blocks()]);
    dist       = 1-(overlap / min([len(a),len(b)]));
    print(a,'|',dist)
    return dist;

def distance(a,b):
    if min(len(a),len(b)) < _min_title_len:
        return 1.0;
    a,b        = a.lower(), b.lower();
    s          = SM(None,a,b);
    overlap    = sum([block.size for block in s.get_matching_blocks()]);
    #substring  = max([block.size for block in s.get_matching_blocks()]);
    dist       = 1-(overlap / len(b));
    print(a,'|',b,'|',dist)
    return dist;

def distance_new(a,b): #TODO: TEST AND IMPROVE!
    if min(len(a),len(b)) < _min_title_len:
        return 1.0;
    a,b        = a.lower(), b.lower();
    s          = SM(None,a,b);
    overlap    = sum([block.size for block in s.get_matching_blocks()]);
    substring  = max([block.size for block in s.get_matching_blocks()]);
    dist       = 1-(substring/overlap)#1-(overlap / len(b));
    print(a,'|',b,'|',dist)
    return dist;

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

def bing_web_search(query,api_address,api_key,api_tps):
    time.sleep(1.0/api_tps) #TODO: Comment out!
    headers   = {"Ocp-Apim-Subscription-Key": api_key};
    params    = { "q": query, "textDecorations": False, "textFormat": "HTML", "count":3, "responseFilter":["Webpages"]};
    response  = requests.get(api_address, headers=headers, params=params);    response.raise_for_status();
    responses = response.json();
    results   = [{'title':url_complete(page['name'],page['url']), 'url':page['url'], 'snippet':page['snippet'], 'language':page['language']} for page in responses['webPages']['value'] if 'name' in page and 'url' in page and 'snippet' in page and 'language' in page] if 'webPages' in responses and 'value' in responses['webPages'] and len(responses['webPages']['value'])>0 else [];
    return results;


def get_best_match(search_function,args,refobj,great_score,ok_score,max_rel_diff,cur):
    #-----------------------------------------------------------------------------------------------------
    #TITLE = True if 'title' in refobj and refobj['title'] else False;
    query = args[0]; #refobj['title'] if TITLE else refobj['reference'];
    #-----------------------------------------------------------------------------------------------------
    results = None;
    rows    = cur.execute("SELECT title,url,snippet,language,title_dist,refstr_dist FROM queries WHERE query=?",(query,)).fetchall();
    if len(rows) > 0:
        print('Found query "', query, '" in database, skipping web search.');
        results = [{'title':title,'url':url,'snippet':snippet,'language':language,'title_dist':title_dist,'refstr_dist':refstr_dist} for title,url,snippet,language,title_dist,refstr_dist in rows];
    else:
        print('Did not find query "', query, '" in database, calling Bing Web API...');
        results = search_function(*args);
    #-----------------------------------------------------------------------------------------------------
    if len(results) > 0:
        print('____________________________________________________________________________________________________________\n____________________________________________________________________________________________________________\n'+query+'\n____________________________________________________________________________________________________________');#,results[0][0]['id'],'\n',results[0][0]['title'],'\n',results[0][1],'\n-------------------------------------------');
    else:
        return None;
    results_ = [];
    j = 0;
    for result in results:
        result['title']    = None if not 'title'    in result else result['title'];
        result['url']      = None if not 'url'      in result else result['url'];
        result['snippet']  = None if not 'snippet'  in result else result['snippet'];
        result['language'] = None if not 'language' in result else result['language'];
        j  += 1;
        if result['title'] and 'title' in refobj and refobj['title']:
            print('Comparing refobj title to website title:');
            dist = distance(result['title'],refobj['title']);
            cur.execute("INSERT INTO queries VALUES(?,?,?,?,?,?,?,?,?)",(query,result['title'],result['url'],result['snippet'],result['language'],dist,None,dist<=max_rel_diff[0],dist<=max_rel_diff[0] and j==1));
            if dist <= max_rel_diff[0]:
                result['refstr_dist'],result['title_dist'] = None,dist;
                results_.append(result);
                continue;
            else:
                print('Titles too different with distance',dist);
        if result['title'] and 'reference' in refobj and refobj['reference']:
            print('Comparing refobj refstr to website title:');
            dist = distance(result['title'],refobj['reference']);
            cur.execute("INSERT INTO queries VALUES(?,?,?,?,?,?,?,?,?)",(query,result['title'],result['url'],result['snippet'],result['language'],None,dist,dist<=max_rel_diff[1],dist<=max_rel_diff[1] and j==1,));
            if dist <= max_rel_diff[1]:
                result['refstr_dist'],result['title_dist'] = dist,None;
                results_.append(result);
                continue;
            else:
                print('Title and reference too different with distance',dist);
        cur.execute("INSERT INTO queries VALUES(?,?,?,?,?,?,?,?,?)",(query,result['title'],result['url'],result['snippet'],result['language'],None,None,None,None,));
    if len(results) == 0:
        cur.execute("INSERT INTO queries VALUES(?,?,?,?,?,?,?,?,?)",(query,None,None,None,None,None,None,None,None,));
    print(len(results_),' of 3 results left after checking.');
    return results_[0]['url'] if len(results_)>0 else None;

def find(refobjects,index,api_address,api_key,api_tps,field,great_score,ok_score,max_rel_diff,cur):
    ids = [];
    for i in range(len(refobjects)):
        if ('sowiport_url' in refobjects[i] and refobjects[i]['sowiport_url']) or ('crossref_url' in refobjects[i] and refobjects[i]['crossref_url']) or ('dnb_url' in refobjects[i] and refobjects[i]['dnb_url']) or ('openalex_url' in refobjects[i] and refobjects[i]['openalex_url']):
            continue;
        ID    = None;
        query = None;
        if 'reference' in refobjects[i] and refobjects[i]['reference']:
            query = refobjects[i]['reference'][:_max_val_len];
            if 'title' in refobjects[i] and refobjects[i]['title']:
                query += ' prefer:"'+refobjects[i]['title'][:_max_val_len]+'"';
        else:
            print('Neither title nor reference in refobject!');
            continue;
        ID = get_best_match(bing_web_search,[query,api_address,api_key,api_tps],refobjects[i],great_score,ok_score,max_rel_diff,cur);
        if ID != None:
            refobjects[i][field[:-1]] = ID;
            ids.append(ID);
    return set(ids), refobjects;

def search(field,index,api_address,api_key,api_tps,great_score,ok_score,max_rel_diff,recheck):
    #----------------------------------------------------------------------------------------------------------------------------------
    body            = { '_op_type': 'update', '_index': index, '_id': None, '_source': { 'doc': { 'has_'+field: True, field: None } } }; #TODO: The scroll query is both wrong and does not work!
    #scr_query       = { "ids": { "values": _ids } } if _ids else {'bool':{'must_not':[{'term':{'has_'+field: True}},{'exists':{'field':'sowiport_ids'}},{'exists':{'field':'crossref_ids'}},{'exists':{'field':'dnb_ids'}},{'exists':{'field':'openalex_ids'}}]}} if not recheck else {'bool':{'must_not':[{'exists':{'field':'sowiport_ids'}},{'exists':{'field':'crossref_ids'}},{'exists':{'field':'dnb_ids'}},{'exists':{'field':'openalex_ids'}}]}};
    scr_query       = { "ids": { "values": _ids } } if _ids else {'bool':{'must_not':[{'term':{'has_'+field: True}}]}} if not recheck else {'match_all':{}};
    con             = sqlite3.connect(_query_db);
    cur             = con.cursor();
    #----------------------------------------------------------------------------------------------------------------------------------
    cur.execute("CREATE TABLE IF NOT EXISTS queries(query TEXT, title TEXT, url TEXT, snippet TEXT, language TEXT, title_dist REAL, refstr_dist REAL, matched INT, used INT, UNIQUE (query,url) ON CONFLICT REPLACE)");
    cur.execute("CREATE INDEX IF NOT EXISTS queries_query_index ON queries(query)"); #TODO: May want to load as dictionary and then executemany inserts at the end of session or every n iterations
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
                new_ids, new_refobjects        = find(previous_refobjects,index,api_address,api_key,api_tps,field,great_score,ok_score,max_rel_diff,cur) if isinstance(previous_refobjects,list) else (set([]),previous_refobjects);
                ids                           |= new_ids;
                body['_source']['doc'][refobj] = new_refobjects; # The updated ones
                con.commit();
                print('-->',refobj,'gave',['','no '][len(new_ids)==0]+'ids',', '.join(new_ids),'\n');
            print('------------------------------------------------\n-- overall ids --------------------------------\n'+', '.join(ids)+'\n------------------------------------------------');
            body['_source']['doc'][field]        = list(ids) #if len(ids) > 0 else None;
            body['_source']['doc']['has_'+field] = True      #if len(ids) > 0 else False;
            yield body; #TODO: not sure if anything got updated...
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
