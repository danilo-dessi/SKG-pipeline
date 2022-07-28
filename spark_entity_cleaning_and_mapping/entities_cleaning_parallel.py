import json
import nltk
import pyspark
from tqdm import tqdm
from nltk.corpus import stopwords
from nltk.corpus import wordnet
from nltk.corpus import wordnet_ic
from nltk.corpus.reader import wordnet as wordnet_reader
import re
import spacy

sc = pyspark.SparkContext()


# ------------------------ DATA LOADING ------------------------------------------

rdd = sc.textFile('extracted_triples/')
def map_extract_triples(json_str: str) -> list:
    data = json.loads(json_str)
    
    triple_keys = [key for key in data.keys() if 'triple' in key.lower()]
    triples_list = []
    for key in triple_keys:
        for one_tuple in data[key]:
            triples_list.append(tuple(one_tuple))
    return triples_list

rdd = rdd.flatMap(map_extract_triples)
print(f'>> {rdd.count()} have been loaded.')


# ------------------------ ACRONYM RESOLUTION------------------------------------------

def map_remove_acronyms(triple: tuple) -> tuple:
    re_s = r'[(\s][\s]*([A-Z0-9][a-z]?)+[\s]*[)\s]'
    t0 = re.sub(re_s, ' ', ' ' + triple[0] + ' ')
    t2 = re.sub(re_s, ' ', ' ' + triple[2] + ' ')
    
    return (t0, triple[1], t2)
rdd = rdd.map(map_remove_acronyms)
print(f'>> Acronyms have been resolved')


# ------------------------ PUNCTUATION REMOVAL ------------------------------------------

def map_remove_punctuation(triple: tuple) -> tuple:
    to_keep = {"'", "_", "-", "%", " "}
    t0 = "".join([c if c.isalnum() or c in to_keep else " " for c in triple[0]])
    t2 = "".join([c if c.isalnum() or c in to_keep else " " for c in triple[2]])
    
    return (t0, triple[1], t2)
rdd = rdd.map(map_remove_punctuation)
print(f'>> Punctuation has been removed')


# ------------------------ EXTRA SPACES REMOVAL ------------------------------------------

def map_clean(triple: tuple) -> tuple:
    re_s = r'\s+'
    t0 = " ".join([token for token in re.sub(re_s, ' ', triple[0]).strip().split()])
    t2 = " ".join([token for token in re.sub(re_s, ' ', triple[2]).strip().split()])
    return (t0, triple[1], t2)

rdd = rdd.map(map_clean)
print(f'>> Spaces have been removed')


# ------------------------ ENTITIES LEMMATIZATION ------------------------------------------

WNL = nltk.WordNetLemmatizer()

def map_lemmatize(triple: tuple) -> tuple:
    t0 = ' '.join([WNL.lemmatize(word.lower()) for word in triple[0].split()])
    t2 = ' '.join([WNL.lemmatize(word.lower()) for word in triple[2].split()])
    return (t0, triple[1], t2)

rdd = rdd.map(map_lemmatize)
print(f'>> Entities have been lemmatized')


# ------------------------ STOP WORDS REMOVAL ------------------------------------------

STOPS = set(stopwords.words('english'))
def map_remove_stop_words(triple: tuple) -> tuple:
    t0 = ' '.join([lemma for lemma in triple[0].split() if lemma not in STOPS])
    t2 = ' '.join([lemma for lemma in triple[2].split() if lemma not in STOPS])
    return (t0, triple[1], t2)

rdd = rdd.map(map_remove_stop_words)


# ------------------------ CHECK OF THE OPERATIONS ABOVE ------------------------------------------
tlist = rdd.collect()
print(tlist[:5])


# ------------------------ GENERIC ENTITIES REMOVAL WITH INFORMATION CONTENT ------------------------
unique_tokens_list = rdd.flatMap(lambda triple: triple[0].split() + triple[2].split()).distinct().collect()
# It creates a dictionary and associate a unique IC score to all of them
UNIQUE_TOKENS_ICS = dict()
DEFAULT_IC = -1
for token in unique_tokens_list:
    try:
        UNIQUE_TOKENS_ICS[token] = wordnet_reader.information_content(wordnet.synsets(token)[0], IC_DICT)
    except:
        UNIQUE_TOKENS_ICS[token] = DEFAULT_IC

def map_add_ic(triple: tuple) -> tuple:
    ics = [UNIQUE_TOKENS_ICS[token] for token in triple[0].split()]
    if DEFAULT_IC in ics:
        ic0 = DEFAULT_IC
    else:  # sum() non vuole funzionare
        ic0 = 0
        for ic in ics:
            ic0 += ic
        
    ics = [UNIQUE_TOKENS_ICS[token] for token in triple[2].split()]
    if DEFAULT_IC in ics:
        ic2 = DEFAULT_IC
    else:  # sum() non vuole funzionare
        ic2 = 0
        for ic in ics:
            ic2 += ic
    
    return ((triple[0], ic0), triple[1], (triple[2], ic2))

def filter_ic(triple: tuple) -> bool:
    is_t0_generic = 0 <= triple[0][1] <= 5
    is_t2_generic = 0 <= triple[2][1] <= 5
    return not (is_t0_generic or is_t2_generic)

def map_remove_ic(triple: tuple) -> tuple:
    return (triple[0][0], triple[1], triple[2][0])

rdd = rdd.map(map_add_ic)
rdd = rdd.filter(filter_ic)
rdd = rdd.map(map_remove_ic)
print(f'>> Generic Entities Removed')


# ------------------------ MAPPING TO CSO ------------------------------------------

CSO_FILE_PATH = 'CSO.3.3.csv'
cso_tuples = sc.textFile(f'{CSO_FILE_PATH}').map(lambda row: tuple(row.split(','))).collect()

cso_dict = {}

for t in cso_tuples:
    if len(t) == 3:
        (s,p,o) = t
        if p == '\"<http://cso.kmi.open.ac.uk/schema/cso#relatedEquivalent>\"':
            ss = s.replace('<https://cso.kmi.open.ac.uk/topics/', '')[1:-2]
            oo = o.replace('<https://cso.kmi.open.ac.uk/topics/', '')[1:-2]
            #print(s, ss)
            if ss not in cso_dict:
                cso_dict[ss] = ss
            if oo not in cso_dict:
                cso_dict[oo] = cso_dict[ss]
                
                
NO_CSO_MATCH = None
def map_entity_to_CSO(entity: str) -> tuple:
    entity_cmp = entity.replace(' ', '_')  
    res = cso_dict.get(entity_cmp, NO_CSO_MATCH)
    return entity, res  

rdd_only_entities = rdd.flatMap(lambda triple: [triple[0], triple[2]]).distinct()  
rdd_entities_to_cso = rdd_only_entities.map(map_entity_to_CSO)  
rdd_entities_to_cso = rdd_entities_to_cso.filter(lambda t: t[1] is not NO_CSO_MATCH)  
print(f'>> Number of links to CSO: {rdd_entities_to_cso.count()}')
#print(rdd_entities_to_cso.collect())



# ------------------------ MAPPING TO DBPEDIA ------------------------------------------
SPACY_MODEL = spacy.blank('en')
SPACY_MODEL.add_pipe('dbpedia_spotlight',
                     config={'process': 'annotate', 'confidence': 0.85});   

NO_DBPEDIA_MATCH = None
def map_entity_to_DBpedia(entity: str) -> tuple:
    dbp_link = SPACY_MODEL(entity)
    res = [(ent.text, ent.kb_id_, float(ent._.dbpedia_raw_result['@similarityScore'])) for ent in dbp_link.ents]
    max_score_entity = (NO_DBPEDIA_MATCH, NO_DBPEDIA_MATCH, 0)  
   
    for r in res:
        if r[2] > max_score_entity[2]:
            max_score_entity = r
    
    dbpedia_entity = max_score_entity[0]
    return entity, dbpedia_entity

rdd_entities_to_dbpedia = rdd_only_entities.map(map_entity_to_DBpedia)  
rdd_entities_to_dbpedia = rdd_entities_to_dbpedia.filter(lambda t: t[1] is not NO_DBPEDIA_MATCH)  
print(f'>> Number of links to DBpedia:  {rdd_entities_to_dbpedia.count()}.')
rdd_entities_to_dbpedia.collect()

