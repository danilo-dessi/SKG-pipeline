# command to run the stanford core nlp server
# nohup java -mx8g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -port 9050 -timeout 15000 >/dev/null 2>&1 &

from nltk.tokenize.treebank import TreebankWordDetokenizer
from stanfordcorenlp import StanfordCoreNLP
from nltk.corpus import stopwords
import multiprocessing as mp
import networkx as nx
import itertools
import json
import nltk
import sys
import ast
import os
import re

dataset_dump_dir = '../../dataset/computer_science/'
dygiepp_output_dump_dir = '../../outputs/dygiepp_output/'
output_dir = '../../outputs/extracted_triples/'
stops = list(stopwords.words('english')) + ['it', 'we', 'they', 'its']

# used to check if an openie entity is a real entity of the domain
def checkEntity(e, e_list):
	eresult = None
	if e not in stops:
		for ei in e_list:
			if e in ei or ei in e:
				eresult = ei
	return eresult


def findTokens(s, tokens):
	for i in range(len(s)):
		try:
			if s[i : i + len(tokens)] == tokens:
				return i , i + len(tokens)
		except:
			return -1,-1
	return -1,-1


def pairwise(iterable):
	it = iter(iterable)
	a = next(it, None)
	for b in it:
		yield (a, b)
		a = b


def mapEntityAcronyms(acronyms, e):
	if e in acronyms:
		return acronyms[e]
	return e


def detectAcronyms(elist):
	acronyms = {}
	regex_acronym = re.compile("\(.*")

	for e in elist:
		e_cleaned_without_acr = regex_acronym.sub('', e).strip().lower()
		base_acronym = ''.join([token[0] for token in nltk.word_tokenize(e_cleaned_without_acr)])
		potential_acrs = [ acr.replace('( ', '').replace(' )', '').replace('(', '').replace(')', '').lower()  for acr in regex_acronym.findall(e)]
		for acr in potential_acrs:
			if acr == base_acronym:
				acronyms[acr] = e_cleaned_without_acr
				acronyms[e] = e_cleaned_without_acr
	return acronyms


def getDygieppResults(dresult):
	sentences = dresult['sentences']
	dner = dresult['predicted_ner']
	drelations = dresult['predicted_relations']

	text = [token for sentence in sentences for token in sentence]
	sentence2data = {}

	for i in range(len(sentences)):
		entities = []
		relations = []
		for ner_el in dner[i]:
			e = ' '.join(text[ner_el[0]:ner_el[1]+1])
			e_type = ner_el[2]
			entities += [(e, e_type)]	

		for relations_el in drelations[i]:
			r = relations_el[4]
			#if r == 'CONJUNCTION':
			#	continue
			e1 = ' '.join(text[relations_el[0]:relations_el[1]+1])
			e2 = ' '.join(text[relations_el[2]:relations_el[3]+1])
			relations += [(e1, r, e2)]

		sentence2data[i] = {'entities' :  entities, 'relations' : relations}
	return sentence2data


'''def solveCoref(corenlp_out):
	tokens = [token for sentence in corenlp_out['sentences'] for token in sentence['tokens']]
	print([t['lemma'] for t in tokens])

	for coref_num in corenlp_out['corefs']:
		representative_sentence = None
		representative_span = None
		coref_list = corenlp_out['corefs'][coref_num]
		print(coref_num)
		tokenSpan2lemma = {}
		for el in coref_list:
			if el['isRepresentativeMention'] == True:
				#print('Sentence:')
				#print(' '.join([t['originalText'] for t in corenlp_out['sentences'][el['sentNum']+1]['tokens']]))
				represenative_tokens = tokens[el['startIndex']:el['endIndex']]
				represenative_lemma = [t['lemma'] for t in represenative_tokens]
				print('Representative\n', represenative_lemma, el['startIndex'], el['endIndex'])
		
		for el in coref_list:
			if el['isRepresentativeMention'] == False:
				tokenSpan2lemma[el['startIndex'], el['endIndex']] = represenative_lemma
				#print((el['startIndex'], el['endIndex']))
				other_tokens = tokens[el['startIndex']:el['endIndex']]
				other_lemma = [t['lemma'] for t in other_tokens]
				print('others\n', other_lemma, el['startIndex'], el['endIndex'])
	print('-----------------------------------------------')
'''


def getOpenieTriples(corenlp_out, dygiepp, cso_topics):
	relations = []
	for i in range(len(corenlp_out['sentences'])): #sentence in corenlp_out['sentences']:
		sentence = corenlp_out['sentences'][i]
		openie = sentence['openie']

		if i < len(dygiepp.keys()):
			dygiepp_sentence_entities = [x for (x, xtype) in dygiepp[i]['entities']]
			#print(dygiepp_sentence_entities)
		acronyms = detectAcronyms(dygiepp_sentence_entities + cso_topics)

		for el in openie:
			#print(el)
			subj = el['subject']
			obj = el['object']
			relation_token_numbers = el['relationSpan']

			#check if the relation is a verb
			relation_tokens = [t['lemma'] for t in sentence['tokens'] \
					if t['index'] > relation_token_numbers[0] and \
						t['index'] <= relation_token_numbers[1]  and \
						t['pos'].startswith('VB') ]

			# check if there is a passive
			relation = None
			passive = False
			if relation_tokens != []:
				if len(relation_tokens) == 1:
					relation = relation_tokens[0]
				else:
					if relation_tokens[-2] == 'be': #passive
						passive = True
					relation = relation_tokens[-1]

			#check on subject and obejct. They must exist as entities
			checked_subj = checkEntity(subj, dygiepp_sentence_entities + cso_topics)
			checked_obj = checkEntity(obj, dygiepp_sentence_entities + cso_topics)

			if checked_subj is not None and checked_obj is not None and relation is not None:
				checked_subj = mapEntityAcronyms(acronyms, checked_subj)
				checked_obj = mapEntityAcronyms(acronyms, checked_obj)
				if not passive:
					#print((checked_subj, relation, checked_obj))
					relations += [(checked_subj, relation, checked_obj)]
				else:
					#print((checked_obj, relation, checked_subj), '#passive')
					relations += [(checked_obj, relation, checked_subj)]
	return set(relations)


def getPosTriples(corenlp_out, dygiepp, cso_topics):
	triples = []
	for i in range(len(corenlp_out['sentences'])): 
		sentence = corenlp_out['sentences'][i]
		sentence_tokens_text = [t['originalText'] for t in sentence['tokens']]
		sentence_tokens_text_lemma = [t['lemma'] for t in sentence['tokens']]
		sentence_tokens_pos = [t['pos'] for t in sentence['tokens']]

		if i < len(dygiepp.keys()):
			dygiepp_sentence_entities = [x for (x, xtype) in dygiepp[i]['entities']]
		acronyms = detectAcronyms(dygiepp_sentence_entities + cso_topics)

		entities_in_sentence = []
		for e in set(dygiepp_sentence_entities + cso_topics):
			start, end = findTokens(sentence_tokens_text, nltk.word_tokenize(e))
			if start != -1:
				entities_in_sentence += [(start, end)]

		#check verbs between each pair of entities
		for ((starti, endi), (startj, endj)) in itertools.combinations(entities_in_sentence, 2):
			ei = ' '.join(sentence_tokens_text[starti:endi])
			ej = ' '.join(sentence_tokens_text[startj:endj])
			ei = mapEntityAcronyms(acronyms, ei)
			ej = mapEntityAcronyms(acronyms, ej)

			verb_relations = []
			if endi < startj and startj - endi <= 10:
				verb_pattern = ''
				for k, pos in enumerate(sentence_tokens_pos[endi + 1:startj]):
					sentence_tokens_text_window = sentence_tokens_text_lemma[endi + 1:startj]
					if 'VB' in pos:
						verb_pattern += ' ' + sentence_tokens_text_window[k]
					elif verb_pattern != '':
						verb_relations += [verb_pattern.strip()]
						triples += [(ei, verb_pattern.strip(), ej)]
						verb_pattern = ''

			elif endj < starti and starti - endj <= 10:
				verb_pattern = ''
				for k, pos in enumerate(sentence_tokens_pos[endj + 1:starti]):
					sentence_tokens_text_window = sentence_tokens_text_lemma[endj + 1:starti]
					if 'VB' in pos:
						verb_pattern += ' ' + sentence_tokens_text_window[k]
					elif verb_pattern != '':
						verb_relations += [verb_pattern.strip()]
						triples += [(ej, verb_pattern.strip(), ei)]
						verb_pattern = ''

	# managing passive
	new_triples = []
	for (s,p,o) in triples:
		p_tokens = nltk.word_tokenize(p)
		v = p_tokens[-1]
		if 'be' in p_tokens[:-1] and len(p_tokens) > 1:
			new_triples += [(o, v, s)]
			#print((s,p,o), 'PASSIVE->', (o,v,s))
		else:
			new_triples += [(s,v,o)]
			#print((s,p,o), '->', (s,v,o))
	return set(new_triples)


def getDependencyTriples(corenlp_out, dygiepp, cso_topics):
	triples = []
	validPatterns = [
			('nsubj', 'obj'), 
			('acl:relcl', 'obj'), 
			('nsubj', 'obj', 'conj'), 
			('conj', 'obl', 'nsubj:pass'), 
			('acl', 'obj'), 
			('nmod', 'nsubj', 'obj'),
			('obl', 'nsubj:pass'),
			('nsubj', 'obj', 'nmod'),
			('acl:relcl', 'obl'),
			('obl', 'acl'),
			('nmod', 'obj', 'acl'),
			('acl', 'obj', 'nmod'),
		]
	
	for i in range(len(corenlp_out['sentences'])): 
		sentence = corenlp_out['sentences'][i]
		sentence_tokens_text = [t['originalText'] for t in sentence['tokens']]
		dependencies = sentence['basicDependencies']
		tokens = sentence['tokens']
		
		#graph creation
		g = nx.Graph()
		for dep in dependencies:
			governor_token_number = dep['governor']
			dependent_token_number = dep['dependent']
			g.add_node(governor_token_number, postag=tokens[governor_token_number - 1]['pos'], text=tokens[governor_token_number - 1]['lemma'])
			g.add_node(dependent_token_number, postag=tokens[dependent_token_number - 1]['pos'], text=tokens[governor_token_number - 1]['lemma'])
			g.add_edge(governor_token_number, dependent_token_number, label=dep['dep'])

		if i < len(dygiepp.keys()):
			dygiepp_sentence_entities = [x for (x, xtype) in dygiepp[i]['entities']]
		acronyms = detectAcronyms(dygiepp_sentence_entities + cso_topics)
	
		#finidng position of entities as token numbers
		entities_in_sentence = []
		for e in set(dygiepp_sentence_entities + cso_topics):
			start, end = findTokens(sentence_tokens_text, nltk.word_tokenize(e))
			if start != -1:
				entities_in_sentence += [(start, end)]

		# check path between entity pairs
		for ((starti, endi), (startj, endj)) in itertools.combinations(entities_in_sentence, 2):
			ei = ' '.join(sentence_tokens_text[starti:endi])
			ej = ' '.join(sentence_tokens_text[startj:endj])
			ei = mapEntityAcronyms(acronyms, ei)
			ej = mapEntityAcronyms(acronyms, ej)
			paths = list(nx.all_simple_paths(G=g, source=endi, target=endj, cutoff=4))

			# check if there are verbs
			for path in paths:
				# dependencies path
				path_dep = [g.edges()[a,b]['label'] for (a,b) in pairwise(path)]
				v = False
				verbs = ''
				for node in path:
					if g.nodes[node]['postag'].startswith('VB'):
						v = True
						verbs += g.nodes[node]['text'] + ' '
				verbs = verbs.strip()

				if tuple(path_dep) in validPatterns and v:
					#if tuple(path_dep) == ('nsubj:pass', 'obl', 'conj'):
					#	print(sentence_tokens_text)
					#	print((ei, verbs, ej))
					triples += [(ei, verbs, ej)]
				elif tuple(path_dep)[::-1] in validPatterns and v:
					#if tuple(path_dep)[::-1]  == ('nsubj:pass', 'obl', 'conj'):
					#	print(sentence_tokens_text)
					#	print((ei, verbs, ej))
					triples += [(ej, verbs, ei)]
	return set(triples)

# this is used to map entities to acronyms and prepare the list of relations of each file that can be saved
# similarly to the other extractor tools
def manageEntitiesAndDygieepRelations(dygiepp, cso_topics):
	entities = []
	e2type = {}
	ok_entities = []
	ok_relations = []

	for sentence in dygiepp:
		entities += [x for (x, xtype) in dygiepp[sentence]['entities']]
		for (x, xtype) in dygiepp[sentence]['entities']:
			e2type[x] = xtype

	entities += cso_topics
	acronyms = detectAcronyms(entities)
	for e in entities:
		if e in e2type:
			ok_entities += [(mapEntityAcronyms(acronyms, e), e2type[e])]
		else:
			ok_entities += [(mapEntityAcronyms(acronyms, e), 'CSO Topic')]
	ok_entities = set(ok_entities)

	for sentence in dygiepp:
		ok_relations += [(mapEntityAcronyms(acronyms, s), p, mapEntityAcronyms(acronyms, o)) for (s,p,o) in dygiepp[sentence]['relations']]

	return list(set(ok_entities)), list(set(ok_relations))


def extraction(filename):
	if filename[-5:] != '.json':
		return

	print('> processing: ' + filename)
	fw = open(output_dir + filename, 'w+')

	print('> processing: ' + filename + ' metadata reading')
	f = open(dataset_dump_dir + filename, 'r')
	paper2metadata = {}
	for row in f:
		drow = json.loads(row)
		paper_id = drow['_id']
		paper2metadata[paper_id] = drow['_source']
	f.close()

	print('> processing: ' + filename + ' dygiepp reading')
	f = open(dygiepp_output_dump_dir + filename, 'r')
	paper2dygiepp = {}
	for row in f:
		drow = json.loads(row)
		paper2dygiepp[drow['doc_key']] = getDygieppResults(drow)
		#print(paper2dygiepp[drow['doc_key']])
	f.close()

	nlp = StanfordCoreNLP('http://localhost', port=9050)
	paper2openie = {}
	print('> processing: ' + filename + ' core nlp extraction')
	for paper_id in paper2metadata:
		if paper_id in paper2dygiepp:
			corenlp_out = {}
			props = {'annotators': 'openie,tokenize,pos,depparse', 'pipelineLanguage': 'en', 'outputFormat': 'json'}
			try:
				text_data = paper2metadata[paper_id]['papertitle'].encode('utf8', 'ignore').decode('ascii', 'ignore') + '. ' + paper2metadata[paper_id]['abstract'].encode('utf8', 'ignore').decode('ascii', 'ignore')
				corenlp_out = json.loads(nlp.annotate(text_data, properties=props))
				openie_triples = getOpenieTriples(corenlp_out, paper2dygiepp[paper_id], paper2metadata[paper_id]['cso_semantic_topics'] +   paper2metadata[paper_id]['cso_syntactic_topics'])
				pos_triples = getPosTriples(corenlp_out,  paper2dygiepp[paper_id], paper2metadata[paper_id]['cso_semantic_topics'] +   paper2metadata[paper_id]['cso_syntactic_topics'])
				dependency_triples = getDependencyTriples(corenlp_out,  paper2dygiepp[paper_id], paper2metadata[paper_id]['cso_semantic_topics'] + paper2metadata[paper_id]['cso_syntactic_topics'])
				entities, dygiepp_triples = manageEntitiesAndDygieepRelations(paper2dygiepp[paper_id], paper2metadata[paper_id]['cso_semantic_topics'] +   paper2metadata[paper_id]['cso_syntactic_topics'])
			
				data_input_for_dygepp = json.dump({
						'doc_key' : str(paper_id),
						'entities' : list(entities),
						'openie_triples' : list(openie_triples),
						'pos_triples' : list(pos_triples),
						'dependency_triples' : list(dependency_triples),
						'dygiepp_triples' : list(dygiepp_triples)
					},fw)
				fw.write('\n')
			except Exception as e:
				print(e)
	fw.flush()
	fw.close()


if __name__ == '__main__':


	files_to_parse = [filename for filename in os.listdir(dataset_dump_dir)]
	pool = mp.Pool(10)
	result = pool.map(extraction, files_to_parse)
	
	#detectAcronyms([('machine learning (ML)', 'A'), ('danilo dessi', 'B'), ('natural language processing (NLP)', 'C'), ('Natural Language Processing (NLP)', 'C')])


	

	 








