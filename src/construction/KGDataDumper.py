from sentence_transformers import SentenceTransformer
from nltk.tokenize import word_tokenize
from sentence_transformers import util
import pandas as pd
import numpy as np
import pickle
import time
import csv 
import os

class KGDataDumper:
	def __init__(self, dygiepp_pair2info, pos_pair2info, openie_pair2info, dep_pair2info, e2cso, e2dbpedia, e2wikidata, e2type):

		self.dygiepp_pair2info = dygiepp_pair2info
		self.pos_pair2info = pos_pair2info
		self.openie_pair2info = openie_pair2info
		self.dep_pair2info = dep_pair2info
		self.e2cso = e2cso
		self.e2dbpedia = e2dbpedia
		self.e2wikidata = e2wikidata
		self.e2type = e2type

		self.pair2info = {}
		self.aikg2cso = {}
		self.aikg2wikidata = {}
		self.aikg2dbpedia = {}
		self.validDomainRelRange = set()
		self.label2aikg_entity = {}
		self.triples = []

		self.triples_csv_filename = './cskg_data/aikg_triples.csv'

	def collectInfo(self):
		pairs = set(self.dygiepp_pair2info.keys()) | set(self.pos_pair2info.keys()) | set(self.openie_pair2info.keys()) | set(self.dep_pair2info.keys())
		
		for (s,o) in pairs:
			if (s,o) not in self.pair2info:
				self.pair2info[(s,o)] = {}
			
			if (s,o) in self.dygiepp_pair2info.keys():
				for rel in self.dygiepp_pair2info[(s,o)]:
					if rel not in self.pair2info[(s,o)]:
						self.pair2info[(s,o)][rel] = {'files' : list(self.dygiepp_pair2info[(s,o)][rel])}
						self.pair2info[(s,o)][rel]['source'] = ['dygiepp']				
					else:
						self.pair2info[(s,o)][rel]['files'] += list(self.dygiepp_pair2info[(s,o)][rel])
						self.pair2info[(s,o)][rel]['source'] += ['dygiepp']

			if (s,o) in self.pos_pair2info.keys():
				for rel in self.pos_pair2info[(s,o)]:
					if rel not in self.pair2info[(s,o)]:
						self.pair2info[(s,o)][rel] = {'files' : list(self.pos_pair2info[(s,o)][rel])}
						self.pair2info[(s,o)][rel]['source'] = ['pos tagger']				
					else:
						self.pair2info[(s,o)][rel]['files'] += list(self.pos_pair2info[(s,o)][rel])
						self.pair2info[(s,o)][rel]['source'] += ['pos tagger']


			if (s,o) in self.openie_pair2info.keys():
				for rel in self.openie_pair2info[(s,o)]:
					if rel not in self.pair2info[(s,o)]:
						self.pair2info[(s,o)][rel] = {'files' : list(self.openie_pair2info[(s,o)][rel])}
						self.pair2info[(s,o)][rel]['source'] = ['openie']				
					else:
						self.pair2info[(s,o)][rel]['files'] += list(self.openie_pair2info[(s,o)][rel])
						self.pair2info[(s,o)][rel]['source'] += ['openie']

			if (s,o) in self.dep_pair2info.keys():
				for rel in self.dep_pair2info[(s,o)]:
					if rel not in self.pair2info[(s,o)]:
						self.pair2info[(s,o)][rel] = {'files' : list(self.dep_pair2info[(s,o)][rel])}
						self.pair2info[(s,o)][rel]['source'] = ['dependency tagger']				
					else:
						self.pair2info[(s,o)][rel]['files'] += list(self.dep_pair2info[(s,o)][rel])
						self.pair2info[(s,o)][rel]['source'] += ['dependency tagger']



	def mergeEntities(self):

		cso2aikg = {}
		wikidata2aikg = {}
		dbpedia2aikg = {}

		for (s,o) in self.pair2info:
			
			if s in self.e2cso:
				if self.e2cso[s] not in cso2aikg:
					cso2aikg[self.e2cso[s]] = []
				cso2aikg[self.e2cso[s]] += [s]
			if s in self.e2dbpedia:
				if self.e2dbpedia[s] not in dbpedia2aikg:
					dbpedia2aikg[self.e2dbpedia[s]] = []
				dbpedia2aikg[self.e2dbpedia[s]] += [s]
			if s in self.e2wikidata:
				if self.e2wikidata[s] not in wikidata2aikg:
					wikidata2aikg[self.e2wikidata[s]] = []
				wikidata2aikg[self.e2wikidata[s]] += [s]

			if o in self.e2cso:
				if self.e2cso[o] not in cso2aikg:
					cso2aikg[self.e2cso[o]] = []
				cso2aikg[self.e2cso[o]] += [o]
			if o in self.e2dbpedia:
				if self.e2dbpedia[o] not in dbpedia2aikg:
					dbpedia2aikg[self.e2dbpedia[o]] = []
				dbpedia2aikg[self.e2dbpedia[o]] += [o]
			if o in self.e2wikidata:
				if self.e2wikidata[o] not in wikidata2aikg:
					wikidata2aikg[self.e2wikidata[o]] = []
				wikidata2aikg[self.e2wikidata[o]] += [o]

		# merging with cso
		for csoe, aikg_entities_labels in cso2aikg.items():
			aikg_entity = max(list(set(aikg_entities_labels)), key=len) #longest label
			
			for label in list(set(aikg_entities_labels)):
				self.label2aikg_entity[label] = aikg_entity
			self.aikg2cso[aikg_entity] = csoe


		# merging with dbpedia
		for dbe, aikg_entities_labels in dbpedia2aikg.items():
			
			# check if there exists an entity
			aikg_entity = None
			for label in list(set(aikg_entities_labels)):
				if label in self.label2aikg_entity:
					aikg_entity = self.label2aikg_entity[label]
					break

			if aikg_entity == None:
				aikg_entity = max(list(set(aikg_entities_labels)), key=len)
			
			for label in list(set(aikg_entities_labels)):
				self.label2aikg_entity[label] = aikg_entity
			self.aikg2dbpedia[aikg_entity] = dbe


		# merging with wikidata
		for wde, aikg_entities_labels in wikidata2aikg.items():
			
			# check if there exists an entity
			aikg_entity = None
			for label in list(set(aikg_entities_labels)):
				if label in self.label2aikg_entity:
					aikg_entity = self.label2aikg_entity[label]
					break

			if aikg_entity == None:
				aikg_entity = max(list(set(aikg_entities_labels)), key=len)

			for label in list(set(aikg_entities_labels)):
				self.label2aikg_entity[label] = aikg_entity
			self.aikg2wikidata[aikg_entity] = wde


	# function used by mergeEntitiesEuristic
	def mergeEntitiesEmbeddings(self, model, entities):

		paraphrases = util.paraphrase_mining(model, entities, query_chunk_size=100, corpus_chunk_size=10000, batch_size=256, top_k=5, show_progress_bar=False)

		for paraphrase in paraphrases:
			score, i, j = paraphrase
			ei = entities[i] # entity
			ej = entities[j] # entity 

			# since the results are ordered, the loop is stopped when the similarity is lower than 0.9
			if score < 0.9:
				break

			if ei not in self.label2aikg_entity and ej not in self.label2aikg_entity:
				self.label2aikg_entity[ej] = ei
				#print(ej, '->', ei, ' : ', score)
			elif ei not in self.label2aikg_entity and ej in self.label2aikg_entity:
				self.label2aikg_entity[ei] = self.label2aikg_entity[ej]
				#print(ei, '->', ej, '->',  self.label2aikg_entity[ej], ' : ', score)
			elif ei in self.label2aikg_entity and ej not in self.label2aikg_entity:
				self.label2aikg_entity[ej] = self.label2aikg_entity[ei]
				#print(ej, '->', ei, '->',  self.label2aikg_entity[ei], ' : ', score)


	def mergeEntitiesEuristic(self):
		# sentence-transformers/paraphrase-distilroberta-base-v2
		model = SentenceTransformer('sentence-transformers/paraphrase-distilroberta-base-v2')
		word2entities = {}

		for (s,o) in self.pair2info:
			stokens = word_tokenize(s)
			otokens = word_tokenize(o)

			for t in stokens:
				if t not in word2entities:
					word2entities[t] = set()
				word2entities[t].add(s)

			for t in otokens:
				if t not in word2entities:
					word2entities[t] = set()
				word2entities[t].add(o)

		wordcount = len(word2entities)
		for word, entities in word2entities.items():
			#print(wordcount, word, len(entities))
			wordcount -= 1
			if len(entities) > 1:
				self.mergeEntitiesEmbeddings(model, list(entities))

			

	def createTriplesData(self):

		# triple creation starting from the existing relationships between pairs of entities. The merging of entities based on the approaches above is 
		# performed here.
		for (s,o) in self.pair2info:
			s_aikg = self.label2aikg_entity[s] if s in self.label2aikg_entity else s
			o_aikg = self.label2aikg_entity[o] if o in self.label2aikg_entity else o
			stype = self.e2type[s_aikg].replace('OtherScientificTerm', 'OtherEntity')
			otype = self.e2type[o_aikg].replace('OtherScientificTerm', 'OtherEntity')

			for rel in self.pair2info[(s,o)]:
				if s_aikg != o_aikg:
					self.triples += [(s_aikg, rel, o_aikg, len(set(self.pair2info[(s,o)][rel]['files'])), self.pair2info[(s,o)][rel]['source'], self.pair2info[(s,o)][rel]['files'], stype, otype)]


		# merging triples after entity mapping and merging
		triples2info = {}
		for (s, p, o, support, sources, files, stype, otype) in self.triples:
			if (s,p,o) not in triples2info.keys():
				triples2info[(s,p,o)] = {
					'support' : len(files),
					'sources' : set(sources),
					'files' : set(files),
					'subj_type' : stype,
					'obj_type' : otype,
					'source_len': len(sources) # add the number of suources based on the definition of support
				}
			else:
				new_sources = triples2info[(s,p,o)]['sources'] | set(sources)
				triples2info[(s,p,o)]['sources'] = new_sources

				new_files = triples2info[(s,p,o)]['files'] | set(files)
				triples2info[(s,p,o)]['files'] = new_files

				triples2info[(s,p,o)]['source_len'] = len(new_sources)
				triples2info[(s,p,o)]['support'] = len(new_files)

		#saving merged triples in dataframe
		subjs = []
		rels = []
		objs = []
		supports = []
		sources = []
		files = []
		subj_types = []
		obj_types = []
		source_lens = []
		for (s,p,o) in triples2info.keys():
			subjs += [s]
			rels += [p]
			objs += [o]
			supports += [triples2info[(s,p,o)]['support']]
			sources += [triples2info[(s,p,o)]['sources']]
			files += [triples2info[(s,p,o)]['files']]
			subj_types += [triples2info[(s,p,o)]['subj_type']]
			obj_types += [triples2info[(s,p,o)]['obj_type']]
			source_lens += [triples2info[(s,p,o)]['source_len']]

		merged_triples = pd.DataFrame({'subj' : subjs, 'rel' : rels, 'obj' : objs, 'support' : supports, 'sources' : sources, 'files' : files, 'subj_type' : subj_types, 'obj_type' : obj_types, 'source_len': source_lens})
		merged_triples.sort_values(by=['support'], inplace=True)
		merged_triples.to_csv(self.triples_csv_filename, index=False)

		print('>>>> Number of triples:', len(triples2info.keys()))

		self.saveAsPickle(self.label2aikg_entity, 'label2aikg_entity')
		self.saveAsPickle(self.aikg2cso, 'aikg2cso')
		self.saveAsPickle(self.aikg2dbpedia, 'aikg2dbpedia')
		self.saveAsPickle(self.aikg2wikidata, 'aikg2wikidata')


	def saveAsPickle(self, data, objectName):
		pickle_out = open('./cskg_data/' + objectName + '.pickle', 'wb')
		pickle.dump(data, pickle_out)
		pickle_out.close()
		

	def run(self):
		if not os.path.exists('./cskg_data/'):
			os.makedirs('./cskg_data/')

		self.collectInfo()
		self.mergeEntities()
		self.mergeEntitiesEuristic	()
		self.createTriplesData()







