from sentence_transformers import SentenceTransformer
from SPARQLWrapper import SPARQLWrapper, JSON
from django.utils.encoding import smart_str
from sentence_transformers import util
from multiprocessing import Process
from urllib.parse import unquote
from random import shuffle
import networkx as nx
import pandas as pd
import numpy as np
import rdflib
import urllib
import random
import pickle 
import torch
import json
import time
import csv
import os


class EntitiesMapper:
	def __init__(self, entities, all_pairs):
		self.entities = entities 
		self.e2cso = {}
		self.e2wikidata = {}
		self.e2dbpedia = {}
		self.e2alternativeLabels = {}
		self.all_pairs = all_pairs
		#self.e2id = {}
		#self.id2e = {}
		self.e2neighbors = {}

		self.cso_map = {}
		self.dbpedia_map = {}
		self.emb_map = {}
		self.g = nx.Graph()

		self.csoResourcePath = '../../resources/CSO.3.1.csv'
		self.mappedTriples = {} # main output of this class





	def linkThroughCSO(self):
		print('- \t >> Mapping with cso started')

		entities_to_explore = set(self.entities) - set(self.e2cso.keys())
		if len(entities_to_explore) <= 0:
			return

		cso = rdflib.Graph()

		with open(self.csoResourcePath, 'r', encoding='utf-8') as csv_file:
			csv_reader = csv.reader(csv_file, delimiter=',')
			for s,p,o  in csv_reader:
				s = s[1:-1]
				p = p[1:-1]
				o = o[1:-1]
				
				entity = s.replace('https://cso.kmi.open.ac.uk/topics/', '').replace('_', ' ')
				if entity in entities_to_explore:
					self.e2cso[entity] = s
					if p == 'http://www.w3.org/2002/07/owl#sameAs':
						if 'wikidata' in o:
							self.e2wikidata[entity] = o
						if 'dbpedia' in o:
							self.e2dbpedia[entity] = o

				entity = o.replace('https://cso.kmi.open.ac.uk/topics/', '').replace('_', ' ')
				if entity in self.entities:
					self.e2cso[entity] = o
		pickle_out = open("../../resources/e2cso.pickle","wb")
		pickle.dump(self.e2cso, pickle_out)
		pickle_out.close()
		print('- \t >> Mapped to CSO:', len(self.e2cso))


	def linkThroughWikidata(self):
		print('- \t >> Mapping with wikidata started')
		timepoint = time.time()
		#sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
		entities_to_explore = list(set(self.entities) - set(self.e2wikidata.keys()))
		if len(entities_to_explore) <= 0:
			return

		#print('sorting entities...')
		entities_to_explore = sorted(entities_to_explore, key=lambda x:len(x), reverse=True)
		#print('sorted')
		c = 0
		print('- \t >> Entities to be linked to wikidata:', len(entities_to_explore))

		while c < len(entities_to_explore):
			e = entities_to_explore[c]
			
			query = """
					SELECT DISTINCT ?entity ?altLabel
					WHERE{
							{
								?entity  <http://www.w3.org/2000/01/rdf-schema#label> \"""" + e +"""\"@en .
                                ?entity wdt:31+ wd:Q21198 .
								 OPTIONAL {
                                    ?entity <http://www.w3.org/2004/02/skos/core#altLabel> ?altLabel .
                                    FILTER(LANG(?altLabel) = 'en')
                                }
								SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }

							} UNION {
								?entity  <http://www.w3.org/2000/01/rdf-schema#label> \"""" + e +"""\"@en .
                          		FILTER NOT EXISTS {?entity <http://schema.org/description> "Wikimedia disambiguation page"@en}
								 OPTIONAL {
                                    ?entity <http://www.w3.org/2004/02/skos/core#altLabel> ?altLabel .
                                    FILTER(LANG(?altLabel) = 'en')
                                }
								SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }

							} UNION {
								?entity  <http://www.w3.org/2000/01/rdf-schema#label> \"""" + e +"""\"@en .
                                ?entity wdt:31/wdt:P279* wd:Q21198 .
								 OPTIONAL {
                                    ?entity <http://www.w3.org/2004/02/skos/core#altLabel> ?altLabel .
                                    FILTER(LANG(?altLabel) = 'en')
                                }
								SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }

							} UNION {
								?entity  <http://www.w3.org/2000/01/rdf-schema#label> \"""" + e +"""\"@en .
                                ?entity wdt:P279+ wd:Q21198 .
								 OPTIONAL {
                                    ?entity <http://www.w3.org/2004/02/skos/core#altLabel> ?altLabel .
                                    FILTER(LANG(?altLabel) = 'en')
                                }
								SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }

							} UNION {
								?entity  <http://www.w3.org/2000/01/rdf-schema#label> \"""" + e +"""\"@en .
                                ?entity  wdt:P361+ wd:Q21198 . 
								 OPTIONAL {
                                    ?entity <http://www.w3.org/2004/02/skos/core#altLabel> ?altLabel .
                                    FILTER(LANG(?altLabel) = 'en')
                                }
								SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
		
							 } UNION {
								?entity  <http://www.w3.org/2000/01/rdf-schema#label> \"""" + e +"""\"@en .
                                ?entity  wdt:P1269+ wd:Q21198 . 
								 OPTIONAL {
                                    ?entity <http://www.w3.org/2004/02/skos/core#altLabel> ?altLabel .
                                    FILTER(LANG(?altLabel) = 'en')
                                }
								SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
		
							 } UNION {
								?entity <http://www.w3.org/2004/02/skos/core#altLabel> \"""" + e +"""\"@en .
                                ?entity wdt:31+ wd:Q21198 .
								 OPTIONAL {
                                    ?entity <http://www.w3.org/2004/02/skos/core#altLabel> ?altLabel .
                                    FILTER(LANG(?altLabel) = 'en')
                                }
								SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }

							} UNION {
								?entity  <http://www.w3.org/2004/02/skos/core#altLabel> \"""" + e +"""\"@en .
                          		FILTER NOT EXISTS {?entity <http://schema.org/description> "Wikimedia disambiguation page"@en}
								 OPTIONAL {
                                    ?entity <http://www.w3.org/2004/02/skos/core#altLabel> ?altLabel .
                                    FILTER(LANG(?altLabel) = 'en')
                                }
								SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }

							} UNION {
								?entity <http://www.w3.org/2004/02/skos/core#altLabel> \"""" + e +"""\"@en .
                                ?entity wdt:31/wdt:P279* wd:Q21198 .
								 OPTIONAL {
                                    ?entity <http://www.w3.org/2004/02/skos/core#altLabel> ?altLabel .
                                    FILTER(LANG(?altLabel) = 'en')
                                }
								SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }

							} UNION {
								?entity <http://www.w3.org/2004/02/skos/core#altLabel> \"""" + e +"""\"@en .
                                ?entity wdt:P279+ wd:Q21198 .
								 OPTIONAL {
                                    ?entity <http://www.w3.org/2004/02/skos/core#altLabel> ?altLabel .
                                    FILTER(LANG(?altLabel) = 'en')
                                }
								SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }

							} UNION {
								?entity <http://www.w3.org/2004/02/skos/core#altLabel> \"""" + e +"""\"@en .
                                ?entity  wdt:P361+ wd:Q21198 . 
								 OPTIONAL {
                                    ?entity <http://www.w3.org/2004/02/skos/core#altLabel> ?altLabel .
                                    FILTER(LANG(?altLabel) = 'en')
                                }
								SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
		
							 } UNION {
								?entity <http://www.w3.org/2004/02/skos/core#altLabel> \"""" + e +"""\"@en .
                                ?entity  wdt:P1269+ wd:Q21198 . 
								 OPTIONAL {
                                    ?entity <http://www.w3.org/2004/02/skos/core#altLabel> ?altLabel .
                                    FILTER(LANG(?altLabel) = 'en')
                                }
								SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
		
							 } 
						}
					"""

			url = 'https://query.wikidata.org/sparql'
			data = urllib.parse.urlencode({'query': query}).encode()
			headers = {"Accept": "application/sparql-results+json"}
			#print(e)
			try:
				req = urllib.request.Request(url, data=data, headers=headers)
				response = urllib.request.urlopen(req)

				if response.status == 200:

					result = response.read().decode('ascii', errors='ignore')
					jresponse = json.loads(result)
					variables = jresponse['head']['vars']
					for binding in jresponse['results']['bindings']:

						if 'entity' in binding and e not in self.e2wikidata:
							if 'http://www.wikidata.org/entity/Q' in binding['entity']['value']: # no wikidata:PXYZ
								self.e2wikidata[e] = binding['entity']['value']
								#print('>    ', binding['entity']['value'])

						if 'altLabel' in binding:
							if binding['altLabel']['value'].lower() in entities_to_explore and binding['altLabel']['value'].lower()  not in self.e2wikidata:
								if 'http://www.wikidata.org/entity/Q' in binding['entity']['value']:
									self.e2wikidata[binding['altLabel']['value'].lower()] = binding['entity']['value']
									#print('>    alt', binding['altLabel']['value'].lower(), binding['entity']['value'])

				c += 1
				if c % 1000 == 0:
					print('- \t\t >> Wikidata Processed', c, 'entities in {:.2f} secs.'.format(time.time() - timepoint))

				if c % 10000 == 0:
					pickle_out = open("../../resources/e2wikidata.pickle","wb")
					pickle.dump(self.e2wikidata, pickle_out)
					pickle_out.flush()
					pickle_out.close()
					#print('- \t >> Saving', len(self.e2wikidata), 'mappings')
				#raise urllib.error.HTTPError(req.full_url, '100', 'ciao', {'pippo':'pippo'}, fp=None)

			except urllib.error.HTTPError as err:
				# Return code error (e.g. 404, 501, ...)
				#print('Error 409', response.headers)
				print(err)
				#print('HTTPError: {}'.format(ex.code))
				print(err.headers)
				print('sleeping...')
				time.sleep(60)
			except Exception as ex:
				print(ex)

		pickle_out = open("../../resources/e2wikidata.pickle","wb")
		pickle.dump(self.e2wikidata, pickle_out)
		pickle_out.close()
		print('> Mapped to Wikidata:', len(self.e2wikidata))


	def findNeiighbors(self):

		for s,o in self.all_pairs:
			if s not in self.e2neighbors:
				self.e2neighbors[s] = []
			if len(self.e2neighbors[s]) < 20:
				self.e2neighbors[s] += [o]

			if o not in self.e2neighbors:
				self.e2neighbors[o] = []
			if len(self.e2neighbors[o]) < 20:
				self.e2neighbors[o] += [s]


		'''eid = 0
		for s,o in self.all_pairs:
			if s not in self.e2id and (s in self.entities or o in self.entities):
				self.e2id[s] = eid
				self.id2e[eid] = s
				eid += 1

			if o not in self.e2id and (s in self.entities or o in self.entities):
				self.e2id[o] = eid
				self.id2e[eid] = o
				eid += 1

			if s in self.entities or o in self.entities:
				self.g.add_edge(self.e2id[s], self.e2id[o])
		'''

	def linkThroughDBpediaSpotLight(self):
		print('- \t >> Mapping with dbpedia spotlight started')
		
		entities_to_explore = set(self.entities) - set(self.e2dbpedia.keys())
		if len(entities_to_explore) <= 0:
			return

		self.findNeiighbors()

		c = 0
		timepoint = time.time()
		for e in entities_to_explore:
			if e not in self.e2dbpedia:
				#eid = self.e2id[e]
				#neighbors_ids = list(self.g.neighbors(eid))
				neighbors = self.e2neighbors[e]

				#content = [e] + [self.id2e[nid] for nid in neighbors_ids[:20]]
				content = [e] + neighbors
				shuffle(content)
				content = ' '.join(content)

				url = 'https://api.dbpedia-spotlight.org/en/annotate'
				data = urllib.parse.urlencode({'text': content})
				headers = {"Accept": "application/json"}
				
				try:
					req = urllib.request.Request(url + '?' + data, headers=headers)
					response = urllib.request.urlopen(req)
					if response.status == 200:

						result = response.read().decode('ascii', errors='ignore')
						jresponse = json.loads(result)
						
						if 'Resources' in jresponse:
							for resource in jresponse['Resources']:
								if resource['@surfaceForm'] == e and float(resource['@similarityScore']) >= 0.8:
									self.e2dbpedia[e] = resource['@URI']
									break

				except urllib.error.HTTPError as e:
					print('HTTPError: {}'.format(e.code))

				c += 1
				if c % 10000 == 0:
					print('- \t\t >> DBpedia Processed', c, 'entities in', (time.time() - timepoint), 'secs')
					pickle_out = open("../../resources/e2dbpedia.pickle","wb")
					pickle.dump(self.e2dbpedia, pickle_out)
					pickle_out.close()

		pickle_out = open("../../resources/e2dbpedia.pickle","wb")
		pickle.dump(self.e2dbpedia, pickle_out)
		pickle_out.close()
		print('- \t >> Mapped to DBpedia:', len(self.e2dbpedia))
		

	'''def save(self):

		pickle_out = open("../../resources/e2cso.pickle","wb")
		pickle.dump(self.e2cso, pickle_out)
		pickle_out.close()

		pickle_out = open("../../resources/e2wikidata.pickle","wb")
		pickle.dump(self.e2wikidata, pickle_out)
		pickle_out.close()

		pickle_out = open("../../resources/e2dbpedia.pickle","wb")
		pickle.dump(self.e2dbpedia, pickle_out)
		pickle_out.close()

		pickle_out = open("../../resources/e2alternativeLabels.pickle","wb")
		pickle.dump(self.e2alternativeLabels, pickle_out)
		pickle_out.close()
	'''

	def load(self):

		p_cso = Process(target=self.linkThroughCSO)
		#p_wikidata = Process(target=self.linkThroughWikidata)
		p_dbpedia = Process(target=self.linkThroughDBpediaSpotLight)

		if os.path.exists("../../resources/e2cso.pickle"):
			f = open("../../resources/e2cso.pickle","rb")
			self.e2cso = pickle.load(f)
			f.close()
		p_cso.start()
			
		if os.path.exists("../../resources/e2dbpedia.pickle"):
			f = open("../../resources/e2dbpedia.pickle","rb")
			self.e2dbpedia = pickle.load(f)
			f.close()
		p_dbpedia.start()
			
		'''if os.path.exists("../../resources/e2wikidata.pickle"):
			f = open("../../resources/e2wikidata.pickle","rb")
			self.e2wikidata = pickle.load(f)
			f.close()
		
		p_wikidata.start()
		'''
		try: p_cso.join() 
		except: pass 
		#try: p_wikidata.join() 
		#except: pass 
		try: p_dbpedia.join()
		except: pass 


	
	def run(self):
		print('\t>> Entities to be mapped:', len(self.entities))
		self.load()


	def getMaps(self):
		return self.e2cso, self.e2dbpedia, self.e2wikidata



if __name__ == '__main__':

	entities = ['neural network', 'artificial neural network', 'ann', 'computer science', 'ontology alignment', 'convolutional neural network', 'ontology matching', 'neural network', 'cnn', 'deep learning', 'precision', 'recall', 'ontology', 'semantic web']
	triples = [('cnn',  'convolutional neural network'), \
				('deep learning', 'convolutional neural network'), \
				('deep learning', 'computer science'), \
				('deep learning',  'recall'), \
				('precision', 'recall'), \
				('machine learning',  'deep learning'), \
				('ontology alignment', 'ontology'), \
				('ontology alignment','ontology matching'), \
				('ontology', 'semantic web'), \
				('ontology alignment', 'ontology'), \
				('ontology', 'computer science'), \
				('neural network', 'artificial neural network'), \
				('machine learning',  'ann'), \
				('cnn',  'ann'), \
				('cnn',  'semantic web') \
			]
	mapper = EntitiesMapper(entities, triples)
	mapper.run()














