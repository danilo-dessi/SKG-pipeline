from rdflib.namespace import OWL, RDF, RDFS, FOAF, XSD, SKOS
from django.core.exceptions import ValidationError
from rdflib import Graph, URIRef, Literal, BNode
from django.core.validators import URLValidator
from rdflib.namespace import Namespace
from datetime import datetime
import pandas as pd
import pickle
import json
import gzip
import time
import sys
import csv 
import ast
import re
import os



class RDFer:

	def __init__(self, data_trusted='', data_classified='', kgname='./cskg_data/cskg'):

		self.kgname = kgname

		self.rules_file = '../../resources/onto-design-table-CSKG.csv'
		self.verb_map_file = '../../resources/CSKG_VerbNet_verb_map.csv'

		self.oproperty2inverse = {}
		self.validDomainRelRange = set()
		self.statement_id = 0
		self.paper_set = set() #used to save all the mag ids of papers

		# ANALYSIS OF THE GRAPH
		self.triples = []

		#DATA
		self.data_trusted_df = None
		self.data_classified_df = None

		#OUTPUT
		self.cskg2cso = None
		self.cskg2wikidata = None
		self.cskg2dbpedia = None
		self.label2cskg_entity = None
		self.emap = {}
		self.g = Graph()
		self.gtriples_list = None # list of triple to be add in cskg
		self.g_onto_discarded_list = None # list of triples discarded by the ontology 

		#NAMESPACES
		self.CSKG_NAMESPACE = Namespace("http://scholkg.kmi.open.ac.uk/cskg/ontology#")
		self.CSKG_NAMESPACE_RESOURCE = Namespace("http://scholkg.kmi.open.ac.uk/cskg/resource/")
		self.CSO_NAMESPACE = Namespace("https://cso.kmi.open.ac.uk/topics/")
		self.DC = Namespace("http://purl.org/dc/terms/")
		self.WIKI_NAMESPACE = Namespace("http://www.wikidata.org/entity/")
		self.PROVO = Namespace('http://www.w3.org/ns/prov#')
		self.DBPEDIA = Namespace('http://dbpedia.org/resource/')

		self.g.bind("cskg-ont", self.CSKG_NAMESPACE)
		self.g.bind("cskg", self.CSKG_NAMESPACE_RESOURCE)
		self.g.bind("provo", self.PROVO)
		self.g.bind("cso", self.CSO_NAMESPACE)
		self.g.bind("owl", OWL)
		self.g.bind('dbpedia', self.DBPEDIA)
		self.g.bind('wd', self.WIKI_NAMESPACE)

		self.RESEARCH_ENTITY = self.CSKG_NAMESPACE.ResearchEntity
		self.METHOD = self.CSKG_NAMESPACE.Method
		self.OTHER_ENTITY = self.CSKG_NAMESPACE.OtherEntity
		self.TASK = self.CSKG_NAMESPACE.Task
		self.MATERIAL = self.CSKG_NAMESPACE.Material
		self.METRIC = self.CSKG_NAMESPACE.Metric
		self.TOPIC = self.CSKG_NAMESPACE.CSOTopic
		self.MAG_PAPER = self.CSKG_NAMESPACE.MagPaper
		self.WIKIDATA = self.CSKG_NAMESPACE.Wikidata

		#DATA PROPERTIES
		self.HAS_SUPPORT = "hasSupport"	# Research triple data property
		self.IS_INFERRED = "isInferredByTransitivity"	# Research triple data property
		self.IS_INVERSE = "isInverse"	# Research triple data property


	def createClassesStructure(self):
		self.g.add((self.RESEARCH_ENTITY, RDF.type, OWL.Class))

		self.g.add((self.METHOD, RDF.type, OWL.Class))
		self.g.add((self.METHOD, RDFS.comment, Literal("A specific approach, usually adopted to address a task.  Some examples include ‘neural networks’, decision trees’, ‘principal component analysis’, ‘support vector machine’, and ‘fuzzy logic’.")))

		self.g.add((self.OTHER_ENTITY, RDF.type, OWL.Class))
		self.g.add((self.OTHER_ENTITY, RDFS.comment, Literal("A significant entity that we were unable to classify as task, method, metric, or material. It usually refers to real world entities that are used by or affect tasks and methods. Some examples include ‘password’, ‘keyboard’, ‘fingerprint’, and ‘pixel’.")))

		self.g.add((self.TASK, RDF.type, OWL.Class))
		self.g.add((self.TASK, RDFS.comment, Literal("A piece of work to carry out, usually to solve a specific challenge. Some examples include ‘knowledge discovery’, ‘dimensionality reduction’, ‘computer vision’, and ‘authentication’.")))

		self.g.add((self.MATERIAL, RDF.type, OWL.Class))
		self.g.add((self.MATERIAL, RDFS.comment, Literal("An object that is processed, used, or returned by methods in order to pursue a task. In computer science it is typically  a data set, a knowledge base, or a system. Some examples include ‘vocabulary', 'biometric data', 'Wordnet', and 'social network'.")))

		self.g.add((self.METRIC, RDF.type, OWL.Class))
		self.g.add((self.METRIC, RDFS.comment, Literal("A measure of quantitative assessment, commonly used for comparing or assessing the performance of a method.  Some examples include ‘word error rate', 'minimum classification error', 'normalized mutual information', and 'fault exposure ratio'.")))

		self.g.add((self.MAG_PAPER, RDF.type, OWL.Class))
		self.g.add((self.MAG_PAPER, RDFS.comment, Literal("A paper indexed in the Microsoft Academic Graph dataset")))

		self.g.add((self.METHOD, RDFS.subClassOf, self.RESEARCH_ENTITY))
		self.g.add((self.OTHER_ENTITY, RDFS.subClassOf, self.RESEARCH_ENTITY))
		self.g.add((self.TASK, RDFS.subClassOf, self.RESEARCH_ENTITY))
		self.g.add((self.MATERIAL, RDFS.subClassOf, self.RESEARCH_ENTITY))
		self.g.add((self.METRIC, RDFS.subClassOf, self.RESEARCH_ENTITY))

		#ADD our Statement class
		self.g.add((self.CSKG_NAMESPACE.Statement, RDFS.subClassOf, RDF.Statement))



	def defineObjectProperties(self):
		rel2info = {}

		property2domain = {}
		property2range = {}
		property2inverse = {}
		isTransitiveProperty = {}
		property2up_property = {}

		onto_rules = pd.read_csv(self.rules_file)	
		verb_map = pd.read_csv(self.verb_map_file)

		# retrieving the inverse label of relations and creating obj property descriptions
		for i, r in verb_map.iterrows():
			rel = r['predicate']
			inverse = r['opposite']

			rel_desc = ''
			if rel == 'uses': 
				rel_desc = 'It is designed by the Used-for relationships extracted by the DyGIE++. It is also designed based on the following predicates extracted by the NLP pipeline: '
			elif rel == 'includes':
				rel_desc =  "It is designed by the Feature-of and Part-of relationships extracted by the DyGIE++. It is also designed based on the following predicates extracted by the NLP pipeline: '"
			elif rel == 'evaluates':
				rel_desc = "It is designed from the Evaluate-for relationships extracted by the DyGIE++. It is also designed based on the following predicates extracted by the NLP pipeline: '"
			else:
				rel_desc = "It is designed based on the following predicates extracted by the NLP pipeline: "

			verb_list = [r['v' + str(j)] for j in range(34) if  str(r['v' + str(j)]) != 'nan']
			rel_desc += ','.join(verb_list)
			rel2info[rel] = {'inverse' : inverse, 'description' : rel_desc}


		# create upper object properties based on verb predicates
		for rel in rel2info:
			if rel != 'skos:broader/is/hyponym-of':
				oproperty_uri = URIRef(self.CSKG_NAMESPACE + rel)
				self.g.add((oproperty_uri, RDF.type, OWL.ObjectProperty))
				rel_inverse = str(rel2info[rel]['inverse']).replace('by', 'By')
				if rel_inverse != 'nan':
					oproperty_inverse_uri = URIRef(self.CSKG_NAMESPACE + rel_inverse)
					self.g.add((oproperty_inverse_uri, RDF.type, OWL.ObjectProperty))
					self.g.add((oproperty_inverse_uri, OWL.inverseOf, oproperty_uri))

		# creating object properties in the schema based on the domain and range constraints
		for i, row in onto_rules.iterrows():
			#print(row)
			kind_s = row['subj']
			kind_o = row['obj']
			rel = row['rel']
			validity = row['valid'] == 'y'
			transitive = row['transitivity']

			s_class = URIRef(self.CSKG_NAMESPACE + kind_s)
			o_class = URIRef(self.CSKG_NAMESPACE + kind_o)

			if rel != 'skos:broader/is/hyponym-of' and validity:

				self.validDomainRelRange.add((kind_s, rel, kind_o))
				
				oproperty_uri = URIRef(self.CSKG_NAMESPACE + rel)
				if rel.replace(kind_o, '') in rel2info and str(rel2info[rel.replace(kind_o, '')]['inverse']) != 'nan':
					oproperty_inverse_uri = URIRef(self.CSKG_NAMESPACE + kind_o.lower() + rel2info[rel.replace(kind_o, '')]['inverse'].title().replace('by', 'By'))
					#self.types2rel_inverse[(o_class, s_class)] = oproperty_inverse_uri
					self.oproperty2inverse[oproperty_uri] = oproperty_inverse_uri #global to apply also to data
			
				if oproperty_uri not in property2domain:
					property2domain[oproperty_uri] = set()
				property2domain[oproperty_uri].add(s_class)
				property2range[oproperty_uri] = o_class
				property2inverse[oproperty_uri] = oproperty_inverse_uri
				isTransitiveProperty[oproperty_uri] = transitive == 'y'
				property2up_property[oproperty_uri] = URIRef(self.CSKG_NAMESPACE + rel.replace(kind_o, ''))
				if str(rel2info[rel.replace(kind_o, '')]['inverse']) != 'nan':
					property2up_property[oproperty_inverse_uri] = URIRef(self.CSKG_NAMESPACE + rel2info[rel.replace(kind_o, '')]['inverse'].replace('by', 'By'))

		self.oproperty2inverse[SKOS.broader] = SKOS.narrower #global to apply also to data

		for objectPropertyUri in property2domain:
			self.g.add((objectPropertyUri, RDF.type, OWL.ObjectProperty))
			self.g.add((property2inverse[objectPropertyUri], RDF.type, OWL.ObjectProperty))
			self.g.add((objectPropertyUri, RDFS.subPropertyOf, property2up_property[objectPropertyUri]))
			self.g.add((objectPropertyUri, OWL.inverseOf, property2inverse[objectPropertyUri]))
			self.g.add((property2inverse[objectPropertyUri], OWL.inverseOf, objectPropertyUri))
			self.g.add((property2inverse[objectPropertyUri], RDFS.subPropertyOf, property2up_property[property2inverse[objectPropertyUri]]))

			if isTransitiveProperty[objectPropertyUri]:	
				self.g.add((objectPropertyUri, RDF.type, OWL.TransitiveProperty))
				self.g.add((property2inverse[objectPropertyUri], RDF.type, OWL.TransitiveProperty))


			# add domain as union of various classes
			domain_bnode = BNode()
			self.g.add((objectPropertyUri, RDFS.domain, domain_bnode))
			self.g.add((objectPropertyUri, RDFS.range, property2range[objectPropertyUri]))
			self.g.add((property2inverse[objectPropertyUri], RDFS.domain, property2range[objectPropertyUri]))
			self.g.add((property2inverse[objectPropertyUri], RDFS.range, domain_bnode))

			domain_classes = [x for x in sorted(property2domain[objectPropertyUri])]

			bnode_first = BNode()
			self.g.add((domain_bnode, OWL.unionOf, bnode_first))
			self.g.add((bnode_first, RDF.first, domain_classes[0]))
				
			for i in range(1,len(domain_classes)):
				if i < len(domain_classes) - 1:
					bnode_rest = BNode()
					self.g.add((bnode_rest, RDF.first, domain_classes[i]))
					self.g.add((bnode_first, RDF.rest, bnode_rest))
					bnode_first = bnode_rest

				elif i == len(domain_classes) - 1:
					bnode_rest = BNode()
					self.g.add((bnode_rest, RDF.first, domain_classes[i]))
					self.g.add((bnode_first, RDF.rest, bnode_rest))
					self.g.add((bnode_rest, RDF.rest, RDF.nil))	
			

			# add comments	
			domain_classes_clean = None
			range_clean = None
			p = None
			if objectPropertyUri.n3() == '<' + self.CSKG_NAMESPACE + 'conjunction>':
				domain_classes_clean = ['Metric', 'Method', 'Task', 'OtherEntity', 'Material']
				range_clean = 'Metric, Method, Task, OtherEntity, Material'
				p = 'conjunction'

			elif objectPropertyUri.n3().startswith('<' + self.CSKG_NAMESPACE + 'interactsWith'):
				domain_classes_clean = ['Metric', 'Method', 'Task', 'OtherEntity', 'Material']
				range_clean = 'Metric, Method, Task, OtherEntity, Material'
				p = 'interactsWith'

			elif objectPropertyUri.n3().startswith('<' + self.CSKG_NAMESPACE + 'basedOn'):
				domain_classes_clean = [ x[x.index("#") + 1:] for x in domain_classes]
				range_clean = property2range[objectPropertyUri]
				range_clean = range_clean[range_clean.index("#") + 1:]

				#cleaning of the property
				p = 'basedOn'

			elif objectPropertyUri.n3().startswith('<' + self.CSKG_NAMESPACE + 'convertsTo'):
				domain_classes_clean = [ x[x.index("#") + 1:] for x in domain_classes]
				range_clean = property2range[objectPropertyUri]
				range_clean = range_clean[range_clean.index("#") + 1:]

				#cleaning of the property
				p = 'convertsTo'

			elif objectPropertyUri.n3().startswith('<' + self.CSKG_NAMESPACE + 'contributesTo'):
				domain_classes_clean = [ x[x.index("#") + 1:] for x in domain_classes]
				range_clean = property2range[objectPropertyUri]
				range_clean = range_clean[range_clean.index("#") + 1:]

				#cleaning of the property
				p = 'contributesTo'

			else: 
				domain_classes_clean = [ x[x.index("#") + 1:] for x in domain_classes]
				range_clean = property2range[objectPropertyUri]
				range_clean = range_clean[range_clean.index("#") + 1:]

				#cleaning of the property
				p = objectPropertyUri[objectPropertyUri.index("#") + 1:]
				m = re.search("[A-Z]", p)
				p = p[:m.start()]	

			comment = 'This relation indicates that an instance belonging to one class in { ' + ', '.join(domain_classes_clean) + ' } ' \
						+ p + ' an instance of  ' + range_clean + '. '

			comment += rel2info[p]['description']
			self.g.add((objectPropertyUri, RDFS.comment, Literal(comment)))

			if objectPropertyUri.n3() != '<' + self.CSKG_NAMESPACE + 'conjunction>' and \
				not objectPropertyUri.n3().startswith('<' + self.CSKG_NAMESPACE + 'interactsWith') and \
				not objectPropertyUri.n3().startswith('<' + self.CSKG_NAMESPACE + 'basedOn') :
				pi = property2inverse[objectPropertyUri]
				pi = pi[pi.index("#") + 1:]
				m = re.search("[A-Z]", pi)
				pi = pi[m.start():]	
				pi = pi.lower()
				comment = 'This relation indicates that an instance	 of class ' + range_clean + \
						 ' was ' + pi.replace('by', '') + ' by' + ' an instance belonging to one of the following classes { ' + ', '.join(domain_classes_clean)  + ' }. '
				comment += rel2info[p]['description']
				
				self.g.add((property2inverse[objectPropertyUri], RDFS.comment, Literal(comment)))



	def defineDataProperties(self):
		self.g.add((URIRef(self.CSKG_NAMESPACE + self.HAS_SUPPORT), RDF.type, OWL.DatatypeProperty))
		self.g.add((URIRef(self.CSKG_NAMESPACE + self.HAS_SUPPORT), RDFS.domain, self.CSKG_NAMESPACE.Statement))
		self.g.add((URIRef(self.CSKG_NAMESPACE + self.HAS_SUPPORT), RDFS.range, RDFS.Literal))
		self.g.add((URIRef(self.CSKG_NAMESPACE + self.HAS_SUPPORT), RDFS.comment, Literal("This property indicates the number of papers from where the predicate between subject and object comes from.")))

		self.g.add((URIRef(self.CSKG_NAMESPACE + self.IS_INFERRED), RDF.type, OWL.DatatypeProperty))
		self.g.add((URIRef(self.CSKG_NAMESPACE + self.IS_INFERRED), RDFS.domain, self.CSKG_NAMESPACE.Statement))
		self.g.add((URIRef(self.CSKG_NAMESPACE + self.IS_INFERRED), RDFS.range, RDFS.Literal))
		self.g.add((URIRef(self.CSKG_NAMESPACE + self.IS_INFERRED), RDFS.comment, Literal("This property indicates if the statement was inferred by transitivity. If 'false' it means that it was derived directly from the papers. If 'true' it means that it was inferred when computing the transitive closure of AI-KG.")))

		self.g.add((URIRef(self.CSKG_NAMESPACE + self.IS_INVERSE), RDF.type, OWL.DatatypeProperty))
		self.g.add((URIRef(self.CSKG_NAMESPACE + self.IS_INVERSE), RDFS.domain, self.CSKG_NAMESPACE.Statement))
		self.g.add((URIRef(self.CSKG_NAMESPACE + self.IS_INVERSE), RDFS.range, RDFS.Literal))
		self.g.add((URIRef(self.CSKG_NAMESPACE + self.IS_INVERSE), RDFS.comment, Literal("This property indicates if the statement was derived by inferring the inverse relation of a relation originally extracted from the corpus of paper.")))



	def loadData(self):

		pickle_in = open('../construction/cskg_data/cskg2cso.pickle', 'rb')
		self.cskg2cso = pickle.load(pickle_in)
		pickle_in.close()

		pickle_in = open('../construction/cskg_data/cskg2wikidata.pickle', 'rb')
		self.cskg2wikidata = pickle.load(pickle_in)
		pickle_in.close()

		pickle_in = open('../construction/cskg_data/cskg2dbpedia.pickle', 'rb')
		self.cskg2dbpedia = pickle.load(pickle_in)
		pickle_in.close()

		pickle_in = open('../../resources/only_embeddings_label2cskg_entity.pickle', 'rb')
		self.label2cskg_entity = pickle.load(pickle_in)
		#self.label2cskg_entity = {}

		self.data_trusted_df = pd.read_csv(data_trusted)
		self.data_classified_df = pd.read_csv(data_classified)
		self.data_classified_df = self.data_classified_df[self.data_classified_df['predicted_labels']==1]

	# 0= trusted data, 1 = classified data
	def populate(self):
		print('> RDF-efication of %d triples started'%(len(self.gtriples_list)))
		counter = 0
		stime = time.time()
		#n_rdf_triples = 0
		for (s, rel, o, sup, tools, files, stype, otype) in self.gtriples_list:
			#print(counter)
			counter += 1

			#if counter <= 43000001:
			#	self.statement_id += 2
			#	continue

			# reification of 1M triples
			if counter % 250000 == 0:
				index  = int(counter / 250000)
				name = self.kgname + '_' + str(index) +'.ttl'
				self.g.serialize(destination=name, format='turtle')
				with open(name, 'rb') as src, gzip.open(name + '.gz', 'wb') as dst:
					dst.writelines(src)
				os.remove(name)
				#n_rdf_triples += len(list(self.g.triples((None, None, None))))
				print('>> added %d triples in %f'%(counter, time.time() - stime))
				del self.g
				self.g = Graph()

			if (stype, rel + otype, otype) in self.validDomainRelRange or (rel == 'skos:broader/is/hyponym-of' and stype == otype):

				s_uri = URIRef(self.CSKG_NAMESPACE_RESOURCE + str(s).replace(' ', '_'))
				o_uri = URIRef(self.CSKG_NAMESPACE_RESOURCE + str(o).replace(' ', '_'))

				stype_uri = URIRef(self.CSKG_NAMESPACE + stype)
				otype_uri = URIRef(self.CSKG_NAMESPACE + otype)

				# adding entity types
				self.g.add((s_uri, RDF.type, stype_uri))
				self.g.add((o_uri, RDF.type, otype_uri))

				#adding entity labels
				self.g.add((s_uri, RDFS.label, Literal(s)))
				self.g.add((o_uri, RDFS.label, Literal(o)))

				# creation of the relationhip uri
				if rel == 'skos:broader/is/hyponym-of':
					rel_uri = SKOS.broader
				else:
					rel_uri = URIRef(self.CSKG_NAMESPACE + rel + otype)

				#statement creation
				statement_x = URIRef(self.CSKG_NAMESPACE_RESOURCE + 'statement_' + str(self.statement_id))
				self.g.add((statement_x, RDF.type, self.CSKG_NAMESPACE.Statement))
				self.g.add((statement_x, RDF.type, self.PROVO.Entity))

				# add of subject, predicate, and object
				self.g.add((statement_x, RDF.subject, s_uri))
				self.g.add((statement_x, RDF.predicate, rel_uri))
				self.g.add((statement_x, RDF.object, o_uri))

				# add source type to the statement
				for source_type in tools:
					if source_type == 'pos tagger':
						self.g.add((statement_x, self.PROVO.wasGeneratedBy, self.CSKG_NAMESPACE_RESOURCE.PoSTagger))
					elif source_type == 'openie':
						self.g.add((statement_x, self.PROVO.wasGeneratedBy, self.CSKG_NAMESPACE_RESOURCE.OpenIE))
					elif source_type == 'dependency tagger':
						self.g.add((statement_x, self.PROVO.wasGeneratedBy, self.CSKG_NAMESPACE_RESOURCE.DependencyTagger))
					elif source_type == 'dygiepp':
						self.g.add((statement_x, self.PROVO.wasGeneratedBy, URIRef(self.CSKG_NAMESPACE_RESOURCE.DyGIEpp)))

				#support 
				self.g.add((statement_x, URIRef(self.CSKG_NAMESPACE + self.HAS_SUPPORT),  Literal(int(sup), datatype=XSD.integer)))

				for file in files:
					mag_uri = URIRef(self.CSKG_NAMESPACE_RESOURCE + file.replace('.json', ''))
					self.g.add((statement_x, self.PROVO.wasDerivedFrom,  mag_uri))
					self.paper_set.add(file.replace('.json', ''))
					
				self.statement_id += 1

				if rel_uri in self.oproperty2inverse:
					statement_x = URIRef(self.CSKG_NAMESPACE_RESOURCE + 'statement_' + str(self.statement_id))
					self.g.add((statement_x, RDF.type, self.CSKG_NAMESPACE.Statement))
					self.g.add((statement_x, RDF.type, self.PROVO.Entity))

					#yes inverse
					self.g.add((statement_x, URIRef(self.CSKG_NAMESPACE + self.IS_INVERSE),  Literal('true', datatype=XSD.boolean)))

					# add of subject, predicate, and object
					self.g.add((statement_x, RDF.subject, o_uri))
					self.g.add((statement_x, RDF.predicate, self.oproperty2inverse[rel_uri]))
					self.g.add((statement_x, RDF.object, s_uri))

					# add source type to the statement
					for source_type in tools:
						if source_type == 'pos tagger':
							self.g.add((statement_x, self.PROVO.wasGeneratedBy, self.CSKG_NAMESPACE_RESOURCE.PoSTagger))
						elif source_type == 'openie':
							self.g.add((statement_x, self.PROVO.wasGeneratedBy, self.CSKG_NAMESPACE_RESOURCE.OpenIE))
						elif source_type == 'dependency tagger':
							self.g.add((statement_x, self.PROVO.wasGeneratedBy, self.CSKG_NAMESPACE_RESOURCE.DependencyTagger))
						elif source_type == 'dygiepp':
							self.g.add((statement_x, self.PROVO.wasGeneratedBy, URIRef(self.CSKG_NAMESPACE_RESOURCE.DyGIEpp)))

					#support 
					self.g.add((statement_x, URIRef(self.CSKG_NAMESPACE + self.HAS_SUPPORT),  Literal(int(sup), datatype=XSD.integer)))

					for file in files:
						mag_uri = URIRef(self.CSKG_NAMESPACE_RESOURCE + file.replace('.json', ''))
						self.g.add((statement_x, self.PROVO.wasDerivedFrom,  mag_uri))

					
					self.statement_id += 1
		
		# linkage to external resources
		for cskge, csoe in self.cskg2cso.items():
			cskge_uri = URIRef(self.CSKG_NAMESPACE_RESOURCE + cskge.replace(' ', '_'))
			self.g.add((cskge_uri, OWL.sameAs, URIRef(csoe)))
		for cskge, dbe in self.cskg2dbpedia.items():
			cskge_uri = URIRef(self.CSKG_NAMESPACE_RESOURCE + cskge.replace(' ', '_'))
			self.g.add((cskge_uri, OWL.sameAs, URIRef(dbe)))
		for cskge, wde in self.cskg2wikidata.items():
			cskge_uri = URIRef(self.CSKG_NAMESPACE_RESOURCE + cskge.replace(' ', '_'))
			self.g.add((cskge_uri, OWL.sameAs, URIRef(wde)))

		# add other labels
		for label, cskge in self.label2cskg_entity.items():
			cskge_uri = URIRef(self.CSKG_NAMESPACE_RESOURCE + cskge.replace(' ', '_'))
			self.g.add((cskge_uri, RDFS.label, Literal(label)))


	def apply_ontology(self):
		
		self.data_trusted_df['source_len'] = [len(ast.literal_eval(x)) for x in self.data_trusted_df['sources']]
		e2t = pickle.load(open('../../resources/e2selected_type.pickle', 'rb'))

		valid_onto = 0
		discarded_by_onto = 0

		gtriples_set = set()
		gdiscarded_set = set()

		stime = time.time()
		print(self.data_trusted_df.shape, self.data_classified_df.shape)
		for i, r in self.data_trusted_df.iterrows():
			
			if i % 50000 == 0:
				print('>> status trusted:' + str(i) + '/' + str(self.data_trusted_df.shape[0]), '-', time.time() - stime)
				stime = time.time()

			s = r['subj']
			rel = r['rel']
			o = r['obj']
			sup = int(r['support'])
			tools = r['sources']
			sources_len = r['source_len']
			files = r['files']
			stype = r['subj_type']
			otype = r['obj_type']

			#tools = ast.literal_eval(tools)
			#if 'dependency tagger' in tools:
			#	tools.discard('dependency tagger')
			#	tools.add('pos tagger')
			#tools = str(tools)

			# used to have consistent types and avoid bugs during the handling of the triples
			if s in e2t:
				try:
					stype = e2t[s]
					if stype == 'Generic' or stype == 'OtherScientificTerm' or stype == 'CSOTopic':
						stype = 'OtherEntity'
				except Exception as e:
					print(e)

			if o in e2t:
				try:
					otype = e2t[o]
					if otype == 'Generic' or otype == 'OtherScientificTerm' or otype == 'CSOTopic':
						otype = 'OtherEntity'
				except:
					print(o)
			########################################################################################

			if (stype, rel + otype, otype) in self.validDomainRelRange or (rel == 'skos:broader/is/hyponym-of' and stype == otype) or rel=='conjunction':
				gtriples_set.add((s, rel, o, sup, tools, files, stype, otype))
				valid_onto += 1
			else:
				gdiscarded_set.add((s, rel, o, sup, tools, files, stype, otype))
				discarded_by_onto += 1

		print('> valid & discarded high support', valid_onto, discarded_by_onto)
		ii = 0
		for i, r in self.data_classified_df.iterrows():

			if ii % 50000 == 0:
				print('>> status classified:' + str(ii) + '/' + str(self.data_classified_df.shape[0]), '-', time.time() - stime)
				stime = time.time()
			ii += 1

			s = r['subj']
			rel = r['rel']
			o = r['obj']
			sup = int(r['support'])
			tools = r['sources']
			sources_len = r['source_len']
			files = r['files']
			stype = r['subj_type']
			otype = r['obj_type']

			#tools = ast.literal_eval(tools)
			#if 'dependency tagger' in tools:
			#	tools.discard('dependency tagger')
			#	tools.add('pos tagger')
			#tools = str(tools)

			# used to have consistent types and avoid bugs during the handling of the triples
			if s in e2t:
				try:
					stype = e2t[s]
					if stype == 'Generic' or stype == 'OtherScientificTerm' or stype == 'CSOTopic':
						stype = 'OtherEntity'
				except Exception as e:
					print(e)

			if o in e2t:
				try:
					otype = e2t[o]
					if otype == 'Generic' or otype == 'OtherScientificTerm' or otype == 'CSOTopic':
						otype = 'OtherEntity'
				except:
					print(o)
			########################################################################################

			if (stype, rel + otype, otype) in self.validDomainRelRange or (rel == 'skos:broader/is/hyponym-of' and stype == otype) or rel=='conjunction':
				gtriples_set.add((s, rel, o, sup, tools, files, stype, otype))
				valid_onto += 1
			else:
				gdiscarded_set.add((s, rel, o, sup, tools, files, stype, otype))
				discarded_by_onto += 1

		print('> valid & discarded total', valid_onto, discarded_by_onto)

		gtriples_list, df = self.merge(gtriples_set)
		df.to_csv(self.kgname + '_final_data.csv', index=False)
		self.gtriples_list = gtriples_list 
		
		gdiscarded_list, df = self.merge(gdiscarded_set)
		df.to_csv(self.kgname + '_final_data_discarded.csv', index=False)
		self.g_onto_discarded_list = gdiscarded_list



	#used to solve duplicates triples
	def merge(self, triple_set):
		triples2info = {}
		subjs = []
		rels = []
		objs = []
		supports = []
		sources = []
		files = []
		subj_types = []
		obj_types = []
		merged_list = []

		for (s, p, o, support, tools, file_list, stype, otype) in triple_set:
			if (s, p, o) not in triples2info:
				triples2info[(s, p, o)]  = {
					'support' : int(support),
					'sources' : set(ast.literal_eval(tools)),
					'file_list' : set(ast.literal_eval(file_list)),
					'subj_type' : stype,
					'obj_type' : otype
				}
			else:
				triples2info[(s, p, o)]  = {
					'support' : len(triples2info[(s, p, o)]['file_list']) + len(files),
					'sources' : triples2info[(s, p, o)]['sources'] | set(ast.literal_eval(tools)),
					'file_list' : triples2info[(s, p, o)]['file_list'] | set(ast.literal_eval(file_list)),
					'subj_type' : stype,
					'obj_type' : otype
				}

		for (s,p,o) in triples2info.keys():
			subjs += [s]
			rels += [p]
			objs += [o]
			supports += [len(set(triples2info[(s,p,o)]['file_list']))]
			sources += [set(triples2info[(s,p,o)]['sources'])]
			files += [set(triples2info[(s,p,o)]['file_list'])]
			subj_types += [triples2info[(s,p,o)]['subj_type']]
			obj_types += [triples2info[(s,p,o)]['obj_type']]

			merged_list += [(s,p,o, len(set(triples2info[(s,p,o)]['file_list'])), set(triples2info[(s,p,o)]['sources']), set(triples2info[(s,p,o)]['file_list']), triples2info[(s,p,o)]['subj_type'], triples2info[(s,p,o)]['obj_type'])]

		merged_triples_df = pd.DataFrame({'subj' : subjs, 'rel' : rels, 'obj' : objs, 'support' : supports, 'sources' : sources, 'files' : files, 'subj_type' : subj_types, 'obj_type' : obj_types})
	
		return merged_list, merged_triples_df


	def addPaperInfo(self):
		d = '../../dataset/computer_science/'
		validator = URLValidator()
		for file in os.listdir(d):
			if file[-4:] == 'json':
				print(d+file)
				with open(d+file, 'r') as f:
					for line in f:
						dline = json.loads(line)
						magid = dline['_id']
						doi = dline['_source']['doi']

						if magid in self.paper_set:
							if 'urls' in dline['_source']:
								urls = dline['_source']['urls']
							else:
								urls = []
							title = dline['_source']['papertitle']

							self.g.add((URIRef(self.CSKG_NAMESPACE_RESOURCE + magid), RDF.type, self.CSKG_NAMESPACE.MagPaper))
							self.g.add((URIRef(self.CSKG_NAMESPACE_RESOURCE + magid), self.DC.title, Literal(title)))

							if doi != "":
								try:
									validator('https://doi.org/' + doi)
									self.g.add((URIRef(self.CSKG_NAMESPACE_RESOURCE + magid), self.CSKG_NAMESPACE.hasDOI, Literal('https://doi.org/' + doi)))
								except ValidationError as e:
									print(e, '\nskipped:', doi)

							for url in urls:
								self.g.add((URIRef(self.CSKG_NAMESPACE_RESOURCE + magid), self.CSKG_NAMESPACE.findableAt, Literal(url)))


	def run(self):
		self.createClassesStructure()
		self.defineObjectProperties()
		self.defineDataProperties()
		self.g.serialize(destination=self.kgname + '-onto.ttl', format='turtle') # save only the ontology

		self.loadData()
		self.apply_ontology()
		self.populate()
		self.addPaperInfo()

		#self.g.serialize(destination=self.kgname + '.ttl', format='turtle')
		name = self.kgname + '.ttl'
		self.g.serialize(destination=name, format='turtle')
		with open(name, 'rb') as src, gzip.open(name + '.gz', 'wb') as dst:
			dst.writelines(src)
		
		print('> KG saved in', self.kgname)
		print('> Number of statements:', self.statement_id)



if __name__ == '__main__':

	if len(sys.argv) == 4:
		data_trusted = sys.argv[1]
		data_classified = sys.argv[2]
		output_kg = sys.argv[3]
	else:
		print('> python RDFer.py DATA_RELIABLE DATA_CLASSIFIED KG_NAME')
		exit(1)

	rdfer = RDFer(data_trusted, data_classified, output_kg)	
	rdfer.run()










