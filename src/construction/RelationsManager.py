
from scipy import spatial
import pandas as pd
import numpy as np
import collections
import time
import csv

class RelationsManager:


	def __init__(self, dygieep_relations2files, stanfordcore_pos_relations2files, stanfordcore_openie_relations2files, stanfordcore_dep_relations2files):
		self.dygieep_relations2files = dygieep_relations2files
		self.stanfordcore_pos_relations2files = stanfordcore_pos_relations2files
		self.stanfordcore_openie_relations2files = stanfordcore_openie_relations2files
		self.stanfordcore_dep_relations2files = stanfordcore_dep_relations2files

		self.verb_map_path = '../../resources/CSKG_VerbNet_verb_map.csv'
		self.verb_map = {}

		self.dygiepp_pair2info = {}
		self.pos_pair2info = {}
		self.openie_pair2info = {}
		self.dep_pair2info = {}


	def loadVerbMap(self):
		verb_info = pd.read_csv(self.verb_map_path, sep=',')
		for i,r in verb_info.iterrows():
			for j in range(34):
				verb = r['v' + str(j)]
				if str(verb) != 'nan':
					self.verb_map[verb] = r['predicate']

	def bestLabelDygiepp(self):
		pairs = {}
		for (s,p,o), files in self.dygieep_relations2files.items():
			if (s,o) not in pairs:
				pairs[(s,o)] = {}
				pairs[(s,o)][p] = files

		self.dygiepp_pair2info = pairs


	def mapVerbRelations(self, verb_relations2files):
		new_verb_relations2files = {}

		for (s,p,o), files in verb_relations2files.items():
			if p in self.verb_map:
				mapped_verb = self.verb_map[p]

				if (s, mapped_verb, o) not in new_verb_relations2files:
					new_verb_relations2files[(s, mapped_verb, o)] = []
				new_verb_relations2files[(s, mapped_verb, o)] += files
				
		return new_verb_relations2files


	def labelSelector(self, verb_relations2files):
		pairs = {}
		for (s,p,o), files in verb_relations2files.items():
			if (s,o) not in pairs:
				pairs[(s,o)] = {}
			pairs[(s,o)][p] = files

		return pairs

	

	def mapDygieppRelations(self):
		dygieep_relations2files = {}
		
		# the mapping of dygiepp is done accordingly to the mapping defined by AIKG_VerbNet_verb_map.csv 
		for (s,p,o), files in self.dygieep_relations2files.items():
			if p == 'USED-FOR':
				dygieep_relations2files[(o, 'uses', s)] = files
			elif p == 'FEATURE-OF' or p == 'PART-OF':
				dygieep_relations2files[(o, 'includes', s)] = files
			elif p == 'EVALUATE-FOR':
				dygieep_relations2files[(s, 'analyzes', o)] = files
			elif p == 'HYPONYM-OF':
				dygieep_relations2files[(s, 'skos:broader/is/hyponym-of', o)] = files
			elif p == 'COMPARE':
				dygieep_relations2files[(s,'matches',o)] = files

		self.dygieep_relations2files = dygieep_relations2files



	def run(self):
		start = time.time()
		self.loadVerbMap()

		self.stanfordcore_pos_relations2files = self.mapVerbRelations(self.stanfordcore_pos_relations2files)
		self.pos_pair2info = self.labelSelector(self.stanfordcore_pos_relations2files)

		self.stanfordcore_openie_relations2files = self.mapVerbRelations(self.stanfordcore_openie_relations2files)
		self.openie_pair2info = self.labelSelector(self.stanfordcore_openie_relations2files)

		self.stanfordcore_dep_relations2files = self.mapVerbRelations(self.stanfordcore_dep_relations2files)
		self.dep_pair2info = self.labelSelector(self.stanfordcore_dep_relations2files)

		self.mapDygieppRelations()
		self.bestLabelDygiepp()
				
		

	def get(self):
		return self.dygiepp_pair2info, self.pos_pair2info, self.openie_pair2info, self.dep_pair2info
		













