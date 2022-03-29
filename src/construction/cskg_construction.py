
from EntitiesValidator import EntitiesValidator
from RelationsManager import RelationsManager
from EntitiesCleaner import EntitiesCleaner
from EntitiesMapper import EntitiesMapper
from KGDataDumper import KGDataDumper
import pickle
import json
import os
import gc




class TriplesGenerator:
	def __init__(self):
		self.entities2files = {}
		self.dygiepp2files = {}
		self.openie2files = {}
		self.pos2files = {}
		self.dependency2files = {}
		self.data_extracted_dir = '../../outputs/extracted_triples/'
		self.e2selected_type = {}


	############ Data Loading #######################################################################################################

	def addDataInTripleDict(self, dic, triples_list, doc_key):
		for (s,p,o) in triples_list:
			if (s,p,o) not in dic:
				dic[(s,p,o)] = []
			dic[(s,p,o)] += [doc_key]


	def loadData(self):
		for filename in os.listdir(self.data_extracted_dir):
			if filename[-5:] == '.json':
				f = open(self.data_extracted_dir + filename, 'r').readlines()[:10]
				for row in f:
					paper_data = json.loads(row.strip())
					self.addDataInTripleDict(self.dygiepp2files, paper_data['dygiepp_triples'], paper_data['doc_key'])
					self.addDataInTripleDict(self.openie2files, paper_data['openie_triples'], paper_data['doc_key'])
					self.addDataInTripleDict(self.pos2files, paper_data['pos_triples'], paper_data['doc_key'])
					self.addDataInTripleDict(self.dependency2files, paper_data['dependency_triples'], paper_data['doc_key'])
					for (e, etype) in paper_data['entities']:
						if (e, etype) not in self.entities2files:
							self.entities2files[(e, etype)] = []
						self.entities2files[(e, etype)] += [paper_data['doc_key']]

	###################################################################################################################################

	########### CLeaning of entities ##################################################################################################

	def applyCleanerMap(self, relations2files, cleaner_map):
		tool_triples2files = {}
		for (s,p,o),files in relations2files.items():
			if s in cleaner_map and o in cleaner_map:
				if (cleaner_map[s],p,cleaner_map[o]) in tool_triples2files:
					tool_triples2files[(cleaner_map[s],p,cleaner_map[o])].update(set(files))
				else:
					tool_triples2files[(cleaner_map[s],p,cleaner_map[o])] = set(files)
		return tool_triples2files


	def updateThroughCleanerMap(self, cleaner_map):
		tmp_entities2files = {}
		for (e, e_type),files in self.entities2files.items():
			if e in cleaner_map:
				if (cleaner_map[e], e_type) in tmp_entities2files:
					tmp_entities2files[(cleaner_map[e], e_type)].update(set(files))
				else:
					tmp_entities2files[(cleaner_map[e], e_type)] = set(files)
		self.entities2files = tmp_entities2files

		self.dygiepp2files = self.applyCleanerMap(self.dygiepp2files, cleaner_map)
		self.pos2files = self.applyCleanerMap(self.pos2files, cleaner_map)
		self.openie2files = self.applyCleanerMap(self.openie2files, cleaner_map)
		self.dependency2files = self.applyCleanerMap(self.dependency2files, cleaner_map)

	###################################################################################################################################
	
	############ Validation of entities ###############################################################################################

	def applyValidEntities(self, validEntities, relations2files):
		new_relations2files = {}
		for (s,p,o),files in relations2files.items():
			if s in validEntities and o in validEntities:
				if (s,p,o) in new_relations2files:
					new_relations2files[(s,p,o)].update(set(files))
				else:
					new_relations2files[(s,p,o)] = set(files)
		return new_relations2files
	
	def updateThroughValidEntities(self, validEntities):

		tmp_entities2files = {}
		for (e, e_type),files in self.entities2files.items():
			if e in validEntities:
				if (e, e_type) in tmp_entities2files:
					tmp_entities2files[(e, e_type)].update(set(files))
				else:
					tmp_entities2files[(e, e_type)] = set(files)
		self.entities2files = tmp_entities2files

		self.dygiepp2files = self.applyValidEntities(validEntities, self.dygiepp2files)
		self.openie2files = self.applyValidEntities(validEntities, self.openie2files)
		self.pos2files = self.applyValidEntities(validEntities, self.pos2files)
		self.dependency2files = self.applyValidEntities(validEntities, self.dependency2files)

	###################################################################################################################################
	

	def entitiesTyping(self):
		self.e2types = {}
		for (e, e_type), files in self.entities2files.items():
			if e not in self.e2types:
				self.e2types[e] = {}
			
			if e_type != 'Generic':
				self.e2types[e][e_type] = len(files)
			else:
				if 'OtherScientificTerm' in self.e2types[e]:
					self.e2types[e]['OtherScientificTerm'] += len(files)
				else:
					self.e2types[e]['OtherScientificTerm'] = len(files)

		for e in self.e2types:
			occurence_count = self.e2types[e]
			
			# most frequent ignoring OtherEntity and CSOTopic
			selected_type = None
			max_freq = 0
			for etype,freq in dict(occurence_count).items():
				if etype != 'OtherScientificTerm' and etype != 'CSOTopic' and freq > max_freq:
					selected_type = etype
					max_freq = freq

			# if no Material, Method, etc. the CSOTopic is the type
			if selected_type == None and 'CSOTopic' in dict(occurence_count).keys():
				selected_type = 'OtherScientificTerm'
			elif selected_type == None:
				selected_type = 'OtherScientificTerm'

			self.e2selected_type[e] = selected_type
		
		with open('../../resources/e2selected_type.pickle', 'wb') as f:
			pickle.dump(self.e2selected_type, f)


	def createCheckpoint(self, name, els):
		with open('./ckpts/' + name + '.pickle', 'wb') as f:
			pickle.dump(els, f)

	def loadCheckpoint(self, name):
		with open('./ckpts/' + name + '.pickle', 'rb') as f:		
			return pickle.load(f)

	def run(self):
		print('--------------------------------------')
		print('>> Loading')
		try:
			self.dygiepp2files, self.openie2files, self.pos2files, self.dependency2files, self.entities2files = self.loadCheckpoint('loading')
		except:
			self.loadData()
			self.createCheckpoint('loading', (self.dygiepp2files, self.openie2files, self.pos2files, self.dependency2files, self.entities2files))
		print(' \t- dygiepp triples:\t', len(self.dygiepp2files))
		print(' \t- openie triples:\t', len(self.openie2files))
		print(' \t- pos triples:\t\t', len(self.pos2files))
		print(' \t- dep triples:\t\t', len(self.dependency2files))
		print('--------------------------------------')


		print('>> Entity cleaning')
		try:
			self.dygiepp2files, self.openie2files, self.pos2files, self.dependency2files, self.entities2files = self.loadCheckpoint('cleaning')
		except:
			ec = EntitiesCleaner(set([e for (e,e_type) in self.entities2files.keys()]))
			ec.run()
			cleaner_map = ec.get()
			self.updateThroughCleanerMap(cleaner_map)
			del cleaner_map
			gc.collect()
			self.createCheckpoint('cleaning', (self.dygiepp2files, self.openie2files, self.pos2files, self.dependency2files, self.entities2files))
		print(' \t- dygiepp triples:\t', len(self.dygiepp2files))
		print(' \t- openie triples:\t', len(self.openie2files))
		print(' \t- pos triples:\t\t', len(self.pos2files))
		print(' \t- dep triples:\t\t', len(self.dependency2files))
		print('--------------------------------------')

		print('>> Entity validation')
		try:
			self.dygiepp2files, self.openie2files, self.pos2files, self.dependency2files, self.entities2files = self.loadCheckpoint('validation')
		except:
			ev = EntitiesValidator(set([e for (e,e_type) in self.entities2files.keys()]))	
			ev.run()
			valid_entities = ev.get()
			self.updateThroughValidEntities(valid_entities)
			del ev
			gc.collect()
			self.createCheckpoint('validation', (self.dygiepp2files, self.openie2files, self.pos2files, self.dependency2files, self.entities2files))
		print(' \t- dygiepp triples:\t', len(self.dygiepp2files))
		print(' \t- openie triples:\t', len(self.openie2files))
		print(' \t- pos triples:\t\t', len(self.pos2files))
		print(' \t- dep triples:\t\t', len(self.dependency2files))
		print('--------------------------------------')


		print('>> Relations handling')
		try:
			self.dygiepp_pair2info, self.openie_pair2info, self.pos_pair2info, self.dep_pair2info = self.loadCheckpoint('relations_handler')
		except:
			rm = RelationsManager(self.dygiepp2files, self.pos2files, self.openie2files, self.dependency2files)
			rm.run()
			self.dygiepp_pair2info, self.pos_pair2info, self.openie_pair2info, self.dep_pair2info = rm.get()
			del rm
			del self.dygiepp2files
			del self.openie2files
			del self.pos2files
			del self.dependency2files
			gc.collect()
			self.createCheckpoint('relations_handler', (self.dygiepp_pair2info, self.openie_pair2info, self.pos_pair2info, self.dep_pair2info))
		print(' \t- dygiepp pairs:\t', len(self.dygiepp_pair2info))
		print(' \t- openie pairs:\t\t', len(self.openie_pair2info))
		print(' \t- pos pairs:\t\t', len(self.pos_pair2info))
		print(' \t- dep pairs:\t\t', len(self.dep_pair2info))
		print('--------------------------------------')

		print('>> Mapping to external resources')
		try:
			self.e2cso, self.e2dbpedia, self.e2wikidata = self.loadCheckpoint('mapping')
		except:
			all_pairs = set(self.dygiepp_pair2info.keys()) | set(self.pos_pair2info.keys()) | set(self.openie_pair2info.keys()) | set(self.dep_pair2info.keys())
			mapper = EntitiesMapper([e for e, t in self.entities2files.keys()], all_pairs)
			mapper.run()
			self.e2cso, self.e2dbpedia, self.e2wikidata = mapper.getMaps()
			del mapper
			gc.collect()
			self.createCheckpoint('mapping', (self.e2cso, self.e2dbpedia, self.e2wikidata))
		print('--------------------------------------')

		print('>> Data dumping and merging')
		self.entitiesTyping()
		dumper = KGDataDumper(self.dygiepp_pair2info, self.pos_pair2info, self.openie_pair2info, self.dep_pair2info, self.e2cso, self.e2dbpedia, self.e2wikidata, self.e2selected_type)	
		dumper.run()
		print('--------------------------------------')




if __name__ == '__main__':
	if not os.path.exists('./ckpts/'):
		os.makedirs('./ckpts/')
	tg = TriplesGenerator()
	tg.run()
	


