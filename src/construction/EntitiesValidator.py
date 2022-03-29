from nltk.corpus import wordnet as wn
from nltk.corpus import wordnet_ic
from urllib.parse import unquote
import pickle
import json
import nltk
import csv
import os



class EntitiesValidator:
	
	def __init__(self, entities):
		self.inputEntities = entities
		self.csoResourcePath = '../../resources/CSO.3.1.csv'
		self.blacklist_path = '../../resources/blacklist.txt'
		self.mag_topics_dir = '../../dataset/computer_science/'
		self.csoTopics = set()
		self.magTopics = set()
		self.validEntities = set()
		self.blacklist = set()


	def loadCSOTopics(self):
		with open(self.csoResourcePath, 'r', encoding='utf-8') as csv_file:
			csv_reader = csv.reader(csv_file, delimiter=',')
			for row in csv_reader:
				t1 = unquote(row[0]).replace('<https://', '')[:-1]
				t2 = unquote(row[2]).replace('<https://', '')[:-1]
				if t1.startswith('cso.kmi.open.ac.uk/topics/'):
					t1 = t1.split('/')[-1]
					self.csoTopics.add(t1.lower().replace('_',' '))
				if t2.startswith('cso.kmi.open.ac.uk/topics/'):
					t2 = t2.split('/')[-1]
					self.csoTopics.add(t2.lower().replace('_',' '))


	def loadBlacklist(self):
		with open(self.blacklist_path) as f:
			for line in f.readlines():
				self.blacklist.add(line.strip())


	def loadMAGTopics(self):
		for filename in os.listdir(self.mag_topics_dir):
			topics = []
			if filename[-5:] == '.json': 
				f = open(self.mag_topics_dir + filename, 'r')
				for row in f:
					paper_data = json.loads(row.strip())
					topics += paper_data['_source']['topics']
				f.close()
		self.magTopics = set(topics)


	def validation(self):
		brown_ic = wordnet_ic.ic('ic-brown.dat')
		semcor_ic = wordnet_ic.ic('ic-semcor.dat')
		for e in self.inputEntities:
			if e in self.blacklist or len(e) <= 2 or e.isdigit() or e[0].isdigit() or len(nltk.word_tokenize(e)) >= 7:# # no blacklist, no 1-character entities, no only numbers, no entities that start with a number, no entities with more than 7 tokens
				continue			
  
			if e in self.csoTopics:
				self.validEntities.add(e)
			elif e in self.magTopics:
				self.validEntities.add(e)
			else:
				valid = True
				for synset in wn.synsets(e):
					ic_value = semcor_ic['n'][synset.offset()]
					if ic_value <= 4 and ic_value > 0:
						valid = False
						#print(e, 'NOT', ic_value)
						break
				if valid:
					self.validEntities.add(e)

	def run(self):
		self.loadCSOTopics()
		self.loadBlacklist()
		self.loadMAGTopics()
		self.validation()


	def get(self):
		return self.validEntities



if __name__ == '__main__':
	ev = EntitiesValidator(['computer science', 'danilo', 'svm', 'machine learning', 'fghjjjj', 'hello', 'method', 'methods', 'test', 'neural networks', 'a320', '320', '320a'], '../dataset/AI_whitelisted_parsed/')
	ev.run()
	print(ev.get())











