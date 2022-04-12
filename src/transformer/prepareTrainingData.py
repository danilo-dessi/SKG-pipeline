from sklearn.preprocessing import MultiLabelBinarizer
import pandas as pd
import pickle
import random
import csv
import sys
import ast
import re
import os


'''
This script is used to prepare the triples to train a classifier <s,p,o> -> [0,1].
They are prepared for a sequence classification task. Each triple <s,p,o> is converted in a string s + p + o

'''


def save(obj, obj_path):
	f = open(obj_path, 'wb')
	pickle.dump(obj, f)
	f.flush()
	f.close()

if len(sys.argv) == 4:
	version = sys.argv[1]
	s1 = int(sys.argv[2])
	s2 = int(sys.argv[3])
else:
	print('python prepareTrainingData.py s1 s2')
	exit(1)

if not os.path.exists('./dataset/'):
    os.makedirs('./dataset/')

line_counter = 0
p2id = {}
pid = 0


data = pd.read_csv('aikg_triples.csv')
print('\t>> Dataframe all triples size:', data.shape)
#data['source_len'] = [len(ast.literal_eval(x)) for x in data['sources']]

datav = data[(data['subj'] != data['obj']) & ((data['support'] >= s1) | (data['source_len'] >= s2))]
print('\t>> Data realiable dataframe size:', datav.shape)

all_entities = set()
all_relations = set()
all_triples = set()

subjects = data['subj']
relations = data['rel']
objects = data['obj']
all_triples = set(list(zip(subjects, relations, objects)))
print('\t>> all triples:', len(all_triples))

subjects = datav['subj']
relations = datav['rel']
objects = datav['obj']
v_triples = set(list(zip(subjects, relations, objects)))
print('\t>> reliable triples:', len(v_triples))

v_entities = set()
v_relations = set()

texts = []
labels = []
for (s,p,o) in v_triples:
	texts += [str(s) + ' ' + str(p) + ' ' + str(o)]
	labels += [1]
	v_entities.add(s)
	v_entities.add(o)
	v_relations.add(p)
print('> v triples:', len(v_triples))

#negative samples generation
c = len(v_triples)

v_entities = list(v_entities)
v_relations = list(v_relations)
v_entities_len = len(v_entities) - 1
v_relations_len = len(v_relations) - 1
c =len(v_triples)
for (s,p,o) in v_triples:

	# replace of the subject or object of triples
	if c % 2 == 0:
		on = random.randint(0, v_entities_len)
		o_new = v_entities[on]

		if (s,p,o_new) not in all_triples:
			texts += [str(s) + ' ' + str(p) + ' ' + str(o_new)]
			labels += [0]
	else:
		sn = random.randint(0, v_entities_len)
		s_new = v_entities[sn]

		if (s_new,p,o) not in all_triples:
			texts += [str(s_new) + ' ' + str(p) + ' ' + str(o)]
			labels += [0]

	if c % 25000 == 0:
		print('\t>> remaining to be generated:', c)

	c -= 1


c = list(zip(texts, labels))
random.shuffle(c)
random.shuffle(c)
random.shuffle(c)
random.shuffle(c)
random.shuffle(c)
texts, labels = zip(*c)
texts = list(texts)
labels = list(labels)


idx = int(0.8 * len(texts))

train_texts = texts[:idx]
train_labels = labels[:idx]
test_texts = texts[idx:]
test_labels = labels[idx:]

save(train_texts, './dataset/full_train_texts.pickle')
save(train_labels, './dataset/full_train_labels.pickle')
save(test_texts, './dataset/full_test_texts.pickle')
save(test_labels, './dataset/full_test_labels.pickle')
	

print('\t>> train positive:', sum(train_labels))
print('\t>> train negative:', len(train_labels) - sum(train_labels))
print('\t>> test positive:', sum(test_labels))
print('\t>> test negative:', len(test_labels) - sum(test_labels))
