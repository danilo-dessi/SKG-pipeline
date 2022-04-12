from transformers import AutoModelForSequenceClassification
from sklearn.preprocessing import MultiLabelBinarizer
from transformers import TrainingArguments
from transformers import AutoTokenizer
from transformers import Trainer
import pandas as pd
import torch
import csv
import ast
import sys
import re



class MyDataset(torch.utils.data.Dataset):
	def __init__(self, encodings, labels):
		self.encodings = encodings
		self.labels = labels

	def __getitem__(self, idx):
		item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
		item['labels'] = torch.tensor(self.labels[idx]).float()
		return item

	def __len__(self):
		return len(self.labels)


model_name = 'bert-base-uncased'
output = 'output'
version = None
if len(sys.argv) == 4:
	model_name = sys.argv[1]
	s1 = int(sys.argv[2])
	s2 = int(sys.argv[3])
else:
	print('python applyModel MODEL s1 s2')
	exit(1)

data = pd.read_csv('../construction/aikg_data/aikg_triples.csv')
print('> data size:', data.shape)

datav = data[(data['subj'] != data['obj']) & ((data['support'] >= s1) | (data['source_len'] >= s2))]
datatocheck = data[(data['subj'] != data['obj']) & ((data['support'] < s1) & (data['source_len'] < s2))]

print('\t>> num reliable triples dataframe size:', datav.shape)
print('\t>> unrealiable triples to check datframe size:', datatocheck.shape)

datav.to_csv('triples_reliable.csv', index=False)

datatocheck = datatocheck.sample(frac=1).reset_index(drop=True)

subjects = datatocheck['subj']
relations = datatocheck['rel']
objects = datatocheck['obj']
tocheck_triples = list(zip(subjects, relations, objects))

texts = []
labels = []
for (s,p,o) in tocheck_triples:
	texts += [str(s) + ' ' + str(p) + ' ' + str(o)]
	labels += [(0,1)]
print('\t>> unrealiable triples to check:', len(texts))


tokenizer = AutoTokenizer.from_pretrained(model_name)
encodings = tokenizer(texts, truncation=True, padding=True)
dataset = MyDataset(encodings, labels)

training_args = TrainingArguments(
	output_dir='./tune_predict/',    	# output directory
	logging_dir='./tune_predict/logs',  # directory for storing logs
	per_device_train_batch_size=128,
	logging_strategy='no',
	log_level='critical',
	per_device_eval_batch_size=128
)

model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=2)
trainer = Trainer(model=model, args=training_args)

predictions, label_ids, metrics = trainer.predict(dataset)
predictions = torch.from_numpy(predictions).sigmoid() > 0.5
predictions = predictions.int()

predicted_labels = [1 if tuple(x) == (0,1) else 0 for x in predictions] 
print('\t>> number of predicted 1 (consistent triples)', predicted_labels.count(1))
print('\t>> number of predicted 0 (discarded triples)', predicted_labels.count(0))

datatocheck['predicted_labels'] = predicted_labels
datatocheck.to_csv('triples_classified.csv', index=False)




