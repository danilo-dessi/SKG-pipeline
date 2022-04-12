
from sklearn.metrics import f1_score, recall_score, precision_score, classification_report
from transformers import AutoModelForSequenceClassification
from sklearn.preprocessing import LabelBinarizer, MultiLabelBinarizer
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import confusion_matrix
from transformers import TrainingArguments
from transformers import AutoTokenizer
from transformers import Trainer
from torch import nn
import random
import pickle
import torch
import sys
import csv
import ast
import re
import os

# For reference
#https://colab.research.google.com/drive/1X7l8pM6t4VLqxQVJ23ssIxmrsc4Kpc5q?usp=sharing#scrollTo=1eVCRpcLUW-y

# Tuning on a sequence classification task
#"bert-base-cased"
#'roberta-base'
#'all-mpnet-base-v2'
# sentence-transformers/stsb-roberta-large
#roberta-large

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


def load(obj_path):
	f = open(obj_path, 'rb')
	return pickle.load(f)

model_name = 'bert-base-uncased'
if len(sys.argv) == 2:
	model_name = sys.argv[1]
else:
	print('python tune_predict_binary  BERT_MODEL VERSION')
	exit(1)


train_texts = load('./dataset/full_train_texts.pickle')
train_labels_original = load('./dataset/full_train_labels.pickle')
general_test_texts = load('./dataset/full_test_texts.pickle')
general_test_labels_original = load('./dataset/full_test_labels.pickle')

train_labels = [(1,0) if label == 0 else (0,1) for label in train_labels_original ]
general_test_labels = [(1,0) if label == 0 else (0,1) for label in general_test_labels_original ]

print('> train size:', len(train_texts))
print('> test size', len(general_test_labels))

tokenizer = AutoTokenizer.from_pretrained(model_name)
train_encodings = tokenizer(train_texts, truncation=True, padding=True)
general_test_encodings = tokenizer(general_test_texts, truncation=True, padding=True)

train_dataset = MyDataset(train_encodings, train_labels)
general_test_dataset = MyDataset(general_test_encodings, general_test_labels)


training_args = TrainingArguments(
	output_dir='./ckpts/',    # output directory
	num_train_epochs=10,             # total number of training epochs
	logging_dir='./ckpts/logs',    # directory for storing logs
	per_device_train_batch_size=128,
	logging_strategy='no',
	save_strategy='no',
	per_device_eval_batch_size=128
)

model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=2)
trainer = Trainer(
	model=model,                         # the instantiated Transformers model to be trained
	args=training_args,                  # training arguments, defined above
	train_dataset=train_dataset			 # training dataset
	)

trainer.train()

print('> prediction on test set')
predictions, label_ids, metrics = trainer.predict(general_test_dataset)
predictions = torch.from_numpy(predictions).sigmoid() > 0.5
predictions = predictions.int()

# printing examples for manuale exploration of the results
print('\t>> example predictions:', [1 if tuple(x) == (0,1) else 0 for x in predictions[:20]] )
print('\t>> example labels     :', [1 if tuple(x) == (0,1) else 0 for x in list(general_test_labels)[:20]] )

# evaluation on a test set
print(classification_report(general_test_labels, predictions))

if not os.path.exists('./tuned-transformer/'):
	os.makedirs('./tuned-transformer/')

trainer.save_model('./tuned-transformer/')
tokenizer.save_pretrained('./tuned-transformer/')
print('\t>> output model in:', './tuned-transformer/')

print('------------------------------------------------------------------------------')

	







