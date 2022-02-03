import json
import nltk
import os

data_path = '../../dataset/'
data_output_dir = '../../outputs/dygiepp_input/'

try:
    os.mkdir(data_output_dir)
    print("Directory " , data_output_dir ,  " Created ") 
except FileExistsError:
    print("Directory " , data_output_dir ,  " already exists")

c = 0
for file in os.listdir(data_path):
	if file[-5:] != '.json':
		continue
	fw = open(data_output_dir + file, 'w+')

	with open(data_path + file, 'r', encoding='utf-8') as f:
		print('> processing:', file)
		content = f.read()
		myjson = json.loads(content)
		for hit in myjson['hits']['hits']: 
			c += 1
			if c == 10:
				exit(1)
			source = hit['_source']

			if 'id' in source:
				paper_id = source['id']
			else:
				paper_id = ''
				continue

			if 'papertitle' in source:
				title = source['papertitle']
			else:
				title = ''
				continue

			if 'abstract' in source:
				abstract = source['abstract']
			else:
				abstract = ''
				continue

			if 'doi' in source:
				doi = source['doi']
			else:
				doi = ''

			if 'topics' in source:
				topics = source['topics']
			else:
				topics = []

			if 'cso_syntactic_topics' in source:
				cso_syntactic_topics = source['cso_syntactic_topics']
			else:
				cso_syntactic_topics = []

			if 'cso_semantic_topics' in source:
				cso_semantic_topics = source['cso_semantic_topics']
			else:
				cso_semantic_topics = []

			if 'cso_enhanced_topics' in source:
				cso_enhanced_topics = source['cso_enhanced_topics']
			else:
				cso_enhanced_topics = []

			sentences = nltk.sent_tokenize(abstract)
			if len(sentences) <= 15: # maximum abstracts with 15 sentences
				
				sentences_tokenized = []
				for s in sentences:
					tokens = [ t for t in nltk.word_tokenize(s.encode('utf8', 'ignore').decode('ascii', 'ignore')) if t not in ['']]
					# sentences: maximum 250 tokens, at least 5 tokens, at maximum 5 dots
					if len(tokens) <= 250 and len(tokens) >= 5:
						sentences_tokenized += [tokens]
				sentences_tokenized = [s for s in sentences_tokenized if s != []] # no empty sentences after ignoring ascii
				
				if len(sentences_tokenized) >= 1:
					data_input_for_dygepp = json.dump({
													'clusters' : [[] for x in range(len(sentences_tokenized))],
													'sentences' : sentences_tokenized,
													'ner' : [[] for x in range(len(sentences_tokenized))],
													'relations' : [[] for x in range(len(sentences_tokenized))],  
													'doc_key' : str(paper_id)
												},fw)
					fw.write('\n')
	
	fw.flush()
	fw.close()


