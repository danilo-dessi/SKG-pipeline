import json
import nltk
import os

data_path = '../../dataset/computer_science/'
data_output_dir = '../../outputs/cso_output/'

try:
    os.mkdir(data_output_dir)
    print("Directory " , data_output_dir ,  " Created ") 
except FileExistsError:
    print("Directory " , data_output_dir ,  " already exists")

already_parsed = os.listdir(data_output_dir)
files_to_parse = [filename for filename in os.listdir(data_path) if filename not in already_parsed]
print('> total files:', len(os.listdir(data_path)))
print('> already parsed:', len(already_parsed))
print('> files_to_parse:', len(files_to_parse))

for file in sorted(files_to_parse):
	if file[-5:] != '.json':
		continue
	
	fw = open(data_output_dir + file, 'w+')

	with open(data_path + file, 'r', encoding='utf-8') as f:
		print('> processing:', file)

		for paper_row in f:
			paper = json.loads(paper_row)
			source = paper['_source']
			print(paper['_source'].keys())
			exit(1)#c += 1
			#if c == 100:
			#	exit(1)

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

			sentences = nltk.sent_tokenize(abstract)
			if len(sentences) <= 20: # maximum abstracts with 15 sentences
				
				sentences_tokenized = []
				for s in sentences:
					tokens = [ t for t in nltk.word_tokenize(s.encode('utf8', 'ignore').decode('ascii', 'ignore')) if t not in ['']]
					# sentences: maximum 250 tokens, at least 5 tokens, at maximum 5 dots
					if len(tokens) <= 250 and len(tokens) >= 5:
						sentences_tokenized += [tokens]
				sentences_tokenized = [nltk.word_tokenize(title.encode('utf8', 'ignore').decode('ascii', 'ignore'))] + sentences_tokenized
				sentences_tokenized = [s for s in sentences_tokenized if len(s) >= 2] # no empty sentences after ignoring ascii, at least two tokens
				
				if len(sentences_tokenized) >= 1:
					data_input_for_dygepp = json.dump({
													'clusters' : [[] for x in range(len(sentences_tokenized))],
													'sentences' : sentences_tokenized,
													'ner' : [[] for x in range(len(sentences_tokenized))],
													'relations' : [[] for x in range(len(sentences_tokenized))],  
													'doc_key' : str(paper_id),
													'dataset' : 'scierc'
												},fw)
					fw.write('\n')
	
	fw.flush()
	fw.close()


