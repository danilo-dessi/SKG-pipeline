# command to run the stanford core nlp server
# nohup java -mx8g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -port 9050 -timeout 15000 >/dev/null 2>&1 &

from stanfordcorenlp import StanfordCoreNLP
import multiprocessing as mp
import json
import sys
import os


data_path = '../../dataset/'
output_dir = '../../outputs/corenlp_output/'
nlp = StanfordCoreNLP('http://localhost', port=9050)

def corenlp_extraction(filename):
	print('> processing:', filename)

	f = open(data_path + filename, 'r', encoding='utf-8')
	content = f.read()
	json_content = json.loads(content)
	f.close()

	fw = open(output_dir + filename, 'w+')

	for hit in json_content['hits']['hits']: 
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

		corenlp_out = {}
		props = {'annotators': 'openie,tokenize,pos,depparse', 'pipelineLanguage': 'en', 'outputFormat': 'json'}
		try:
			corenlp_out = json.loads(nlp.annotate(title.encode('utf8', 'ignore').decode('ascii', 'ignore') + '. ' + abstract.encode('utf8', 'ignore').decode('ascii', 'ignore'), properties=props))
		except Exception as e:
			print(e)
	
		json.dump({
					'doc_key' : paper_id,
					'corenlp_output' : corenlp_out
				},fw)
		fw.flush()
	fw.close()



if __name__ == '__main__':

	if len(sys.argv) != 2:
		print('python stanfordcore_nlp_extractor.py NUM_PROCESSES')
		exit(1)

	number_of_processes = int(sys.argv[1])	

	try:
		os.mkdir(output_dir)
		print("Directory" , output_dir ,  "created ") 
	except FileExistsError:
		print("Directory" , output_dir ,  "already exists")

	already_parsed = os.listdir(output_dir)
	files_to_parse = [filename for filename in os.listdir(data_path) if filename not in already_parsed]
	print('> Start')
	pool = mp.Pool(number_of_processes)
	result = pool.map(corenlp_extraction, files_to_parse)





	

	 








