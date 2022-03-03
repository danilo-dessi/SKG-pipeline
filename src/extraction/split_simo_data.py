import json
import os

c = 0
fw = open('../../dataset/computer_science/computer_science_' + str(int(c/7500)) + '.json', 'w+')

already_parsed = os.listdir('../../dataset/computer_science/')
files_to_parse = [filename for filename in os.listdir(data_path) if filename not in already_parsed]
print(len(already_parsed), len(files_to_parse), files_to_parse)

for file in os.listdir('../../dataset/simo/'):

	if file[-5:] != '.json':
		continue

	
	papers = None
	with open('../../dataset/simo/' + file, 'r', encoding='utf-8') as f:
		print('> processing:', file)
		content = f.read()
		papers = json.loads(content)

	for paper in papers: 
		c += 1
		json.dump(paper, fw)
		fw.write('\n')

		if c % 7500 == 0:
			fw = open('../../dataset/computer_science/computer_science_' + str(int(c/7500)) + '.json', 'w+')

fw.flush()
fw.close()