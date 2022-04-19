import pandas as pd
import collections
import pickle


df = pd.read_csv('../src/construction/cskg_data/cskg_triples.csv')

'''subjs = df['subj']
objs = df['obj']

entities = subjs + objs
counter=collections.Counter(entities)
ef = sorted([ e,c for (e,c) in counter.items()], key=lambda x : x[1])
print(ef)
'''

sf = {}
for i,r in df.iterrows():
	s = r['support']
	if s not in sf:
		sf[s] = 0
	sf[s] += 1


sorted_sf = sorted(sf.items(), key=lambda x : x[1], reverse=True)

f = open('sf.pkl', 'wb')
pickle.dump(sf, f)
f.close()

for s,f in sorted_sf:
	print(s,f)






