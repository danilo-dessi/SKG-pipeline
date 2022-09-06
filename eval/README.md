# Content of the gold standard used for the evaluation

The gold standard CS-KG3600 is provided as CSV file. Its columns are:

- *subj* the subject/head of the triple

- *rel* the predicate of the triple

- *obj* the object/tail of the triple

- *support*	the support indicating the number of papers

- *sources*	the sources that recognized the triple

- *files* the MAG ids of papers that are used to generate the triple

- *subj_type* the type of the subject/head
	
- *obj_type* the type of the object/tail

- *discarded_by_classifier* it has value 1 if the triple was discarded by the classifier, nothing otherwise

- *discarded_by_onto* it has value 1 if the triple was discarded by the ontology, nothing otherwise

- *random_generated* it has velaue 1 if the triple was randomly generated, nothing otherwise

- *cskg* it has value 1 if the triple is part of the knowledge graph, 0 otherwise

- *ann1* annotations from expert 

- *ann2*  annotations from expert 

- *ann3*  annotations from expert 

- *gs* gold standard computed as majority vote of *ann1*, *ann2*, and *ann3*


We additionally provide all the triples from CS-KG3600 in three different files split by the origin sub-domains Information Retrieval (IR), Machine Learning (ML) and Natural Language Processing (NLP).
