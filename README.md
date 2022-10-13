# SCICERO: A deep learning and NLP approach forgenerating scientific knowledge graphsin the computer science domain

This repository provides the code used to build scholarly knowledge graphs as described in:

``` D. Dessí, F. Osborne, D.R. Recupero et al., SCICERO: A deep learning and NLP approach forgenerating scientific knowledge graphs in the computer science domain, Knowledge-Based Systems (2022), doi: https://doi.org/10.1016/j.knosys.2022.109945. ```


If you like our work, please cite us:

```
@article{dessi2022scicero,
title = {SCICERO: A deep learning and NLP approach for generating scientific knowledgegraphs in the computer science domain},
journal = {Knowledge-Based Systems},pages = {109945},
year = {2022},
issn = {0950-7051},
doi = {https://doi.org/10.1016/j.knosys.2022.109945},
url = {https://www.sciencedirect.com/science/article/pii/S0950705122010383},
author = {Danilo Dessí and Francesco Osborne and Diego Reforgiato Recupero and DavideBuscaldi and Enrico Motta},
keywords = {Knowledge graph, Scholarly domain, Scientific facts, Artificial intelligence}}

```


## Content of the repository

- **/src** This folder contains all the source code.

- **/src/extraction/** contains the scripts that can be used to extract the entities and relationships with DyGIEpp and Stanford Core NLP.

- **/src/construction/** contains the scripts used to generate the triples that needs to be validated and trasnfiormed into RDF.

- **/src/transformer/** contains the scripts used to finetune a transformer model to validate low supported triples.

- **/src/rdfmaker/** contains the scripts to map the triples in the designed ontology and produce the .ttl files composing the generated knowledge graph.

- **spark_entity_cleaning_and_mapping/** a folder that contains the first efforts to move the whole project on the [Apache Spark](https://spark.apache.org/) framework. Please note that this is not necessary to create the knowledge graph as described in the paper.


## How to use

**IMPORTANT** Due to legal issues we are not able to provide the full original data. We provide a sample dataset that can be used to use our pipeline.

SCICERO has been tested on python 3.9. We strongly recommend to use the same python version. 

Please install the required libreries using ```pip install -r requirements.txt ```


### Extraction process (example on the sample data)

Go to **/src/extraction/** and execute:

1. ```python data_preparation_dygiepp.py```

2. ```./run_dygiepp.sh```

3. ```./run_corenlp.sh```

Missing output folders will be automatically created.
 


### Triples Generation

This part of the pipeline will clean and merge the entities, will map the entities to external resources, will map the verbs and will create a set of triples that needs to be validated by the next steps before to create the final knowledge graph.


Go to **/src/construction/** and execute:

```python cskg_construction.py ```


Please note the execution of this phase might require more than 3 hours due to the mapping to external resources over Internet and the system used for testing the pipeline.



### Triples Validation

Go to **/src/transformer/** and execute to prepare the triple for the validation step:

1. Download the model (*tuned-transformer.zip*) from https://zenodo.org/record/6628472#.YqIH_-xBw6A and save it under **/src/transformer/tuned-transformer/**

2. To apply the model on the triples accordingly to predefined thresholds in our paper you can run: ```python applyModel.py tuned-transformer/ 3 3```. The command has as interface ```python applyModel.py MODEL_NAME SUPPORT_S1 SUPPORT_S2``` where:
	
	1. *MODEL_NAME* is the model saved under *./tuned-transformer/*

	2. *SUPPORT_S1* and *SUPPORT_S2* are the two thresholds you can use to select the reliable triples for the finetunig step to select which triples must be validated.


Alternatively, if you prefer to finetune the SciBERT transformer model on other data while executing this pipeline you can do as follows:

1. ```python prepareTrainingData.py SUPPORT_S1 SUPPORT_S2```

2. To finetune the SciBERT transformer model on new data you must execute ```python finetuner.py``` which will save the model under *./tuned-transformer/*.

3. Apply the model on the triples accordingly to predefined thresholds by using: ```python applyModel.py MODEL_NAME SUPPORT_S1 SUPPORT_S2```



### Mapping to the Ontology and Final Knowledge Graph Generation

Go to **/src/rdfmaker/** and run:

1. Download the *only_embeddings_label2cskg_entity.pickle* from https://zenodo.org/record/6628472#.YqIH_-xBw6A and move it under **/resources/**

2. Run ```python RDFer.py ../transformer/triples_reliable.csv ../transformer/triples_classified.csv my_new_kg ```

The interface of the command is ```python RDFer.py DATA_RELIABLE DATA_CLASSIFIED KG_NAME``` where:

- *DATA_RELIABLE*  and *DATA_CLASSIFIED* are the files that have been generated in the triples validation step

- *KG_NAME* is the name you can choose for your KG

The output will generate 3 files:

- *my_new_kg.ttl* the knowledge graph in turtle format 

- *my_new_kg_final_data.csv* the data contained by the knowledge graph in csv format

- *my_new_kg_final_data.csv* the data discarded by the pipeline because it does not comply with the ontology


## The Computer Science Knowledge Graph

The user of this github repository can access to the dump of the created resource, its sparql endpoint, and additional material through a dedicated [website](https://scholkg.kmi.open.ac.uk/).


## Contacts

If you have any questions about our work or issues with the repository (e.g., bugs), please contact Danilo Dessì by email \(danilo_dessi@unica.it\)


Enjoy generating knowledge graphs:)

























