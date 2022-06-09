# Scientific Knowledge Graph Exctraction Pipeline

This repository has the purpose to provide the code used to build scholarly knowledge graphs as described in:

``` Danilo Dessì, Francesco Osborne, Diego Reforgiato Recupero, Davide Buscaldi, and Enrico Motta. (2022). THIS IS THE TITLE. UNDER REVIEW```


If you use our work or our data, please cite us:

```












```

## Content of the repository

- **/src** This folder contains all the source code.

- **/src/extraction/** contains the scripts that can be used to extract the entities and relationships with DyGIEpp and Stanford Core NLP.

- **/src/construction/** contains the scripts used to generate the triples that needs to be validated and trasnfiormed into RDF.

- **/src/transformer/** contains the scripts used to finetune a transformer model to validate low supported triples.

- **/src/rdfmaker/** contains the scripts to map the triples in the designed ontology and produce the .ttl files composing the generated knowledge graph.


## How to use

**IMPORTANT** Due to legal issues we are not able to provide the full original data. We provide a sample dataset that can be used to use our pipeline.


#### Extraction process (example on the sample data)

Go to **/src/extraction/** and execute:

1. ```python data_preparation_dygiepp.py```

2. ```./run_dygiepp.sh```

3. ```./run_corenlp.sh```

Missing output folders will be automatically created.
 

#### Triples Generation

Go to **/src/construction/** and execute:

```python cskg_construction.py ```


#### Triples Validation

Go to **/src/transformer/** and execute to prepare the triple for the validation step:

1. Download the model from https://zenodo.org/record/6624360#.YqHuCexBw6A and save it under **/src/transformer/tuned-transformer/**

2. To apply the model on the triples accordingly to predefined thresholds in our paper you can run: ```python applyModel.py tuned-transformer/ 3 3```. Generally, the parameters of the command ```python applyModel.py MODEL_NAME SUPPORT_S1 SUPPORT_S2``` indicate
	
	1. *MODEL_NAME* is the model saved under *./tuned-transformer/*

	2. *SUPPORT_S1* and *SUPPORT_S2* are the two thresholds you would like to use to select the reliable triples for the finetunig step to select which triples must be validated.


If you want to finetune a new model you can use:

1. ```python prepareTrainingData.py SUPPORT_S1 SUPPORT_S2```

2. To finetune the transformer model you should execute ```python finetuner.py``` which will save the model under *./tuned-transformer/*.

3. Apply the model on the triples accordingly to predefined thresholds by using: ```python applyModel.py MODEL_NAME SUPPORT_S1 SUPPORT_S2```


#### Mapping to the Ontology and Knowledge Graph Generation

Go to **/src/rdfmaker/** and run:

```python RDFer.py ../transformer/triples_reliable.csv triples_classified.csv cskg```

```python RDFer.py DATA_RELIABLE DATA_CLASSIFIED KG_NAME```

- *DATA_RELIABLE*  and *DATA_CLASSIFIED* are the files that have been generated in the triples validation step

- *KG_NAME* is the name you can choose for your KG

## Contacts

If you have any questions about our work or issues with the repository (e.g., bugs), please contact Danilo Dessì by email \(danilo_dessi@unica.it\)


Enjoy generating knowledge graphs:)

























