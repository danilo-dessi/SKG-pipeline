# Scientific Knowledge Graph Exctraction Pipeline

This repository contains the source code of the work described in:

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

Please download the [data]() from and copy it in the main folder of the project '/'

##### Extraction process

Go to **/src/extraction/** and execute:

1. ```python data_preparation_dygiepp.py```

2. ```./run_dygiepp.sh```

3. ```./run_corenlp.sh```

Output folders will be automatically created.
 

##### Triples Generation

Go to **/src/transformer/** and execute:

```python cskg_construction.py ```


##### Triples Validation

Go to **/src/transformer/** and execute to prepare the triple for the validation step:

```python prepareTrainingData.py SUPPORT_S1 SUPPORT_S2```

To finetune the transformer model you should execute ```python finetuner.py``` which will save the model under *./tuned-transformer/*.

Finally, you can apply the model on the triples accordingly to predefined thresholds by using: ```python applyModel.py MODEL_NAME SUPPORT_S1 SUPPORT_S2```

- *MODEL_NAME* is the model saved under *./tuned-transformer/*

- *SUPPORT_S1* and *SUPPORT_S2* are the two thresholds you would like to use to select the reliable triples for the finetunig step to select which triples must be validated.

##### Mapping to the Ontology and Knowledge Graph Generation

Go to **/src/rdfmaker/** and run:

```python RDFer.py DATA_RELIABLE DATA_CLASSIFIED KG_NAME```

- *DATA_RELIABLE*  and *DATA_CLASSIFIED* are the files that have been generated in the triples validation step

- *KG_NAME* is the name you can choose for your KG

## Contacts

If you have any questions about our work or issues with the repository (e.g., bugs), please contact Danilo Dessì by email \(danilo_dessi@unica.it\)


Enjoy generating knowledge graphs:)

























