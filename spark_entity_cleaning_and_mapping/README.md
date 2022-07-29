# Entity Handling using spark

This directory contains the first efforts to move the whole project on a Apache Spark framenwork to make it scalable and be applicable to the the millions of research papers available today.

It contains:

- *extracted_triples/* which contains an example of data extracted by the extractor tools in ```/SKG-pipeline/tree/main/src/extraction```

- *entities_cleaning_parallel.py* source code that uses the data in *extracted_triples/* and performs the operations in a parallel setting 

- *CSO.3.3.csv* the 3.3 version of CSO downloaded on July 28, 2022. Please refer to https://cso.kmi.open.ac.uk/downloads for more details. 


The user can find information on how to submit the code in a spark cluster by referring to the [public documentation](https://spark.apache.org/docs/latest/submitting-applications.html).
