[![Gitter](https://img.shields.io/gitter/room/DAVFoundation/DAV-Contributors.svg?style=flat-square)](https://gitter.im/OPTIMA)


# OPTIMA: Optimal Virtual Data Model for Querying Large Heterogeneous Data
OPTIMA is an Ontology-Based Big Data Access (Smeantic Data Lakes) that supports two types of virtual data model GRAPH and TABULAR and predicts the optimal one based on the SPARQL query behavior using deep learning algorithm.

Currently OPTIMA integrates:
- Two different virtual data models, GRAPH and TABULAR, to join and aggregate data.
- Deep learning method to select automatically the optimal virtual model based on query.
- State-of-the-art Big Data engines Apache Spark and Spark Graphx and supports out-of-the-box five data sources Neo4j, MongoDB, MySQL, Cassandra and CSV.

## Usage (Live Demo)

We provide a [live demo version](http:/) of OPTIMA, which can be used to try our software without registration.
Additionally, [this short video](docs/OPTIMA-demo.mp4) shows an example of a query execution in case the predicted optimal virtual data model is GRAPH.

![video](docs/optima-demo.mp4)


To enable the reproducibility of OPTIMA, we refer first to the baiscs on the following  Wiki page: [OPTIMA Basics](https://github.com/). which also helps understand the installation steps hereafter.

## Architecture Overview

OPTIMA' virtual data model prediction component selects the optimal virtual data model GRAPH or TABULAR based on the query behavior.
The rest of the OBDA components make use of the selected virtual data model GRAPH or TABULAR for querying.

![OPTIMA architecture](docs/Optima_architecture.png)

## Getting Started
__Local setup:__

```
git clone https://github.com/chahrazedbb/OPTIMA.git
cd OPTIMA
mvn package
cd target
```

The depp learning model built on top of OPTIMA aims to select the optimal virtual data model GRAPH or TABULAR based on the query behavior. This component addresses the challenge of selecting optimal virtual data model by receiving the SPARQL query as input and predicts the cost against each virtual data model using deep neural network.

### TorchServe
- To be able to use the OPTIMA-machine-learning model in OPTIMA project we deploy the model as a service using TorchServe. TorchServe is a flexible tool for serving PyTorch torschripted models.
  - install TorchServe and torch-model-archiver following the guidlines in [serve documentation page](https://github.com/pytorch/serve).
  - Download OPTIMA pre-trained model from [OPTIMA-ML repo]()
  - Archive the OPTIMA pre-trained model using the model archiver.

    ```
    torch-model-archiver --model-name OPTIMA_trained_model --version 1.0 --model-file model.py --serialized-file OPTIMA_trained_model.pth --handler virtual_model_handler.py
    ```
  - Start TorchServe to serve the model
    ```
    torchserve --start --ncs --model-store model_store --models OPTIMA_trained_model.mar
    ```

OPTIMA uses Spark as query engine and implements two virtual data models GRAPH and TABULAR. For GRAPh Virtual Data Model the Spark Graphx API is used and For TABULAR Virtual Data Model, the Apache Spark API is used. Therefore, Spark with both libriries (Spark Graphx and Apache Spark) has to be installed beforehand. The selection of Optimal Virtural Data Model GRAPH or TABULAR is based on deep learning model based on query bevahior introduced above.

### Spark
- Download Spark from the [Spark official website](https://spark.apache.org/downloads.html). In order for Spark to run in a cluster, you need to configure a 'standalone cluster' following guidelines in the [official documentation page](https://spark.apache.org/docs/2.2.0/spark-standalone.html).


### Spark Graphx
- Download Spark GraphX API (https://spark.apache.org/docs/latest/graphx-programming-guide.html) from the [Spark official website](https://spark.apache.org/...). In order for Spark to run in a cluster, you need to configure a 'standalone cluster' following guidelines in the [official documentation page](https://spark.apache.org/docs/2.2.0/spark-standalone.html).


__DISCLAIMER:__
- The steps above are valid to run in Ubuntu 20.04.4 LTS, Maven and intellij IDE

__Major Versions:__
- Scala 2.11
- Graphx 2.3.4
- Spark 2.3.4

__Troubleshooting:__
- If any error raised check if node version compatibility.

## Technical Workflow
After starting OPTIMA, you can test it by using one of the queries available in [queries repo](evaluation/queries).

### Perquisite Input and output
To enable querying distributed heterogeneous large data source using the ontology-based big data access principles, to do so OPTIMA requires the following inputs:   
- Data sources (CSV file) 
- Query file (query.sparql) 
- Mapping file (mapping.ttl) are available in [evaluation repo](evaluation)
Output is the SAPRQL query result 

### Example
The query can be tested using GRAPH and TABULAR to get an overview about time difference between the two models. GRAPH can be faster than TABULAR for some queries and vice versa.

```
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>

SELECT  DISTINCT ?label ?comment ?propertyNum1 ?product
WHERE {
	?product rdfs:label ?label .
	?product rdfs:comment ?comment .
	?product bsbm:productPropertyNumeric1 ?propertyNum1 .
	?product rdf:type bsbm:Product .
	FILTER (?label = "ahchoo") .
	FILTER (?propertyNum1 <= 1000) .
}
```


## Evaluation
We use an adopted version of [BSBM benchmark](bizer2009berlin) where five tables Product, Offer, Review, Person and Producer are distributed among different data storage.
The tables are loaded in five different data sources Cassandra, MongoDB, CSV, Neo4j and MySQL.

To test OPTIMA you can use the input files and queries available in [evaluation repo](evaluation).

## Extensibility
OPTIMA can be extented by adding new connectors. For example to connect to parquet database use the parquet connector as follows:

#### For TABULAR
``` spark.read.options(options).parquet(sourcePath)```
#### For GRAPH
``` sqlContext.read.(sourcePath).rdd ```

## Publications and How to cite
We are currently preparing a scientific paper for a peer-reviewed publication. Please refer to [references.bib](references.bib) for BibTex references, which we will update continuously.

## Contact
For any setup difficulties or other inquiries, please contact , or ask directly on [Gitter chat](https://gitter.im/).

*Not only for issues, but also for successes, let us know either way :)*

License
-------

This project is openly shared under the terms of the __Apache License
v2.0__ ([read for more](./LICENSE)).


