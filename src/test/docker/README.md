# Docker images for integration testing


Docker images have been created to use for integration tests on local machines 
and also in CI (Travis). These images are published in DockerHub (https://hub.docker.com/) so that they can be retrieved and used by CI.

## SOLR

To build (change tags e.g. v1 - > v2):

```
cd src/test/docker/solr7
docker build . -t biocache-it-solr7:v1
```

To run locally:

```
docker run --name biocache-it-solr7 -d -p 8983:8983 -t biocache-it-solr7:v1
```

To publish (where "3579a7d6de70" is the  image ID)
```
docker tag 3579a7d6de70 djtfmartin/biocache-it-solr7:v1
docker push djtfmartin/biocache-it-solr7

```


## CASSANDRA

To build

```
cd src/test/docker/cassandra3
docker build . -t biocache-it-cass3:v1
```

To run locally
```
docker run --name biocache-it-cass3:v1 -d -p 9042:9042 -t biocache-it-cass3:v1
```

To publish - the published images are used by travis. 
```
docker tag 3579a7d6de70 djtfmartin/biocache-it-cass3:v1
docker push djtfmartin/biocache-it-cass3

```

## Changing data in docker images

The data in the SOLR and Cassandra instances is intended to support the integration tests in this project. Changing the data may necessitate updating tests. Similarly, to use more or different data in the tests, the images will need to be rebuilt.

There is two methods for adding data to cassandra. Both require running the existing docker image for cassandra and SOLR.

1. Export CSV from cassandra in the running container, edit and re-import.
* Connect to the docker instance using `docker exec -it /bin/bash <image-id>`
* Export existing data using Cassandra CQL `COPY` command
* Modify the data in a text editor 
* Re-import using Cassandra CQL `COPY` command (removing the existing data with CQL and `TRUNCATE` if required)

2. Load data using biocache-store. This requires that the data is registered available in the collectory. For more general details of ALA architecture, see http://github.com/AtlasOfLivingAustralia/documentation

* Download the latest biocache-store distribution zip from here (please adjust version number):

https://nexus.ala.org.au/service/local/artifact/maven/redirect?g=au.org.ala&a=biocache-store&v=2.4.7&c=distribution&e=zip&r=releases

* Unzip the biocache-store-distribution.zip

* Create a configuration file in the path /data/biocache/config/biocache-config.properties

```
db=cassandra3
cassandra.hosts=localhost
cassandra.port=9042
local.node.ip=127.0.0.1
cassandra.pool=biocache-store-pool
cassandra.async.updates.threads=1
cassandra.keyspace=occ
exclude.sensitive.values=false
extra.misc.fields=
solr.collection=biocache
solr.home=http://localhost:8983/solr/biocache
sds.enabled=false
```

* Load the using the following commands:
    * `load <data-resource-uid>`
    * `process -dr <data-resource-uid>`    
    * `sample -dr <data-resource-uid> `    (optional)    
    * `index -dr <data-resource-uid> `   





`






