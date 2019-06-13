# Docker images for integration testing


Docker images have been created to use for integration tests on local machines 
and also in CI (Travis).

## SOLR

To build (change tags e.g. v1 - > v2):

```
cd src/test/docker/solr7
docker build . -t biocache-it-solr7:v1
```

To run locally
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
docker build . -t biocache-it-cass3:v1
```

To run locally
```
docker build . -t biocache-it-cass3:v1
```

To publish 
```
docker tag 3579a7d6de70 djtfmartin/biocache-it-cass3:v1
docker push djtfmartin/biocache-it-cass3

```

