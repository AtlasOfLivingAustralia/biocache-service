# Docker images for integration testing


Docker images have been created to use integration tests on local machines but also in CI (Travis).


To build:


## SOLR 7

```
cd src/test/docker/solr7
docker build . -t biocache-it-solr7:v1
```

To test
```
docker run --name biocache-it-solr7 -d -p 8983:8983 -t biocache-it-solr7:v1
```


## SOLR 8

```
docker build . -t biocache-it-solr8:v1
```

docker build . -t biocache-it-cass3:v1




docker run --name biocache-it-solr7 -d -p 8983:8983 -t biocache-it-solr7:v1
docker run --name biocache-it-solr7 -d -p 8983:8983 -t biocache-it-solr8:v1
docker run --name biocache-it-cass3 -d -p 9042:9042 -t biocache-it-cass3:v1