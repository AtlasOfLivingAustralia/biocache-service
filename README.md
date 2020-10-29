biocache-service [![Build Status](https://travis-ci.com/AtlasOfLivingAustralia/biocache-service.svg?branch=develop)](http://travis-ci.com/AtlasOfLivingAustralia/biocache-service) [![Coverage Status](https://coveralls.io/repos/github/AtlasOfLivingAustralia/biocache-service/badge.svg)](https://coveralls.io/github/AtlasOfLivingAustralia/biocache-service)
================

Occurrence &amp; mapping webservices.

Theses services are documented here https://api.ala.org.au/apps/biocache

## Versions

There are currently two supported versions:

* 1.9.x  - SOLR 4 and Cassandra 1.2.x. See the 1.9.x branch.
* 2.x - SOLR 7 with SOLR Cloud support and Cassandra 3.x. See the master branch.

## Dev Setup

see wiki: https://github.com/AtlasOfLivingAustralia/biocache-service/wiki

## Integration Tests

Integration testing is supported using docker containers.
To start the required containers, run the following:

```
docker-compose -f src/test/docker/solr7-cassandra3.yml up -d
```

To shutdown, run the following:
```
docker-compose -f src/test/docker/solr7-cassandra3.yml kill
```


Pre-requistes are Docker version 17+. For more details see this [readme](/src/test/docker/README.md).
