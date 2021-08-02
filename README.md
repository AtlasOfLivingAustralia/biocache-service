biocache-service [![Build Status](https://travis-ci.com/AtlasOfLivingAustralia/biocache-service.svg?branch=develop)](http://travis-ci.com/AtlasOfLivingAustralia/biocache-service) [![Coverage Status](https://coveralls.io/repos/github/AtlasOfLivingAustralia/biocache-service/badge.svg)](https://coveralls.io/github/AtlasOfLivingAustralia/biocache-service)
================

Occurrence &amp; mapping webservices.

Theses services are documented here https://api.ala.org.au/apps/biocache

## Versions

There are currently two supported versions:

* 3.x - SOLR 8 with SOLR Cloud support and Cassandra 3.x. See master and develop branches
* 2.7.x - Legacy branch, SOLR 7 with SOLR Cloud support and Cassandra 3.x. See the 2.7.x branch.


## Dev Setup

see wiki: https://github.com/AtlasOfLivingAustralia/biocache-service/wiki

## Integration Tests

Integration testing is supported using docker containers.
To start the required containers, run the following:

```
mvn docker:start
```

To shutdown, run the following:
```
mvn docker:stop
```

Prerequisites are Docker version 17+. For more details see this [readme](/src/test/docker/README.md).

Tests: please follow the conventions of the Maven Surefire plugin 
or unit tests and those of the Maven Failsafe plugin for integration tests. To run the integration tests 
just run the verify phase, e.g.: `mvn clean verify`
