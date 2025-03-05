# biocache-service

Master branch [![Build Status](https://api.travis-ci.com/AtlasOfLivingAustralia/biocache-service.svg?branch=master)](https://app.travis-ci.com/github/AtlasOfLivingAustralia/biocache-service)  [![Coverage Status](https://coveralls.io/repos/github/AtlasOfLivingAustralia/biocache-service/badge.svg?branch=master)](https://coveralls.io/github/AtlasOfLivingAustralia/biocache-service?branch=master) Develop branch [![Build Status](https://api.travis-ci.com/AtlasOfLivingAustralia/biocache-service.svg?branch=develop)](https://app.travis-ci.com/github/AtlasOfLivingAustralia/biocache-service) [![Coverage Status](https://coveralls.io/repos/github/AtlasOfLivingAustralia/biocache-service/badge.svg?branch=develop)](https://coveralls.io/github/AtlasOfLivingAustralia/biocache-service?branch=develop)


Occurrence &amp; mapping webservices.

These services are documented here https://api.ala.org.au/apps/biocache

## Versions

There are currently two supported versions:

* 3.x - SOLR 8 with SOLR Cloud support and Cassandra 3.x. See master and develop branches
* 2.7.x - Legacy branch, SOLR 7 with SOLR Cloud support and Cassandra 3.x. See the 2.7.x branch.

## Development environment Setup

SOLR and Cassandra are required by Biocache servcie.

We can run those two docker instances

or SSH tunnel in our test servers
```
ssh -L 8983:localhost:8983 aws-solr-test-1.ala
ssh -L 9042:localhost:9042 aws-cass-test-1.ala
```

see wiki: https://github.com/AtlasOfLivingAustralia/biocache-service/wiki

## Integration Tests

Integration testing is supported using docker containers for SOLR and Cassandra.
To start the required containers, run the following:

```
./gradlew composeUp
```

To shutdown, run the following:
```
./gradlew composeDown
```

```
./gradlew bootRun
```


Prerequisites are Docker version 17+. For more details see this [readme](/src/test/docker/README.md).

To run the integration tests just run the check task, e.g.: `./gradlew clean check`

