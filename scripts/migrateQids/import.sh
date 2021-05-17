#!/bin/sh

# OPTIONAL (if it does not exist): create keyspace 'biocache'
cqlsh -e "CREATE KEYSPACE biocache WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;};"

# OPTIONAL (if it does not exist): create table 'userassertions'
cqlsh -e "CREATE TABLE biocache.qid ( key text PRIMARY KEY, value text );"

# Report
echo "records in Cassandra before the import"
cqlsh -e "SELECT count(*) FROM biocache.qid;"

# Import
# We need to change the default ESCAPE character otherwise CQLSH will remove the escape characters
# and leave an invalid json blob
cqlsh -e "COPY biocache.qid FROM '/data/tmp/qid.csv' WITH CHUNKSIZE=1 AND DELIMITER='\t' AND ESCAPE='$';"

# Report
echo "records in Cassandra after the import"
cqlsh -e "SELECT count(*) FROM biocache.qid;"
