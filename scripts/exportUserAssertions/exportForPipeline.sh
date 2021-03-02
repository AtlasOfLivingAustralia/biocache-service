#!/bin/sh
#
# Export userassertions in the new Cassandra for use in the pipeline.
#

# export table
cqlsh -e "COPY userassertions TO '/data/tmp/userassertions.dump' WITH DELIMITER='\t' AND HEADER=true;"

# Report
echo "records exported: $(wc -l /data/tmp/userassertions.dump)"
echo ""
echo "Output: /data/tmp/userassertions.dump"
echo "Import into pipelines: ./la-pipelines import-user-assertions --dumpFilePath=/data/tmp/userassertions.dump"
echo "User assertions are merged into the pipeline during: './la-pipelines index'"
