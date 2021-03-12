#!/bin/sh
#
# Export user assertions from biocache-store's Cassandra
#
# Requires python3, write access to /data/tmp, cqlsh, local Cassandra node.
#

# dump the table
cqlsh -e "copy occ.qid TO '/data/tmp/qid_dump.cql' with DELIMITER='\t' and header=true;";

# format for new schema
python3 qid_format.py

# Report
echo "records exported: $(wc -l /data/tmp/qid.csv)"
echo ""
echo "Output: /data/tmp/qid.csv"
echo "Import into the new Cassandra: 'import.sh /data/tmp/qid.csv'"