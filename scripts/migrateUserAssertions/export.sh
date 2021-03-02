#!/bin/sh
#
# Export user assertions from biocache-store's Cassandra
#
# Requires python3, write access to /data/tmp, cqlsh, local Cassandra node.
#

# dump the table
cqlsh -e "copy occ.qa TO '/data/tmp/qa_dump.cql' with DELIMITER='\t' and header=true;";

# format for new schema
python3 qa_format.py

# OPTIONAL (>30mins): remove quality assertions that do not belong to any occurrence records in occ.occ
awk 'BEGIN {print "select rowkey from occ.occ where rowkey IN ("} NR>1 {print "'\''"prev"'\'',";} {prev=$1} END {print "'\''"prev "'\'');";}' /data/tmp/qa.csv > /data/tmp/find.valid.rowkeys.cql
cqlsh -f find.valid.rowkeys.cql > valid.rowkeys
python3 qa_remove_invalid_rowkeys.py

//String oldDatasetID = null;
        String snapshot = (String) data.getOrDefault("snapshot", null);
        if (StringUtils.isNotEmpty(snapshot)) {
          Map snapshotMap = (Map) Json.parseJson(snapshot);
          Map attribution = (Map) snapshotMap.get("attribution");
          if (attribution != null) {
            oldDatasetID = (String) attribution.get("dataResourceUid");
          }
        }

        String datasetID = (newDatasetID != null) ? newDatasetID : oldDatasetID;


# Report
echo "records exported: $(wc -l /data/tmp/qa.final.csv)"
echo ""
echo "Output: /data/tmp/qa.final.csv"
echo "Import into the new Cassandra: 'import.sh /data/tmp/qa.final.csv'"