import csv, json, sys;
from collections import defaultdict
from cassandra.cluster import Cluster

# Purpose:
# To copy user properties from userdetails mysql table "profiles" to cassandra table "userproperty".
#
# Prerequisites:
# 1. python and cassandra-driver
#   pip install cassandra-driver
# 2. cassandra table "userproperties". biocache-service will create it on the first addition of a property,
#    but you can create it manually with cqlsh:
#   CREATE TABLE biocache.userproperty (
#        key text PRIMARY KEY,
#        value text
#    );
#
# Steps:
# 1. dump userdetails "profiles" table, but only for custom properties, e.g.
#   mysql userdetailsdb -e "select * from profiles where profiles.property like 'ala-hub.%' or profiles.property like 'null.%' or profiles.property like 'biocache-hub.%';" > userProperties.tsv
# 2. import userProperties.tsv to cassandra at localhost:9042, keyspace "biocache", table "userproperty"
#   python3 convertTsvToJson.py < userProperties.tsv

data = [dict(r) for r in csv.DictReader(sys.stdin, delimiter="\t")]

aggregatedByUserId = defaultdict(dict)

# Iterate over the list of dictionaries
for r in data:
    # Aggregate the data by r.userid into a map
    aggregatedByUserId[r['userid']][r['property']] = r['value']

# connect to cassandra
cluster = Cluster(['localhost'])
session = cluster.connect()

# insert query
query = """
INSERT INTO biocache.userproperty (key, value)
VALUES (%s, %s)
"""

# write to cassandra
for k, v in aggregatedByUserId.items():
    text = json.dumps({**{'alaId': k}, **{'properties': v} })
    session.execute(query, (k, text))

session.shutdown()
cluster.shutdown()
