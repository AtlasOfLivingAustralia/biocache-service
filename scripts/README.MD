# Access to Solr cluster from local dev environment

The script `solr-cluster.sh` will setup the host entries, virtual interfaces and ssh tunnels needed to access the solr cluster via zookeeper. 

```
# ./solr-cluster.sh
usage: ./solr-cluster.sh < test | prod > < up | down >
```

First argument is the environment `test` or `prod`
Second argument is set `up` or tear `down` the connection

eg:
```
# ./solr-cluster.sh test up
up nci3-zookeeper-1.ala nci3-zookeeper-2.ala nci3-zookeeper-3.ala nci3-solr-1.ala nci3-solr-2.ala nci3-solr-3.ala nci3-solr-4.ala
ssh -f -N nci3-zookeeper-1.ala -L 172.30.1.1:2181:localhost:2181
ssh -f -N nci3-zookeeper-2.ala -L 172.30.1.2:2181:localhost:2181
ssh -f -N nci3-zookeeper-3.ala -L 172.30.1.3:2181:localhost:2181
ssh -f -N nci3-solr-1.ala -L 172.30.2.1:8983:localhost:8983
ssh -f -N nci3-solr-2.ala -L 172.30.2.2:8983:localhost:8983
ssh -f -N nci3-solr-3.ala -L 172.30.2.3:8983:localhost:8983
ssh -f -N nci3-solr-4.ala -L 172.30.2.4:8983:localhost:8983
```

# Managing Data Quality wiki pages
Each assertions has a corrisponding wiki page to document the assertions details https://github.com/AtlasOfLivingAustralia/ala-dataquality/wiki. These wiki pages contain structured data needs to be kept in sync with the list of assertions from `biocache-service`.

Updating the sections within each assertion's wiki page can be automated:

clone the wiki repo:
```
# git clone https://github.com/AtlasOfLivingAustralia/ala-dataquality.wiki.git
```

run the script:
```
# groovy update-assertions-wiki.groovy <path to wiki>
```

review the changes before committing back to github.

# Pipelines field and assertions mapping scripts

#Config 
`config/field-mapping.yml`
```
dwcTerms: https://raw.githubusercontent.com/tdwg/dwc/master/vocabulary/term_versions.csv
biocache:
    fields: https://biocache-ws.ala.org.au/ws/index/fields?isMisc=true
    solr:
        host: http://localhost:8984
        collection: biocache
pipelines:
    fields:
    solr:
        host: http://localhost:8985
        collection: biocache

fields:
    mappingJson: deprecated-fields.json
    export: pipeline_field_mappings.csv

assertions:
    mappingJson: assertions-mapping.json
    export: pipeline_assertion_mappings.csv
```

## Field Mapping
The Field mapping / schema diff script attempts to find field mappings between the `biocache` _legacy_ and the `pipelines` solr schemas.
To assist in this process the field list is retrieved from the biocache web service and the TDWG Darwin Core terms are used to compare the field names.

The field mapping script produces the following two outputs:
 - `deprecated-fields.json` contain a json map of biocache field names to pipelines field names.
 - `pipelines_field_mappings.csv` is a summary of the mappings between pipelines fields, biocache fields, DwC terms, list of biocache field exposed via the web service.

Run the script using the following command.
```
# groovy schema-diff.groovy
```

To define mappings between fields they can be added to the `deprecated-fields.json`. If an existing mapping is defined then is used

## Assertions Mapping

The assertions mapping compares a facet of the `assertions` field from the `biocache` _legacy_ and `pipelines` schemas. This mapping is also compared against the `org.gbif.api.vocabulary.OccurrenceIssue` enum. 

The field mapping script produces the following two outputs:
- `assertions-mapping.json` contain a json map of biocache assertions to pipelines assertions.
- `pipelines_assertion_mappings.csv` is a summary of the mappings between pipelines assertions, biocache assertions and`org.gbif.api.vocabulary.OccurrenceIssue` enum.

Run the script using the following command.
```
# groovy assertions-diff.groovy
```

To define mappings between assertions they can be added to the `assertions-mapping.json`. If an existing mapping is defined then is used
