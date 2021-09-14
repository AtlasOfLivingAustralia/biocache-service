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
