## SOLR fields

Field names in solr now use DwC terms were applicable. 

#### Data processing
Where is ingested from a data provider and processed the supplied data is stored in a field name prefixed with `raw_` with the processed data in the un-prefixed field name. For data that has is not processed on ingestion the data is stored with an un-prefixed field name.  

#### Field mappings
Field mappings translate legacy field names to their new names. 
Field mappings on a `null` name denote that the field has been deprecated.

#### Enum value mappings
Enumerated field values have been standardised to a vocabulary. A mapping of the legacy to the new vocabulary can be maintained. 

#### Handling of processed vs raw fields
Since raw field data may be loaded into a field with or without a `raw_` prefix there is no way of determining is data is raw or processed.  

The `pipelines-field-config.json`:

- fieldNameMapping: map of legacy to new field names
- fieldValueMapping: map of field names that contain enumerated value mappings
- processedFields: list of fields that are to be treated as processed. 