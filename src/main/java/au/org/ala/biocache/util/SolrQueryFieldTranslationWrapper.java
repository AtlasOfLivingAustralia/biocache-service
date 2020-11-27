package au.org.ala.biocache.util;

import org.apache.solr.client.solrj.SolrQuery;

import java.util.Arrays;
import java.util.Map;

public class SolrQueryFieldTranslationWrapper extends SolrQuery  {

    final private Map<String, String> deprecatedFields;

    public SolrQueryFieldTranslationWrapper(Map<String, String> deprecatedFields) {

        super();
        this.deprecatedFields = deprecatedFields;
    }

    public SolrQueryFieldTranslationWrapper(Map<String, String> deprecatedFields, String q) {
        super(q);
        this.deprecatedFields = deprecatedFields;

        this.setQuery(q);
    }

    public SolrQueryFieldTranslationWrapper(Map<String, String> deprecatedFields, String k, String v, String... params) {
        super(k, v, params);
        this.deprecatedFields = deprecatedFields;
    }


    @Override
    public SolrQuery setFilterQueries(String... fq) {

        if (fq == null) {
            super.setFilterQueries(null);
        }

        return super.setFilterQueries(Arrays.stream(fq).map(this::translateQueryFields).toArray(String[]::new));
    }

    @Override
    public SolrQuery addFilterQuery(String... fq) {

        if (fq == null) {
            super.setFilterQueries(null);
        }

        return super.addFilterQuery(Arrays.stream(fq).map(this::translateQueryFields).toArray(String[]::new));
    }

    @Override
    public SolrQuery addFacetQuery(String f) {
        return super.addFacetQuery(f);
    }

    @Override
    public SolrQuery setFields(String... fields) {

        if (fields == null) {
            super.setFields(null);
        }

        return super.setFields(translateFieldArray(fields));
        // TODO: PIPELINES: fields items may be passed as a comma separated list of field names
        // return super.setFields(Arrays.stream(fields).map(this::translateField).toArray(String[]::new));
    }

    @Override
    public SolrQueryFieldTranslationWrapper getCopy() {

        SolrQueryFieldTranslationWrapper query = new SolrQueryFieldTranslationWrapper(deprecatedFields);

        getParameterNames().stream()
                .forEach((String paramName) ->
                        query.setParam(paramName, this.getParams(paramName))
                );

        return query;
    }

    @Override
    public SolrQuery addField(String field) {

        return super.addField(translateField(field));
    }

    @Override
    public SolrQuery setQuery(String query) {
        return super.setQuery(translateQueryFields(query));
    }

    @Override
    public SolrQuery addFacetField(String... fields) {

        return super.addFacetField(translateFieldArray(fields));
    }

    private String translateQueryFields(String query) {

        if (query == null) {
            return null;
        }

        return this.deprecatedFields.entrySet()
                .stream()
                .reduce(query,
                        (String transformedQuery, Map.Entry<String, String> deprecatedField) ->
                                transformedQuery.replaceAll("(^|\\s|-|\\()" + deprecatedField.getKey() + ":", "$1" + deprecatedField.getValue() + ":"),
                        (a, e) -> { throw new IllegalStateException("unable to combine deprecated field replacement, use sequential stream"); });
    }

    public String translateField(String fieldName) {

        if (fieldName == null) {
            return null;
        }

        String translatedFieldName = this.deprecatedFields.get(fieldName);

        return translatedFieldName != null ? translatedFieldName : fieldName;
    }

    private String[] translateFieldList(String fls) {

        if (fls == null) {
            return null;
        }

        return translateFieldArray(fls.split(","));
    }

    public String[] translateFieldArray(String ...fields) {

        if (fields == null) {
            return null;
        }

        return Arrays.stream(fields)
                .map(this::translateField)
                .toArray(String[]::new);
    }
}
