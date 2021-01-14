package au.org.ala.biocache.util;

import org.apache.solr.client.solrj.SolrQuery;

import java.util.Arrays;

public class FieldMappedSolrQuery extends SolrQuery  {

    final private FieldMappingUtil fieldMappingUtil;

    public FieldMappedSolrQuery(FieldMappingUtil fieldMappingUtil) {

        super();
        this.fieldMappingUtil = fieldMappingUtil;
    }

    public FieldMappedSolrQuery(FieldMappingUtil fieldMappingUtil, String q) {
        super(q);
        this.fieldMappingUtil = fieldMappingUtil;

        this.setQuery(q);
    }

    public FieldMappedSolrQuery(FieldMappingUtil fieldMappingUtil, String k, String v, String... params) {
        super(k, v, params);
        this.fieldMappingUtil = fieldMappingUtil;
    }


    @Override
    public SolrQuery setFilterQueries(String... fq) {

        if (fq == null) {
            super.setFilterQueries(null);
        }

        return super.setFilterQueries(Arrays.stream(fq).map(fieldMappingUtil::translateQueryFields).toArray(String[]::new));
    }

    @Override
    public SolrQuery addFilterQuery(String... fq) {

        if (fq == null) {
            super.setFilterQueries(null);
        }

        return super.addFilterQuery(Arrays.stream(fq).map(fieldMappingUtil::translateQueryFields).toArray(String[]::new));
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

        return super.setFields(fieldMappingUtil.translateFieldArray(fields));
        // TODO: PIPELINES: fields items may be passed as a comma separated list of field names
        // return super.setFields(Arrays.stream(fields).map(this::translateField).toArray(String[]::new));
    }

    @Override
    public SolrQuery addNumericRangeFacet(String field, Number start, Number end, Number gap) {
        return super.addNumericRangeFacet(fieldMappingUtil.translateFieldName(field), start, end, gap);
    }

    @Override
    public FieldMappedSolrQuery getCopy() {

        FieldMappedSolrQuery query = new FieldMappedSolrQuery(fieldMappingUtil);

        getParameterNames().stream()
                .forEach((String paramName) ->
                        query.setParam(paramName, this.getParams(paramName))
                );

        return query;
    }

    @Override
    public SolrQuery addField(String field) {

        return super.addField(fieldMappingUtil.translateFieldName(field));
    }

    @Override
    public SolrQuery setQuery(String query) {
        return super.setQuery(fieldMappingUtil.translateQueryFields(query));
    }

    @Override
    public SolrQuery addFacetField(String... fields) {

        return super.addFacetField(fieldMappingUtil.translateFieldArray(fields));
    }

}
