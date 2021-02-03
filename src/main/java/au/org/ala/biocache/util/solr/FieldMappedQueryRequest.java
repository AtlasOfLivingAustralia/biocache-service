package au.org.ala.biocache.util.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;

import java.io.IOException;
import java.util.Collection;

public class FieldMappedQueryRequest extends QueryRequest {

    final private SolrParams query;
    final FieldMappingUtil fieldMappingUtil;

    public FieldMappedQueryRequest(FieldMappingUtil fieldMappingUtil) {

        this(fieldMappingUtil, null);
    }

    public FieldMappedQueryRequest(FieldMappingUtil fieldMappingUtil, SolrParams q) {

        this(fieldMappingUtil, q, METHOD.GET);
    }

    public FieldMappedQueryRequest(FieldMappingUtil fieldMappingUtil, SolrParams q, METHOD method) {

        super();

        this.setMethod(method);
        this.fieldMappingUtil = fieldMappingUtil;
        this.query = translateSolrParams(q);
    }

    SolrParams translateSolrParams(SolrParams params) {

        // TODO: PIPELINES: translate the params performing field mappings
        ModifiableSolrParams translatedSolrParams = ModifiableSolrParams.of(params);
//        translatedSolrParams.getParameterNamesIterator().forEachRemaining((String paramName) -> {
//            switch (paramName) {
//                case "q":
//                    translatedSolrParams.set("q", fieldMappingUtil.translateQueryFields(translatedSolrParams.get("q")));
//                    break;
//
//                case "fl":
//                    translatedSolrParams.set("fl", fieldMappingUtil.translateFieldArray(translatedSolrParams.getParams("fl")));
//                    break;
//            }
//        });

        return translatedSolrParams;
    }

    QueryResponse translateQueryResponse(QueryResponse queryResponse) {

        // TODO: PIPELINES: translate the query response performing reverse field mappings

        return queryResponse;
    }

    public String getPath() {
        String qt = this.query == null ? null : this.query.get("qt");
        if (qt == null) {
            qt = super.getPath();
        }

        return qt != null && qt.startsWith("/") ? qt : "/select";
    }

    @Override
    public Collection<ContentStream> getContentStreams() {
        return null;
    }

    @Override
    protected QueryResponse createResponse(SolrClient client) {
        return new QueryResponse(client);
    }

    @Override
    public SolrParams getParams() {
        return this.query;
    }
}
