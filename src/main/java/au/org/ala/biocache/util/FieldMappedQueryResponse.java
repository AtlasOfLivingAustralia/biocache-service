package au.org.ala.biocache.util;

import au.org.ala.biocache.util.solr.FieldMappingUtil;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FieldMappedQueryResponse extends QueryResponse {

    final private FieldMappingUtil fieldMappingUtil;
    final private QueryResponse queryResponse;

    FieldMappedQueryResponse(QueryResponse queryResponse, FieldMappingUtil fieldMappingUtil) {

        this.fieldMappingUtil = fieldMappingUtil;
        this.queryResponse = queryResponse;
    }

    @Override
    public void setResponse(NamedList<Object> res) {
        queryResponse.setResponse(res);
    }

    @Override
    public void removeFacets() {
        queryResponse.removeFacets();
    }

    @Override
    public NamedList<Object> getHeader() {
        return queryResponse.getHeader();
    }

    @Override
    public SolrDocumentList getResults() {
        return queryResponse.getResults();
    }

    @Override
    public NamedList<ArrayList> getSortValues() {
        return queryResponse.getSortValues();
    }

    @Override
    public Map<String, Object> getDebugMap() {
        return queryResponse.getDebugMap();
    }

    @Override
    public Map<String, Object> getExplainMap() {
        return queryResponse.getExplainMap();
    }

    @Override
    public Map<String, Integer> getFacetQuery() {
        return queryResponse.getFacetQuery();
    }

    @Override
    public Map<String, SolrDocumentList> getExpandedResults() {
        return queryResponse.getExpandedResults();
    }

    @Override
    public GroupResponse getGroupResponse() {
        return queryResponse.getGroupResponse();
    }

    @Override
    public Map<String, Map<String, List<String>>> getHighlighting() {
        return queryResponse.getHighlighting();
    }

    @Override
    public SpellCheckResponse getSpellCheckResponse() {
        return queryResponse.getSpellCheckResponse();
    }

    @Override
    public ClusteringResponse getClusteringResponse() {
        return queryResponse.getClusteringResponse();
    }

    @Override
    public SuggesterResponse getSuggesterResponse() {
        return queryResponse.getSuggesterResponse();
    }

    @Override
    public TermsResponse getTermsResponse() {
        return queryResponse.getTermsResponse();
    }

    @Override
    public NamedList<SolrDocumentList> getMoreLikeThis() {
        return queryResponse.getMoreLikeThis();
    }

    @Override
    public List<FacetField> getFacetFields() {
        return queryResponse.getFacetFields();
    }

    @Override
    public List<FacetField> getFacetDates() {
        return queryResponse.getFacetDates();
    }

    @Override
    public List<RangeFacet> getFacetRanges() {
        return queryResponse.getFacetRanges();
    }

    @Override
    public NamedList<List<PivotField>> getFacetPivot() {
        return queryResponse.getFacetPivot();
    }

    @Override
    public List<IntervalFacet> getIntervalFacets() {
        return queryResponse.getIntervalFacets();
    }

    @Override
    public FacetField getFacetField(String name) {
        return queryResponse.getFacetField(name);
    }

    @Override
    public FacetField getFacetDate(String name) {
        return queryResponse.getFacetDate(name);
    }

    @Override
    public List<FacetField> getLimitingFacets() {
        return queryResponse.getLimitingFacets();
    }

    @Override
    public <T> List<T> getBeans(Class<T> type) {
        return queryResponse.getBeans(type);
    }

    @Override
    public Map<String, FieldStatsInfo> getFieldStatsInfo() {
        return queryResponse.getFieldStatsInfo();
    }

    @Override
    public String getNextCursorMark() {
        return queryResponse.getNextCursorMark();
    }

    @Override
    public long getElapsedTime() {
        return queryResponse.getElapsedTime();
    }

    @Override
    public void setElapsedTime(long elapsedTime) {
        queryResponse.setElapsedTime(elapsedTime);
    }

    @Override
    public NamedList<Object> getResponse() {
        return queryResponse.getResponse();
    }

    @Override
    public String toString() {
        return queryResponse.toString();
    }

    @Override
    public NamedList getResponseHeader() {
        return queryResponse.getResponseHeader();
    }

    @Override
    public int getStatus() {
        return queryResponse.getStatus();
    }

    @Override
    public int getQTime() {
        return queryResponse.getQTime();
    }

    @Override
    public String getRequestUrl() {
        return queryResponse.getRequestUrl();
    }

    @Override
    public void setRequestUrl(String requestUrl) {
        queryResponse.setRequestUrl(requestUrl);
    }

    public static byte[] serializable(SolrResponse response) {
        return SolrResponse.serializable(response);
    }

    public static SolrResponse deserialize(byte[] bytes) {
        return SolrResponse.deserialize(bytes);
    }
}
