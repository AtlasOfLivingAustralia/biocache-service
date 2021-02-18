package au.org.ala.biocache.util.solr;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;

import java.util.*;
import java.util.function.Function;

public class FieldMappedQueryResponse extends QueryResponse {

    final private QueryResponse delegate;
    final private Map<String, String> facetMap;
    final private Map<String, String> reverseFacetMap;
    final private Set<String> deprecatedFacets;

    public FieldMappedQueryResponse(Map<String, String> facetMap,
                                    QueryResponse delegate) {
        this.facetMap = facetMap;
        this.delegate = delegate;

        if (facetMap == null) {
            reverseFacetMap = null;
            deprecatedFacets = null;
        } else {

            reverseFacetMap = new HashMap<>();
            deprecatedFacets = new HashSet<>();

            this.facetMap.entrySet()
                .stream()
                .forEach((Map.Entry<String, String> facetMapping) -> {

                    if (facetMapping.getValue() == null) {
                        deprecatedFacets.add(facetMapping.getKey());
                    } else {
                        reverseFacetMap.put(facetMapping.getValue(), facetMapping.getKey());
                    }
                });
        }
    }


    @Override
    public void setResponse(NamedList<Object> res) {
        delegate.setResponse(res);
    }

    @Override
    public void removeFacets() {
        delegate.removeFacets();
    }

    @Override
    public NamedList<Object> getHeader() {
        return delegate.getHeader();
    }

    @Override
    public SolrDocumentList getResults() {
        return delegate.getResults();
    }

    @Override
    public NamedList<ArrayList> getSortValues() {
        return delegate.getSortValues();
    }

    @Override
    public Map<String, Object> getDebugMap() {
        return delegate.getDebugMap();
    }

    @Override
    public Map<String, String> getExplainMap() {
        return delegate.getExplainMap();
    }

    @Override
    public Map<String, Integer> getFacetQuery() {
        return delegate.getFacetQuery();
    }

    @Override
    public Map<String, SolrDocumentList> getExpandedResults() {
        return delegate.getExpandedResults();
    }

    @Override
    public GroupResponse getGroupResponse() {
        return delegate.getGroupResponse();
    }

    @Override
    public Map<String, Map<String, List<String>>> getHighlighting() {
        return delegate.getHighlighting();
    }

    @Override
    public SpellCheckResponse getSpellCheckResponse() {
        return delegate.getSpellCheckResponse();
    }

    @Override
    public ClusteringResponse getClusteringResponse() {
        return delegate.getClusteringResponse();
    }

    @Override
    public SuggesterResponse getSuggesterResponse() {
        return delegate.getSuggesterResponse();
    }

    @Override
    public TermsResponse getTermsResponse() {
        return delegate.getTermsResponse();
    }

    @Override
    public NamedList<SolrDocumentList> getMoreLikeThis() {
        return delegate.getMoreLikeThis();
    }

    @Override
    public List<FacetField> getFacetFields() {
        return translateFacetFields(delegate.getFacetFields());
    }

    @Override
    public List<FacetField> getFacetDates() {
        return translateFacetFields(delegate.getFacetDates());
    }

    @Override
    public List<RangeFacet> getFacetRanges() {
/*
        List<FacetField> translatedFacetFields = new ArrayList<>();

        for (RangeFacet rangeFacet : delegate.getFacetRanges()) {

            RangeFacet translatedRangeFacet = rangeFacet;

            String legacyFacetName = reverseFacetMap.get(rangeFacet.getName());

            if (legacyFacetName != null) {
                translatedRangeFacet = new RangeFacet() {

                }
                for (FacetField.Count facetFieldCount : rangeFacet.getValues()) {
                    translatedRangeFacet.add(facetFieldCount.getName(), facetFieldCount.getCount());
                }
            }

            translatedFacetFields.add(translatedRangeFacet);
        }

        for (String deprecatedFacetName : deprecatedFacets) {

            FacetField deprecatedFacet = new FacetField(deprecatedFacetName);

            translatedFacetFields.add(deprecatedFacet);
        }

        return translatedFacetFields;
*/

        return delegate.getFacetRanges();
    }

    private List<FacetField> translateFacetFields(List<FacetField> facetFields) {

        List<FacetField> translatedFacetFields = new ArrayList<>();

        if (facetFields != null) {

            for (FacetField facetField : facetFields) {

                FacetField translatedFacetField = facetField;

                String legacyFacetName = reverseFacetMap.get(facetField.getName());

                if (legacyFacetName != null) {
                    translatedFacetField = new FacetField(legacyFacetName);
                    for (FacetField.Count facetFieldCount : facetField.getValues()) {
                        translatedFacetField.add(facetFieldCount.getName(), facetFieldCount.getCount());
                    }
                }

                translatedFacetFields.add(translatedFacetField);
            }
        }

        if (deprecatedFacets != null) {

            for (String deprecatedFacetName : deprecatedFacets) {

                FacetField deprecatedFacet = new FacetField(deprecatedFacetName);

                translatedFacetFields.add(deprecatedFacet);
            }
        }

        return translatedFacetFields;
    }

    @Override
    public NamedList<List<PivotField>> getFacetPivot() {
        return delegate.getFacetPivot();
    }

    @Override
    public List<IntervalFacet> getIntervalFacets() {

        // TODO: PIPELINES: unable to wrap IntervalFacet due to private constructor
        return delegate.getIntervalFacets();
    }

    @Override
    public FacetField getFacetField(String name) {

//        return delegate.getFacetField(name);
        return translateFacetField(name, (String n) -> {
            FacetField facetField = delegate.getFacetField(n);
            return facetField;
        });
    }

    @Override
    public FacetField getFacetDate(String name) {

        return translateFacetField(name, (String n) -> delegate.getFacetDate(n));
    }

    private FacetField translateFacetField(String name, Function<String, FacetField> callback) {

        String newFacetFieldName = facetMap.getOrDefault(name, "\0");

        FacetField translatedFacetField;

        if ("\0".equals(newFacetFieldName)) {
            translatedFacetField = callback.apply(name);
        } else if (newFacetFieldName == null) {

            translatedFacetField = new FacetField(name);

        } else {

            translatedFacetField = new FacetField(name);
            FacetField newFacetField = callback.apply(newFacetFieldName);

            if (newFacetField != null) {
                for (FacetField.Count facetFieldCount : newFacetField.getValues()) {
                    translatedFacetField.add(facetFieldCount.getName(), facetFieldCount.getCount());
                }
            }
        }

        return translatedFacetField;
    }

    @Override
    public List<FacetField> getLimitingFacets() {
        return translateFacetFields(delegate.getLimitingFacets());
    }

    @Override
    public <T> List<T> getBeans(Class<T> type) {
        return delegate.getBeans(type);
    }

    @Override
    public Map<String, FieldStatsInfo> getFieldStatsInfo() {
        return delegate.getFieldStatsInfo();
    }

    @Override
    public String getNextCursorMark() {
        return delegate.getNextCursorMark();
    }

    @Override
    public long getElapsedTime() {
        return delegate.getElapsedTime();
    }

    @Override
    public void setElapsedTime(long elapsedTime) {
        delegate.setElapsedTime(elapsedTime);
    }

    @Override
    public NamedList<Object> getResponse() {
        return delegate.getResponse();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public NamedList getResponseHeader() {
        return delegate.getResponseHeader();
    }

    @Override
    public int getStatus() {
        return delegate.getStatus();
    }

    @Override
    public int getQTime() {
        return delegate.getQTime();
    }

    @Override
    public String getRequestUrl() {
        return delegate.getRequestUrl();
    }

    @Override
    public void setRequestUrl(String requestUrl) {
        delegate.setRequestUrl(requestUrl);
    }

    public static byte[] serializable(SolrResponse response) {
        return SolrResponse.serializable(response);
    }

    public static SolrResponse deserialize(byte[] bytes) {
        return SolrResponse.deserialize(bytes);
    }


}
