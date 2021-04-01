package au.org.ala.biocache.util.solr;

import com.google.common.base.Strings;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;

import java.util.*;

public class FieldMappedQueryResponse extends QueryResponse {

    private static final Logger logger = Logger.getLogger(FieldMappedQueryResponse.class);

    final private FieldMappedSolrParams solrParams;
    final private QueryResponse delegate;

    SolrDocumentList _results;
    private List<FacetField> _facetFields = null;
    private List<FacetField> _facetDates = null;
    private List<RangeFacet> _rangeFacets = null;
    private List<IntervalFacet> _intervalFacets = null;

    public FieldMappedQueryResponse(FieldMappedSolrParams solrParams,
                                    QueryResponse delegate) {

        this.solrParams = solrParams;
        this.delegate = delegate;
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

        if (this._results == null) {

            this._results = delegate.getResults();

            if (this._results == null) {
                return null;
            }

            logger.debug("before results translation: " + this._results);

            Map<String, String[]> flMappings = this.solrParams.paramsInverseTranslations.get("fl");

            if (flMappings == null) {
                return _results;
            }

            this._results.forEach((SolrDocument solrDocument) -> {

                Set<Pair<String, Object>> addFields = new HashSet();
                Set<String> removeFields = new HashSet();

                for (String fieldName : solrDocument.getFieldNames()) {

                    String[] legacyFieldNames = flMappings.get(fieldName);

                    if (legacyFieldNames != null) {

                        boolean removeMappedField = true;
                        for (String legacyFieldName : legacyFieldNames) {

                            if (fieldName.equals(legacyFieldName)) {
                                removeMappedField = false;
                            } else if (legacyFieldName != null) {
                                addFields.add(Pair.of(legacyFieldName, solrDocument.getFieldValue(fieldName)));
                            }
                        }

                        if (removeMappedField) {
                            removeFields.add(fieldName);
                        }
                    }
                }

                addFields.forEach((Pair<String, Object> field) -> solrDocument.setField(field.getLeft(), field.getRight()));

                removeFields.forEach(solrDocument::removeFields);
            });

            logger.debug("after results translation: " + this._results);
        }

        return this._results;
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
    public Map<String, Object> getExplainMap() {
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

        if (this._facetFields == null) {

            List<FacetField> facetFields = delegate.getFacetFields();

            if (facetFields == null) {
                return null;
            }

            this._facetFields = new ArrayList<>();
            Map<String, String[]> facetMappings = this.solrParams.paramsInverseTranslations.get("facet.field");

            if (facetMappings != null) {

                for (FacetField facetField : facetFields) {

                    String facetName = facetField.getName();
                    String[] legacyFacetNames = facetMappings.getOrDefault(facetName, new String[0]);

                    for (String legacyFacetName : legacyFacetNames) {

                        if (facetName.equals(legacyFacetName)) {

                            this._facetFields.add(facetField);

                        } else if (legacyFacetName != null) {

                            FacetField legacyFacet = new FacetField(legacyFacetName);
                            for (FacetField.Count facetFieldCount : facetField.getValues()) {
                                legacyFacet.add(facetFieldCount.getName(), facetFieldCount.getCount());
                            }

                            this._facetFields.add(legacyFacet);
                        }
                    }
                }
            }
        }

        return this._facetFields;
    }

    @Override
    public List<FacetField> getFacetDates() {

        if (this._facetDates == null) {

            List<FacetField> facetFields = delegate.getFacetDates();

            if (facetFields == null) {
                return null;
            }

            this._facetDates = new ArrayList<>();
            Map<String, String[]> facetMappings = this.solrParams.paramsInverseTranslations.get("facet.field");

            if (facetMappings != null) {

                for (FacetField facetField : facetFields) {

                    String facetName = facetField.getName();
                    String[] legacyFacetNames = facetMappings.getOrDefault(facetName, new String[0]);

                    for (String legacyFacetName : legacyFacetNames) {

                        if (facetName.equals(legacyFacetName)) {

                            this._facetDates.add(facetField);

                        } else if (legacyFacetName != null) {

                            FacetField legacyFacet = new FacetField(legacyFacetName);
                            for (FacetField.Count facetFieldCount : facetField.getValues()) {
                                legacyFacet.add(facetFieldCount.getName(), facetFieldCount.getCount());
                            }

                            this._facetDates.add(legacyFacet);
                        }
                    }
                }
            }
        }

        return this._facetDates;
    }

    @Override
    public List<RangeFacet> getFacetRanges() {

        if (this._rangeFacets == null) {

            List<RangeFacet> facetRanges = delegate.getFacetRanges();

            if (facetRanges == null) {
                return null;
            }

            this._rangeFacets = new ArrayList<>();
            Map<String, String[]> facetMappings = this.solrParams.paramsInverseTranslations.get("facet.range");

            if (facetMappings != null) {

                for (RangeFacet rangeFacet : facetRanges) {

                    String facetName = rangeFacet.getName();
                    String[] legacyFacetNames = facetMappings.getOrDefault(facetName, new String[0]);

                    for (String legacyFacetName : legacyFacetNames) {

                        if (facetName.equals(legacyFacetName)) {

                            this._rangeFacets.add(rangeFacet);

                        } else if (legacyFacetName != null) {

                            RangeFacet legacyFacet = new WrappedRangeFacet(legacyFacetName, rangeFacet);

                            this._rangeFacets.add(legacyFacet);
                        }
                    }
                }
            }
        }

        return this._rangeFacets;
    }

    @Override
    public NamedList<List<PivotField>> getFacetPivot() {
        return delegate.getFacetPivot();
    }

    @Override
    public List<IntervalFacet> getIntervalFacets() {

        if (this._intervalFacets == null) {

            List<IntervalFacet> intervalFacets = delegate.getIntervalFacets();

            if (intervalFacets == null) {
                return null;
            }

            this._intervalFacets = new ArrayList<>();
            Map<String, String[]> facetMappings = this.solrParams.paramsInverseTranslations.get("facet.interval");

            if (facetMappings != null) {

                for (IntervalFacet intervalFacet : intervalFacets) {

                    String facetName = intervalFacet.getField();
                    String[] legacyFacetNames = facetMappings.getOrDefault(facetName, new String[0]);

                    for (String legacyFacetName : legacyFacetNames) {

                        if (facetName.equals(legacyFacetName)) {

                            this._intervalFacets.add(intervalFacet);

                        } else if (legacyFacetName != null) {

//                            IntervalFacet legacyFacet = new IntervalFacet() {
//
//                                public String getField() {
//                                    return legacyFacetName;
//                                }
//
//                                public List<IntervalFacet.Count> getIntervals() {
//                                    return intervalFacet.getIntervals();
//                                }
//                            };
//
//                            this._intervalFacets.add(legacyFacet);
                        }
                    }
                }
            }
        }

        return this._intervalFacets;
    }

    @Override
    public FacetField getFacetField(String name) {

        List<FacetField> facetFields = this.getFacetFields();

        if (facetFields == null || facetFields.isEmpty()) {

            return null;
        }

        // find the facet field filtered by facet.name
        return facetFields.stream()
                .filter((FacetField ff) -> ff.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    @Override
    public FacetField getFacetDate(String name) {

        List<FacetField> facetFields = this.getFacetDates();

        if (facetFields == null || facetFields.isEmpty()) {

            return null;
        }

        // find the facet date filtered by facet.name
        return facetFields.stream()
                .filter((FacetField ff) -> ff.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<FacetField> getLimitingFacets() {
        return delegate.getLimitingFacets();
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

    class WrappedRangeFacet<B, G> extends RangeFacet<B, G> {

        protected WrappedRangeFacet(String name, B start, B end, G gap, Number before, Number after, Number between) {
            super(name, start, end, gap, before, after, between);
        }

        public WrappedRangeFacet(String name, RangeFacet<B, G> rangedFacet) {
            super(name,
                    rangedFacet.getStart(),
                    rangedFacet.getEnd(),
                    rangedFacet.getGap(),
                    rangedFacet.getBefore(),
                    rangedFacet.getAfter(),
                    rangedFacet.getBetween());
        }
    }
}
