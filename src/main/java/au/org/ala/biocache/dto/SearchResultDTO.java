/***************************************************************************
 * Copyright (C) 2010 Atlas of Living Australia
 * All Rights Reserved.
 *
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.biocache.dto;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.response.QueryResponse;

import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * DTO to represents the results from a Lucene search
 *
 * NC: Changed the result type to OccurrenceIndex
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
public class SearchResultDTO {

    /** Maximum number of results returned from a query */
    private long pageSize = 10;
    /** Current page of results (not currently used) */
    private long startIndex = 0;
    /** Total number of results for the match (indept of resultsPerPage) */
    private long totalRecords = 0;
    /** Field to sort results by */
    private String sort;
    /** Direction to sort results by (asc || desc) */
    private String dir = "asc";
    /** Status code to be set by Controller (e.g. OK) */
    private String status;
    /** An error message to return to the requester */
    private String errorMessage;
    /** List of results from search */
    private List<OccurrenceIndex> occurrences;
    /** List of facet results from search */
    private List<FacetResultDTO> facetResults;
    /** SOLR query response following search */
    private QueryResponse qr;
    /** The original query */
    private String query;
    /** Stores the URL parameter that should be provided to Download and View Map */
    private String urlParameters;
    /** Stores the title for the query - this is dependent on the type of query that has been executed */
    private String queryTitle;
    /**
     * Stores a map a facets that have been applied to the query.  This will include details that a
     * necessary to display on clients.
     */
    private Map<String,Facet> activeFacetMap;

    /**
     * Constructor with 2 args
     *
     * @param searchResults
     */
    public SearchResultDTO(List<OccurrenceIndex> searchResults) {
        this.occurrences = searchResults;
    }

    /**
     * Constructor with no args
     */
    public SearchResultDTO() {}

    /*
     * Getters & Setters
     */
    
    public long getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(long start) {
        this.startIndex = start;
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(long totalRecords) {
        this.totalRecords = totalRecords;
    }

    public long getPageSize() {
        return pageSize;
    }

    public void setPageSize(long pageSize) {
        this.pageSize = pageSize;
    }
    
    @JsonIgnore
    public long getCurrentPage() {
        return this.startIndex/pageSize;
    }
   
    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public String getSort() {
        return sort;
    }

    public void setSort(String sort) {
        this.sort = sort;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<OccurrenceIndex> getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(List<OccurrenceIndex> values) {
        this.occurrences = values;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    //@JsonIgnore
    public Collection<FacetResultDTO> getFacetResults() {
        return facetResults;
    }

    public void setFacetResults(List<FacetResultDTO> facetResults) {
        this.facetResults = facetResults;
    }

    @JsonIgnore
    public QueryResponse getQr() {
        return qr;
    }
    
    @JsonIgnore
    public void setQr(QueryResponse qr) {
        this.qr = qr;
    }

    public String getUrlParameters() {
        return urlParameters;
    }

    public void setUrlParameters(String urlParameters) {
        this.urlParameters = urlParameters;
    }

    public String getQueryTitle() {
        return queryTitle;
    }

    public void setQueryTitle(String queryTitle) {
        this.queryTitle = queryTitle;
    }

    /**
     * @return the errorMessage
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * @param errorMessage the errorMessage to set
     */
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * @return the activeFacetMap
     */
    public Map<String, Facet> getActiveFacetMap() {
        return activeFacetMap;
    }

    /**
     * @param activeFacetMap the activeFacetMap to set
     */
    public void setActiveFacetMap(Map<String, Facet> activeFacetMap) {
        this.activeFacetMap = activeFacetMap;
    }
    
    
}
