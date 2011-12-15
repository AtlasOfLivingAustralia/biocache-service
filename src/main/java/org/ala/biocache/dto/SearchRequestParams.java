/* *************************************************************************
 *  Copyright (C) 2011 Atlas of Living Australia
 *  All Rights Reserved.
 * 
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 * 
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/

package org.ala.biocache.dto;

import org.apache.commons.lang.StringUtils;

/**
 * Data Transfer Object to represent the request parameters required to search
 * for occurrence records against biocache-service.
 * 
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
public class SearchRequestParams {
    /** Only used to store the formattedQuery to be passed around in biocache-service**/
    protected String formattedQuery =null; 
    protected String q = "";
    protected String[] fq = {""}; // must not be null
    protected String fl="";
    /**
     * The facets to be included by the search
     * Initialised with the default facets to use
     */
    protected String[] facets = {"basis_of_record",
                                "type_status",
                                "institution_uid",
                                "collection_uid",
                                "data_resource_uid",
                                "collector",
                                "country",
                                "state",
                                "biogeographic_region",
                                "species_guid",
                                "rank",
                                "species_group",
                                "kingdom",
                                "family",
                                "subspecies_name",
                                "raw_taxon_name",
                                "uncertainty",
                                "interaction",
                                "month",
                                "year",
                                "state_conservation",
                                "raw_state_conservation",
                                "sensitive",
                                "assertions",
                                "multimedia",
                                "geospatial_kosher"};
    protected Integer start = 0;
    /*
     * The limit for the number of facets to return 
     */
    protected Integer flimit = 30;
    protected Integer pageSize = 10;
    protected String sort = "score";
    protected String dir = "asc";
    private String displayString;
    /**  The query context to be used for the search.  This will be used to generate extra query filters based on the search technology */
    protected String qc ="";
    /** To disable facets */
    protected Boolean facet = true;
   
    
    /**
     * Custom toString method to produce a String to be used as the request parameters
     * for the Biocache Service webservices
     * 
     * @return request parameters string
     */
    @Override
    public String toString() {        
        StringBuilder req = new StringBuilder();
        req.append("q=").append(q);
        req.append("&fq=").append(StringUtils.join(fq, "&fq="));
        req.append("&start=").append(start);
        req.append("&pageSize=").append(pageSize);
        req.append("&sort=").append(sort);
        req.append("&dir=").append(dir);
        req.append("&qc=").append(qc);
        //
        if(facets.length > 0 && facet)
            req.append("&facets=").append(StringUtils.join(facets, "&facets="));
        if (flimit != 30) 
            req.append("&flimit=").append(flimit);
        if (fl.length() > 0)
            req.append("&fl=").append(fl);
        if(StringUtils.isNotEmpty(formattedQuery))
            req.append("&formattedQuery=").append(formattedQuery);
        if(!facet)
            req.append("&facet=false");
        return req.toString();
    }
    /**
     * Constructs the params to be returned in the result 
     * @return
     */
    public String getUrlParams(){
        StringBuilder req = new StringBuilder();
        req.append("?q=").append(q);
        if(fq.length>0 && !fq[0].equals(""))
            req.append("&fq=").append(StringUtils.join(fq, "&fq="));
        req.append("&qc=").append(qc);
        return req.toString();
    }

    /**
     * Get the value of q
     *
     * @return the value of q
     */
    public String getQ() {
        return q;
    }

    /**
     * Set the value of q
     *
     * @param q new value of q
     */
    public void setQ(String query) {
        this.q = query;
    }
    
    /**
     * Get the value of fq
     *
     * @return the value of fq
     */
    public String[] getFq() {
        return fq;
    }

    /**
     * Set the value of fq
     *
     * @param fq new value of fq
     */
    public void setFq(String[] filterQuery) {
        this.fq = filterQuery;
    }
    
    /**
     * Get the value of start
     *
     * @return the value of start
     */
    public Integer getStart() {
        return start;
    }

    /**
     * Set the value of start
     *
     * @param start new value of start
     */
    public void setStart(Integer startIndex) {
        this.start = startIndex;
    }

    /**
     * Get the value of pageSize
     *
     * @return the value of pageSize
     */
    public Integer getPageSize() {
        return pageSize;
    }

    /**
     * Set the value of pageSize
     *
     * @param pageSize new value of pageSize
     */
    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * Get the value of sort
     *
     * @return the value of sort
     */
    public String getSort() {
        return sort;
    }

    /**
     * Set the value of sort
     *
     * @param sort new value of sort
     */
    public void setSort(String sortField) {
        this.sort = sortField;
    }

    /**
     * Get the value of dir
     *
     * @return the value of dir
     */
    public String getDir() {
        return dir;
    }

    /**
     * Set the value of dir
     *
     * @param dir new value of dir
     */
    public void setDir(String sortDirection) {
        this.dir = sortDirection;
    }

    public String getDisplayString() {
        return displayString;
    }

    public void setDisplayString(String displayString) {
        this.displayString = displayString;
    }

    public String[] getFacets() {
        return facets;
    }

    public void setFacets(String[] facets) {
        this.facets = facets;
    }

    public Integer getFlimit() {
        return flimit;
    }

    public void setFlimit(Integer flimit) {
        this.flimit = flimit;
    }

    public String getQc() {
        return qc;
    }

    public void setQc(String qc) {
        this.qc = qc;
    }
    public String getFl() {
        return fl;
    }

    public void setFl(String fl) {
        this.fl = fl;
    }

    /**
     * @return the formattedQuery
     */
    public String getFormattedQuery() {
        return formattedQuery;
    }

    /**
     * @param formattedQuery the formattedQuery to set
     */
    public void setFormattedQuery(String formattedQuery) {
        this.formattedQuery = formattedQuery;
    }

    public Boolean getFacet() {
        return facet;
    }

    public void setFacet(Boolean facet) {
        this.facet = facet;
    }
    
}
