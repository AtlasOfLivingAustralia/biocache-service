/**************************************************************************
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
package au.org.ala.biocache.dto;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the request parameters required to perform
 * a spatial search on occurrence records against biocache-service.
 *
 * Strictly a bean with no inherent logic other than validation through annotations.
 *
 * @author "Natasha Carter <Natasha.Carter@csiro.au>"
 */
@Schema(name = "Spatial search parameters")
public class SpatialSearchRequestParams {

    @Parameter(name="q", description = "Main search query. Examples 'q=Kangaroo' or 'q=vernacularName:red'")
    protected String q = "*:*";

    @Parameter(name="fq", description = "Filter queries. Examples 'fq=stateProvince:Victoria&fq=stateProvince:Queensland")
    protected String[] fq = {}; // must not be null

    @Parameter(name="qId", description = "Query ID for persisted queries")
    protected Long qId = null;  // "qid:12312321"

    @Parameter(name="fl", description = "Fields to return in the search response. Optional")
    protected String fl = OccurrenceIndex.defaultFields;

    @Parameter(name="facets", description = "The facets to be included by the search")
    protected String[] facets = FacetThemes.getAllFacetsLimited();

    @Parameter(name="flimit", description = "The limit for the number of facet values to return")
    protected Integer flimit = 30;

    @Parameter(name="fsort", description = "The sort order in which to return the facets.  Either 'count' or 'index'")
    protected String fsort = "";

    @Parameter(name="foffset", description = "The offset of facets to return.  Used in conjunction to flimit")
    protected Integer foffset = 0;

    @Parameter(name="fprefix", description = "The prefix to limit facet values")
    protected String fprefix ="";

    @Parameter(name="start", description = "Paging start index")
    protected Integer start = 0;

    @Parameter(name="pageSize", description = "The prefix to limit facet values")
    protected Integer pageSize = 10;

    @Parameter(name="sort", description = "The sort field to use")
    protected String sort = "score";

    @Parameter(name="dir", description = "Direction of sort")
    protected String dir = "asc";

    @Parameter(name="includeMultivalues", description = "Include multi values")
    protected Boolean includeMultivalues = false;

    @Parameter(name="qc", description = "The query context to be used for the search. This will be used to generate extra query filters.")
    protected String qc = "";

    @Parameter(name="facet", description = "Enable/disable facets")
    protected Boolean facet = FacetThemes.getFacetDefault();

    @Parameter(name="qualityProfile", description = "The quality profile to use, null for default")
    protected String qualityProfile;

    @Parameter(name="disableAllQualityFilters", description = "Disable all default filters")
    protected boolean disableAllQualityFilters = false;

    @Parameter(name="disableQualityFilter", description = "Default filters to disable (currently can only disable on category, so it's a list of disabled category name)")
    protected List<String> disableQualityFilter = new ArrayList<>();

    @Parameter(name="radius", description = "Radius for a spatial search")
    protected Float radius = null;

    @Parameter(name="lat", description = "Radius for a spatial search")
    protected Float lat = null;

    @Parameter(name="lon", description = "Radius for a spatial search")
    protected Float lon = null;

    @Parameter(name="wkt", description = "Radius for a spatial search")
    protected String wkt = "";

    public String getQ() {
        return q;
    }

    public void setQ(String q) {
        this.q = q;
    }

    public String[] getFq() {
        return fq;
    }

    public void setFq(String[] fq) {
        this.fq = fq;
    }

    public Long getqId() {
        return qId;
    }

    public void setqId(Long qId) {
        this.qId = qId;
    }

    public String getFl() {
        return fl;
    }

    public void setFl(String fl) {
        this.fl = fl;
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

    public String getFsort() {
        return fsort;
    }

    public void setFsort(String fsort) {
        this.fsort = fsort;
    }

    public Integer getFoffset() {
        return foffset;
    }

    public void setFoffset(Integer foffset) {
        this.foffset = foffset;
    }

    public String getFprefix() {
        return fprefix;
    }

    public void setFprefix(String fprefix) {
        this.fprefix = fprefix;
    }

    public Integer getStart() {
        return start;
    }

    public void setStart(Integer start) {
        this.start = start;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public String getSort() {
        return sort;
    }

    public void setSort(String sort) {
        this.sort = sort;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public Boolean getIncludeMultivalues() {
        return includeMultivalues;
    }

    public void setIncludeMultivalues(Boolean includeMultivalues) {
        this.includeMultivalues = includeMultivalues;
    }

    public String getQc() {
        return qc;
    }

    public void setQc(String qc) {
        this.qc = qc;
    }

    public Boolean getFacet() {
        return facet;
    }

    public void setFacet(Boolean facet) {
        this.facet = facet;
    }

    public String getQualityProfile() {
        return qualityProfile;
    }

    public void setQualityProfile(String qualityProfile) {
        this.qualityProfile = qualityProfile;
    }

    public boolean isDisableAllQualityFilters() {
        return disableAllQualityFilters;
    }

    public void setDisableAllQualityFilters(boolean disableAllQualityFilters) {
        this.disableAllQualityFilters = disableAllQualityFilters;
    }

    public List<String> getDisableQualityFilter() {
        return disableQualityFilter;
    }

    public void setDisableQualityFilter(List<String> disableQualityFilter) {
        this.disableQualityFilter = disableQualityFilter;
    }

    public Float getRadius() {
        return radius;
    }

    public void setRadius(Float radius) {
        this.radius = radius;
    }

    public Float getLat() {
        return lat;
    }

    public void setLat(Float lat) {
        this.lat = lat;
    }

    public Float getLon() {
        return lon;
    }

    public void setLon(Float lon) {
        this.lon = lon;
    }

    public String getWkt() {
        return wkt;
    }

    public void setWkt(String wkt) {
        this.wkt = wkt;
    }
}
