/**************************************************************************
 *  Copyright (C) 2013 Atlas of Living Australia
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

import au.org.ala.biocache.util.QueryFormatUtils;
import com.google.common.base.Strings;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Data Transfer Object to represent the request parameters required to search
 * for occurrence records against biocache-service.
 * 
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
public class SearchRequestDTO {

    /** log4 j logger */
    private static final Logger logger = Logger.getLogger(SearchRequestDTO.class);

    /** Only used to store the formattedQuery to be passed around in biocache-service**/

    protected Long qId = null;  // "qid:12312321"
    protected String formattedQuery = null;
    protected String q = "*:*";
    protected String[] fq = {}; // must not be null
    protected String[] formattedFq = {}; // must not be null

    /**
     * When the default OccurrenceIndex mapped SOLR fields are stored=false and docValues=true fl must include them
     */
    protected String fl = OccurrenceIndex.defaultFields;

    /**
     * The facets to be included by the search
     * Initialised with the default facets to use
     */
    protected String[] facets =  new String[0]; //FacetThemes.getAllFacetsLimited();
    protected Integer facetsMax = 30; //FacetThemes.getFacetsMax();
    /** To disable facets */
    protected Boolean facet = true; //FacetThemes.getFacetDefault();

    protected Integer start = 0;
    /*
     * The limit for the number of facets to return 
     */
    protected Integer flimit = 30;
    /** The sort order in which to return the facets.  Either count or index.  When empty string the default values are used as defined in the Theme based facets */
    protected String fsort = "";
    /** The offset of facets to return.  Used in conjunction to flimit */
    protected Integer foffset = 0;
    /** The prefix to limit facet values*/
    protected String fprefix = "";
    protected Integer pageSize = 10;
    protected String sort = "";
    protected String dir = "asc";
    private String displayString;

    protected Boolean includeMultivalues = false;

    /**  The query context to be used for the search.  This will be used to generate extra query filters based on the search technology */
    protected String qc = "";


    /** The quality profile to use, null for default */
    protected String qualityProfile;
    /** If to disable all default filters*/
    protected boolean disableAllQualityFilters = false;
    /** Default filters to disable (currently can only disable on category, so it's a list of disabled category name)*/
    protected List<String> disableQualityFilter = new ArrayList<>();

    /**
     * Custom toString method to produce a String to be used as the request parameters
     * for the Biocache Service webservices
     * 
     * @return request parameters string
     */
    @Override
    public String toString() {        
        return toString(false);
    }

    /**
     * Produce a URI encoded query string for use in java.util.URI, etc
     *
     * @return encoded query string
     */
    public String getEncodedParams() {
        return toString(true);
    }

    /**
     * Common code to output a param string with conditional encoding of values
     *
     * @param encodeParams
     * @return query string
     */
    protected String toString(Boolean encodeParams) {
        StringBuilder req = new StringBuilder();
        boolean isFacet = this.getFacet() == null ? true : this.getFacet();
        req.append("q=").append(conditionalEncode(q, encodeParams));
        if (fq.length > 0) {
            for (String it : fq) {
                req.append("&fq=").append(conditionalEncode(it, encodeParams));
            }
        }
        req.append("&start=").append(start);
        req.append("&pageSize=").append(pageSize);
        req.append("&sort=").append(sort);
        req.append("&dir=").append(dir);
        req.append("&qc=").append(qc);
        if (facets != null && facets.length > 0 && isFacet) {
            for (String f : facets) {
                req.append("&facets=").append(conditionalEncode(f, encodeParams));
            }
        }
        if (flimit != 30)
            req.append("&flimit=").append(flimit);
        if (fl.length() > 0)
            req.append("&fl=").append(conditionalEncode(fl, encodeParams));
        if(StringUtils.isNotEmpty(formattedQuery))
            req.append("&formattedQuery=").append(conditionalEncode(formattedQuery, encodeParams));
        req.append("&facet=" + isFacet);
        if(!"".equals(fsort))
            req.append("&fsort=").append(fsort);
        if(foffset > 0)
            req.append("&foffset=").append(foffset);
        if(!"".equals(fprefix))
            req.append("&fprefix=").append(fprefix);

        if (!Strings.isNullOrEmpty(qualityProfile)) {
            req.append("&qualityProfile=").append(conditionalEncode(qualityProfile, encodeParams));
        }
        if (disableAllQualityFilters) {
            req.append("&disableAllQualityFilters=true");
        }
        for (String filter : disableQualityFilter) {
            req.append("&disableQualityFilter=").append(conditionalEncode(filter, encodeParams));
        }

        return req.toString();
    }

    /**
     * URI encode the param value if isEncoded is true
     *
     * @param input
     * @param isEncoded
     * @return query string
     */
    protected String conditionalEncode(String input, Boolean isEncoded) {
        String output;

        if (isEncoded) {
            try {
                output = URIUtil.encodeWithinQuery(input, "UTF-8");
            } catch (URIException e) {
                logger.warn("URIUtil encoding error: " + e.getMessage(), e);
                output = input;
            }
        } else {
            output = input;
        }

        return output;
    }

    /**
     * Constructs the params to be returned in the result 
     * @return req
     */
    public String getUrlParams(){
        StringBuilder req = new StringBuilder();
        if(qId != null){
            req.append("?q=qid:").append(conditionalEncode(qId.toString(), true));
        } else {
            req.append("?q=").append(conditionalEncode(q, true));
        }

        if (disableAllQualityFilters) {
            req.append("&disableAllQualityFilters=true");
        }

        if (StringUtils.isNotBlank(qualityProfile)) {
            req.append("&qualityProfile=").append(qualityProfile);
        }

        for (String disableQualityFilter: this.disableQualityFilter) {
            req.append("&disableQualityFilter=").append(disableQualityFilter);
        }

        for(String f : fq){
            //only add the fq if it is not the query context
          if(f.length()>0 && !f.equals(qc)) {
              req.append("&fq=").append(conditionalEncode(f, true));
          }
        }
        if(qc != ""){
            req.append("&qc=").append(conditionalEncode(qc, true));
        }
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
     * @param query new value of q
     */
    public void setQ(String query) {
        QueryFormatUtils.assertNoSensitiveValues(SearchRequestDTO.class, "q", query);
        this.q = query;
    }

    public Long getQId() {
        return qId;
    }

    public void setQId(Long qId) {
        this.qId = qId;
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
     * @param filterQuery new value of fq
     */
    public void setFq(String[] filterQuery) {
        QueryFormatUtils.assertNoSensitiveValues(SearchRequestDTO.class, "fq", filterQuery);
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
    public void setStart(Integer start) {
        this.start = start;
    }

    /**
     * Set the value of start
     *
     * @param startIndex new value of start
     */
    public void setStartIndex(Integer startIndex) {
        this.start = startIndex;
    }

    /**
     * Set the value of start
     */
    public Integer getStartIndex() {
        return this.start;
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
    public void setSort(String sort) {
        QueryFormatUtils.assertNoSensitiveValues(SearchRequestDTO.class, "sort", sort);
        this.sort = sort;
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
     * @param sortDirection new value of dir
     */
    public void setDir(String sortDirection) {
        QueryFormatUtils.assertNoSensitiveValues(SearchRequestDTO.class, "sortDirection", sortDirection);
        this.dir = sortDirection;
    }

    public String getDisplayString() {
        return displayString;
    }

    public void setDisplayString(String displayString) {
        QueryFormatUtils.assertNoSensitiveValues(SearchRequestDTO.class, "displayString", displayString);
        this.displayString = displayString;
    }

    public String[] getFacets() {
        return facets;
    }

    public void setFacets(String[] facets) {
        QueryFormatUtils.assertNoSensitiveValues(SearchRequestDTO.class, "facets", facets);

        if (facets != null && facets.length == 1 && facets[0].contains(",")) facets = facets[0].split(",");

        //remove empty facets
        List<String> list = new ArrayList<String>();
        if (facets != null) {
            for (String f : facets) {
                //limit facets terms
                if (StringUtils.isNotEmpty(f) && list.size() < facetsMax) {
                    list.add(f);
                }
            }
        }
        this.facets = list.toArray(new String[0]);
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
        QueryFormatUtils.assertNoSensitiveValues(SearchRequestDTO.class, "qc", qc);
        this.qc = qc;
    }
    public String getFl() {
        return fl;
    }

    public void setFl(String fl) {
        QueryFormatUtils.assertNoSensitiveValues(SearchRequestDTO.class, "fl", fl);
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
        QueryFormatUtils.assertNoSensitiveValues(SearchRequestDTO.class, "formattedQuery", formattedQuery);
        this.formattedQuery = formattedQuery;
    }

    public Boolean getFacet() {
        return facet;
    }

    public void setFacet(Boolean facet) {
        this.facet = facet;
    }
	/**
	 * @return the fsort
	 */
	public String getFsort() {
		return fsort;
	}
	/**
	 * @param fsort the fsort to set
	 */
	public void setFsort(String fsort) {
        QueryFormatUtils.assertNoSensitiveValues(SearchRequestDTO.class, "fsort", fsort);
        this.fsort = fsort;
	}
	/**
	 * @return the foffset
	 */
	public Integer getFoffset() {
		return foffset;
	}
	/**
	 * @param foffset the foffset to set
	 */
	public void setFoffset(Integer foffset) {
		this.foffset = foffset;
	}
	/**
	 * @return the fprefix
	 */
	public String getFprefix() {
		return fprefix;
	}
	/**
	 * @param fprefix the fprefix to set
	 */
	public void setFprefix(String fprefix) {
		this.fprefix = fprefix;
	}

    public Boolean getIncludeMultivalues() {
        return includeMultivalues;
    }

    public void setIncludeMultivalues(Boolean includeMultivalues) {
        this.includeMultivalues = includeMultivalues;
    }

    public String[] getFormattedFq() {
        return formattedFq;
    }

    public void setFormattedFq(String[] formattedFq) {
        QueryFormatUtils.assertNoSensitiveValues(SearchRequestDTO.class, "formattedFq", formattedFq);
        this.formattedFq = formattedFq;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchRequestDTO that = (SearchRequestDTO) o;
        return disableAllQualityFilters == that.disableAllQualityFilters &&
                Objects.equals(formattedQuery, that.formattedQuery) &&
                Objects.equals(q, that.q) &&
                Arrays.equals(fq, that.fq) &&
                Objects.equals(fl, that.fl) &&
                Arrays.equals(facets, that.facets) &&
                Objects.equals(start, that.start) &&
                Objects.equals(facetsMax, that.facetsMax) &&
                Objects.equals(flimit, that.flimit) &&
                Objects.equals(fsort, that.fsort) &&
                Objects.equals(foffset, that.foffset) &&
                Objects.equals(fprefix, that.fprefix) &&
                Objects.equals(pageSize, that.pageSize) &&
                Objects.equals(sort, that.sort) &&
                Objects.equals(dir, that.dir) &&
                Objects.equals(displayString, that.displayString) &&
                Objects.equals(qc, that.qc) &&
                Objects.equals(facet, that.facet) &&
                Objects.equals(qualityProfile, that.qualityProfile) &&
                Objects.equals(disableQualityFilter, that.disableQualityFilter);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(formattedQuery, q, fl, start, facetsMax, flimit, fsort, foffset, fprefix, pageSize, sort, dir, displayString, qc, facet, qualityProfile, disableAllQualityFilters, disableQualityFilter);
        result = 31 * result + Arrays.hashCode(fq);
        result = 31 * result + Arrays.hashCode(facets);
        return result;
    }
 
}
