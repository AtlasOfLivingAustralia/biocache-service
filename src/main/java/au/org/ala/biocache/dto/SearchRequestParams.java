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

import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;

/**
 * Data Transfer Object to represent the request parameters required to search
 * for occurrence records against biocache-service.
 * 
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
public class SearchRequestParams {
    /** Only used to store the formattedQuery to be passed around in biocache-service**/

    protected Long qId = null;  // "qid:12312321"
    protected String formattedQuery = null;
    protected String q = "*:*";
    protected String[] fq = {""}; // must not be null
    protected String fl = "";
    /**
     * The facets to be included by the search
     * Initialised with the default facets to use
     */
    protected String[] facets = FacetThemes.allFacets;
    protected Integer start = 0;
    /*
     * The limit for the number of facets to return 
     */
    protected Integer flimit = 30;
    /** The sort order in which to return the facets.  Either count or index.  When empty string the default values are used as defined in the Theme based facets */
    protected String fsort="";
    /** The offset of facets to return.  Used in conjunction to flimit */
    protected Integer foffset = 0;
    /** The prefix to limit facet values*/
    protected String fprefix ="";
    protected Integer pageSize = 10;
    protected String sort = "score";
    protected String dir = "asc";
    private String displayString;
    /**  The query context to be used for the search.  This will be used to generate extra query filters based on the search technology */
    protected String qc = "";
    /** To disable facets */
    protected Boolean facet = true;
    /** log4 j logger */
    private static final Logger logger = Logger.getLogger(SearchRequestParams.class);
    
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
        if (facets.length > 0 && facet) {
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
        if(!facet)
            req.append("&facet=false");
        if(!"".equals(fsort))
            req.append("&fsort=").append(fsort);
        if(foffset > 0)
            req.append("&foffset=").append(foffset);
        if(!"".equals(fprefix))
            req.append("&fprefix=").append(fprefix);

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
                output = URIUtil.encodeWithinQuery(input);
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
            req.append("?q=qid:").append(qId);
        } else {
            try{
                req.append("?q=").append(URLEncoder.encode(q, "UTF-8"));
            } catch(UnsupportedEncodingException e){}
        }

        for(String f : fq){
            //only add the fq if it is not the query context
          if(f.length()>0 && !f.equals(qc))
            try{
                req.append("&fq=").append(URLEncoder.encode(f, "UTF-8"));
            } catch(UnsupportedEncodingException e){}
        }
//        if(fq.length>0 && !fq[0].equals(""))
//            req.append("&fq=").append(StringUtils.join(fq, "&fq="));
        if(qc != ""){
            req.append("&qc=").append(qc);
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

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((dir == null) ? 0 : dir.hashCode());
      result = prime * result
          + ((displayString == null) ? 0 : displayString.hashCode());
      result = prime * result + ((facet == null) ? 0 : facet.hashCode());
      result = prime * result + Arrays.hashCode(facets);
      result = prime * result + ((fl == null) ? 0 : fl.hashCode());
      result = prime * result + ((flimit == null) ? 0 : flimit.hashCode());
      result = prime * result + ((foffset == null) ? 0 : foffset.hashCode());
      result = prime * result
          + ((formattedQuery == null) ? 0 : formattedQuery.hashCode());
      result = prime * result + ((fprefix == null) ? 0 : fprefix.hashCode());
      result = prime * result + Arrays.hashCode(fq);
      result = prime * result + ((fsort == null) ? 0 : fsort.hashCode());
      result = prime * result + ((pageSize == null) ? 0 : pageSize.hashCode());
      result = prime * result + ((q == null) ? 0 : q.hashCode());
      result = prime * result + ((qc == null) ? 0 : qc.hashCode());
      result = prime * result + ((sort == null) ? 0 : sort.hashCode());
      result = prime * result + ((start == null) ? 0 : start.hashCode());
      return result;
  }
 
}
