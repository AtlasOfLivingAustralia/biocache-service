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

import org.apache.commons.lang.ArrayUtils;

import java.util.Arrays;

/**
 * Data Transfer Object to represent the request parameters required to perform
 * a spatial search on occurrence records against biocache-service.
 *
 * @author "Natasha Carter <Natasha.Carter@csiro.au>"
 */
public class SpatialSearchRequestParams extends SearchRequestParams {

    public final static String[] gkFq = new String[]{OccurrenceIndex.GEOSPATIAL_KOSHER + ":true"};

    protected Float radius = null;
    protected Float lat = null;
    protected Float lon = null;
    protected String wkt = "";
    protected Boolean gk = false;   //include only the geospatially kosher records

    /**
     * Custom toString method to produce a String to be used as the request parameters
     * for the Biocache Service webservices
     *
     * @return request parameters string
     */
    @Override
    public String toString() {
        return addSpatialParams(super.toString(), false);
    }

    /**
     * Produce a URI encoded query string for use in java.util.URI, etc
     *
     * @return
     */
    public String getEncodedParams() {
        return addSpatialParams(super.getEncodedParams(), true);
    }

    /**
     * Add the spatial params to the param string
     *
     * @param paramString
     * @return
     */
    protected String addSpatialParams(String paramString, Boolean encodeParams) {
        StringBuilder req = new StringBuilder(paramString);
        if (lat != null && lon != null && radius != null) {
            req.append("&lat=").append(lat);
            req.append("&lon=").append(lon);
            req.append("&radius=").append(radius);
        }
        if(wkt != null && wkt.length() >0)
            req.append("&wkt=").append(super.conditionalEncode(wkt, encodeParams));
        if(gk)
            req.append("&gk=true");
        return req.toString();
    }

    @Override
    public String getUrlParams(){
        StringBuilder req = new StringBuilder(super.getUrlParams());
        if (lat != null && lon != null && radius != null) {
            req.append("&lat=").append(conditionalEncode(lat.toString(), true));
            req.append("&lon=").append(conditionalEncode(lon.toString(), true));
            req.append("&radius=").append(conditionalEncode(radius.toString(), true));
        }
        if(wkt != null && wkt.length() >0)
            req.append("&wkt=").append(conditionalEncode(wkt, true));
        return req.toString();
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

    public Float getRadius() {
        return radius;
    }

    public void setRadius(Float radius) {
        this.radius = radius;
    }

    public String getWkt() {
        return wkt;
    }

    public void setWkt(String wkt) {
        this.wkt =wkt;
        //NC: switched the replace statement so that the incorrect WKT format required for the OLD Spatial Plugin is replaced with correct values.
        if(wkt != null){
            this.wkt = wkt.replace(':', ' ');
        }
    }
    /**
     * @return the gk
     */
    public Boolean getGk() {
        return gk;
    }

    /**
     * @param gk the gk to set
     */
    public void setGk(Boolean gk) {
        this.gk = gk;
    }

    /**
     * Get the value of fq.  
     * 
     * Adds the gk FQ if necessary...
     *
     * @return the value of fq
     */
    public String[] getFq() {
        if(gk) {
            return (String[]) ArrayUtils.addAll(fq, gkFq);
        } else {
            return fq;
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((gk == null) ? 0 : gk.hashCode());
        result = prime * result + Arrays.hashCode(gkFq);
        result = prime * result + ((lat == null) ? 0 : lat.hashCode());
        result = prime * result + ((lon == null) ? 0 : lon.hashCode());
        result = prime * result + ((radius == null) ? 0 : radius.hashCode());
        result = prime * result + ((wkt == null) ? 0 : wkt.hashCode());
        return result;
    }      
}
