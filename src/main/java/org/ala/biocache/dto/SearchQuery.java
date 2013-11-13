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
package org.ala.biocache.dto;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * A model object to aid in the sharing and manipulation of query values
 *  
 * @author Natasha
 */
public class SearchQuery {
    private String query;
    private ArrayList<String> filterQuery;
    private String type;
    private String displayString;
    private String entityQuery;

    public SearchQuery(String query, String type, String[] filterQuery){
        this.query = query;
        this.type = type;
        if(filterQuery != null)
            this.filterQuery = new ArrayList<String>(Arrays.asList(filterQuery));

    }
    public SearchQuery(String query, String type){
        this(query, type, null);
    }

    public String[] getFilterQuery() {
        if(filterQuery != null)
            return filterQuery.toArray(new String[]{});
        return null;
    }

    public void addToFilterQuery(String fq) {
        if(filterQuery == null)
            filterQuery = new ArrayList<String>();
        filterQuery.add(fq);
    }
    public void removeFromFilterQuery(String fq){
        if(filterQuery != null)
            filterQuery.remove(fq);
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDisplayString() {
        return displayString;
    }

    public void setDisplayString(String displayString) {
        this.displayString = displayString;
    }

    public String getEntityQuery() {
        return entityQuery;
    }

    public void setEntityQuery(String entityQuery) {
        this.entityQuery = entityQuery;
    }
}
