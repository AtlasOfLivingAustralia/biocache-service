/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

/**
 * Stores the details for the "SOURCES" facet information.
 * This class exists to hide the details from the web clients.
 * We are able to display sources without exposing the "confidence"
 * SOLR field
 *
 * TODO Rename everything to more fitting names
 *
 * @author Natasha Carter
 */
public enum OccurrenceSource {
    NON_CITIZEN("[0 TO 2]", "Published datasets"),
    CITZEN("[2 TO *]", "Individual sightings");
    /** The range to use in the SOLR query */
    private String range;
    /** The name to be displayed  */
    private String displayName;
    public static final String FACET_NAME = "SOURCES";
    private static final Map<String,OccurrenceSource> displayNameLookup
          = new HashMap<String,OccurrenceSource>();
    private static final Map<String, OccurrenceSource> rangeLookup
          = new HashMap<String, OccurrenceSource>();

    static {
         for (OccurrenceSource os : EnumSet.allOf(OccurrenceSource.class)) {
             displayNameLookup.put(os.getDisplayName().toLowerCase(), os);
             rangeLookup.put(os.getRange(), os);
         }
    }

    private OccurrenceSource(String r, String d){
        range = r;
        displayName = d;
    }
    
    public String getRange(){
        return range;
    }
    
    public String getDisplayName(){
        return displayName;
    }
    
    public static OccurrenceSource getForDisplayName(String name){
        if(StringUtils.isBlank(name))
            return null;
        return displayNameLookup.get(name.toLowerCase());
    }
    
    public static OccurrenceSource getForRange(String range){
        if(StringUtils.isBlank(range))
            return null;
        return rangeLookup.get(range);
    }
}
