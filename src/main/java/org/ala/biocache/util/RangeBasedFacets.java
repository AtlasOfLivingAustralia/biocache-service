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
package org.ala.biocache.util;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableBiMap;

public class RangeBasedFacets {
	
    public static Map<String, ImmutableBiMap<String,String>> rangeFacets = new HashMap<String, ImmutableBiMap<String,String>>();
    
    public static Map<String,String> getRangeMap(String name){
        if(rangeFacets.containsKey(name))
            return rangeFacets.get(name);
        return null;
    }
    
    public static Map<String,String> getTitleMap(String name){
        if(rangeFacets.containsKey(name))
            return rangeFacets.get(name).inverse();
        return null;
    }
    
    static{
        //construct the bi directional map for the uncertainty ranges
        ImmutableBiMap<String, String> map = new ImmutableBiMap.Builder<String,String>()
                .put("coordinate_uncertainty:[0 TO 100]", "less than 100")
                .put("coordinate_uncertainty:[101 TO 500]","between 100 and 500")
                .put("coordinate_uncertainty:[501 TO 1000]", "between 500 and 1000")
                .put("coordinate_uncertainty:[1001 TO 5000]", "between 1000 and 5000")
                .put("coordinate_uncertainty:[5001 TO 10000]", "between 5000 and 10000")
                .put("coordinate_uncertainty:[10001 TO *]", "greater than 10000")
                .put("-coordinate_uncertainty:[* TO *]", "Unknown").build();

        rangeFacets.put("uncertainty", map);
    }
}
