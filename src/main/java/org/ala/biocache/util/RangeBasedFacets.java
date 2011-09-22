package org.ala.biocache.util;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.ala.biocache.dto.OccurrenceSource;
import org.apache.commons.lang.StringUtils;

import com.google.common.collect.BiMap;
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
