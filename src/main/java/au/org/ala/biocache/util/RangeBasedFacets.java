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
package au.org.ala.biocache.util;

import com.google.common.collect.ImmutableBiMap;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

@Component("rangeBasedFacets")
public class RangeBasedFacets {

    @Inject
    private AbstractMessageSource messageSource;

    private Map<String, ImmutableBiMap<String,String>> rangeFacets = new HashMap<String, ImmutableBiMap<String,String>>();
    
    public Map<String,String> getRangeMap(String name){
        init();
        if(rangeFacets.containsKey(name))
            return rangeFacets.get(name);
        return null;
    }
    
    public Map<String,String> getTitleMap(String name){
        init();
        if(rangeFacets.containsKey(name))
            return rangeFacets.get(name).inverse();
        return null;
    }
    
    private void init() {
        String less_than = messageSource.getMessage("rangefacet.less_than", null, "less than {0}", null);
        String between = messageSource.getMessage("rangefacet.between", null, "between {0} and {1}", null);
        String greater_than = messageSource.getMessage("rangefacet.greater_than", null, "greater than {0}", null);
        String unknown = messageSource.getMessage("rangefacet.unknown", null, "Unknown", null);

        //construct the bi directional map for the uncertainty ranges
        ImmutableBiMap<String, String> map = new ImmutableBiMap.Builder<String,String>()
                .put("coordinate_uncertainty:[0 TO 100]", MessageFormat.format(less_than, "100"))
                .put("coordinate_uncertainty:[101 TO 500]", MessageFormat.format(between, "100", "500"))
                .put("coordinate_uncertainty:[501 TO 1000]", MessageFormat.format(between, "500", "1000"))
                .put("coordinate_uncertainty:[1001 TO 5000]", MessageFormat.format(between,"1000","5000"))
                .put("coordinate_uncertainty:[5001 TO 10000]", MessageFormat.format(between,"5000","10000"))
                .put("coordinate_uncertainty:[10001 TO *]", MessageFormat.format(greater_than,"10000"))
                .put("-coordinate_uncertainty:[* TO *]", unknown).build();

        rangeFacets.put("uncertainty", map);
    }
}
