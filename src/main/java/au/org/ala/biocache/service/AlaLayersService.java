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
package au.org.ala.biocache.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

/**
 * The ALA Spatial portal implementation for the layer service.
 * Metadata information will be cached from spatial webservices.
 * 
 * @author Natasha Carter (natasha.carter@csiro.au)
 */
@Component("layersService")
public class AlaLayersService implements LayersService {
	
    private Map<String,String> idToNameMap = new HashMap<String, String>();
    private List<Map<String,Object>> layers = new ArrayList<Map<String,Object>>();
    
    //NC 20131018: Allow cache to be disabled via config (enabled by default)
    @Value("${caches.layers.enabled:true}")
    protected Boolean enabled = null;
    
    @Value("${spatial.layers.url:http://spatial.ala.org.au/ws/fields}")
    protected String spatialUrl;
    
    @Inject
    private RestOperations restTemplate; // NB MappingJacksonHttpMessageConverter() injected by Spring
    
    @Override
    public Map<String, String> getLayerNameMap() {        
        return idToNameMap;
    }
    
    @Scheduled(fixedDelay = 43200000)// schedule to run every 12 hours
    public void refreshCache(){
        //initialise the cache based on the values at http://spatial.ala.org.au/ws/fields
        if(enabled){
            //create a tmp map
            Map<String,String> tmpMap = new HashMap<String,String>();
            layers = restTemplate.getForObject(spatialUrl, List.class);
            for(Map<String,Object> values : layers){
                tmpMap.put((String)values.get("id"), (String)values.get("desc"));
            }
            idToNameMap = tmpMap;
        }
    }
    @Override
    public String getName(String code) {        
        return idToNameMap!=null?idToNameMap.get(code):null;
    }
}
