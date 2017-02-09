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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import javax.inject.Inject;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * The ALA Spatial portal implementation for the layer service.
 * Metadata information will be cached from spatial webservices.
 * 
 * @author Natasha Carter (natasha.carter@csiro.au)
 */
@Component("layersService")
public class AlaLayersService implements LayersService {

    private final static Logger logger = LoggerFactory.getLogger(AlaLayersService.class);

    private Map<String,String> idToNameMap = new HashMap<String, String>();
    private List<Map<String,Object>> layers = new ArrayList<Map<String,Object>>();
    private Map<String,String> extraLayers = new HashMap<String,String>();
    
    //NC 20131018: Allow cache to be disabled via config (enabled by default)
    @Value("${caches.layers.enabled:true}")
    protected Boolean enabled = null;
    
    @Value("${spatial.layers.url:http://spatial.ala.org.au/ws/fields}")
    protected String spatialUrl;

    @Value("${layers.service.download.sample:true}")
    protected Boolean layersServiceAnalysisLayers;

    @Value("${layers.service.url:http://spatial.ala.org.au/ws}")
    protected String layersServiceUrl;

    protected Map<String, Integer> distributions = new HashMap<String, Integer>();
    protected Map<String, Integer> checklists = new HashMap<String, Integer>();
    protected Map<String, Integer> tracks = new HashMap<String, Integer>();
    
    @Inject
    private RestOperations restTemplate; // NB MappingJacksonHttpMessageConverter() injected by Spring

    private CountDownLatch wait = new CountDownLatch(1);
    
    @Override
    public Map<String, String> getLayerNameMap() {
        try {
            wait.await();
        } catch (InterruptedException e) {
        }
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

            distributions = initDistribution("distributions");
            checklists = initDistribution("checklists");

            //TODO: initialize tracks only when webservices are available
            //tracks = initDistribution("tracks");
        }
        wait.countDown();
    }

    @Override
    public String getName(String code) {
        try {
            wait.await();
        } catch (InterruptedException e) {
        }
        return idToNameMap.get(code);
    }

    public String findAnalysisLayerName(String analysisLayer, String layersServiceUrl) {
        String url = this.layersServiceUrl;
        if (layersServiceUrl != null) url = layersServiceUrl;

        if (!layersServiceAnalysisLayers) {
            return null;
        }

        if (extraLayers.containsKey(analysisLayer)) {
            return extraLayers.get(analysisLayer);
        }

        String found = null;
        String intersectUrl = null;
        try {
            //get analysis layer display name
            intersectUrl = url + "/intersect/" + URLEncoder.encode(analysisLayer, "UTF-8") + "/1/1";
            List json = restTemplate.getForObject(intersectUrl, List.class);
            if (json != null && json.size() > 0) {
                found = (String) ((Map) json.get(0)).get("layername");
            }

            extraLayers.put(analysisLayer, found);
        } catch (Exception ex) {
            logger.error("RestTemplate error for " + url + ": " + ex.getMessage(), ex);
        }

        return found;
    }

    public Integer getDistributionsCount(String lsid){
        try {
            wait.await();
        } catch (InterruptedException e) {
        }

        Integer count = distributions.get(lsid);

        return count != null ? count : 0;
    }

    public Integer getChecklistsCount(String lsid){
        try {
            wait.await();
        } catch (InterruptedException e) {
        }

        Integer count = checklists.get(lsid);

        return count != null ? count : 0;
    }

    public Integer getTracksCount(String lsid){
        try {
            wait.await();
        } catch (InterruptedException e) {
        }

        Integer count = tracks.get(lsid);

        return count != null ? count : 0;
    }

    private Map initDistribution(String type) {
        Map<String, Integer> map = new HashMap<String, Integer>();

        String url = null;
        try {
            //get distributions
            url = layersServiceUrl + "/" + type;
            List json = restTemplate.getForObject(url, List.class);
            if (json != null) {
                for (int i=0;i<json.size();i++) {
                    String s = (String) ((Map) json.get(i)).get("lsid");
                    if (StringUtils.isNotEmpty(s)) {
                        Integer count = map.get(s);
                        if (count == null) count = 0;
                        count = count + 1;
                        map.put(s, count);
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("RestTemplate error for " + url + ": " + ex.getMessage(), ex);
        }

        return map;
    }
}
