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
package au.org.ala.biocache.util;

import au.org.ala.biocache.service.RestartDataService;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Provides access to the collection and institution codes and names from the Collectory.
 * Uses the registry webservices to get a map of codes & names for institutions and collections
 * and caches these. Cache is automatically updated after a configurable timeout period.
 *
 * NC 2013-0925 Changed the collection cache to be async scheduled
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
@Component("collectionsCache")
public class CollectionsCache {

    protected LinkedHashMap<String, String> dataResources = RestartDataService.get(this, "dataResources", new TypeReference<LinkedHashMap>(){}, LinkedHashMap.class);
    protected LinkedHashMap<String, String> dataProviders = RestartDataService.get(this, "dataProviders", new TypeReference<LinkedHashMap>(){}, LinkedHashMap.class);
    protected LinkedHashMap<String, String> tempDataResources = RestartDataService.get(this, "tempDataResources", new TypeReference<LinkedHashMap>(){}, LinkedHashMap.class);
    protected LinkedHashMap<String, Integer> downloadLimits = RestartDataService.get(this, "downloadLimits", new TypeReference<LinkedHashMap>(){}, LinkedHashMap.class);
    protected LinkedHashMap<String, String> institutions = RestartDataService.get(this, "institutions", new TypeReference<LinkedHashMap>(){}, LinkedHashMap.class);
    protected LinkedHashMap<String, String> collections = RestartDataService.get(this, "collections", new TypeReference<LinkedHashMap>(){}, LinkedHashMap.class);
    protected LinkedHashMap<String, String> dataHubs = RestartDataService.get(this, "dataHubs", new TypeReference<LinkedHashMap>(){}, LinkedHashMap.class);
    protected List<String> institution_uid = null;
    protected List<String> collection_uid = null;
    protected List<String> data_resource_uid = null;
    protected List<String> data_provider_uid = null;
    protected List<String> data_hub_uid = null;

    @Value("${registry.url:http://collections.ala.org.au/ws}")
    protected String registryUrl;

    //NC 20131018: Allow cache to be disabled via config (enabled by default)
    @Value("${caches.collections.enabled:true}")
    protected Boolean enabled = null;
    /** Spring injected RestTemplate object */
    @Inject
    private RestOperations restTemplate; // NB MappingJacksonHttpMessageConverter() injected by Spring
    /** Log4J logger */
    private final static Logger logger = Logger.getLogger(CollectionsCache.class);  
    
    /**
     * Get the institutions
     *
     * @return
     */
    public LinkedHashMap<String, String> getInstitutions() {
        return this.institutions;
    }
    
    public LinkedHashMap<String, String> getDataResources(){
        return this.dataResources;
    }

    public LinkedHashMap<String, String> getDataProviders(){
        return this.dataProviders;
    }

    public LinkedHashMap<String, String> getTempDataResources(){
        return this.tempDataResources;
    }

    public LinkedHashMap<String, String> getCollections() {
        return this.collections;
    }
    
    public LinkedHashMap<String, String> getDataHubs() {
        return this.dataHubs;
    }

    public LinkedHashMap<String, Integer> getDownloadLimits(){
        return downloadLimits;
    }


    @PostConstruct
    public void init() {
        updateCache();
    }

    /**
     * Update the entity types (fields)
     */
    @Scheduled(fixedDelay = 3600000L) //every hour
    public void updateCache() {
        Thread thread = new Thread() {
            @Override
            public void run() {
                if(enabled){
                    logger.info("Updating collectory cache...");
                    LinkedHashMap m;
                    m = getCodesMap(ResourceType.COLLECTION, collection_uid);
                    if (m != null && m.size() > 0) collections = m;

                    m = getCodesMap(ResourceType.INSTITUTION, institution_uid);
                    if (m != null && m.size() > 0) institutions = m;

                    m = getCodesMap(ResourceType.DATA_RESOURCE,data_resource_uid);
                    if (m != null && m.size() > 0) dataResources = m;

                    m = getCodesMap(ResourceType.DATA_PROVIDER, data_provider_uid);
                    if (m != null && m.size() > 0) dataProviders = m;

                    m = getCodesMap(ResourceType.TEMP_DATA_RESOURCE, null);
                    if (m != null && m.size() > 0) tempDataResources = m;

                    m = dataHubs = getCodesMap(ResourceType.DATA_HUB, data_hub_uid);
                    if (m != null && m.size() > 0) dataHubs = m;

                    dataResources.putAll(tempDataResources);
                } else{
                    logger.info("Collectory cache has been disabled");
                }
            }
        };

        if (collections.size() > 0) {
            //data already exists, do not wait
            thread.start();
        } else {
            //wait
            thread.run();
        }
    }
    
    /**
     * Do the web services call. Uses RestTemplate.
     *
     * @param type
     * @return
     */
    protected LinkedHashMap<String,String> getCodesMap(ResourceType type, List<String> guids) {
        LinkedHashMap<String, String> entityMap = null;
        logger.info("Updating code map with " + guids);
        try {
            // grab cached values (map) in case WS is not available (uses reflection)
            Field f = CollectionsCache.class.getDeclaredField(type.getType() + "s"); // field is plural form
            entityMap = (LinkedHashMap<String, String>) f.get(this);
            logger.debug("checking map size: " + entityMap.size());
        } catch (Exception ex) {
            logger.error("Java reflection error: " + ex.getMessage(), ex);
        }

        try {
            entityMap = new LinkedHashMap<String, String>(); // reset now we're inside the try
            final String jsonUri = registryUrl + "/" + type.getType() + ".json";
            logger.debug("Requesting: " + jsonUri);
            List<LinkedHashMap<String, String>> entities = restTemplate.getForObject(jsonUri, List.class);
            logger.debug("number of entities = " + entities.size());

            for (LinkedHashMap<String, String> je : entities) {
                if(addToCodeMap(je.get("uid"), guids)){
                    entityMap.put(je.get("uid"), je.get("name"));
                }
            }
        } catch (Exception ex) {
            logger.error("RestTemplate error: " + ex.getMessage(), ex);
        }

        return entityMap;
    }

    private boolean addToCodeMap(String uid, List<String> guids){
        if(guids != null){
            return guids.contains(uid);
        }
        return true;
    }
    
    /**
     * Inner enum class
     */
    public enum ResourceType {
        INSTITUTION("institution"),
        COLLECTION("collection"),
        DATA_RESOURCE("dataResource"),
        DATA_PROVIDER("dataProvider"),
        TEMP_DATA_RESOURCE("tempDataResource"),
        DATA_HUB("dataHub");

        private String type;

        ResourceType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }
}
