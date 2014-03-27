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

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.List;
import javax.inject.Inject;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

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

    protected LinkedHashMap<String, String> dataResources = new LinkedHashMap<String, String>();
    protected LinkedHashMap<String, String> dataProviders = new LinkedHashMap<String, String>();
    protected LinkedHashMap<String, String> tempDataResources = new LinkedHashMap<String, String>();
    protected LinkedHashMap<String, Integer> downloadLimits = new LinkedHashMap<String, Integer>();
    protected LinkedHashMap<String, String> institutions = new LinkedHashMap<String, String>();
    protected LinkedHashMap<String, String> collections = new LinkedHashMap<String, String>();
    protected LinkedHashMap<String, String> dataHubs =  new LinkedHashMap<String,String>();
    protected List<String> institution_uid = null;
    protected List<String> collection_uid = null;
    protected List<String> data_resource_uid = null;
    protected List<String> data_provider_uid = null;
    protected List<String> data_hub_uid = null;

    @Value("${registry.url:http://collections.ala.org.au/ws}")
    protected String registryUrl;

    //NC 20131018: Allow cache to be disabled via config (enabled by default)
    @Value("${caches.collections.enabled:true}")
    protected Boolean enabled =null;
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
    
    /**
     * @deprecated Unnecessary because updateUidLists provides the updated lists...
     * @param inguids
     * @param coguids
     * @return
     */
    @Deprecated
    public LinkedHashMap<String, String> getInstitutions(List<String> inguids, List<String> coguids){
        return this.institutions;
    }

    /**
     * @deprecated Unnecessary because updateUidLists provides the updated lists...
     * @param inguids
     * @param coguids
     * @return
     */
    @Deprecated
    public LinkedHashMap<String, String> getCollections(List<String> inguids, List<String>coguids){        
        return this.collections;
    }
    
    /**
     * @deprecated Unnecessary because updateUidLists provides the updated lists...
     * @param inguids
     * @param coguids
     * @return
     */
    @Deprecated
    public LinkedHashMap<String, String> getDataResources(List<String> inguids, List<String>coguids){
        return this.dataResources;
    }

    /**
     * @deprecated Unnecessary because updateUidLists provides the updated lists...
     * @param inguids
     * @param coguids
     * @return
     */
    @Deprecated
    public LinkedHashMap<String, String> getDataProviders(List<String> inguids, List<String>coguids){     
        return this.dataProviders;
    }

    public LinkedHashMap<String, Integer> getDownloadLimits(){
        //checkDownloadCacheAge();                  		
        return downloadLimits;        
    }

    public void updateUidLists(List<String> couids, List<String> inuids, List<String> druids, List<String> dpuids, List<String>dhuids){
        boolean refresh =data_resource_uid == null;
        this.collection_uid = couids;
        this.institution_uid = inuids;
        this.data_resource_uid = druids;
        this.data_provider_uid = dpuids;
        this.data_hub_uid = dhuids;
        if(refresh){
            updateCache();
        }
    }

    /**
     * Update the entity types (fields)
     */
    @Scheduled(fixedDelay = 3600000L) //every hour
    public void updateCache() {
        if(enabled){
            logger.info("Updating collectory cache...");
            
            this.collections = getCodesMap(ResourceType.COLLECTION, collection_uid);
            this.institutions = getCodesMap(ResourceType.INSTITUTION, institution_uid);
            this.dataResources = getCodesMap(ResourceType.DATA_RESOURCE,data_resource_uid);
            this.dataProviders = getCodesMap(ResourceType.DATA_PROVIDER, data_provider_uid);
            this.tempDataResources = getCodesMap(ResourceType.TEMP_DATA_RESOURCE,null);
            this.dataHubs = getCodesMap(ResourceType.DATA_HUB, data_hub_uid);
            this.dataResources.putAll(tempDataResources);
        } else{
            logger.info("Collectory cache has been disabled");
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
                    //logger.debug("uid = " + je.get("uid") + " & name = " + je.get("name"));
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
