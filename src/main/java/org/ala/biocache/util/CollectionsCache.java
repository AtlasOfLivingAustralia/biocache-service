/* *************************************************************************
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

package org.ala.biocache.util;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import javax.inject.Inject;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

/**
 * Provides access to the collection and institution codes and names from the Collectory.
 * Uses the Collectory webservices to get a map of codes & names for institutions and collections
 * and caches these. Cache is automatically updated after a configurable timeout period.
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
    protected Date lastUpdated = new Date();
    protected Date lastDownloadLimitUpdate = new Date();
    protected Long timeout = 3600000L; // in milliseconds (1 hour)
    protected String collectoryUriPrefix = "http://collections.ala.org.au";
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
        checkCacheAge();
        return this.institutions;
    }
    
    public LinkedHashMap<String, String> getDataResources(){
        checkCacheAge();
        return this.dataResources;
    }

    public LinkedHashMap<String, String> getDataProviders(){
        checkCacheAge();
        return this.dataProviders;
    }

    public LinkedHashMap<String, String> getTempDataResources(){
        checkCacheAge();
        return this.tempDataResources;
    }

    public LinkedHashMap<String, String> getCollections() {
        checkCacheAge();
        return this.collections;
    }
    
    public LinkedHashMap<String, String> getDataHubs() {
        checkCacheAge();
        return this.dataHubs;
    }

    public LinkedHashMap<String, String> getInstitutions(List<String> inguids, List<String> coguids){
        checkCacheAge(inguids, coguids);
        return this.institutions;
    }

    public LinkedHashMap<String, String> getCollections(List<String> inguids, List<String>coguids){
        checkCacheAge(inguids, coguids);
        return this.collections;
    }
    
    public LinkedHashMap<String, String> getDataResources(List<String> inguids, List<String>coguids){
        checkCacheAge(inguids, coguids);
        return this.dataResources;
    }

    public LinkedHashMap<String, String> getDataProviders(List<String> inguids, List<String>coguids){
        checkCacheAge(inguids, coguids);
        return this.dataProviders;
    }

    public LinkedHashMap<String, Integer> getDownloadLimits(){
        checkDownloadCacheAge();                  		
        return downloadLimits;        
    }

    protected void checkCacheAge(){
        checkCacheAge(null, null);
    }
    protected void checkDownloadCacheAge(){
        checkCacheAge();
        Date currentDate = new Date();
        Long timeSinceUpdate = currentDate.getTime() - lastDownloadLimitUpdate.getTime();
        if (timeSinceUpdate > this.timeout || downloadLimits.size() < 1) {
            //update the download limits 
            logger.debug("Starting to populate download limits....");
            String jsonUri = collectoryUriPrefix +"/lookup/summary/";
            for(String druid : dataResources.keySet()){
                //lookup the download limit
                java.util.Map<String, Object> properties = restTemplate.getForObject(jsonUri+druid+".json", java.util.Map.class);
                try{
                    Integer limit = (Integer)(properties.get("downloadLimit"));
                    downloadLimits.put(druid,  limit);
                    //logger.debug(druid +" & limit " + limit);
                }
                catch(Exception e){
                    logger.error(e.getMessage(),e);
                }
            }
            logger.debug("The download limit map : " + downloadLimits);
        }
    }

    /**
     * Check age of cache and retrieve new values from Collections webservices if needed.
     */
    protected void checkCacheAge(List<String> inguids,List<String> coguids) {
        Date currentDate = new Date();
        Long timeSinceUpdate = currentDate.getTime() - lastUpdated.getTime();
        logger.debug("timeSinceUpdate = " + timeSinceUpdate + " collections: " + collections.size() + " institutions: " + institutions.size());
        
        if (timeSinceUpdate > this.timeout || institutions.size() < 1 || collections.size() < 1) {
            updateCache(inguids, coguids);
            lastUpdated = new Date(); // update timestamp
        }
    }

    public void updateCache() {
        updateCache(null,null);
    }

    /**
     * Update the entity types (fields)
     */
    protected void updateCache(List<String> inguids, List<String> coguids) {
        logger.info("Updating collectory cache...");
        this.collections = getCodesMap(ResourceType.COLLECTION, coguids);
        this.institutions = getCodesMap(ResourceType.INSTITUTION, inguids);
        this.dataResources = getCodesMap(ResourceType.DATA_RESOURCE,null);
        this.dataProviders = getCodesMap(ResourceType.DATA_PROVIDER,null);
        this.tempDataResources = getCodesMap(ResourceType.TEMP_DATA_RESOURCE,null);
        this.dataHubs = getCodesMap(ResourceType.DATA_HUB, null);
        this.dataResources.putAll(tempDataResources);
        
    }
    
    /**
     * Do the web services call. Uses RestTemplate.
     *
     * @param type
     * @return
     */
    protected LinkedHashMap getCodesMap(ResourceType type, List<String> guids) {
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
            final String jsonUri = collectoryUriPrefix +"/ws/"+ type.getType() + ".json";
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

    /*
     * Getter and setters
     */
    public String getCollectoryUriPrefix() {
        return collectoryUriPrefix;
    }

    public void setCollectoryUriPrefix(String collectoryUriPrefix) {
        this.collectoryUriPrefix = collectoryUriPrefix;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }
}
