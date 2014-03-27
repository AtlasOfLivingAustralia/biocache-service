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

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

/**
 * Implementation of SpeciesLookupService.java that calls the bie-service application
 * via JSON REST web services.
 * 
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
@Component("speciesLookupService")
public class SpeciesLookupRestService implements SpeciesLookupService {
    
    private final static Logger logger = Logger.getLogger(SpeciesLookupRestService.class);
	
    @Inject
    @Qualifier("restTemplate")
    private RestOperations restTemplate; // NB MappingJacksonHttpMessageConverter() injected by Spring

    /** URI prefix for bie-service - may be overridden in properties file */
    @Value("${service.bie.ws.url:http://bie.ala.org.au/ws}")
    protected String bieUriPrefix;

    //NC 20131018: Allow service to be disabled via config (enabled by default)
    @Value("${service.bie.enabled:true}")
    protected Boolean enabled;
    
    /**
     * @see SpeciesLookupService#getGuidForName(String)
     * 
     * @param name
     * @return guid
     */
    @Override
    public String getGuidForName(String name) {
        String guid = null;
        if(enabled){
            
            try {
                final String jsonUri = bieUriPrefix + "/guid/" + name;
                logger.info("Requesting: " + jsonUri);
                List<Object> jsonList = restTemplate.getForObject(jsonUri, List.class);
                
                if (!jsonList.isEmpty()) {
                    Map<String, String> jsonMap = (Map<String, String>) jsonList.get(0);
                    if (jsonMap.containsKey("acceptedIdentifier")) {
                        guid = jsonMap.get("acceptedIdentifier");
                    }
                }
            } catch (Exception ex) {
                logger.error("RestTemplate error: " + ex.getMessage(), ex);
            }
        }
        
        return guid;
    }

    /**
     * Lookup the accepted name for a GUID
     *
     * @return
     */
    @Override
    public String getAcceptedNameForGuid(String guid) {
        String acceptedName = "";
        if(enabled){
            try {
                final String jsonUri = bieUriPrefix + "/species/shortProfile/" + guid + ".json";
                logger.info("Requesting: " + jsonUri);
                Map<String, String> jsonMap = restTemplate.getForObject(jsonUri, Map.class);
    
                if (jsonMap.containsKey("scientificName")) {
                    acceptedName = jsonMap.get("scientificName");
                }
    
            } catch (Exception ex) {
                logger.error("RestTemplate error: " + ex.getMessage(), ex);
            }
        }

        return acceptedName;
    }
    
    
    /**
     *
     * @param guids
     * @return
     */
    @Override
    public List<String> getNamesForGuids(List<String> guids) {
        List<String> names = null;
        if(enabled){
            try {
                final String jsonUri = bieUriPrefix + "/species/namesFromGuids.json";
                String params = "?guid=" + StringUtils.join(guids, "&guid=");
                names = restTemplate.postForObject(jsonUri + params, null, List.class);
            } catch (Exception ex) {
                logger.error("Requested URI: " + bieUriPrefix + "/species/namesFromGuids.json");
                logger.error("With POST body: guid=" + StringUtils.join(guids, "&guid="));
                logger.error("RestTemplate error: " + ex.getMessage(), ex);
            }
        }
        
        return names;
    }

    @Override
    public List<Map<String, String>> getNameDetailsForGuids(List<String> guids) {
        List<Map<String,String>> results =null;
        if(enabled){
            final String url = bieUriPrefix + "/species/guids/bulklookup.json";
            try{
                //String jsonString="";
                Map searchDTOList = restTemplate.postForObject(url, guids, Map.class);
                //System.out.println(test);
                results = (List<Map<String,String>>)searchDTOList.get("searchDTOList");
            } catch (Exception ex) {
                logger.error("Requested URI: " + url);
                logger.error("With POST body: guid=" + StringUtils.join(guids, "&guid="));
                logger.error("RestTemplate error: " + ex.getMessage(), ex);
            }
        }
        return results;
    }

    @Override
    public Map<String, List<Map<String, String>>> getSynonymDetailsForGuids(
            List<String> guids) {
        Map<String,List<Map<String, String>>> results = null;
        if(enabled){
            final String url = bieUriPrefix + "/species/bulklookup/namesFromGuids.json";
            try{
                results = restTemplate.postForObject(url, guids, Map.class); 
            } catch(Exception ex){
                logger.error("Requested URI: " + url);
                logger.error("With POST body: guid=" + StringUtils.join(guids, "&guid="));
                logger.error("RestTemplate error: " + ex.getMessage(), ex);
            }
        }
        return results;
    }
}
