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
package org.ala.biocache.dao;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

/**
 * Implementation of BieService.java that calls the bie-webapp application
 * via JSON REST web services.
 * 
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
@Component("bieRestService")
public class BieRestService implements BieService {
    @Inject
    @Qualifier("restTemplate")
    private RestOperations restTemplate; // NB MappingJacksonHttpMessageConverter() injected by Spring

    /** URI prefix for bie-service - may be overridden in properties file */
    protected String bieUriPrefix = "http://bie.ala.org.au/ws";

    private final static Logger logger = Logger.getLogger(BieRestService.class);
    
    /**
     * @see org.ala.biocache.dao.BieService#getGuidForName(String)
     * 
     * @param name
     * @return guid
     */
    @Override
    public String getGuidForName(String name) {
        String guid = null;
        
        try {
            final String jsonUri = bieUriPrefix + "/ws/guid/" + name;            
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
            //searchResults.setStatus("Error: " + ex.getMessage());
        }
        
        return guid;
    }

    /**
     * Lookup the accepted name for a GUID
     *
     * @return
     */
    @Override
    public String getAcceptedNameForGuide(String guid) {
        String acceptedName = "";

        try {
            final String jsonUri = bieUriPrefix + "/species/shortProfile/" + guid + ".json";
            logger.info("Requesting: " + jsonUri);
            Map<String, String> jsonMap = restTemplate.getForObject(jsonUri, Map.class);

            if (jsonMap.containsKey("scientificName")) {
                acceptedName = jsonMap.get("scientificName");
            }

        } catch (Exception ex) {
            logger.error("RestTemplate error: " + ex.getMessage(), ex);
            //searchResults.setStatus("Error: " + ex.getMessage());
        }

        return acceptedName;
    }
    
    
    /**
     * @see org.ala.hubs.service.BieService#getNamesForGuids(java.util.List)
     *
     * @param guids
     * @return
     */
    @Override
    public List<String> getNamesForGuids(List<String> guids) {
        List<String> names = null;

        try {
            final String jsonUri = bieUriPrefix + "/species/namesFromGuids.json";
            String params = "?guid=" + StringUtils.join(guids, "&guid=");
            names = restTemplate.postForObject(jsonUri + params, null, List.class);
        } catch (Exception ex) {
            logger.error("Requested URI: " + bieUriPrefix + "/species/namesFromGuids.json");
            logger.error("With POST body: guid=" + StringUtils.join(guids, "&guid="));
            logger.error("RestTemplate error: " + ex.getMessage(), ex);
        }
        
        return names;
    }

    public String getBieUriPrefix() {
        return bieUriPrefix;
    }

    public void setBieUriPrefix(String bieUriPrefix) {
        this.bieUriPrefix = bieUriPrefix;
    }

}
