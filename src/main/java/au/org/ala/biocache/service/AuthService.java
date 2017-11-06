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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service to lookup and cache user details from auth.ala.org.au (CAS)
 *
 * NC 2013-01-09: Copied across from hubs-webapp.
 * This is because we want to substitute the values as early as possible to
 * prevent large maps of users being passed around.
 *
 * User: dos009
 * Date: 21/11/12
 * Time: 10:38 AM
 */
@Component("authService")
public class AuthService {

    private final static Logger logger = Logger.getLogger(AuthService.class);
    @Inject
    protected RestOperations restTemplate; // NB MappingJacksonHttpMessageConverter() injected by Spring
    @Value("${auth.user.details.url}")
    protected String userDetailsUrl = null;
    @Value("${auth.user.names.id.path:getUserList}")
    protected String userNamesForIdPath = null;
    @Value("${auth.usernames.for.numeric.id.path:getUserListWithIds}")
    protected String userNamesForNumericIdPath = null;
    @Value("${auth.usernames.full.path:getUserList}")
    protected String userNamesFullPath = null;
    @Value("${auth.user.details.path:getUserDetails}")
    protected String userDetailsPath = null;
    @Value("${auth.startup.initialise:false}")
    protected boolean startupInitialise = false;
    @Value("${caches.auth.enabled:true}")
    protected Boolean enabled = true;
    // Keep a reference to the output Map in case subsequent web service lookups fail
    protected Map<String, String> userNamesById = RestartDataService.get(this, "userNamesById", new TypeReference<HashMap<String, String>>(){}, HashMap.class);
    protected Map<String, String> userNamesByNumericIds = RestartDataService.get(this, "userNamesByNumericIds", new TypeReference<HashMap<String, String>>(){}, HashMap.class);
    protected Map<String, String> userEmailToId = RestartDataService.get(this, "userEmailToId", new TypeReference<HashMap<String, String>>(){}, HashMap.class);

    public AuthService() {
        logger.info("Instantiating AuthService: " + this);
        if(startupInitialise){
            logger.info("Loading auth caches now");
            reloadCaches();
        }
    }
    
    public Map<String, String> getMapOfAllUserNamesById() {
        return userNamesById;
    }

    public Map<String, String> getMapOfAllUserNamesByNumericId() {
        return userNamesByNumericIds;
    }

    public Map<String, String> getMapOfEmailToId() {
        return userEmailToId;
    }

    /**
     * Returns the display name to be used by a client.
     * 
     * Performs a lookup based on the email id and the numeric id.
     * 
     * @param value
     * @return
     */
    public String getDisplayNameFor(String value){
        String displayName = value;
        if(value != null){
            if(userNamesById.containsKey(value)){
                displayName = userNamesById.get(value);
            } else if(userNamesByNumericIds.containsKey(value)){
                displayName=userNamesByNumericIds.get(value);
            } else {
                displayName = displayName.replaceAll("\\@\\w+", "@..");
            }
        }
        return displayName;
    }
    
    public String substituteEmailAddress(String raw){
      return raw == null ? raw : raw.replaceAll("\\@\\w+", "@..");
    }

    private void loadMapOfAllUserNamesById() {
        final String jsonUri = userDetailsUrl + userNamesForIdPath;
        try {
            logger.info("authCache requesting: " + jsonUri);
            Map m = restTemplate.postForObject(jsonUri, null, Map.class);
            if (m != null && m.size() > 0) userNamesById = m;
        } catch (Exception ex) {
            logger.error("RestTemplate error for " + jsonUri + ": " + ex.getMessage(), ex);
        }
    }

    private void loadMapOfAllUserNamesByNumericId() {
        final String jsonUri = userDetailsUrl + userNamesForNumericIdPath;
        try {
            logger.info("authCache requesting: " + jsonUri);
            Map m = restTemplate.postForObject(jsonUri, null, Map.class);
            if (m != null && m.size() > 0) userNamesByNumericIds = m;
        } catch (Exception ex) {
            logger.error("RestTemplate error for " + jsonUri + ": " + ex.getMessage(), ex);
        }
    }

    private void loadMapOfEmailToUserId() {
        final String jsonUri = userDetailsUrl + userNamesFullPath;
        try {
            logger.info("authCache requesting: " + jsonUri);
            Map m = restTemplate.postForObject(jsonUri, null, Map.class);
            if (m != null && m.size() > 0) userEmailToId = m;
            logger.info("authCache userEmail cache: " + userEmailToId.size());
            if(userEmailToId.size()>0){
                String email = userEmailToId.keySet().iterator().next();
                String id = userEmailToId.get(email);
                logger.info("authCache userEmail example: " + email +" -> " + id);
            }
        } catch (Exception ex) {
            logger.error("RestTemplate error for " + jsonUri + ": " + ex.getMessage(), ex);
        }
    }

    @Scheduled(fixedDelay = 600000) // schedule to run every 10 min
    //@Async NC 2013-07-29: Disabled the Async so that we don't get bombarded with calls.
    public void reloadCaches() {
        Thread thread = new Thread() {
            @Override
            public void run() {
                if(enabled){
                    logger.info("Triggering reload of auth user names");
                    loadMapOfAllUserNamesById();
                    loadMapOfAllUserNamesByNumericId();
                    loadMapOfEmailToUserId();
                    logger.info("Finished reload of auth user names");
                } else{
                    logger.info("Authentication Cache has been disabled");
                }
            }
        };

        if (userDetailsPath.length() > 0) {
            thread.start();
        } else {
            thread.run();
        }

    }

    public List getUserRoles(String userId) {
        List roles = new ArrayList();
        try {
            final String jsonUri = userDetailsUrl + userDetailsPath + "?userName=" + userId;
            logger.info("authCache requesting: " + jsonUri);
            roles = (List) restTemplate.postForObject(jsonUri, null, Map.class).get("roles");
        } catch (Exception ex) {
            logger.error("RestTemplate error: " + ex.getMessage(), ex);
        }

        return roles;
    }

    public Map<String,?> getUserDetails(String userId) {
        Map<String,?> userDetails
                = new HashMap<>();
        try {
            final String jsonUri = userDetailsUrl + userDetailsPath + "?userName=" + userId;
            logger.info("authCache requesting: " + jsonUri);
            userDetails = (Map) restTemplate.postForObject(jsonUri, null, Map.class);
        } catch (Exception ex) {
            logger.error("RestTemplate error: " + ex.getMessage(), ex);
        }

        return userDetails;
    }
}