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

import au.org.ala.biocache.dto.AuthenticatedUser;
import au.org.ala.biocache.dto.DownloadRequestDTO;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import javax.inject.Inject;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.servlet.http.HttpServletRequest;
import java.util.*;

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
    public static final String LEGACY_X_ALA_USER_ID_HEADER = "X-ALA-userId";
    @Inject
    protected RestOperations restTemplate; // NB MappingJacksonHttpMessageConverter() injected by Spring
    // e.g. https://auth-test.ala.org.au/userdetails/userDetails/
    @Value("${auth.user.details.url:}")
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

    @Value("${auth.legacy.apikey.enabled:true}")
    protected Boolean legacyApiKeyEnabled = true;
    @Value("${auth.legacy.emailonly.downloads.enabled:true}")
    protected Boolean emailOnlyEnabled = true;

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
        if (StringUtils.isNotBlank(userDetailsUrl)) {
            final String jsonUri = userDetailsUrl + userNamesForIdPath;
            try {
                logger.debug("authCache requesting: " + jsonUri);
                Map m = restTemplate.postForObject(jsonUri, null, Map.class);
                if (m != null && m.size() > 0) {
                    userNamesById = m;
                }
            } catch (Exception ex) {
                logger.error("RestTemplate error for " + jsonUri + ": " + ex.getMessage(), ex);
            }
        }
    }

    private void loadMapOfAllUserNamesByNumericId() {
        if (StringUtils.isNotBlank(userDetailsUrl)) {
            final String jsonUri = userDetailsUrl + userNamesForNumericIdPath;
            try {
                logger.debug("authCache requesting: " + jsonUri);
                Map m = restTemplate.postForObject(jsonUri, null, Map.class);
                if (m != null && m.size() > 0) {
                    userNamesByNumericIds = m;
                }
            } catch (Exception ex) {
                logger.error("RestTemplate error for " + jsonUri + ": " + ex.getMessage(), ex);
            }
        }
    }

    private void loadMapOfEmailToUserId() {
        if (StringUtils.isNotBlank(userDetailsUrl)) {
            final String jsonUri = userDetailsUrl + userNamesFullPath;
            try {
                logger.debug("authCache requesting: " + jsonUri);
                Map m = restTemplate.postForObject(jsonUri, null, Map.class);
                if (m != null && m.size() > 0) {
                    userEmailToId = m;
                }
                logger.debug("authCache userEmail cache: " + userEmailToId.size());
                if (!userEmailToId.isEmpty()) {
                    String email = userEmailToId.keySet().iterator().next();
                    String id = userEmailToId.get(email);
                    logger.info("authCache userEmail example: " + email + " -> " + id);
                }
            } catch (Exception ex) {
                logger.error("RestTemplate error for " + jsonUri + ": " + ex.getMessage(), ex);
            }
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

    @Deprecated
    private List<String> getUserRoles(String userId) {
        List<String> roles = new ArrayList<>();
        if (StringUtils.isNotBlank(userDetailsUrl)) {
            final String jsonUri = userDetailsUrl + userDetailsPath + "?userName=" + userId;
            logger.info("authCache requesting: " + jsonUri);
            roles = (List) restTemplate.postForObject(jsonUri, null, Map.class).get("roles");
        }
        return roles;
    }

    @Deprecated
    private Map<String,?> getUserDetails(String userId) {
        Map<String,?> userDetails = new HashMap<>();
        if (StringUtils.isNotBlank(userDetailsUrl)){
            final String jsonUri = userDetailsUrl + userDetailsPath + "?userName=" + userId;
            logger.info("authCache requesting: " + jsonUri);
            userDetails = (Map) restTemplate.postForObject(jsonUri, null, Map.class);
        }
        return userDetails;
    }

    /**
     * Authentication for download users has 3 routes:
     *
     * 1) Check for JWT / OAuth- user is retrieved from UserPrincipal along with a set of roles, supplied email address is ignored...
     * 2) Legacy API Key and X-Auth-Id - email address retrieved from CAS/Userdetails - email address is ignored...
     * 3) Email address supplied (Galah) - email address is verified - no sensitive access
     */
    public AuthenticatedUser getDownloadUser(DownloadRequestDTO downloadRequestDTO, HttpServletRequest request) {

        // 1) Legacy API Key and X-ALA-userId
        String xAlaUserIdHeader = request.getHeader(LEGACY_X_ALA_USER_ID_HEADER);
        if (legacyApiKeyEnabled && request.isUserInRole(ApiKeyService.ROLE_LEGACY_APIKEY) && xAlaUserIdHeader != null){
            Map<String, Object> userDetails = (Map<String, Object>) getUserDetails(xAlaUserIdHeader);
            boolean activated = (Boolean) userDetails.getOrDefault("activated", true);
            boolean locked = (Boolean) userDetails.getOrDefault("locked", true);
            String firstName = (String) userDetails.getOrDefault("firstName", true);
            String lastName = (String) userDetails.getOrDefault("lastName", true);
            List<String> userRoles = getUserRoles(xAlaUserIdHeader);
            String email = (String) userDetails.getOrDefault("email", null);
            if (email != null && activated && !locked) {
                return new AuthenticatedUser(email, xAlaUserIdHeader, userRoles, Collections.emptyMap(), firstName, lastName);
            } else {
                logger.info("Download request with API key failed " +
                        "- email  " + email +
                        " , activated " + activated +
                        " , locked " + locked);
            }
        }

        // 2) Check for JWT / OAuth
        if (request.getUserPrincipal() != null && request.getUserPrincipal() instanceof PreAuthenticatedAuthenticationToken){
            return (AuthenticatedUser) ((PreAuthenticatedAuthenticationToken) request.getUserPrincipal()).getPrincipal();
        }

        // 3) Email address supplied (Galah / ala4r) - email address is verified - no roles, no sensitive access
        if (emailOnlyEnabled && downloadRequestDTO.getEmail() != null) {
            try {
                new InternetAddress(downloadRequestDTO.getEmail()).validate();
                // verify the email address is registered
                Map<String, Object> userDetails = (Map<String, Object>) getUserDetails(downloadRequestDTO.getEmail());
                boolean activated = (Boolean) userDetails.getOrDefault("activated", true);
                boolean locked = (Boolean) userDetails.getOrDefault("locked", true);
                String firstName = (String) userDetails.getOrDefault("firstName", true);
                String lastName = (String) userDetails.getOrDefault("lastName", true);

                // check the email address is registered to a user
                boolean registeredEmail = userDetails != null && !userDetails.isEmpty();
                // is account activated or locked ?
                if (registeredEmail && activated && !locked){
                    // email is valid and registered
                    return new AuthenticatedUser(downloadRequestDTO.getEmail(), null, Collections.emptyList(),
                            Collections.emptyMap(), firstName, lastName);
                } else {
                    logger.info("Email only download request failed " +
                                    "- registeredEmail  " + registeredEmail +
                                    " , activated " + activated +
                                    " , locked " + locked);
                }
            } catch (AddressException e) {
                // invalid email
                logger.info("Email only download request failed - invalid email " + downloadRequestDTO.getEmail());
            }
        }

        return null;
    }


    /**
     * Authentication for download users has 3 routes:
     *
     * 1) Check for JWT / OAuth- user is retrieved from UserPrincipal along with a set of roles, supplied email address is ignored...
     * 2) Legacy API Key and X-Auth-Id - email address retrieved from CAS/Userdetails - email address is ignored...
     * 3) Email address supplied (Galah) - email address is verified - no sensitive access
     */
    public AuthenticatedUser getRecordViewUser(HttpServletRequest request) {

        // 1) Legacy API Key and X-ALA-userId
        String xAlaUserIdHeader = request.getHeader(LEGACY_X_ALA_USER_ID_HEADER);
        if (legacyApiKeyEnabled && request.isUserInRole(ApiKeyService.ROLE_LEGACY_APIKEY) && xAlaUserIdHeader != null){
            Map<String, Object> userDetails = (Map<String, Object>) getUserDetails(xAlaUserIdHeader);
            boolean activated = (Boolean) userDetails.getOrDefault("activated", true);
            boolean locked = (Boolean) userDetails.getOrDefault("locked", true);
            String firstName = (String) userDetails.getOrDefault("firstName", true);
            String lastName = (String) userDetails.getOrDefault("lastName", true);
            List<String> userRoles = getUserRoles(xAlaUserIdHeader);
            String email = (String) userDetails.getOrDefault("email", null);
            if (email != null && activated && !locked) {
                return new AuthenticatedUser(email, xAlaUserIdHeader, userRoles, Collections.emptyMap(), firstName, lastName);
            } else {
                logger.info("Download request with API key failed " +
                        "- email  " + email +
                        " , activated " + activated +
                        " , locked " + locked);
            }
            return null;
        }

        // 2) Check for JWT / OAuth
        if (request.getUserPrincipal() != null && request.getUserPrincipal() instanceof PreAuthenticatedAuthenticationToken){
            return (AuthenticatedUser) ((PreAuthenticatedAuthenticationToken) request.getUserPrincipal()).getPrincipal();
        }

        return null;
    }


}