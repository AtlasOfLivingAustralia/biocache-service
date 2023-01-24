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

import au.org.ala.biocache.dto.DownloadRequestDTO;
import au.org.ala.ws.security.profile.AlaUserProfile;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import javax.inject.Inject;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import javax.servlet.http.HttpServletRequest;
import java.security.Principal;
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
@Slf4j
public class AuthService {

    private final static Logger logger = Logger.getLogger(AuthService.class);
//    public static final String LEGACY_X_ALA_USER_ID_HEADER = "X-ALA-userId";
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

    @Value("${auth.legacy.emailonly.downloads.enabled:true}")
    protected Boolean emailOnlyEnabled = true;

    // Keep a reference to the output Map in case subsequent web service lookups fail
    protected Map<String, String> userNamesById = RestartDataService.get(this, "userNamesById", new TypeReference<HashMap<String, String>>(){}, HashMap.class);
    protected Map<String, String> userNamesByNumericIds = RestartDataService.get(this, "userNamesByNumericIds", new TypeReference<HashMap<String, String>>(){}, HashMap.class);
    protected Map<String, String> userEmailToId = RestartDataService.get(this, "userEmailToId", new TypeReference<HashMap<String, String>>(){}, HashMap.class);

    public AuthService() {
        if (startupInitialise){
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
    public Set<String> getUserRoles(String userId) {
        Set<String> roles = new HashSet<>();
        if (StringUtils.isNotBlank(userDetailsUrl)) {
            final String jsonUri = userDetailsUrl + userDetailsPath + "?userName=" + userId;
            logger.info("authCache requesting: " + jsonUri);
            roles.addAll((List) restTemplate.postForObject(jsonUri, null, Map.class).getOrDefault("roles", Collections.EMPTY_LIST));
        }
        return roles;
    }

    @Deprecated
    public Map<String,?> getUserDetails(String userId) {
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
    public Optional<AlaUserProfile> getDownloadUser(DownloadRequestDTO downloadRequestDTO, HttpServletRequest request) {

        // 2) Check for JWT / OAuth
        Principal userPrincipal = request.getUserPrincipal();
        if (userPrincipal != null && userPrincipal instanceof AlaUserProfile){
            return Optional.of((AlaUserProfile) userPrincipal);
        }

        // 3) Email address supplied (Galah / ala4r) - email address is verified - no roles, no sensitive access
        if (emailOnlyEnabled && downloadRequestDTO.getEmail() != null) {
            try {
                new InternetAddress(downloadRequestDTO.getEmail()).validate();
                // verify the email address is registered
                return lookupAuthUser(downloadRequestDTO.getEmail(), false);
            } catch (AddressException e) {
                // invalid email
                logger.info("Email only download request failed - invalid email " + downloadRequestDTO.getEmail());
            }
        }

        return Optional.empty();
    }


    /**
     * Authentication for download users has 3 routes:
     *
     * 1) Check for JWT / OAuth- user is retrieved from UserPrincipal along with a set of roles, supplied email address is ignored...
     * 2) Legacy API Key and X-Auth-Id - email address retrieved from CAS/Userdetails - email address is ignored...
     */
    public Optional<AlaUserProfile> getRecordViewUser(HttpServletRequest request) {
        // 2) Check for JWT / OAuth
        Principal userPrincipal = request.getUserPrincipal();

        if (userPrincipal != null && userPrincipal instanceof AlaUserProfile){
            return Optional.of((AlaUserProfile) userPrincipal);
        }
        return Optional.empty();
    }

    /**
     * Use user details services to get user.
     *
     * @param userIdOrEmail
     * @param getRoles
     * @return
     */
    public Optional<AlaUserProfile> lookupAuthUser(String userIdOrEmail, boolean getRoles) {
        Map<String, Object> userDetails = (Map<String, Object>) getUserDetails(userIdOrEmail);
        if (userDetails == null || userDetails.isEmpty()) {
            return Optional.empty();
        }

        String userId = (String) userDetails.getOrDefault("userId", null);
        boolean activated = (Boolean) userDetails.getOrDefault("activated", false);
        boolean locked = (Boolean) userDetails.getOrDefault("locked", true);
        String firstName = (String) userDetails.getOrDefault("firstName", "");
        String lastName = (String) userDetails.getOrDefault("lastName", "");
        String email = (String) userDetails.getOrDefault("email", "");

        Set<String> userRoles = new HashSet<>(Collections.emptySet());
        if (getRoles) {
            userRoles.addAll((List) userDetails.getOrDefault("roles", Collections.EMPTY_LIST));
        }

        if (email != null && activated && !locked) {
            return Optional.of(
                    new AlaUserProfile(userId, email, firstName, lastName, userRoles, Collections.emptyMap())
            );
        } else {
            log.info("Download request with API key failed " +
                    "- email  " + email +
                    " , activated " + activated +
                    " , locked " + locked);
        }
        return Optional.empty();
    }
}