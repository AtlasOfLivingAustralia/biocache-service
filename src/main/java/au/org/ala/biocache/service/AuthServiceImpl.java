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
import au.org.ala.biocache.util.AlaUnvalidatedProfile;
import au.org.ala.ws.security.profile.AlaUserProfile;
import au.org.ala.ws.tokens.TokenService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.http.entity.ContentType;
import org.apache.log4j.Logger;
import org.ehcache.core.EhcacheManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;

import javax.inject.Inject;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
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
public class AuthServiceImpl implements AuthService {

    private final static Logger logger = Logger.getLogger(AuthServiceImpl.class);
//    public static final String LEGACY_X_ALA_USER_ID_HEADER = "X-ALA-userId";
    @Inject
    protected RestOperations restTemplate; // NB MappingJacksonHttpMessageConverter() injected by Spring
    // e.g. https://auth-test.ala.org.au/userdetails/userDetails/
    @Value("${auth.user.details.url:}")
    protected String userDetailsUrl = null;

    @Value("${caches.auth.enabled:true}")
    protected Boolean enabled = true;

    @Value("${auth.legacy.emailonly.downloads.enabled:true}")
    protected Boolean emailOnlyEnabled = true;

    EhcacheManager ehCacheManager;

    @Inject
    private TokenService tokenService;

    public AuthServiceImpl() {

    }

    public String substituteEmailAddress(String raw){
      return raw == null ? raw : raw.replaceAll("\\@\\w+", "@..");
    }

    private Map<String,?> getUserByEmailOrId(String emailOrId) {
        Map<String,?> userDetails = new HashMap<>();
        if (StringUtils.isNotBlank(userDetailsUrl)){
            try {
                final String jsonUri = userDetailsUrl + "getUserDetails?userName=" + emailOrId;
                logger.info("authCache requesting: " + jsonUri);
                HttpHeaders requestHeaders = new HttpHeaders();
                requestHeaders.set("Authorization", tokenService.getAuthToken(false).toAuthorizationHeader());
                HttpEntity<Object> request = new HttpEntity<>(null, requestHeaders);
                userDetails = (Map) restTemplate.postForObject(jsonUri, request, Map.class);
            } catch (Exception ignored) {

            }
        }
        return userDetails;
    }

    /**
     * Authentication for download users has 3 routes:
     *
     * 1) Check for JWT / OAuth- user is retrieved from UserPrincipal along with a set of roles, supplied email address is ignored...
     * 2) Legacy API Key and X-Auth-Id - email address retrieved from CAS/Userdetails - email address is ignored...
     * 3) Email address supplied (Galah) - email address is verified - no sensitive access
     * 4) Email address supplied and emailOnlyEnabled == false - email is not verified - no sensitive access
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
        } else if (!emailOnlyEnabled && downloadRequestDTO.getEmail() != null) {
            // 4) Continue with this unvalidated email
            return Optional.of(new AlaUnvalidatedProfile(downloadRequestDTO.getEmail()));
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
     * @return
     */
    @Cacheable("lookupAuthUser")
    public Optional<AlaUserProfile> lookupAuthUser(String userIdOrEmail, boolean getRoles) {
        Map<String, Object> userDetails = (Map<String, Object>) getUserByEmailOrId(userIdOrEmail);
        if (userDetails == null || userDetails.isEmpty()) {
            return Optional.empty();
        }

        String userId = (String) userDetails.getOrDefault("userid", (String) userDetails.getOrDefault("userId", null));
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
                    new AlaUserProfile() {

                        @Override
                        public String getUserId() { return userId; }

                        @Override
                        public String getEmail() {
                            return email;
                        }

                        @Override
                        public String getGivenName() {
                            return firstName;
                        }

                        @Override
                        public String getFamilyName() {
                            return lastName;
                        }

                        @Override
                        public String getName() {
                            return firstName + " " + lastName;
                        }

                        @Override
                        public String getId() {
                            return null;
                        }

                        @Override
                        public void setId(String id) {}

                        @Override
                        public String getTypedId() {
                            return null;
                        }

                        @Override
                        public String getUsername() {
                            return email;
                        }

                        @Override
                        public Object getAttribute(String name) {
                            return null;
                        }

                        @Override
                        public Map<String, Object> getAttributes() {
                            return null;
                        }

                        @Override
                        public boolean containsAttribute(String name) {
                            return false;
                        }

                        @Override
                        public void addAttribute(String key, Object value) {

                        }

                        @Override
                        public void removeAttribute(String key) {

                        }

                        @Override
                        public void addAuthenticationAttribute(String key, Object value) {

                        }

                        @Override
                        public void removeAuthenticationAttribute(String key) {

                        }

                        @Override
                        public void addRole(String role) {

                        }

                        @Override
                        public void addRoles(Collection<String> roles) {

                        }

                        @Override
                        public Set<String> getRoles() {
                            return userRoles;
                        }

                        @Override
                        public void addPermission(String permission) {

                        }

                        @Override
                        public void addPermissions(Collection<String> permissions) {

                        }

                        @Override
                        public Set<String> getPermissions() {
                            return null;
                        }

                        @Override
                        public boolean isRemembered() {
                            return false;
                        }

                        @Override
                        public void setRemembered(boolean rme) {

                        }

                        @Override
                        public String getClientName() {
                            return null;
                        }

                        @Override
                        public void setClientName(String clientName) {

                        }

                        @Override
                        public String getLinkedId() {
                            return null;
                        }

                        @Override
                        public void setLinkedId(String linkedId) {

                        }

                        @Override
                        public boolean isExpired() {
                            return false;
                        }

                        @Override
                        public Principal asPrincipal() {
                            return null;
                        }
                    }
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
