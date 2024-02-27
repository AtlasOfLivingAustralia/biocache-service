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
public interface AuthService {

    public String substituteEmailAddress(String raw);

    /**
     * Authentication for download users has 3 routes:
     *
     * 1) Check for JWT / OAuth- user is retrieved from UserPrincipal along with a set of roles, supplied email address is ignored...
     * 2) Legacy API Key and X-Auth-Id - email address retrieved from CAS/Userdetails - email address is ignored...
     * 3) Email address supplied (Galah) - email address is verified - no sensitive access
     * 4) Email address supplied and emailOnlyEnabled == false - email is not verified - no sensitive access
     */
    Optional<AlaUserProfile> getDownloadUser(DownloadRequestDTO downloadRequestDTO, HttpServletRequest request);

    /**
     * Authentication for download users has 3 routes:
     *
     * 1) Check for JWT / OAuth- user is retrieved from UserPrincipal along with a set of roles, supplied email address is ignored...
     * 2) Legacy API Key and X-Auth-Id - email address retrieved from CAS/Userdetails - email address is ignored...
     */
    Optional<AlaUserProfile> getRecordViewUser(HttpServletRequest request);

    /**
     * Use user details services to get user.
     *
     * @param userIdOrEmail
     * @return
     */
    Optional<AlaUserProfile> lookupAuthUser(String userIdOrEmail, boolean getRoles);
}
