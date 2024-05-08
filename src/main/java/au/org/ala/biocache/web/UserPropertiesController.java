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
package au.org.ala.biocache.web;

import au.org.ala.biocache.dao.StoreDAO;
import au.org.ala.biocache.dto.*;
import au.org.ala.ws.security.profile.AlaM2MUserProfile;
import au.org.ala.ws.security.profile.AlaUserProfile;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import java.security.Principal;
import java.util.*;

/**
 * This controller provides web services for assertion creation/deletion.
 */
@RestController
public class UserPropertiesController extends AbstractSecureController {

    private final static Logger logger = Logger.getLogger(UserPropertiesController.class);

    @Inject
    private StoreDAO storeDao;

    @Value("${user.properties.max:50}")
    private int maxUserProperties;

    /**
     * Retrieve an array of the assertion codes in use by the processing system
     *
     * @return an array of codes
     * @throws Exception
     */
    @Tag(name = "User Properties", description = "User saved properties")
    @Operation(
            summary = "Retrieve a property",
            tags = "User Properties",
            description = "Get a property value for a user. Required scopes: 'users/read' or User JWT.")
    @Parameters(value = {
            @Parameter(name = "alaId", description = "The user's ALA ID", in = ParameterIn.QUERY, required = true),
            @Parameter(name = "name", description = "The name of the property to get", in = ParameterIn.QUERY, required = false),
            @Parameter(name = "accept", description = "Must be application/json", in = ParameterIn.HEADER, required = true)
    })
    @SecurityRequirement(name = "JWT")
    @RequestMapping(
            value = {"/user/property"},
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_PLAIN_VALUE})
    public @ResponseBody Object get(
            @RequestParam(value = "alaId") String alaId,
            @RequestParam(value = "name") String name,
            HttpServletRequest request
    ) throws Exception {
        String validAlaId = getValidUser(alaId, request, "users/read");

        UserProperty prop = storeDao.get(UserProperty.class, validAlaId).orElse(null);

        if (prop != null && prop.getProperties() != null) {
            if (name != null) {
                return prop.getProperties().get(name);
            } else {
                return prop.getProperties();
            }
        } else {
            return null;
        }
    }

    @Tag(name = "User Properties", description = "User saved properties")
    @Operation(
            summary = "Retrieve a property",
            tags = "User Properties",
            description = "Saves a property value for a user. Required scopes: 'users/write' or User JWT.")
    @Parameters(value = {
            @Parameter(name = "alaId", description = "The user's ALA ID", in = ParameterIn.QUERY, required = true),
            @Parameter(name = "name", description = "The name of the property to get", in = ParameterIn.QUERY, required = true),
            @Parameter(name = "value", description = "The value of the property to set.", in = ParameterIn.QUERY, required = false),
            @Parameter(name = "accept", description = "Must be application/json", in = ParameterIn.HEADER, required = true)
    })
    @SecurityRequirement(name = "JWT")
    @RequestMapping(
            value = {"/user/property"},
            method = RequestMethod.POST,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_PLAIN_VALUE})
    public @ResponseBody String save(
            @RequestParam(value = "alaId") String alaId,
            @RequestParam(value = "name") String name,
            @RequestParam(value = "value") String value,
            HttpServletRequest request
    ) throws Exception {
        String validAlaId = getValidUser(alaId, request, "users/write");

        UserProperty prop = storeDao.get(UserProperty.class, validAlaId).orElse(null);
        if (prop == null) {
            prop = new UserProperty();
            prop.setAlaId(validAlaId);
            prop.setProperties(new HashMap<>());
        } else if (prop.getProperties().size() >= maxUserProperties && !prop.getProperties().containsKey(name)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "User has reached the maximum number of properties");
        }

        if (StringUtils.isEmpty(value)) {
            // remove the property if the value is empty
            prop.getProperties().remove(name);
        } else {
            // add or update if it is new
            prop.getProperties().put(name, value);
        }

        storeDao.put(validAlaId, prop);

        if (prop.getProperties() != null) {
            return prop.getProperties().get(name);
        } else {
            return null;
        }
    }

    @Value("${security.jwt.enabled}")
    private Boolean jwtEnabled;


    private String getValidUser(String alaId, HttpServletRequest request, String m2mScope) {
        // JWT check only
        String validAlaId = null;
        Principal userPrincipal = request.getUserPrincipal();
        if (userPrincipal != null) {
            if (userPrincipal instanceof AlaUserProfile && alaId != null && alaId.equals(((AlaUserProfile) userPrincipal).getUserId())) {
                // only the user can get their own properties
                validAlaId = alaId;
            } else if (userPrincipal instanceof AlaM2MUserProfile && ((AlaM2MUserProfile) userPrincipal).getRoles().contains(m2mScope)) {
                // only M2M with scope users/read (consistent with userdetails) are permitted to get user properties
                validAlaId = alaId;
            }
        }

        if (validAlaId == null) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "User not authorized to access this resource");
        }

        return validAlaId;
    }
}
