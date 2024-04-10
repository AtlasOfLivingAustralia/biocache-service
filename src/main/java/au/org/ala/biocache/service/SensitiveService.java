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

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;

@Component("sensitiveService")
public class SensitiveService {

    protected static final Logger logger = Logger.getLogger(SensitiveService.class);

    /**
     * Set to true to enable downloading of sensitive data
     */
    @Value("${download.auth.sensitive:false}")
    private Boolean downloadAuthSensitive;

    //TODO: this should be retrieved from SDS
    @Value("${sensitiveAccessRoles20:{\n" +
            "\n" +
            "\"ROLE_SDS_ACT\" : \"sensitive:\\\"generalised\\\" AND (cl927:\\\"Australian Captial Territory\\\" OR cl927:\\\"Jervis Bay Territory\\\") AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\"\n" +
            "\"ROLE_SDS_NSW\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"New South Wales (including Coastal Waters)\\\" AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_NZ\" : \"sensitive:\\\"generalised\\\" AND (dataResourceUid:dr2707 OR dataResourceUid:dr812 OR dataResourceUid:dr814 OR dataResourceUid:dr808 OR dataResourceUid:dr806 OR dataResourceUid:dr815 OR dataResourceUid:dr802 OR dataResourceUid:dr805 OR dataResourceUid:dr813) AND -cl927:* AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_NT\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Northern Territory (including Coastal Waters)\\\" AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_QLD\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Queensland (including Coastal Waters)\\\" AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_SA\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"South Australia (including Coastal Waters)\\\" AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_TAS\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Tasmania (including Coastal Waters)\\\" AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_VIC\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Victoria (including Coastal Waters)\\\" AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_WA\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Western Australia (including Coastal Waters)\\\" AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_BIRDLIFE\" : \"sensitive:\\\"generalised\\\" AND (dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\"\n" +
            "\n" +
            "}}")
    protected String sensitiveAccessRoles20 = "{}";

    private JSONObject sensitiveAccessRolesToSolrFilters20;

    @PostConstruct
    public void init() throws ParseException {
        // Simple JSON initialisation, let's follow the default Spring semantics
        sensitiveAccessRolesToSolrFilters20 = (JSONObject) new JSONParser().parse(sensitiveAccessRoles20);
    }

    /**
     * Generates the Solr filter to query sensitive data for the user sensitive roles
     *
     * @return A String with a Solr filter
     */
    public String getSensitiveFq(Set<String> userRoles) {

        if (downloadAuthSensitive == null || !downloadAuthSensitive) {
            return null;
        }

        List<String> sensitiveRoles = new ArrayList<>(sensitiveAccessRolesToSolrFilters20.keySet());
        sensitiveRoles.retainAll(userRoles);

        String sensitiveFq = "";

        for (String sensitiveRole : sensitiveRoles) {
            if (sensitiveFq.length() > 0) {
                sensitiveFq += " OR ";
            }
            sensitiveFq += "(" + sensitiveAccessRolesToSolrFilters20.get(sensitiveRole) + ")";
        }

        if (sensitiveFq.length() == 0) {
            return null;
        }

        return sensitiveFq;
    }
}
