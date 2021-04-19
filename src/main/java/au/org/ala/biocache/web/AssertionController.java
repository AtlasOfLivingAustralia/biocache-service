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
import au.org.ala.biocache.service.AuthService;
import au.org.ala.biocache.util.AssertionUtils;
import au.org.ala.biocache.util.OccurrenceUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocument;
import org.gbif.api.vocabulary.InterpretationRemarkSeverity;
import org.gbif.api.vocabulary.NameUsageIssue;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This controller provides web services for assertion creation/deletion.
 *
 * TODO Add support for API keys so that only registered applications can
 * use these functions.
 */
@Controller
public class AssertionController extends AbstractSecureController {

    private final static Logger logger = Logger.getLogger(AssertionController.class);
    @Inject
    protected AssertionUtils assertionUtils;
    @Value("${registry.url:https://collections.ala.org.au}")
    protected String registryUrl = "https://collections.ala.org.au";
    @Inject
    protected AuthService authService;
    @Inject
    private AbstractMessageSource messageSource;
    @Inject
    private StoreDAO storeDao;
    @Inject
    private OccurrenceUtils occurrenceUtils;

    /**
     * Retrieve an array of the assertion codes in use by the processing system
     *
     * @return an array of codes
     * @throws Exception
     */
    @RequestMapping(value = {"/assertions/codes", "/assertions/codes/"}, method = RequestMethod.GET)
    public @ResponseBody
    ErrorCode[] showCodes() throws Exception {
        return applyi18n(AssertionCodes.getAll());
    }

    @RequestMapping(value = {"/assertions/geospatial/codes", "/assertions/geospatial/codes/"}, method = RequestMethod.GET)
    public @ResponseBody ErrorCode[] showGeospatialCodes() throws Exception {
        return applyi18n(AssertionCodes.getGeospatialCodes());
    }

    @RequestMapping(value = {"/assertions/taxonomic/codes", "/assertions/taxonomic/codes/"}, method = RequestMethod.GET)
    public @ResponseBody ErrorCode[] showTaxonomicCodes() throws Exception {
        return applyi18n(AssertionCodes.getTaxonomicCodes());
    }

    @RequestMapping(value = {"/assertions/temporal/codes", "/assertions/temporal/codes/"}, method = RequestMethod.GET)
    public @ResponseBody ErrorCode[] showTemporalCodes() throws Exception {
        return applyi18n(AssertionCodes.getTemporalCodes());
    }

    @RequestMapping(value = {"/assertions/miscellaneous/codes", "/assertions/miscellaneous/codes/"}, method = RequestMethod.GET)
    public @ResponseBody ErrorCode[] showMiscellaneousCodes() throws Exception {
        return applyi18n(AssertionCodes.getMiscellaneousCodes());
    }

    @RequestMapping(value = {"/assertions/user/codes", "/assertions/user/codes/"}, method = RequestMethod.GET)
    public @ResponseBody ErrorCode[] showUserCodes() throws Exception {
        return applyi18n(AssertionCodes.userAssertionCodes);
    }

    /**
     * Add assertion.
     * 
     * @param recordUuid
     * @param request
     * @param response
     * @throws Exception
     */
    @RequestMapping(value={"/occurrences/assertions/add"}, method = RequestMethod.POST)
    public void addAssertionWithParams(
            @RequestParam(value="recordUuid", required=true) String recordUuid,
            HttpServletRequest request,
            @RequestParam(value = "apiKey", required = true) String apiKey,
            @RequestParam(value = "code", required = true) String code,
            @RequestParam(value = "comment", required = false) String comment,
            @RequestParam(value = "userId", required = true) String userId,
            @RequestParam(value = "userDisplayName", required = true) String userDisplayName,
            @RequestParam(value = "userAssertionStatus", required = false) String userAssertionStatus,
            @RequestParam(value = "assertionUuid", required = false) String assertionUuid,
            HttpServletResponse response) throws Exception {
        addAssertion(recordUuid, request, apiKey, code, comment, userId, userDisplayName, userAssertionStatus, assertionUuid, response);
    }
    /**
     * Adds a bulk list of assertions.
     * 
     * This method expects certain request params to be provided
     * apiKey
     * userId
     * userDisplayName
     * assertions - a json list of assertion maps to be applied.
     * 
     * @param request
     * @param response
     * @throws Exception
     */
    @RequestMapping(value="/bulk/assertions/add", method = RequestMethod.POST)
    public void addBulkAssertions(HttpServletRequest request,
                                  @RequestParam(value = "apiKey", required = true) String apiKey,
                                  @RequestParam(value = "assertions", required = true) String json,
                                  @RequestParam(value = "userId", required = true) String userId,
                                  @RequestParam(value = "userDisplayName", required = true) String userDisplayName,
                                  HttpServletResponse response) throws Exception {
        ObjectMapper om = new ObjectMapper();
        try {
            //check to see that the assertions have come from a valid source before adding
            if (shouldPerformOperation(request, response)) {
                List<Map<String, String>> assertions = om.readValue(json, new TypeReference<List<Map<String, String>>>() {});
                logger.debug("The assertions in a list of maps: " + assertions);

                // uuid -> [assertion, assertion]
                Map<String, List<Map<String, String>>> uuidMappedAssertions = assertions.stream().collect(Collectors.groupingBy(assertion -> (String)assertion.get("recordUuid")));

                uuidMappedAssertions.forEach((uuid, maps) -> {
                    try {
                        UserAssertions userAssertions = storeDao.get(UserAssertions.class, uuid).orElse(new UserAssertions());
                        for (Map<String, String> as : maps) {
                            QualityAssertion qa = new QualityAssertion();
                            Integer code = Integer.parseInt(as.get("code"));
                            qa.setCode(code);
                            qa.setComment(as.get("comment"));
                            qa.setUserId(userId);
                            qa.setUserDisplayName(userDisplayName);
                            userAssertions.add(qa);
                        }

                        if (!userAssertions.isEmpty()) {
                            storeDao.put(uuid, userAssertions);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        } catch(Exception e) {
            logger.error(e.getMessage(),e);
            response.sendError(HttpURLConnection.HTTP_BAD_REQUEST);
        }
    }

    /**
     * add an assertion
     */
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions/add"}, method = RequestMethod.POST)
    public void addAssertion(
       @PathVariable(value="recordUuid") String recordUuid,
        HttpServletRequest request,
       @RequestParam(value = "apiKey", required = true) String apiKey,
       @RequestParam(value = "code", required = true) String code,
       @RequestParam(value = "comment", required = false) String comment,
       @RequestParam(value = "userId", required = true) String userId,
       @RequestParam(value = "userDisplayName", required = true) String userDisplayName,
       @RequestParam(value = "userAssertionStatus", required = false) String userAssertionStatus,
       @RequestParam(value = "assertionUuid", required = false) String assertionUuid,
        HttpServletResponse response) throws Exception {

        if (shouldPerformOperation(request, response)) {
            try {
                logger.debug("Adding assertion to:" + recordUuid + ", code:" + code + ", comment:" + comment
                        + ",userAssertionStatus: " + userAssertionStatus + ", assertionUuid: " + assertionUuid
                        + ", userId:" + userId + ", userDisplayName:" + userDisplayName);

                QualityAssertion qa = new QualityAssertion();
                qa.setUuid(UUID.randomUUID().toString());
                qa.setCode(Integer.parseInt(code));
                qa.setComment(comment);
                qa.setUserId(userId);
                qa.setUserDisplayName(userDisplayName);
                qa.setReferenceRowKey(recordUuid);
                if (code.equals(Integer.toString(AssertionCodes.VERIFIED.getCode()))) {
                    qa.setRelatedUuid(assertionUuid);
                    qa.setQaStatus(Integer.parseInt(userAssertionStatus));
                } else {
                    qa.setQaStatus(AssertionStatus.QA_UNCONFIRMED);
                }
                // get dataResourceUid
                SolrDocument sd = occurrenceUtils.getOcc(recordUuid);
                if (sd != null) {
                    qa.setDataResourceUid((String) sd.getFieldValue("dataResourceUid"));
                }

                UserAssertions existingAssertions = storeDao.get(UserAssertions.class, recordUuid).orElse(new UserAssertions());
                existingAssertions.add(qa);
                storeDao.put(recordUuid, existingAssertions);

                String server = request.getSession().getServletContext().getInitParameter("serverName");
                response.setHeader("Location", server + "/occurrences/" + recordUuid + "/assertions/" + qa.getUuid());
                response.setStatus(HttpServletResponse.SC_CREATED);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            }
        }
    }

    /**
     * Removes an assertion
     * 
     * This version of the method can handle the situation where we use rowKeys as Uuids. Thus
     * URL style rowKeys can be correctly supported.
     * 
     * @param recordUuid
     * @param assertionUuid
     * @param request
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = {"/occurrences/assertions/delete"}, method = RequestMethod.POST)
    public void deleteAssertionWithParams(
            @RequestParam(value="recordUuid", required=true) String recordUuid,
            @RequestParam(value = "apiKey", required = true) String apiKey,
            @RequestParam(value="assertionUuid", required=true) String assertionUuid,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {
        deleteAssertion(recordUuid, apiKey, assertionUuid, request, response);
    }

    /**
     * Remove an assertion
     */
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions/delete"}, method = RequestMethod.POST)
    public void deleteAssertion(
        @PathVariable(value="recordUuid") String recordUuid,
        @RequestParam(value = "apiKey", required = true) String apiKey,
        @RequestParam(value="assertionUuid", required=true) String assertionUuid,
        HttpServletRequest request,
        HttpServletResponse response) throws Exception {

        if(shouldPerformOperation(request, response)) {
            try {
                UserAssertions userAssertions = storeDao.get(UserAssertions.class, recordUuid).orElse(new UserAssertions());
                // if there's any change made
                // TODO: if userAssertions.size() == 0, we actually need to remove the key/value pair instead of having an empty value in Cassandra
                if (userAssertions.deleteUuid(assertionUuid)) {
                    storeDao.put(recordUuid, userAssertions);
                }
                response.setStatus(HttpServletResponse.SC_OK);
            } catch (Exception e) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
            }
        }
    }

    @RequestMapping(value = {"/occurrences/assertions", "/occurrences/assertions/"}, method = RequestMethod.GET)
    public @ResponseBody Object getAssertionWithParams(
            @RequestParam(value="recordUuid", required=true) String recordUuid,
            @RequestParam(value="assertionUuid",required=false) String assertionUuid,
            HttpServletResponse response) throws Exception{
        if(assertionUuid != null){
            return getAssertion(recordUuid, assertionUuid, response);
        } else {
            return getAssertions(recordUuid, response);
        }
    }

    /**
     * Get single assertion
     */
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions/{assertionUuid}", "/occurrences/{recordUuid}/assertions/{assertionUuid}/"}, method = RequestMethod.GET)
    public @ResponseBody
    QualityAssertion getAssertion(
            @PathVariable(value = "recordUuid") String recordUuid,
            @PathVariable(value = "assertionUuid") String assertionUuid,
            HttpServletResponse response) throws Exception {
        UserAssertions userAssertions = storeDao.get(UserAssertions.class, recordUuid).orElse(new UserAssertions());
        if (userAssertions.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else {
            for (QualityAssertion qa : userAssertions) {
                if (qa.getUuid().equals(assertionUuid)) {
                    // do not return the snapshot
                    qa.setSnapshot(null);
                    return qa;
                }
            }
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }
        return null;
    }


    /**
     * Get user assertions
     */
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions", "/occurrences/{recordUuid}/assertions/"}, method = RequestMethod.GET)
    public @ResponseBody List<QualityAssertion> getAssertions(
        @PathVariable(value="recordUuid") String recordUuid,
        HttpServletResponse response
    ) throws Exception {
        UserAssertions userAssertions = storeDao.get(UserAssertions.class, recordUuid).orElse(new UserAssertions());
        for (QualityAssertion qa : userAssertions) {
            qa.setSnapshot(null);
        }

        return userAssertions;
    }

    @Deprecated
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertionQueries", "/occurrences/{recordUuid}/assertionQueries/"}, method = RequestMethod.GET)
    public @ResponseBody
    List<QualityAssertion> getAssertionQueries(
            @PathVariable(value = "recordUuid") String recordUuid,
            HttpServletResponse response
    ) throws Exception {
        return new ArrayList<>();
    }

    public void setAssertionUtils(AssertionUtils assertionUtils) {
        this.assertionUtils = assertionUtils;
    }

    private ErrorCode[] applyi18n(ErrorCode[] errorCodes) {
        //use i18n descriptions
        ErrorCode[] formattedErrorCodes = new ErrorCode[errorCodes.length];
        for (int i = 0; i < errorCodes.length; i++) {
            formattedErrorCodes[i] = new ErrorCode(errorCodes[i].getName(), errorCodes[i].getCode(),
                    errorCodes[i].getFatal(),
                    messageSource.getMessage(errorCodes[i].getName(), null, errorCodes[i].getDescription(), null),
                    ErrorCode.Category.valueOf(errorCodes[i].getCategory()));
        }
        return formattedErrorCodes;
    }

    private ErrorCode[] applyi18n(NameUsageIssue[] nameUsageIssues) {
        //use i18n descriptions
        ErrorCode[] formattedErrorCodes = new ErrorCode[nameUsageIssues.length];
        for (int i = 0; i < nameUsageIssues.length; i++) {
            formattedErrorCodes[i] = new ErrorCode(
                    nameUsageIssues[i].name(),
                    nameUsageIssues[i].ordinal(),
                    nameUsageIssues[i].getSeverity().equals(InterpretationRemarkSeverity.ERROR),
                    messageSource.getMessage(nameUsageIssues[i].name(), null, nameUsageIssues[i].name(), null),
                    ErrorCode.Category.Taxonomic);
        }
        return formattedErrorCodes;
    }

    private ErrorCode[] applyi18n(OccurrenceIssue[] occurrenceIssues) {
        //use i18n descriptions
        ErrorCode[] formattedErrorCodes = new ErrorCode[occurrenceIssues.length];
        for (int i = 0; i < occurrenceIssues.length; i++) {
            formattedErrorCodes[i] = new ErrorCode(
                    occurrenceIssues[i].name(),
                    occurrenceIssues[i].ordinal(),
                    occurrenceIssues[i].getSeverity().equals(InterpretationRemarkSeverity.ERROR),
                    messageSource.getMessage(occurrenceIssues[i].name(), null, occurrenceIssues[i].name(), null),
                    ErrorCode.Category.Geospatial);
        }
        return formattedErrorCodes;
    }
}
