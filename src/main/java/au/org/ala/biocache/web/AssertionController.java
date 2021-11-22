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

import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.service.AssertionService;
import au.org.ala.biocache.util.solr.FieldMappingUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This controller provides web services for assertion creation/deletion.
 */
@RestController
public class AssertionController extends AbstractSecureController {

    private final static Logger logger = Logger.getLogger(AssertionController.class);

    @Value("${registry.url:https://collections.ala.org.au}")
    protected String registryUrl = "https://collections.ala.org.au";
    @Inject
    private AbstractMessageSource messageSource;
    @Inject
    private AssertionService assertionService;
    @Inject
    private FieldMappingUtil fieldMappingUtil;

    /**
     * Retrieve an array of the assertion codes in use by the processing system
     *
     * @return an array of codes
     * @throws Exception
     */
    @Operation(summary = "Retrieve an array of the assertion codes in use by the processing system", tags = "Assertions")
    @RequestMapping(value = {"/assertions/codes", "/assertions/codes.json", "/assertions/codes/"}, method = RequestMethod.GET)
    public @ResponseBody
    Collection<AssertionCode> showCodes(
            @RequestParam(value="deprecated", required=false, defaultValue="false") Boolean isDeprecated
    ) throws Exception {
        return applyi18n(AssertionCodes.getAll(), isDeprecated, true);
    }

    @Operation(summary = "Retrieve an array of the assertion codes in use by users", tags = "Assertions")
    @RequestMapping(value = {"/assertions/user/codes", "/assertions/user/codes.json", "/assertions/user/codes/"}, method = RequestMethod.GET)
    public @ResponseBody Collection<AssertionCode> showUserCodes(
            @RequestParam(value="deprecated", required=false, defaultValue="false") Boolean isDeprecated
    ) throws Exception {
        return applyi18n(AssertionCodes.userAssertionCodes, isDeprecated, false);
    }

    /**
     * Add assertion.
     * 
     * @param recordUuid
     * @param request
     * @param response
     * @throws Exception
     */
    @Operation(summary = "Add an assertion", tags = "Assertions")
    @RequestMapping(value={"/occurrences/assertions/add"}, method = RequestMethod.POST)
    public void addAssertionWithParams(
            @RequestParam(value = "recordUuid", required=true) String recordUuid,
            @RequestParam(value = "apiKey", required = true) String apiKey,
            @RequestParam(value = "code", required = true) String code,
            @RequestParam(value = "comment", required = false) String comment,
            @RequestParam(value = "userId", required = true) String userId,
            @RequestParam(value = "userDisplayName", required = true) String userDisplayName,
            @RequestParam(value = "userAssertionStatus", required = false) String userAssertionStatus,
            @RequestParam(value = "assertionUuid", required = false) String assertionUuid,
            @RequestParam(value = "relatedRecordId", required = false) String relatedRecordId,
            @RequestParam(value = "relatedRecordReason", required = false) String relatedRecordReason,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {

        addAssertion(recordUuid, apiKey, code, comment, userId, userDisplayName, userAssertionStatus, assertionUuid, relatedRecordId, relatedRecordReason, request, response);
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
    @Operation(summary = "Bulk add an assertion", tags = "Assertions")
    @RequestMapping(value="/bulk/assertions/add", method = RequestMethod.POST)
    public void addBulkAssertions(HttpServletRequest request,
                                  @RequestParam(value = "apiKey", required = true) String apiKey,
                                  @RequestParam(value = "assertions", required = true) String json,
                                  @RequestParam(value = "userId", required = true) String userId,
                                  @RequestParam(value = "userDisplayName", required = true) String userDisplayName,
                                  HttpServletResponse response) throws Exception {
        // check to see that the assertions have come from a valid source before adding
        if (shouldPerformOperation(request, response)) {
            ObjectMapper om = new ObjectMapper();
            List<Map<String, String>> assertions = om.readValue(json, new TypeReference<List<Map<String, String>>>() {});
            logger.debug("The assertions in a list of maps: " + assertions);

            // uuid -> [assertion, assertion]
            Map<String, List<Map<String, String>>> uuidMappedAssertions = assertions.stream().collect(Collectors.groupingBy(assertion -> (String)assertion.get("recordUuid")));

            uuidMappedAssertions.forEach((uuid, maps) -> {
                try {
                    UserAssertions userAssertions = new UserAssertions();
                    for (Map<String, String> as : maps) {
                        QualityAssertion qa = new QualityAssertion();
                        Integer code = Integer.parseInt(as.get("code"));
                        qa.setCode(code);
                        if (code.equals(AssertionCodes.VERIFIED.getCode())) {
                            qa.setRelatedUuid(as.get("assertionUuid"));
                            qa.setQaStatus(Integer.parseInt(as.get("userAssertionStatus")));
                        } else {
                            qa.setQaStatus(AssertionStatus.QA_UNCONFIRMED);
                        }

                        qa.setComment(as.get("comment"));
                        qa.setUserId(userId);
                        qa.setUserDisplayName(userDisplayName);
                        qa.setReferenceRowKey(uuid);
                        userAssertions.add(qa);
                    }

                    assertionService.bulkAddAssertions(uuid, userAssertions);
                } catch (IOException e) {
                    logger.error("Failed to bulk add assertions for record: " + uuid);
                    logger.error(e.getMessage(), e);
                }
            });
        }
    }

    /**
     * add an assertion
     */
    @Operation(summary = "Add an assertion (REST style)", tags = "Assertions")
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions/add"}, method = RequestMethod.POST)
    public void addAssertion(
       @PathVariable(value="recordUuid") String recordUuid,
       @RequestParam(value = "apiKey", required = true) String apiKey,
       @RequestParam(value = "code", required = true) String code,
       @RequestParam(value = "comment", required = false) String comment,
       @RequestParam(value = "userId", required = true) String userId,
       @RequestParam(value = "userDisplayName", required = true) String userDisplayName,
       @RequestParam(value = "userAssertionStatus", required = false) String userAssertionStatus,
       @RequestParam(value = "assertionUuid", required = false) String assertionUuid,
       @RequestParam(value = "relatedRecordId", required = false) String relatedRecordId,
       @RequestParam(value = "relatedRecordReason", required = false) String relatedRecordReason,
       HttpServletRequest request,
       HttpServletResponse response) throws Exception {

        if (shouldPerformOperation(request, response)) {
            try {
                Optional<QualityAssertion> qa = assertionService.addAssertion(recordUuid, code, comment, userId, userDisplayName, userAssertionStatus, assertionUuid, relatedRecordId, relatedRecordReason);
                if (qa.isPresent()) {
                    String server = request.getSession().getServletContext().getInitParameter("serverName");
                    response.setHeader("Location", server + "/occurrences/" + recordUuid + "/assertions/" + qa.get().getUuid());
                    response.setStatus(HttpServletResponse.SC_CREATED);
                } else {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
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
    @Operation(summary = "Removes an assertion", tags = "Assertions")
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
    @Operation(summary = "Removes an assertion (REST style)", tags = "Assertions")
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions/delete"}, method = RequestMethod.POST)
    public void deleteAssertion(
        @PathVariable(value="recordUuid") String recordUuid,
        @RequestParam(value = "apiKey", required = true) String apiKey,
        @RequestParam(value="assertionUuid", required=true) String assertionUuid,
        HttpServletRequest request,
        HttpServletResponse response) throws Exception {

        if (shouldPerformOperation(request, response)) {
            try {
                if (assertionService.deleteAssertion(recordUuid, assertionUuid)) {
                    response.setStatus(HttpServletResponse.SC_OK);
                } else {
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "recordUuid " + recordUuid + " or assertionUuid " + assertionUuid + " doesn't exist");
                }
            } catch (IOException e) {
                logger.error("Failed to delete assertion [id: " + assertionUuid + "] for record " + recordUuid);
                logger.error(e.getMessage(), e);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
            }
        }
    }

    @Operation(summary = "Get assertions for a record", tags = "Assertions")
    @RequestMapping(value = {"/occurrences/assertions", "/occurrences/assertions.json", "/occurrences/assertions/"}, method = RequestMethod.GET)
    public @ResponseBody Object getAssertionWithParams(
            @RequestParam(value="recordUuid", required=true) String recordUuid,
            @RequestParam(value="assertionUuid",required=false) String assertionUuid,
            HttpServletResponse response) throws Exception{
        if (assertionUuid != null){
            return getAssertion(recordUuid, assertionUuid, response);
        } else {
            return getAssertions(recordUuid, response);
        }
    }

    /**
     * Get single assertion
     */
    @Operation(summary = "Get a single assertion", tags = "Assertions")
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions/{assertionUuid}", "/occurrences/{recordUuid}/assertions/{assertionUuid}.json", "/occurrences/{recordUuid}/assertions/{assertionUuid}/"}, method = RequestMethod.GET)
    public @ResponseBody
    QualityAssertion getAssertion(
            @PathVariable(value = "recordUuid") String recordUuid,
            @PathVariable(value = "assertionUuid") String assertionUuid,
            HttpServletResponse response) throws Exception {

        try {
            QualityAssertion assertion = assertionService.getAssertion(recordUuid, assertionUuid);
            if (assertion != null) {
                return assertion;
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            }
        } catch (IOException e) {
            logger.error("Failed to get assertion [id: " + assertionUuid + "] for record " + recordUuid);
            logger.error(e.getMessage(), e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return null;
    }


    /**
     * Get user assertions
     */
    @Operation(summary = "Get a assertions for a record (REST style)", tags = "Assertions")
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions", "/occurrences/{recordUuid}/assertions.json", "/occurrences/{recordUuid}/assertions/"}, method = RequestMethod.GET)
    public @ResponseBody List<QualityAssertion> getAssertions(
        @PathVariable(value="recordUuid") String recordUuid,
        HttpServletResponse response
    ) throws Exception {
        try {
            return assertionService.getAssertions(recordUuid);
        } catch (Exception e) {
            logger.error("Failed to get assertions for record " + recordUuid);
            logger.error(e.getMessage(), e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
            return null;
        }
    }

    @Hidden
    @Deprecated
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertionQueries", "/occurrences/{recordUuid}/assertionQueries.json", "/occurrences/{recordUuid}/assertionQueries/"}, method = RequestMethod.GET)
    public @ResponseBody
    List<QualityAssertion> getAssertionQueries(
            @PathVariable(value = "recordUuid") String recordUuid,
            HttpServletResponse response
    ) throws Exception {
        return new ArrayList<>();
    }

    @Operation(summary = "Synchronise assertions into the index", tags = "Monitoring")
    @RequestMapping(value = {"/sync"}, method = RequestMethod.GET)
    public @ResponseBody Boolean indexAll(@RequestParam(value="apiKey", required=true) String apiKey,
                                                        HttpServletResponse response) throws Exception {
        if (isValidKey(apiKey)) {
            if (assertionService.indexAll()) {
                response.setStatus(HttpServletResponse.SC_OK);
            } else {
                response.setStatus(HttpServletResponse.SC_OK, "An index all user assertions job already running. Your request won't be processed.");
            }
        } else {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "An invalid API Key was provided.");
        }
        return null;
    }

    @Operation(summary = "Monitoring the progress of synchronising assertions into the index", tags = "Monitoring")
    @RequestMapping(value = {"/sync/status"}, method = RequestMethod.GET)
    public @ResponseBody String indexAllStatus(@RequestParam(value="apiKey", required=true) String apiKey,
                                                         HttpServletResponse response) throws Exception {
        if (isValidKey(apiKey)) {
            return assertionService.isIndexAllRunning() ? "indexAll task is running" : "No task is running";
        } else {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "An invalid API Key was provided.");
        }
        return null;
    }

    private Collection<AssertionCode> applyi18n(ErrorCode[] errorCodes, boolean includeDeprecated, boolean sortByName) {

        //use i18n descriptions
        List<AssertionCode> formatedAssertionCodes = new ArrayList<>();

        for (ErrorCode errorCode: errorCodes) {

            formatedAssertionCodes.add(new AssertionCode(errorCode.getName(), errorCode.getCode(),
                    errorCode.getFatal(),
                    messageSource.getMessage(errorCode.getName(), null, errorCode.getDescription(), null),
                    ErrorCode.Category.valueOf(errorCode.getCategory()),
                    errorCode.getTermsRequiredToTest())
            );
        }

        if (includeDeprecated) {

            fieldMappingUtil.getFieldValueMappingStream("assertions")
                    .filter((Pair<String, String> assertionMapping) ->
                        Arrays.stream(errorCodes).anyMatch((ErrorCode errorCode) -> errorCode.getName().equals(assertionMapping.getRight()))
                    )
                    .map((Pair<String, String> assertionMapping) -> {
                        AssertionCode assertionCode = new AssertionCode();
                        assertionCode.setName(assertionMapping.getLeft());
                        assertionCode.setDeprecated(true);
                        assertionCode.setNewName(assertionMapping.getRight());

                        return assertionCode;
                    })
                    .forEach(formatedAssertionCodes::add);
        }

        if (sortByName) {
            formatedAssertionCodes.sort(Comparator.comparing(AssertionCode::getName, String.CASE_INSENSITIVE_ORDER));
        }

        return formatedAssertionCodes;
    }
}
