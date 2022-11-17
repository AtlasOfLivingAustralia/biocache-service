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
import io.swagger.annotations.ApiParam;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
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
    @Tag(name="Assertions", description = "Annotations, assertions for data")
    @Operation(summary = "Retrieve an array of the assertion codes in use by the processing system", tags = "Assertions")
    @RequestMapping(value = {
            "/assertions/codes"
//            , "/assertions/codes.json", "/assertions/codes/"
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody
    Collection<AssertionCode> showCodes(
            @RequestParam(value="deprecated", required=false, defaultValue="false") Boolean isDeprecated
    ) throws Exception {
        return applyi18n(AssertionCodes.getAll(), isDeprecated, true);
    }

    @Operation(summary = "Retrieve an array of the assertion codes in use by users", tags = "Assertions")
    @RequestMapping(value = {"/assertions/user/codes"
//            , "/assertions/user/codes.json", "/assertions/user/codes/"
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
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
    @SecurityRequirement(name="JWT")
    @Operation(summary = "Add an assertion", tags = {"Assertions", "Occurrence"})
    @RequestMapping(value={"/occurrences/assertions/add"}, method = RequestMethod.POST)
    public ResponseEntity addAssertionWithParams(
            @RequestParam(value = "recordUuid", required=true) String recordUuid,
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

        return addAssertion(recordUuid, code, userId, userDisplayName, comment, userAssertionStatus, assertionUuid, relatedRecordId, relatedRecordReason, request, response);
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
    @Hidden
    @SecurityRequirement(name="JWT")
    @Operation(summary = "Bulk add an assertion", tags = "Assertions")
    @RequestMapping(value="/bulk/assertions/add", method = RequestMethod.POST)
    public void addBulkAssertions(@RequestParam(value = "assertions", required = true) String json,
                                  @RequestParam(value = "userId", required = true) String userId,
                                  @RequestParam(value = "userDisplayName", required = true) String userDisplayName,
                                  HttpServletRequest request,
                                  HttpServletResponse response) throws Exception {
        // check to see that the assertions have come from a valid source before adding
        if (!shouldPerformOperation(request, response)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Insufficient authentication credentials provided.");
        }

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

    /**
     * add an assertion
     */
    @SecurityRequirement(name="JWT")
    @Operation(summary = "Add an assertion to a record", tags = "Assertions")
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions/add"}, method = RequestMethod.POST)
    @ApiParam(value = "recordUuid", required = true)
    public ResponseEntity addAssertion(
       @PathVariable(value = "recordUuid") String recordUuid,
       @Parameter(description = "Assertion code") @RequestParam(value = "code") String code,
       @Parameter(description = "Atlas user ID") @RequestParam(value = "userId") String userId,
       @RequestParam(value = "userDisplayName") String userDisplayName,
       @RequestParam(value = "comment", required = false) String comment,
       @RequestParam(value = "userAssertionStatus", required = false) String userAssertionStatus,
       @RequestParam(value = "assertionUuid", required = false) String assertionUuid,
       @RequestParam(value = "relatedRecordId", required = false) String relatedRecordId,
       @RequestParam(value = "relatedRecordReason", required = false) String relatedRecordReason,
       HttpServletRequest request,
       HttpServletResponse response) throws Exception {

        if (!shouldPerformOperation(request, response)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Insufficient authentication credentials provided.");
        }

        try {
            Optional<QualityAssertion> qa = assertionService.addAssertion(
                    recordUuid, code, comment, userId, userDisplayName,
                    userAssertionStatus, assertionUuid, relatedRecordId, relatedRecordReason
            );
            if (qa.isPresent()) {

                String server = request.getSession().getServletContext().getInitParameter("serverName");
                return ResponseEntity
                        .created(new URI(server + "/occurrences/" + recordUuid + "/assertions/" + qa.get().getUuid()))
                        .contentLength(0)
                        .build();
            }

            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, " e.getMessage()", e);
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
    @SecurityRequirement(name="JWT")
    @Operation(summary = "Removes an assertion", tags = {"Assertions", "Occurrence"})
    @RequestMapping(value = {"/occurrences/assertions/delete"}, method = { RequestMethod.DELETE })
    public ResponseEntity deleteAssertionWithParams(
            @RequestParam(value = "recordUuid", required=true) String recordUuid,
            @RequestParam(value = "assertionUuid", required=true) String assertionUuid,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {

        return deleteAssertion(recordUuid, assertionUuid, request, response);
    }

    @Deprecated
    @SecurityRequirement(name="JWT")
    @Operation(summary = "Removes an assertion", tags = "Deprecated")
    @RequestMapping(value = {"/occurrences/assertions/delete"}, method = { RequestMethod.POST })
    public ResponseEntity deleteAssertionWithParamsPost(
            @RequestParam(value = "recordUuid", required=true) String recordUuid,
            @RequestParam(value = "assertionUuid", required=true) String assertionUuid,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {

        return deleteAssertion(recordUuid, assertionUuid, request, response);
    }

    /**
     * Remove an assertion
     */
    @SecurityRequirement(name="JWT")
    @Operation(summary = "Removes an assertion from a record", tags = {"Assertions", "Occurrence"})
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions/delete"}, method = RequestMethod.DELETE)
    @ApiParam(value = "recordUuid", required = true)
    public ResponseEntity deleteAssertion(
        @PathVariable(value="recordUuid") String recordUuid,
        @RequestParam(value="assertionUuid", required=true) String assertionUuid,
        HttpServletRequest request,
        HttpServletResponse response) throws Exception {

        if (!shouldPerformOperation(request, response)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Insufficient authentication credentials provided.");
        }

        try {
            if (assertionService.deleteAssertion(recordUuid, assertionUuid)) {
                return ResponseEntity.ok().contentLength(0).build();
            }

            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "recordUuid " + recordUuid + " or assertionUuid " + assertionUuid + " doesn't exist");

        } catch (IOException e) {
            logger.error("Failed to delete assertion [id: " + assertionUuid + "] for record " + recordUuid);
            logger.error(e.getMessage(), e);

            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
        }
    }

    /**
     * Remove an assertion
     */
    @Deprecated
    @SecurityRequirement(name="JWT")
    @Operation(summary = "Deprecated - use HTTP DELETE", tags = "Deprecated")
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions/delete"}, method = RequestMethod.POST)
    @ApiParam(value = "recordUuid", required = true)
    public ResponseEntity deleteAssertionPost(
            @PathVariable(value="recordUuid") String recordUuid,
            @RequestParam(value = "apiKey", required = true) String apiKey,
            @RequestParam(value="assertionUuid", required=true) String assertionUuid,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {

        if (!shouldPerformOperation(request, response)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Insufficient authentication credentials provided.");
        }

        try {

            if (assertionService.deleteAssertion(recordUuid, assertionUuid)) {
                return ResponseEntity.ok().contentLength(0).build();
            }

            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "recordUuid " + recordUuid + " or assertionUuid " + assertionUuid + " doesn't exist");

        } catch (IOException e) {
            logger.error("Failed to delete assertion [id: " + assertionUuid + "] for record " + recordUuid);
            logger.error(e.getMessage(), e);

            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
        }
    }

    @Deprecated
    @SecurityRequirement(name="JWT")
    @Operation(summary = "Get assertions for a record", tags = {"Deprecated"})
    @RequestMapping(value = {"/occurrences/assertions"
//            , "/occurrences/assertions.json", "/occurrences/assertions/"
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
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
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions/{assertionUuid}"
//            , "/occurrences/{recordUuid}/assertions/{assertionUuid}.json", "/occurrences/{recordUuid}/assertions/{assertionUuid}/"
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody
    QualityAssertion getAssertion(
            @ApiParam(value = "recordUuid", required = true) @PathVariable(value = "recordUuid") String recordUuid,
            @ApiParam(value = "assertionUuid", required = true) @PathVariable(value = "assertionUuid") String assertionUuid,
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
    @Operation(summary = "Get a assertions for a record", tags = "Assertions")
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions"
//            , "/occurrences/{recordUuid}/assertions.json", "/occurrences/{recordUuid}/assertions/"
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiParam(value = "recordUuid", required = true)
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

    @Deprecated
    @Operation(summary="Retrieve details fo assertion querries applied to this record", tags = "Deprecated")
    @RequestMapping(value = {
            "/occurrences/{recordUuid}/assertionQueries"
    }, method = RequestMethod.GET)
    @ApiParam(value = "recordUuid", required = true)
    public @ResponseBody
    List<QualityAssertion> getAssertionQueries(
            @PathVariable(value = "recordUuid") String recordUuid
    ) throws Exception {
        return new ArrayList<>();
    }

    @SecurityRequirement(name="JWT")
    @Secured({"ROLE_ADMIN"})
    @Operation(summary = "Synchronise assertions into the index", tags = "Monitoring")
    @RequestMapping(value = {"/sync"}, method = RequestMethod.GET)
    public @ResponseBody Boolean indexAll(HttpServletRequest request,
                                                        HttpServletResponse response) throws Exception {
        if (request.getUserPrincipal() != null) {
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
    public @ResponseBody String indexAllStatus(HttpServletRequest request,
                                                         HttpServletResponse response) throws Exception {
        if (request.getUserPrincipal() != null) {
            return assertionService.isIndexAllRunning() ? "indexAll task is running" : "No task is running";
        } else {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "An invalid API Key was provided.");
        }
        return null;
    }

    private Collection<AssertionCode> applyi18n(ErrorCode[] errorCodes, boolean includeDeprecated, boolean sortByName) {

        //use i18n descriptions
        List<AssertionCode> formattedAssertionCodes = new ArrayList<>();

        for (ErrorCode errorCode: errorCodes) {

            formattedAssertionCodes.add(new AssertionCode(errorCode.getName(), errorCode.getCode(),
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
                    .forEach(formattedAssertionCodes::add);
        }

        if (sortByName) {
            formattedAssertionCodes.sort(Comparator.comparing(AssertionCode::getName, String.CASE_INSENSITIVE_ORDER));
        }

        return formattedAssertionCodes;
    }
}
