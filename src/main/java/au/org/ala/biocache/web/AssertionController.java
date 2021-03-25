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

import au.org.ala.biocache.Store;
import au.org.ala.biocache.model.FullRecord;
import au.org.ala.biocache.model.QualityAssertion;
import au.org.ala.biocache.model.Versions;
import au.org.ala.biocache.service.AssertionService;
import au.org.ala.biocache.service.AuthService;
import au.org.ala.biocache.util.AssertionUtils;
import au.org.ala.biocache.vocab.ErrorCode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;

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
    private AssertionService assertionService;
   
    /**
     * Retrieve an array of the assertion codes in use by the processing system
     *
     * @return an array of codes
     * @throws Exception
     */
    @RequestMapping(value = {"/assertions/codes", "/assertions/codes/"}, method = RequestMethod.GET)
    public @ResponseBody ErrorCode[] showCodes() throws Exception {
        return applyi18n(Store.retrieveAssertionCodes());
    }

    @RequestMapping(value = {"/assertions/geospatial/codes", "/assertions/geospatial/codes/"}, method = RequestMethod.GET)
    public @ResponseBody ErrorCode[] showGeospatialCodes() throws Exception {
        return applyi18n(Store.retrieveGeospatialCodes());
    }

    @RequestMapping(value = {"/assertions/taxonomic/codes", "/assertions/taxonomic/codes/"}, method = RequestMethod.GET)
    public @ResponseBody ErrorCode[] showTaxonomicCodes() throws Exception {
        return applyi18n(Store.retrieveTaxonomicCodes());
    }

    @RequestMapping(value = {"/assertions/temporal/codes", "/assertions/temporal/codes/"}, method = RequestMethod.GET)
    public @ResponseBody ErrorCode[] showTemporalCodes() throws Exception {
        return applyi18n(Store.retrieveTemporalCodes());
    }

    @RequestMapping(value = {"/assertions/miscellaneous/codes", "/assertions/miscellaneous/codes/"}, method = RequestMethod.GET)
    public @ResponseBody ErrorCode[] showMiscellaneousCodes() throws Exception {
        return applyi18n(Store.retrieveMiscellaneousCodes());
    }

    @RequestMapping(value = {"/assertions/user/codes", "/assertions/user/codes/"}, method = RequestMethod.GET)
    public @ResponseBody ErrorCode[] showUserCodes() throws Exception {
        return applyi18n(Store.retrieveUserAssertionCodes());
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
            @RequestParam(value = "relatedRecordId", required = false) String relatedRecordId,
            @RequestParam(value = "relatedRecordReason", required = false) String relatedRecordReason,
            HttpServletResponse response) throws Exception {
        addAssertion(recordUuid, request,apiKey, code, comment, userId, userDisplayName, userAssertionStatus, assertionUuid, relatedRecordId, relatedRecordReason, response);
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
                List<java.util.Map<String, String>> assertions = om.readValue(json, new TypeReference<List<Map<String, String>>>() {
                });
                logger.debug("The assertions in a list of maps: " + assertions);
                java.util.HashMap<String,QualityAssertion> qas = new java.util.HashMap<String,QualityAssertion>(assertions.size());
                for(java.util.Map<String,String> assertion : assertions){
                    String code = assertion.get("code");
                    String comment = assertion.get("comment");
                    String recordUuid = assertion.get("recordUuid");
                    QualityAssertion qa = au.org.ala.biocache.model.QualityAssertion.apply(Integer.parseInt(code));
                    qa.setComment(comment);
                    qa.setUserId(userId);
                    qa.setUserDisplayName(userDisplayName);
                    qas.put(recordUuid, qa);
                }
                if(qas.size()>0){
                    //add the qas in bulk
                    Store.addUserAssertions(qas);
                }
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
       @RequestParam(value = "relatedRecordId", required = false) String relatedRecordId,
       @RequestParam(value = "relatedRecordReason", required = false) String relatedRecordReason,
        HttpServletResponse response) throws Exception {

        if (shouldPerformOperation(request, response)) {
            try {
                QualityAssertion qa = assertionService.addAssertion(recordUuid, code, comment, userId, userDisplayName, userAssertionStatus, assertionUuid, relatedRecordId, relatedRecordReason);

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

        if(shouldPerformOperation(request, response)){
            try{
                Store.deleteUserAssertion(recordUuid, assertionUuid);
                //postNotificationEvent("delete", recordUuid, assertionUuid);
                response.setStatus(HttpServletResponse.SC_OK);
            } catch(Exception e){
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
            }
        }
    }

    /**
     * Generic method to post a record assertion notification.
     * @param type
     * @param recordUuid
     * @param id
     * @deprecated assertion notifications are obtained through biocache ws NOT the collectory. This method should not be called.
     */
    @Deprecated
    private void postNotificationEvent(String type, String recordUuid, String id) {
        //get the processed record so that we can get the collection_uid
        FullRecord processed = Store.getByUuid(recordUuid, Versions.PROCESSED());
        String uid = processed == null ? null : processed.getAttribution().getCollectionUid();

        if (uid != null) {
            final String uri = registryUrl + "/ws/notify";
            HttpClient h = new HttpClient();
            PostMethod m = new PostMethod(uri);

            try {
                m.setRequestEntity(new StringRequestEntity("{ event: 'user annotation', id: '" + id + "', uid: '" + uid + "', type:'" + type + "' }", "text/json", "UTF-8"));

                logger.debug("Adding notification: " + type + ":" + uid + " - " + id);
                int status = h.executeMethod(m);
                logger.debug("STATUS: " + status);
                if (status == 200) {
                    logger.debug("Successfully posted an event to the notification service");
                } else {
                    logger.info("Failed to post an event to the notification service");
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            } finally {
                try {
                    m.releaseConnection();
                } finally {
                    h.getHttpConnectionManager().closeIdleConnections(0L);
                }
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
    public @ResponseBody QualityAssertion getAssertion(
        @PathVariable(value="recordUuid") String recordUuid,
        @PathVariable(value="assertionUuid") String assertionUuid,
        HttpServletResponse response) {
        QualityAssertion qa = assertionUtils.getUserAssertion(recordUuid, assertionUuid);
        if(qa != null){
            return qa;
        } else {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }
    }

    /**
     * Get user assertions
     */
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions", "/occurrences/{recordUuid}/assertions/"}, method = RequestMethod.GET)
    public @ResponseBody List<QualityAssertion> getAssertions(
        @PathVariable(value="recordUuid") String recordUuid,
        HttpServletResponse response
    ) throws Exception {
        List<QualityAssertion> assertions =  assertionUtils.getUserAssertions(recordUuid);
        if (assertions == null){
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "Unrecognised record with ID: " + recordUuid);
            return null;
        } else {
            return assertions;
        }
    }

    public void setAssertionUtils(AssertionUtils assertionUtils) {
        this.assertionUtils = assertionUtils;
    }
    
    private ErrorCode[] applyi18n(ErrorCode[] errorCodes) {
        //use i18n descriptions
        ErrorCode[] formattedErrorCodes = new ErrorCode[errorCodes.length];
        for (int i = 0; i < errorCodes.length; i++) {
            formattedErrorCodes[i] = new ErrorCode(errorCodes[i].getName(), errorCodes[i].getCode(),
                    errorCodes[i].getIsFatal(),
                    messageSource.getMessage(errorCodes[i].getName(), null, errorCodes[i].getDescription(), null),
                    errorCodes[i].getCategory());
        }
        return formattedErrorCodes;
    }
}
