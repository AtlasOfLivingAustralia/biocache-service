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
import au.org.ala.biocache.model.ValidationRule;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.dto.ValidationRuleDTO;
import au.org.ala.biocache.service.AuthService;
import au.org.ala.biocache.service.SpeciesLookupService;
import au.org.ala.biocache.util.AssertionUtils;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.HttpURLConnection;

/**
 * A controller for the submission and lookup of validation rules against occurrence records.
 */
@Controller
public class ValidationRuleController extends AbstractSecureController {

    private final static Logger logger = Logger.getLogger(AssertionController.class);
    @Inject
    protected AssertionUtils assertionUtils;
    @Inject
    protected AuthService authService;
    @Inject
    protected SpeciesLookupService speciesLookupService;
    @Inject
    protected SearchDAO searchDAO;

    @RequestMapping(value = {"/validation/rules", "/validation/rules/", "/assertions/queries", "/assertions/queries/"}, method = RequestMethod.GET)
    public @ResponseBody ValidationRule[] getValidationRules() throws Exception {
        return Store.getValidationRules();
    }

    @RequestMapping(value = {"/validation/rule/{uuid}", "/validation/rule/{uuid}/", "/assertions/query/{uuid}", "/assertions/query/{uuid}/"}, method = RequestMethod.GET)
    public @ResponseBody ValidationRule getValidationRule(@PathVariable(value="uuid") String uuid,HttpServletRequest request) throws Exception {
        String apiKey = request.getParameter("apiKey");
        if(apiKey != null){
            return Store.getValidationRule(apiKey + "|" + uuid);
        } else {
            return Store.getValidationRule(uuid);
        }
    }

    @RequestMapping(value = {"/validation/rules/{uuids}", "/validation/rules/{uuids}/", "/assertions/queries/{uuids}", "/assertions/queries/{uuids}/"}, method = RequestMethod.GET)
    public @ResponseBody ValidationRule[] getValidationRules(@PathVariable(value="uuids") String uuids,HttpServletRequest request) throws Exception {
        String apiKey = request.getParameter("apiKey");
        ValidationRule[] aqs = apiKey != null ? Store.getValidationRules((apiKey+"|"+uuids.replaceAll(",",","+apiKey+"|")).split(",")) : Store.getValidationRules(uuids.split(","));
        //look up the authService userId so that the display value can be used
        for(ValidationRule aq : aqs){
            aq.setUserName(authService.getDisplayNameFor(aq.getUserName()));
        }
        return aqs;
    }

    @RequestMapping(value = {"/assertions/query/{uuid}/apply", "/assertions/query/{uuid}/apply/","/validation/rule/{uuid}/apply", "/validation/rule/{uuid}/apply/"}, method = RequestMethod.GET)
    public @ResponseBody String applyValidationRule(@PathVariable(value="uuid") String uuid,HttpServletRequest request) throws Exception {
        String apiKey = request.getParameter("apiKey");
        if(apiKey != null){
            Store.applyValidationRule(apiKey+"|"+uuid);
        } else {
            Store.applyValidationRule(uuid);
        }
        return "Success";
    }

    @RequestMapping(value={"/validation/rules/rematch"})
    public void reinitialiseRules(HttpServletRequest request, HttpServletResponse response) throws Exception {
        String apiKey = request.getParameter("apiKey");
        ObjectMapper om = new ObjectMapper();
        if(shouldPerformOperation(apiKey, response)){
            int count=0,changed=0,notfound=0;
            for(ValidationRule vr: Store.getValidationRules()){
                //get the ValidationRuleDTO for the raw value
                String rawValue = vr.getRawAssertion();
                ValidationRuleDTO validationRuleDTO = om.readValue(rawValue, ValidationRuleDTO.class);
                if(validationRuleDTO != null && validationRuleDTO.getSpecies()!=null){
                    count++;
                    String guid = speciesLookupService.getGuidForName(validationRuleDTO.getSpecies());
                    if(guid != null){
                        if(!vr.getRawQuery().contains(guid)){
                            logger.warn("GUID has changed OLD:" + vr.getRawQuery() + " new : " + guid);
                            changed++;
                            vr.setWkt(validationRuleDTO.getArea());
                            vr.setRawQuery(getRawQuery(null, guid, vr.getWkt()));
                            //now update it in the store
                            Store.addValidationRule(vr);
                        }
                    }   else{
                        logger.warn("Unable to find species " + validationRuleDTO.getSpecies());
                        notfound++;
                    }
                }
            }
            logger.info("Finished rematching the validation rules to species. Total records checked: " + count + ". Changed: " + changed + " . Not found: " + notfound);
        }
    }

    /**
     * Example expected payload
     *
     * {
     *    "status": "new",
     *    "ignored": false,
     *    "apiKey": "XXXXXXXXXX",
     *    "user": {
     *    "isAdmin": true,
     *    "email": "xxxxxxxx@gmail.com",
     *    "authority": 1000
     *    },
     *    "classification": "invalid",
     *    "area": "MULTIPOLYGON(((137.5 -26,137.5 -25.5,138 -25.5,138 -26,137.5 -26)),((134.5 -29.5,134.5 -29,135 -29,135 -29.5,134.5 -29.5)))",
     *    "lastModified": "2013-01-01T09:05:19",
     *    "id": 5090,
     *    "comment": "",
     *    "species": "Trichoglossus haematodus"
     * }
     *
     * @param request
     * @param response
     * @throws Exception
     */
    @RequestMapping(value={"/assertions/query/add", "/validation/rule/add"}, method = RequestMethod.POST)
    public void addValidationRule(HttpServletRequest request, HttpServletResponse response) throws Exception {

        try {
            String rawValue = org.apache.commons.io.IOUtils.toString(request.getInputStream(), "UTF-8");
            logger.debug("The raw value :" + rawValue);

            try {
                ObjectMapper om = new ObjectMapper();
                ValidationRuleDTO validationRuleDTO = om.readValue(rawValue, ValidationRuleDTO.class);

                //we know that it is a JCU assertion
                if(shouldPerformOperation(validationRuleDTO.getApiKey(), response)){
                    //delete
                    if(validationRuleDTO.getStatus().equals("deleted")){
                        Store.deleteValidationRule(validationRuleDTO.getApiKey() + "|" +validationRuleDTO.getId(), validationRuleDTO.getLastModified());
                    } else {
                        //new or update
                        //does the species exist
                        String guid = speciesLookupService.getGuidForName(validationRuleDTO.getSpecies());
                        if ((guid != null || validationRuleDTO.getQuery() != null) && validationRuleDTO.getId() != null){
                            //check to see if the area is well formed.
                            SpatialSearchRequestParams ssr = new SpatialSearchRequestParams();
                            String query = guid != null ? "lsid:" + guid:validationRuleDTO.getQuery();
                            ssr.setQ(query);
                            ssr.setWkt(validationRuleDTO.getArea());
                            ssr.setFacet(false);
                            try {
                                SolrDocumentList list = searchDAO.findByFulltext(ssr);
                                Long recordCount = list.getNumFound();
                                logger.debug("Validation rule should apply to records: " + recordCount);
                                //now create the query assertion
                                ValidationRule validationRule = new ValidationRule();
                                //NQ: need the id to be populated to construct the correct validation rowkey to allow for updates
                                validationRule.setId(validationRuleDTO.getId().toString());
                                //copy form DTO -> model object for storage
                                validationRule.setApiKey(validationRuleDTO.apiKey);
                                validationRule.setRawAssertion(rawValue);
                                validationRule.setWkt(validationRuleDTO.area);
                                validationRule.setComment(validationRuleDTO.getComment());

                                //auth details
                                String userId = authService.getMapOfEmailToId().get(validationRuleDTO.user.getEmail());
                                validationRule.setUserId(userId);
                                validationRule.setUserEmail(validationRuleDTO.user.getEmail());
                                validationRule.setAuthority(validationRuleDTO.user.getAuthority().toString());

                                validationRule.setRawQuery(getRawQuery(validationRuleDTO.getQuery(), guid, validationRuleDTO.getArea()));
                                if(validationRuleDTO.getStatus().equals("new")){
                                    validationRule.setCreatedDate(validationRuleDTO.getLastModified());
                                }

                                Store.addValidationRule(validationRule);
                            } catch(Exception e) {
                                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unable to construct a valid validation rule from the provided information. " + validationRuleDTO.getId());
                                logger.error("Error constructing query or adding to datastore", e);
                            }
                        } else {
                            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unable to locate species " + validationRuleDTO.getSpecies() + " for validation rule " + validationRuleDTO.getId() );
                        }
                    }
                }
            } catch (Exception e) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage() );
                logger.error("Unable to resolve message to known type", e);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(),e);
            response.sendError(HttpURLConnection.HTTP_BAD_REQUEST);
        }
    }
    private String getRawQuery(String query, String guid, String wkt){
        StringBuilder sb = new StringBuilder("?q=");
        if(guid != null){
            sb.append("lsid:").append(guid);
        } else if(query != null){
            sb.append(query);
        } else {
            sb.append("*:*");
        }
        if(wkt != null){
            sb.append("&wkt=").append(wkt);
        }
        return sb.toString();
    }
    /**
     * Returns a list of query assertions.
     *
     * @param recordUuid
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {"/occurrences/{recordUuid}/validationRules", "/occurrences/{recordUuid}/validationRules/", "/occurrences/{recordUuid}/assertionQueries", "/occurrences/{recordUuid}/assertionQueries/"}, method = RequestMethod.GET)
    public @ResponseBody ValidationRule[] getValidationRules(
            @PathVariable(value="recordUuid") String recordUuid) throws Exception {
        return assertionUtils.getQueryAssertions(recordUuid);
    }

    public void setAssertionUtils(AssertionUtils assertionUtils) {
        this.assertionUtils = assertionUtils;
    }

    public void setAuthService(AuthService authService) {
        this.authService = authService;
    }

    public void setSpeciesLookupService(SpeciesLookupService speciesLookupService) {
        this.speciesLookupService = speciesLookupService;
    }

    public void setSearchDAO(SearchDAO searchDAO) {
        this.searchDAO = searchDAO;
    }
}
