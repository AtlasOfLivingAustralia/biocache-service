/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
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

import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.BreakdownRequestParams;
import au.org.ala.biocache.dto.OccurrenceIndex;
import au.org.ala.biocache.dto.TaxaRankCountDTO;
import io.swagger.annotations.Api;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.annotation.JsonInclude;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

/**
 * A simple controller for providing breakdowns on top of the biocache.
 *
 * @author Dave Martin (David.Martin@csiro.au)
 * @author Natasha Carter (Natasha.Carter@csiro.au)
 */
@Controller
@Api(value = "Breakdowns", description = "Data breakdowns", hidden = true, tags = { "Breakdowns" })
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BreakdownController {

    private final static Logger logger = Logger.getLogger(BreakdownController.class);

    @Inject
    protected SearchDAO searchDAO;

    /**
     * Performs a breakdown based on a collection
     *
     * @param requestParams
     * @param uid
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/breakdown/collections/{uid}*", method = RequestMethod.GET)
    public @ResponseBody
    TaxaRankCountDTO breakdownByCollection(BreakdownRequestParams requestParams,
                                           @PathVariable("uid") String uid, HttpServletResponse response) throws Exception {
        return performBreakdown(OccurrenceIndex.COLLECTION_UID, uid, requestParams, response);
    }

    /**
     * Performs a breakdown based on an institution
     *
     * @param requestParams
     * @param uid
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/breakdown/institutions/{uid}*", method = RequestMethod.GET)
    public @ResponseBody
    TaxaRankCountDTO breakdownByInstitution(BreakdownRequestParams requestParams,
                                            @PathVariable("uid") String uid, HttpServletResponse response) throws Exception {
        return performBreakdown(OccurrenceIndex.INSTITUTION_UID, uid, requestParams, response);
    }

    /**
     * Performs a breakdown based on a data resource
     *
     * @param requestParams
     * @param uid
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/breakdown/dataResources/{uid}*", method = RequestMethod.GET)
    public @ResponseBody
    TaxaRankCountDTO breakdownByDataResource(BreakdownRequestParams requestParams,
                                             @PathVariable("uid") String uid, HttpServletResponse response) throws Exception {
        return performBreakdown(OccurrenceIndex.DATA_RESOURCE_UID, uid, requestParams, response);
    }

    /**
     * Performs a breakdown based on a data provider
     *
     * @param requestParams
     * @param uid
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/breakdown/dataProviders/{uid}*", method = RequestMethod.GET)
    public @ResponseBody
    TaxaRankCountDTO breakdownByDataProvider(BreakdownRequestParams requestParams,
                                             @PathVariable("uid") String uid, HttpServletResponse response) throws Exception {
        return performBreakdown(OccurrenceIndex.DATA_PROVIDER_UID, uid, requestParams, response);
    }

    /**
     * Performs a breakdown based on a data hub
     *
     * @param requestParams
     * @param uid
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/breakdown/dataHubs/{uid}*", method = RequestMethod.GET)
    public @ResponseBody
    TaxaRankCountDTO breakdownByDataHub(BreakdownRequestParams requestParams,
                                        @PathVariable("uid") String uid, HttpServletResponse response) throws Exception {
        return performBreakdown(OccurrenceIndex.DATA_HUB_UID, uid, requestParams, response);
    }
	
	@RequestMapping(value= "/breakdown*", method = RequestMethod.GET)
	public @ResponseBody TaxaRankCountDTO breakdownByQuery(BreakdownRequestParams  breakdownParams,HttpServletResponse response) throws Exception {
        logger.debug(breakdownParams);
	    if(StringUtils.isNotEmpty(breakdownParams.getQ())){
	        if(breakdownParams.getMax() != null || StringUtils.isNotEmpty(breakdownParams.getRank()) || StringUtils.isNotEmpty(breakdownParams.getLevel()))
	            return searchDAO.calculateBreakdown(breakdownParams);
	        else
	            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No context provided for breakdown.  Please supply either max, rank or level as a minimum");
	    }
	    else{
	        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No query provided for breakdown");
	    }
	    return null;
	}
	/**
	 * performs the actual breakdown.  The type of breakdown will depend on which arguments were supplied to the webservice
	 * @param source
	 * @param uid
	 * @param requestParams
	 * @return
	 * @throws Exception
	 */
	private TaxaRankCountDTO performBreakdown(String source, String uid, BreakdownRequestParams requestParams, HttpServletResponse response) throws Exception{
	    StringBuilder sb = new StringBuilder("(");
	    //support CSV list of uids
	    for(String u:uid.split(",")){
	        if(sb.length()>1)
	            sb.append(" OR ");
	        sb.append(source).append(":").append(u);
	    }
	    sb.append(")");
	    
	    requestParams.setQ(sb.toString());
	    return breakdownByQuery(requestParams, response);
	}

    /**
     * Performs a breakdown without limiting the collection or institution
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {"/breakdown/institutions*","/breakdown/collections*", "/breakdown/data-resources*","/breakdowns/data-providers*","/breakdowns/data-hubs*"}, method = RequestMethod.GET)
    public @ResponseBody TaxaRankCountDTO limitBreakdown(BreakdownRequestParams requestParams, HttpServletResponse response) throws Exception {
        return performBreakdown("*", "*", requestParams, response);                
    }
}
