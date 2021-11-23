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
import au.org.ala.biocache.dto.BreakdownRequestDTO;
import au.org.ala.biocache.dto.BreakdownRequestParams;
import au.org.ala.biocache.dto.OccurrenceIndex;
import au.org.ala.biocache.dto.TaxaRankCountDTO;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import io.swagger.v3.oas.annotations.Operation;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springdoc.api.annotations.ParameterObject;
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
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BreakdownController {

    @Inject
    protected SearchDAO searchDAO;

    /**
     * Performs a breakdown based on a collection
     *
     * @param uid
     * @return
     * @throws Exception
     */
    @Operation(summary = "Taxonomic breakdown based on a collection", tags = {"Taxonomy"})
    @RequestMapping(value = "/breakdown/collections/{uid}*", method = RequestMethod.GET)
    @ApiParam(value = "uid", required = true)
    public @ResponseBody
    TaxaRankCountDTO breakdownByCollection(@ParameterObject BreakdownRequestParams breakdownParams,
                                           @PathVariable("uid") String uid,
                                           HttpServletResponse response) throws Exception {
        return performBreakdown(OccurrenceIndex.COLLECTION_UID, uid, breakdownParams, response);
    }

    /**
     * Performs a breakdown based on an institution
     *
     * @param uid
     * @return
     * @throws Exception
     */
    @Operation(summary = "Taxonomic breakdown based on a institution", tags = {"Taxonomy"})
    @RequestMapping(value = "/breakdown/institutions/{uid}*", method = RequestMethod.GET)
    @ApiParam(value = "uid", required = true)
    public @ResponseBody
    TaxaRankCountDTO breakdownByInstitution(@ParameterObject BreakdownRequestParams breakdownParams,
                                            @PathVariable("uid") String uid,
                                            HttpServletResponse response) throws Exception {
        return performBreakdown(OccurrenceIndex.INSTITUTION_UID, uid, breakdownParams, response);
    }

    /**
     * Performs a breakdown based on a data resource
     *
     * @param uid
     * @return
     * @throws Exception
     */
    @Operation(summary = "Taxonomic breakdown based on a data resource", tags = {"Taxonomy"})
    @RequestMapping(value = "/breakdown/dataResources/{uid}*", method = RequestMethod.GET)
    @ApiParam(value = "uid", required = true)
    public @ResponseBody
    TaxaRankCountDTO breakdownByDataResource(@ParameterObject BreakdownRequestParams breakdownParams,
                                             @PathVariable("uid") String uid,
                                             HttpServletResponse response) throws Exception {
        return performBreakdown(OccurrenceIndex.DATA_RESOURCE_UID, uid, breakdownParams, response);
    }

    /**
     * Performs a breakdown based on a data provider
     *
     * @param uid
     * @return
     * @throws Exception
     */
    @Operation(summary = "Taxonomic breakdown based on a data provider", tags = {"Taxonomy"})
    @RequestMapping(value = "/breakdown/dataProviders/{uid}*", method = RequestMethod.GET)
    @ApiParam(value = "uid", required = true)
    public @ResponseBody
    TaxaRankCountDTO breakdownByDataProvider(@ParameterObject BreakdownRequestParams breakdownParams,
                                             @PathVariable("uid") String uid,
                                             HttpServletResponse response) throws Exception {
        return performBreakdown(OccurrenceIndex.DATA_PROVIDER_UID, uid, breakdownParams, response);
    }

    /**
     * Performs a breakdown based on a data hub
     *
     * @param requestParams
     * @param uid
     * @return
     * @throws Exception
     */
    @Operation(summary = "Taxonomic breakdown based on a data hub", tags = {"Taxonomy"})
    @RequestMapping(value = "/breakdown/dataHubs/{uid}*", method = RequestMethod.GET)
    @ApiParam(value = "uid", required = true)
    public @ResponseBody
    TaxaRankCountDTO breakdownByDataHub(BreakdownRequestParams requestParams,
                                        @PathVariable("uid") String uid,
                                        HttpServletResponse response) throws Exception {
        return performBreakdown(OccurrenceIndex.DATA_HUB_UID, uid, requestParams, response);
    }

    @Operation(summary = "A breakdown based on taxon rank", tags = {"Taxonomy"})
	@RequestMapping(value= "/breakdown*", method = RequestMethod.GET)
	public @ResponseBody TaxaRankCountDTO breakdownByQuery(@ParameterObject BreakdownRequestParams breakdownParams, HttpServletResponse response) throws Exception {

	    if (StringUtils.isNotEmpty(breakdownParams.getQ())){
	        if (breakdownParams.getMax() != null || StringUtils.isNotEmpty(breakdownParams.getRank()) || StringUtils.isNotEmpty(breakdownParams.getLevel()))
	            return searchDAO.calculateBreakdown(BreakdownRequestDTO.create(breakdownParams));
	        else
	            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No context provided for breakdown.  Please supply either max, rank or level as a minimum");
	    } else {
	        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No query provided for breakdown");
	    }
	    return null;
	}
	/**
	 * Performs the actual breakdown.  The type of breakdown will depend on which arguments were supplied to the webservice
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
    @Operation(summary = "A breakdown without limiting the collection or institution", tags = {"Taxonomy"})
    @RequestMapping(value = {"/breakdown/institutions*","/breakdown/collections*", "/breakdown/data-resources*","/breakdowns/data-providers*","/breakdowns/data-hubs*"}, method = RequestMethod.GET)
    public @ResponseBody TaxaRankCountDTO limitBreakdown(@ParameterObject BreakdownRequestParams requestParams, HttpServletResponse response) throws Exception {
        return performBreakdown("*", "*", requestParams, response);                
    }
}
