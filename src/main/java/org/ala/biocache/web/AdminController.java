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
package org.ala.biocache.web;

import java.util.List;

import au.org.ala.biocache.Store;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.ala.biocache.service.AuthService;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.apache.log4j.Logger;

/**
 * Provides administration web services for the biocache-service.
 *
 * All services should require an API key.
 *
 * @author "Natasha Carter <Natasha.Carter@csiro.au>"
 */
@Controller
public class AdminController extends AbstractSecureController {

    /** Logger initialisation */
    private final static Logger logger = Logger.getLogger(AdminController.class);
    @Inject
    protected AuthService authService;

    /**
     * Optimises the SOLR index.  Use this API to optimise the index so that the biocache-service
     * can enter read only mode during this process.
     * @param request
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = "/admin/index/optimise", method = RequestMethod.POST)
	public void optimiseIndex(HttpServletRequest request, 
	   HttpServletResponse response) throws Exception {
        String apiKey = request.getParameter("apiKey");
        if(shouldPerformOperation(apiKey, response)){            
            String message = Store.optimiseIndex();
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(message);
        }
    }

    /**
     * Modifies the biocache-store:
     * - reopen the index
     * - enter/exit readonly mode.
     * @param readOnly
     * @param reopenIndex
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/admin/modify*", method = RequestMethod.GET)
    public @ResponseBody List<String> modifyServer(@RequestParam(value = "ro", required = false) Boolean readOnly,
            @RequestParam(value = "reopenIndex", required = false,defaultValue="false") Boolean reopenIndex) throws Exception {
        List<String> actionsPerformed = new java.util.ArrayList<String>(); 
        if(readOnly != null){
            Store.setReadOnly(readOnly);
            actionsPerformed.add("Set readonly = " + readOnly);
        }
        if(reopenIndex){
            Store.reopenIndex();
            actionsPerformed.add("Reopened the index");
        }
        return actionsPerformed;
    }
    /**
     * Reindexes the supplied dr based on modifications since the supplied date.
     * 
     * @param request
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = "/admin/index/reindex", method = RequestMethod.POST)
    public void reindex(HttpServletRequest request, 
            HttpServletResponse response)throws Exception{
        String apiKey = request.getParameter("apiKey");
        if(shouldPerformOperation(apiKey, response)){
            String dataResource = request.getParameter("dataResource");
            String startDate = request.getParameter("startDate");
            logger.info("Reindexing data resource: " + dataResource + " starting at " + startDate);
            Store.reindex(dataResource, startDate);
            response.setStatus(HttpServletResponse.SC_OK);
        }
    }

    /**
     * Returns true when in service is in readonly mode.
     * @return
     */
    @RequestMapping(value="/admin/isReadOnly", method=RequestMethod.GET)
    public @ResponseBody boolean isReadOnly() {
        return Store.isReadOnly();
    }

    /**
     * Returns true when in service is in readonly mode.
     * @return
     */
    @RequestMapping(value="/admin/refreshAuth", method=RequestMethod.GET)
    public @ResponseBody String refreshAuth() {
        authService.reloadCaches();
        return "Done";
    }

    public void setAuthService(AuthService authService) {
        this.authService = authService;
    }
}
