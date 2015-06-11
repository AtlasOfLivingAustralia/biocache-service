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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Controllers that need to perform security checks should extend this class and call shouldPerformOperation
 */
public class AbstractSecureController {

    private final static Logger logger = LoggerFactory.getLogger(AbstractSecureController.class);

    @Value("${api.check.url:https://auth.ala.org.au/apikey/ws/check?apikey=}")
    protected String apiCheckUrl;

    @Value("${api.check.enabled:true}")
    protected Boolean apiKeyCheckedEnabled = true;

    /** Local cache of keys */
    private static Set<String> apiKeys = new HashSet<String>();
    
    public AbstractSecureController(){}

    /**
     * Check that the request has the necessary permissions to perform this request.
     *
     * @param request
     * @param response
     * @return
     * @throws Exception
     */
    public boolean shouldPerformOperation(HttpServletRequest request, HttpServletResponse response) throws Exception {
        String apiKey = request.getParameter("apiKey");
        return shouldPerformOperation(apiKey, response, true);
    }

    /**
     * Check the validity of the supplied key.
     *
     * @param response
     * @return
     * @throws Exception
     */
    public boolean shouldPerformOperation(String apiKey, HttpServletResponse response) throws Exception{
        return shouldPerformOperation(apiKey, response, true);
    }
    
    /**
     * Use a webservice to validate a key
     * 
     * @param keyToTest
     * @return
     */
    public boolean isValidKey(String keyToTest){

        if(!apiKeyCheckedEnabled){
            return true;
        }

        if(StringUtils.isBlank(keyToTest)){
            return false;
        }

    	if(!apiKeys.contains(keyToTest)){
    		//check via a web service
    		try {
    			logger.debug("Checking api key: " + keyToTest);
	    		String url = apiCheckUrl + keyToTest;
	    		ObjectMapper om = new ObjectMapper();
	    		Map<String,Object> response = om.readValue(new URL(url), Map.class);
	    		logger.debug("Checking api key: " + keyToTest + ", valid: " + response.get("valid"));
	    		boolean isValid = (Boolean) response.get("valid");
	    		if(isValid){
	    			apiKeys.add(keyToTest);
	    		}
	    		return isValid; 
    		} catch (Exception e){
    			logger.error(e.getMessage(), e);
    		}
    	} else {
    		return true;
    	}
    	return false;
    }

	/**
     * Returns true when the operation should be performed.
     *
     * @param apiKey
     * @param response
     * @return
     * @throws Exception
     */
    public boolean shouldPerformOperation(String apiKey,HttpServletResponse response, boolean checkReadOnly)throws Exception{
        if(checkReadOnly && Store.isReadOnly()){
            response.sendError(HttpServletResponse.SC_CONFLICT, "Server is in read only mode.  Try again later.");
        } else if(!isValidKey(apiKey)){
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "An invalid API Key was provided.");
        }
        return !response.isCommitted();
    }
}