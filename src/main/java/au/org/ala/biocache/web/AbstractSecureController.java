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
import au.org.ala.biocache.service.AuthService;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Controllers that need to perform security checks should extend this class and call shouldPerformOperation.
 * 
 * NOTE: Even though this has "Abstract" in the class name for historical reasons, it is a non-abstract class.
 */
public class AbstractSecureController {

    private final static Logger logger = LoggerFactory.getLogger(AbstractSecureController.class);

    @Value("${api.check.url:https://auth.ala.org.au/apikey/ws/check?apikey=}")
    protected String apiCheckUrl;

    @Value("${api.check.enabled:true}")
    protected Boolean apiKeyCheckedEnabled = true;

    /** 
     * Temporary local cache of keys 
     **/
    private final LoadingCache<String, Boolean> apiKeyCache;

    @Inject
    protected WebUtils webUtils;
    
    public AbstractSecureController(){
    	apiKeyCache = Caffeine.newBuilder()
    			.maximumSize(1000)
    			.expireAfterWrite(5, TimeUnit.MINUTES)
    			.refreshAfterWrite(5, TimeUnit.MINUTES)
    			.build(key -> checkKey(key, apiCheckUrl, webUtils));
    }

    /**
     * Check the validity of the supplied key, returning false if the store is in read only mode.
     *
     * @param request The request to find the apiKey parameter from
     * @param response The response to check for {@link HttpServletResponse#isCommitted()} and to send errors on if the operation should not be committed
     * @return True if the store is not in read-only mode, the API key is valid, and the response has not already been committed, and false otherwise
     * @throws Exception If the store is in read-only mode, or the API key is invalid.
     */
    public boolean shouldPerformOperation(HttpServletRequest request, HttpServletResponse response) throws Exception {
        String apiKey = request.getParameter("apiKey");
        return shouldPerformOperation(apiKey, response, true);
    }

    /**
     * Check the validity of the supplied key, returning false if the store is in read only mode.
     *
     * @param apiKey The API key to check
     * @param response The response to check for {@link HttpServletResponse#isCommitted()} and to send errors on if the operation should not be committed
     * @return True if the store is not in read-only mode, the API key is valid, and the response has not already been committed, and false otherwise
     * @throws Exception If the store is in read-only mode, or the API key is invalid.
     */
    public boolean shouldPerformOperation(String apiKey, HttpServletResponse response) throws Exception {
        return shouldPerformOperation(apiKey, response, true);
    }
    
    /**
     * Use a webservice to validate a key
     * 
     * @param keyToTest
     * @return True if API key checking is disabled, or the API key is valid, and false otherwise.
     */
    public boolean isValidKey(String keyToTest){
        if(!apiKeyCheckedEnabled){
        	logger.debug("API key checking is disabled");
            return true;
        }

        Boolean cacheResult = apiKeyCache.get(keyToTest);
        if(cacheResult != null) {
        	logger.debug("API key check result: apikey={} checkResult={}", keyToTest, cacheResult);
    		return cacheResult;
    	}
        
    	logger.debug("API key not found: apikey={}", keyToTest);
        return false;
    }

    private static boolean checkKey(String keyToTest, String serviceUrl, WebUtils webUtils) {

        if(StringUtils.isBlank(keyToTest)){
            return false;
        }

        String trimmedKey = keyToTest.trim();
        
		//check via a web service
		try {
			logger.debug("Checking api key: {}", trimmedKey);
    		String url = serviceUrl + trimmedKey;
    		Map<String, Object> response = webUtils.getJson(url);
    		boolean isValid = (Boolean) response.get("valid");
    		logger.debug("Checking api key: {}, valid: {}", trimmedKey, isValid);
    		return isValid;
		} catch (Exception e){
			logger.error("Error checking api key", e);
	    	return false;
		}
    }
    
	/**
     * Returns true when the operation should be performed.
     *
     * @param apiKey The API key to check for validity, after appending it to the api.check.url property.
     * @param response The response to either send an error on, or return false if the {@link HttpServletResponse#isCommitted()} returns true.
     * @param checkReadOnly True to check {@link Store#isReadOnly()}
     * @return True if the operation is able to be performed
     * @throws Exception
     */
    public boolean shouldPerformOperation(String apiKey,HttpServletResponse response, boolean checkReadOnly) throws Exception {
        if(checkReadOnly && Store.isReadOnly()){
            response.sendError(HttpServletResponse.SC_CONFLICT, "Server is in read only mode.  Try again later.");
            return false;
        } else if(!isValidKey(apiKey)){
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "An invalid API Key was provided.");
            return false;
        } else if(response.isCommitted()) {
        	return false;
        }
        
        return true;
    }
}