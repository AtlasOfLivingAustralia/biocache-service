package org.ala.biocache.web;

import java.util.Collections;
import java.util.HashSet;
import java.util.ResourceBundle;
import java.util.Set;
import au.org.ala.biocache.Store;
import javax.servlet.http.HttpServletResponse;

/**
 * 
 * Controllers that need to perform security checks should extend this class and call shouldPerformOperation
 *
 */
public class AbstractSecureController {

    protected Set<String> apiKeys;
    
    public AbstractSecureController(){
      //Initialise the set of API keys that will allow edits to the biocache store
        ResourceBundle rb = ResourceBundle.getBundle("biocache"); 
        apiKeys = new HashSet<String>();
        try{
                      
            String[] keys= rb.getString("apiKeys").split(",");
            Collections.addAll(apiKeys, keys);
        }
        catch(Exception e){
            
        }
    }
    
    public boolean shouldPerformOperation(String apiKey,HttpServletResponse response)throws Exception{
        return shouldPerformOperation(apiKey, response, true);
    }
    
    /**
     * Returns true when the operation should be performed.
     * @param apiKey
     * @param response
     * @return
     * @throws Exception
     */
    public boolean shouldPerformOperation(String apiKey,HttpServletResponse response, boolean checkReadOnly)throws Exception{
        if(checkReadOnly && Store.isReadOnly()){
            response.sendError(HttpServletResponse.SC_CONFLICT, "Server is in read only mode.  Try again later.");
        }
        else if(apiKey == null || !apiKeys.contains(apiKey)){         
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "An invalid API Key was provided.");
        }
        return !response.isCommitted();
    }
    
}
