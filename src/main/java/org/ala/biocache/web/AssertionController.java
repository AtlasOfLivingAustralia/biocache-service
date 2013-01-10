package org.ala.biocache.web;

import au.org.ala.biocache.AssertionQuery;
import au.org.ala.biocache.ErrorCode;
import au.org.ala.biocache.FullRecord;
import au.org.ala.biocache.Store;
import au.org.ala.biocache.QualityAssertion;
import au.org.ala.biocache.Versions;

import org.ala.biocache.dao.BieService;
import org.ala.biocache.dao.SearchDAO;
import org.ala.biocache.dto.SpatialSearchRequestParams;
import org.ala.biocache.service.AuthService;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * This controller provides webservices for assertion creation/deletion.
 *
 * TODO Add support for API keys so that only registered applications can
 * use these functions.
 */
@Controller
public class AssertionController extends AbstractSecureController {

    private final static Logger logger = Logger.getLogger(AssertionController.class);

    //TODO Move this so all classes can refer to the same
    protected String collectoryBaseUrl = "http://collections.ala.org.au"; 
    @Inject
    private BieService bieService;
    @Inject
    protected SearchDAO searchDAO;    
    @Inject
    protected AuthService authService;
   
    /**
     * Retrieve an array of the assertion codes in use by the processing system
     *
     * @return an array of codes
     * @throws Exception
     */
    @RequestMapping(value = {"/assertions/codes"}, method = RequestMethod.GET)
	public @ResponseBody ErrorCode[] showCodes() throws Exception {
        return Store.retrieveAssertionCodes();
    }

    @RequestMapping(value = {"/assertions/geospatial/codes"}, method = RequestMethod.GET)
	public @ResponseBody ErrorCode[] showGeospatialCodes() throws Exception {
        return Store.retrieveGeospatialCodes();
    }

    @RequestMapping(value = {"/assertions/taxonomic/codes"}, method = RequestMethod.GET)
	public @ResponseBody ErrorCode[] showTaxonomicCodes() throws Exception {
        return Store.retrieveTaxonomicCodes();
    }

    @RequestMapping(value = {"/assertions/temporal/codes"}, method = RequestMethod.GET)
	public @ResponseBody ErrorCode[] showTemporalCodes() throws Exception {
        return Store.retrieveTemporalCodes();
    }

    @RequestMapping(value = {"/assertions/miscellaneous/codes"}, method = RequestMethod.GET)
	public @ResponseBody ErrorCode[] showMiscellaneousCodes() throws Exception {
        return Store.retrieveMiscellaneousCodes();
    }

    @RequestMapping(value = {"/assertions/user/codes"}, method = RequestMethod.GET)
	public @ResponseBody ErrorCode[] showUserCodes() throws Exception {
        return Store.retrieveUserAssertionCodes();
    }
    
    @RequestMapping(value = {"/assertions/query/{uuid}"}, method = RequestMethod.GET)
    public @ResponseBody AssertionQuery getQueryAssertion(@PathVariable(value="uuid") String uuid,HttpServletRequest request) throws Exception {
        String apiKey = request.getParameter("apiKey");
        if(apiKey != null)
            return Store.getAssertionQuery(apiKey + "|" + uuid);
        else
            return Store.getAssertionQuery(uuid);
    }
    @RequestMapping(value = {"/assertions/queries/{uuids}"}, method = RequestMethod.GET)
    public @ResponseBody AssertionQuery[] getQueryAssertions(@PathVariable(value="uuids") String uuids,HttpServletRequest request) throws Exception {
        String apiKey = request.getParameter("apiKey");
        AssertionQuery[] aqs = apiKey != null ? Store.getAssertionQueries((apiKey+"|"+uuids.replaceAll(",",","+apiKey+"|")).split(",")) : Store.getAssertionQueries(uuids.split(","));
        //look up the authService userId so that the display value can be used
        for(AssertionQuery aq : aqs){
            aq.setUserName(authService.getDisplayNameFor(aq.getUserName()));
        }
        return aqs;
    }
    @RequestMapping(value = {"/assertions/query/{uuid}/apply"}, method = RequestMethod.GET)
    public @ResponseBody String applyAssertion(@PathVariable(value="uuid") String uuid,HttpServletRequest request) throws Exception {
        String apiKey = request.getParameter("apiKey");
        if(apiKey != null)
            Store.applyAssertionQuery(apiKey+"|"+uuid);
        else
            Store.applyAssertionQuery(uuid);
        return "Success";
    }
    
    @RequestMapping(value={"/assertions/query/add"}, method = RequestMethod.POST)  
    public void addAssertionQuery(HttpServletRequest request,
        HttpServletResponse response) throws Exception {
      ObjectMapper om = new ObjectMapper();
      
      try{
          String rawValue = org.apache.commons.io.IOUtils.toString(request.getInputStream(), "UTF-8");
          //java.util.Map<String, Object> suppliedDetails = om.readValue(rawValue,new org.codehaus.jackson.type.TypeReference<java.util.Map<String, Object>>() {});
          logger.debug("The raw value :" + rawValue);
          
          try{
              au.org.ala.biocache.JCUAssertion jcuAssertion =om.readValue(rawValue, au.org.ala.biocache.JCUAssertion.class);              
              
              //we know that it is a JCU assertion
              if(shouldPerformOperation(jcuAssertion.getApiKey(), response)){
                  //delete
                  if(jcuAssertion.getStatus().equals("deleted")){
                      Store.deleteAssertionQuery(jcuAssertion.getApiKey() + "|" +jcuAssertion.getId(), jcuAssertion.getLastModified());                     
                  }
                  else{
                      //new or update
                      //does the species exist
                      String guid =bieService.getGuidForName(jcuAssertion.getSpecies());
                      if(guid != null){
                          //check to see if the area is well formed.
                          SpatialSearchRequestParams ssr = new SpatialSearchRequestParams();
                          ssr.setQ("lsid:" +guid);
                          ssr.setWkt(jcuAssertion.getArea());
                          ssr.setFacet(false);
                          try{
                              searchDAO.findByFulltext(ssr);
                              //now create the query assertion
                              au.org.ala.biocache.AssertionQuery aq = new au.org.ala.biocache.AssertionQuery(jcuAssertion);
                              aq.setRawAssertion(rawValue);
                              aq.setRawQuery("?q="+ssr.getQ()+"&wkt="+ssr.getWkt());
                              if(jcuAssertion.getStatus().equals("new"))
                                  aq.setCreatedDate(jcuAssertion.getLastModified());                                                            
                              //TODO create a "permanent" query cache so that qids can be used
                              Store.addAssertionQuery(aq);
                          }
                          catch(Exception e){
                              response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unable to construct a valid assertion query from the provided information. " + jcuAssertion.getId());
                              logger.error("Error constructing query or adding to datastore", e);
                          }
                      }
                      else{
                          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unable to locate species " + jcuAssertion.getSpecies() + " for assetion " + jcuAssertion.getId() );
                      }
                  }
              }
          }
          catch(Exception e){
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage() );
            logger.error("Unable to resolve message to known type", e);
          }          
      }
      catch(Exception e){
        logger.error(e.getMessage(),e);
        response.sendError(HttpURLConnection.HTTP_BAD_REQUEST);
      }
    }
    /**
     * This version of the method can handle the situation where we use rowKeys as Uuids. Thus
     * URL style rowKeys can be correctly supported.
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
            HttpServletResponse response) throws Exception{
            
        addAssertion(recordUuid, request,response);
    }

    /**
     * add an assertion
     */
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions/add"}, method = RequestMethod.POST)
	public void addAssertion(
       @PathVariable(value="recordUuid") String recordUuid,
        HttpServletRequest request,
        HttpServletResponse response) throws Exception {

        String code = request.getParameter("code");
        String comment = request.getParameter("comment");
        String userId = request.getParameter("userId");
        String userDisplayName = request.getParameter("userDisplayName");
        String apiKey = request.getParameter("apiKey");

        if(shouldPerformOperation(apiKey, response)){
            try {
                logger.debug("Adding assertion to:"+recordUuid+", code:"+code+", comment:"+comment
                        + ", userId:" +userId + ", userDisplayName:" +userDisplayName);
    
                QualityAssertion qa = au.org.ala.biocache.QualityAssertion.apply(Integer.parseInt(code));
                qa.setComment(comment);
                qa.setUserId(userId);
                qa.setUserDisplayName(userDisplayName);
    
                Store.addUserAssertion(recordUuid, qa);

                if(qa.getUuid() != null) {
                    //send this assertion addition event to the notification service
                    postNotificationEvent("create", recordUuid, qa.getUuid());
                }

                String server = request.getSession().getServletContext().getInitParameter("serverName");
                response.setHeader("Location", server + "/occurrences/" + recordUuid + "/assertions/" + qa.getUuid());
                response.setStatus(HttpServletResponse.SC_CREATED);
            } catch(Exception e){
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
            @RequestParam(value="assertionUuid", required=true) String assertionUuid,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {
        deleteAssertion(recordUuid, assertionUuid, request, response);
    }

    /**
     * Remove an assertion
     */
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions/delete"}, method = RequestMethod.POST)
	public void deleteAssertion(
        @PathVariable(value="recordUuid") String recordUuid,
        @RequestParam(value="assertionUuid", required=true) String assertionUuid,
        HttpServletRequest request,
        HttpServletResponse response) throws Exception {
        String apiKey = request.getParameter("apiKey");
        
        if(shouldPerformOperation(apiKey, response)){
            Store.deleteUserAssertion(recordUuid, assertionUuid);
            postNotificationEvent("delete", recordUuid, assertionUuid);
            response.setStatus(HttpServletResponse.SC_OK);
        }
    }

    /**
     * Generic method to post a record assertion notification.
     * @param type
     * @param recordUuid
     * @param id
     */
    private void postNotificationEvent(String type, String recordUuid, String id) {
        //get the processed record so that we can get the collection_uid
        FullRecord processed = Store.getByUuid(recordUuid, Versions.PROCESSED());
        if(processed == null)
        	processed = Store.getByRowKey(recordUuid, Versions.PROCESSED());
        
        String uid = processed==null?null:processed.getAttribution().getCollectionUid();
        
        if (uid != null) {
            final String uri = collectoryBaseUrl + "/ws/notify";
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

            }
        }
    }
    @RequestMapping(value = {"/occurrences/assertions"}, method = RequestMethod.GET)
    public @ResponseBody Object getAssertionWithParams(
            @RequestParam(value="recordUuid", required=true) String recordUuid,
            @RequestParam(value="assertionUuid",required=false) String assertionUuid,
            HttpServletResponse response) throws Exception{
        if(assertionUuid != null)
            return getAssertion(recordUuid, assertionUuid, response);
        else
            return getAssertions(recordUuid);
    }

    /**
     * Get single assertion
     */
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions/{assertionUuid}"}, method = RequestMethod.GET)
	public @ResponseBody QualityAssertion getAssertion(
        @PathVariable(value="recordUuid") String recordUuid,
        @PathVariable(value="assertionUuid") String assertionUuid,
        HttpServletResponse response) throws Exception {
        QualityAssertion qa = Store.getUserAssertion(recordUuid, assertionUuid);
        if(qa!=null){
            return qa;
        } else {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }
    }

    /**
     * Get systemAssertions
     */
    @RequestMapping(value = {"/occurrences/{recordUuid}/assertions/"}, method = RequestMethod.GET)
	public @ResponseBody List<QualityAssertion> getAssertions(
        @PathVariable(value="recordUuid") String recordUuid) throws Exception {
        return Store.getUserAssertions(recordUuid);
    }
}
