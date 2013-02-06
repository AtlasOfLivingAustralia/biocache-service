/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
 *  All Rights Reserved.
 *
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the Lirecense. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/
package org.ala.biocache.web;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.ZipOutputStream;

import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import au.org.ala.biocache.*;
import org.ala.biocache.dao.SearchDAO;
import org.ala.biocache.dto.*;
import org.ala.biocache.dto.store.OccurrenceDTO;
import org.ala.biocache.service.AuthService;
import org.ala.biocache.util.MimeType;
import org.ala.biocache.util.ParamsCache;
import org.ala.biocache.util.ParamsCacheSizeException;
import org.ala.biocache.util.SearchUtils;
import org.ala.client.appender.RestLevel;
import org.ala.client.model.LogEventType;
import org.ala.client.model.LogEventVO;
import org.ala.client.util.RestfulClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;


import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestOperations;

/**
 * Occurrences controller for the BIE biocache site.
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 * @author "Natasha Carter <Natasha.Carter@csiro.au>" 
 * 
 * History:
 * 1 Sept 10 (MOK011): added restfulClient to retrieve citation information into citation.txt
 * [private void getCitations(Set<String> keys, OutputStream out) throws HttpException, IOException]
 * 
 * 14 Dept 10 (MOK011): modified getCitations function to get csv format data from Citation Service.
 */
@Controller
public class OccurrenceController extends AbstractSecureController {

	/** Logger initialisation */
	private final static Logger logger = Logger.getLogger(OccurrenceController.class);

	/** Fulltext search DAO */
	@Inject
	protected SearchDAO searchDAO;
	/** Data Resource DAO */
	@Inject
	protected SearchUtils searchUtils;
	@Inject
	protected RestfulClient restfulClient;
	  @Inject
    private RestOperations restTemplate;
	  
	  @Inject
    protected AuthService authService;
	
	/** Name of view for site home page */
	private String HOME = "homePage";
	/** Name of view for list of taxa */
	private final String LIST = "occurrences/list";
	/** Name of view for a single taxon */
	private final String SHOW = "occurrences/show";

	protected String hostUrl = "http://localhost:8080/biocache-service";
	protected String bieBaseUrl = "http://bie.ala.org.au/";
	protected String collectoryBaseUrl = "http://collections.ala.org.au";
	protected String citationServiceUrl = collectoryBaseUrl + "/ws/citations";
	protected String summaryServiceUrl  = collectoryBaseUrl + "/ws/summary";
	
	/** The response to be returned for the isAustralian test */
	private final String NOT_AUSTRALIAN = "Not Australian";
	private final String AUSTRALIAN_WITH_OCC = "Australian with occurrences";
	private final String AUSTRALIAN_LSID = "Australian based on LSID";
	protected Pattern austLsidPattern = Pattern.compile("urn:lsid:biodiversity.org.au[a-zA-Z0-9\\.:-]*");
	
	private final String dataFieldDescriptionURL="https://docs.google.com/spreadsheet/ccc?key=0AjNtzhUIIHeNdHhtcFVSM09qZ3c3N3ItUnBBc09TbHc";
	
	/**
	 * Custom handler for the welcome view.
	 * <p>
	 * Note that this handler relies on the RequestToViewNameTranslator to
	 * determine the logical view name based on the request URL: "/welcome.do"
	 * -&gt; "welcome".
	 *
	 * @return viewname to render
	 */
	@RequestMapping("/")
	public String homePageHandler() {
		return HOME;
	}

	/**
	 * Returns the default facets that are applied to a search
	 * @return
	 */
	@RequestMapping("/search/facets")
	public @ResponseBody String[] listAllFacets() {
	    String[] facets = new SearchRequestParams().getFacets();
	    return facets;
	}

	/**
     * Returns the default facets grouped by themes that are applied to a search
     * @return
     */
    @RequestMapping("/search/grouped/facets")
    public @ResponseBody List groupFacets() {
        return FacetThemes.allThemes;
    }
	/**
	 * Returns the content of the messages.properties file.
     * Can also return language specific versions, such as
     * messages_fr.properties if requested via qualifier @PathVariable.
     *
	 * @param response
	 * @throws Exception
	 */
	@RequestMapping("/facets/i18n{qualifier:.*}*")
	public void writei18nPropertiesFile(@PathVariable("qualifier") String qualifier,
                                        HttpServletRequest request,
                                        HttpServletResponse response) throws Exception{
        qualifier = (StringUtils.isNotEmpty(qualifier)) ? qualifier : ".properties";
        logger.debug("qualifier = " + qualifier);
        InputStream is = request.getSession().getServletContext().getResourceAsStream("/WEB-INF/messages" + qualifier);
        OutputStream os = response.getOutputStream();

        if (is != null) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1)
            {
                os.write(buffer, 0, bytesRead);
            }
        }
		os.flush();
        os.close();
	}
	
	/**
	 * Returns a list with the details of the index field
	 * @return
	 * @throws Exception
	 */
	@RequestMapping("index/fields")
	public @ResponseBody Set<IndexFieldDTO> getIndexedFields(@RequestParam(value="fl", required=false) String fields) throws Exception{
	    if(fields == null)
	        return searchDAO.getIndexedFields();
	    else
	        return searchDAO.getIndexFieldDetails(fields.split(","));
	}
	/**
	 * Returns a facet list including the number of distinct values for a field
	 * @param requestParams
	 * @return
	 * @throws Exception
	 */
	@RequestMapping("occurrence/facets")
	public @ResponseBody List<FacetResultDTO> getOccurrenceFacetDetails(SpatialSearchRequestParams requestParams) throws Exception{
	    return searchDAO.getFacetCounts(requestParams);
	}
	
	/**
	 * Returns a list of image urls for the supplied taxon guid. 
	 * An empty list is returned when no images are available.
	 * 
	 * @param guid
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value={"/images/taxon/{guid:.+}.json*","/images/taxon/{guid:.+}*"})
	public @ResponseBody List<String> getImages(@PathVariable("guid") String guid) throws Exception {
	    SpatialSearchRequestParams srp = new SpatialSearchRequestParams();
	    srp.setQ("lsid:" + guid);
	    srp.setPageSize(0);
	    srp.setFacets(new String[]{"image_url"});
	    SearchResultDTO results = searchDAO.findByFulltextSpatialQuery(srp,null);
	    if(results.getFacetResults().size()>0){
	        List<FieldResultDTO> fieldResults =results.getFacetResults().iterator().next().getFieldResult();
	        ArrayList<String> images = new ArrayList<String>(fieldResults.size());
	        for(FieldResultDTO fr : fieldResults)
	            images.add(fr.getLabel());
	        return images;
	    }
	    return Collections.EMPTY_LIST;
	}
	
	/**
	 * Checks to see if the supplied GUID represents an Australian species.
	 * @param guid
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value={"/australian/taxon/{guid:.+}.json*","/australian/taxon/{guid:.+}*" })
	public @ResponseBody AustralianDTO isAustralian(@PathVariable("guid") String guid) throws Exception {
	    //check to see if we have any occurrences on Australia  country:Australia or state != empty
	    SpatialSearchRequestParams requestParams = new SpatialSearchRequestParams();
	    requestParams.setPageSize(0);
	    requestParams.setFacets(new String[]{});
	    String query = "lsid:" +guid + " AND " + "(country:Australia OR state:[* TO *]) AND geospatial_kosher:true";
	    requestParams.setQ(query);
	    AustralianDTO adto= new AustralianDTO();
	    adto.setTaxonGuid(guid);
	    SearchResultDTO results = searchDAO.findByFulltextSpatialQuery(requestParams,null);
	    adto.setHasOccurrenceRecords(results.getTotalRecords() > 0);
	    adto.setIsNSL(austLsidPattern.matcher(guid).matches());
	    if(adto.isHasOccurrences()){
	        //check to see if the records have only been provided by citizen science
	        //TODO change this to a confidence setting after it has been included in the index	        
	        requestParams.setQ("lsid:" + guid + " AND (provenance:\"Published dataset\")");
	        results = searchDAO.findByFulltextSpatialQuery(requestParams,null);
	        adto.setHasCSOnly(results.getTotalRecords()==0);
	    }
	    return adto;
	}

	/**
	 * Returns the complete list of Occurrences
	 */
	@RequestMapping(value = {"/occurrences", "/occurrences/collections", "/occurrences/institutions", "/occurrences/dataResources", "/occurrences/dataProviders", "/occurrences/taxa", "/occurrences/dataHubs"}, method = RequestMethod.GET)
	public @ResponseBody SearchResultDTO listOccurrences(Model model) throws Exception {
        SpatialSearchRequestParams srp = new SpatialSearchRequestParams();
        srp.setQ("*:*");
        return occurrenceSearch(srp);
	}

	/**
	 * Occurrence search page uses SOLR JSON to display results
	 * 
	 * @param model
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value = {"/occurrences/taxon/{guid:.+}.json*","/occurrences/taxon/{guid:.+}*","/occurrences/taxa/{guid:.+}*"}, method = RequestMethod.GET)
	public @ResponseBody SearchResultDTO occurrenceSearchByTaxon(
            SpatialSearchRequestParams requestParams,
            @PathVariable("guid") String guid,
            Model model) throws Exception {
        requestParams.setQ("lsid:" + guid);
        SearchUtils.setDefaultParams(requestParams);
        return occurrenceSearch(requestParams);
	}

    /**
     * Obtains a list of the sources for the supplied guid.
     *
     * I don't think that this should be necessary. We should be able to
     * configure the requestParams facets to contain the collectino_uid, institution_uid
     * data_resource_uid and data_provider_uid
     *
     * It also handle's the logging for the BIE.
     * //TODO Work out what to do with this
     * @param request
     * @throws Exception
     */
    @RequestMapping(value = "/occurrences/taxon/source/{guid:.+}.json*", method = RequestMethod.GET)
    public @ResponseBody List<OccurrenceSourceDTO> sourceByTaxon(
            SpatialSearchRequestParams requestParams,
            @PathVariable("guid") String guid,
            HttpServletRequest request
            )
            throws Exception {
        requestParams.setQ("lsid:" + guid) ;       
        Map<String,Integer> sources = searchDAO.getSourcesForQuery(requestParams);
        //now turn them to a list of OccurenceSourceDTO
        return searchUtils.getSourceInformation(sources);        
    }

  /**
     * Occurrence search for a given collection, institution, data_resource or data_provider.
     *
     * @param requestParams The search parameters
     * @param  uid The uid for collection, institution, data_resource or data_provider
     * @param model
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {"/occurrences/collections/{uid}", "/occurrences/institutions/{uid}",
                             "/occurrences/dataResources/{uid}", "/occurrences/dataProviders/{uid}", "/occurrences/dataHubs/{uid}"}, method = RequestMethod.GET)
    public @ResponseBody SearchResultDTO occurrenceSearchForUID(
            SpatialSearchRequestParams requestParams,
            @PathVariable("uid") String uid,
            Model model)
            throws Exception {
        SearchResultDTO searchResult = new SearchResultDTO();
        // no query so exit method
        if (StringUtils.isEmpty(uid)) {
            return searchResult;
        }

        SearchUtils.setDefaultParams(requestParams);
        //update the request params so the search caters for the supplied uid
        searchUtils.updateCollectionSearchString(requestParams, uid);
        logger.debug("solr query: " + requestParams);
        //searchResult = searchDAO.findByFulltextQuery(requestParams);
        //model.addAttribute("searchResult", searchResult);
        return occurrenceSearch(requestParams);
    }

    /**
     * Spatial search for either a taxon name or full text text search
    
     * @param model
     * @deprecated use {@link #occurrenceSearch(SpatialSearchRequestParams)}
     * @return
     * @throws Exception
     */
    @RequestMapping(value =  "/occurrences/searchByArea*", method = RequestMethod.GET)
    @Deprecated
	public @ResponseBody SearchResultDTO occurrenceSearchByArea(
			SpatialSearchRequestParams requestParams,
			Model model)
            throws Exception {
        SearchResultDTO searchResult = new SearchResultDTO();

        if (StringUtils.isEmpty(requestParams.getQ())) {
			return searchResult;
		}

        //searchUtils.updateSpatial(requestParams);
        searchResult = searchDAO.findByFulltextSpatialQuery(requestParams,null);
		model.addAttribute("searchResult", searchResult);

		if(logger.isDebugEnabled()){
			logger.debug("Returning results set with: "+searchResult.getTotalRecords());
		}

		return searchResult;
	}
    
    private SearchResultDTO occurrenceSearch(SpatialSearchRequestParams requestParams)throws Exception{
        return occurrenceSearch(requestParams,null,null,null);
    }

	/**
	 * Occurrence search page uses SOLR JSON to display results
	 *
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value = {"/occurrences/search.json*","/occurrences/search*"}, method = RequestMethod.GET)
	public @ResponseBody SearchResultDTO occurrenceSearch(SpatialSearchRequestParams requestParams,
	        @RequestParam(value="apiKey", required=false) String apiKey,
	        HttpServletRequest request,
	        HttpServletResponse response) throws Exception {
        // handle empty param values, e.g. &sort=&dir=
        SearchUtils.setDefaultParams(requestParams);
        Map<String,String[]> map = request != null?SearchUtils.getExtraParams(request.getParameterMap()):null;
        if(map != null)
            map.remove("apiKey");

        logger.debug("occurrence search params= " + requestParams);
        if(apiKey == null)
            return  searchDAO.findByFulltextSpatialQuery(requestParams,map);        
        else
            return occurrenceSearchSensitive(requestParams,apiKey,request, response);
	}
	
	public @ResponseBody SearchResultDTO occurrenceSearchSensitive(SpatialSearchRequestParams requestParams,
            @RequestParam(value="apiKey", required=true) String apiKey,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {
        // handle empty param values, e.g. &sort=&dir=
	    if(shouldPerformOperation(apiKey, response, false)){
    	    SearchUtils.setDefaultParams(requestParams); 
    	    Map<String,String[]> map = SearchUtils.getExtraParams(request.getParameterMap());
    	    if(map != null)
    	        map.remove("apiKey");
          logger.debug("occurrence search params= " + requestParams);     
          SearchResultDTO searchResult = searchDAO.findByFulltextSpatialQuery(requestParams, true,map);        
          return searchResult;
	    }
	    return null;
    }

	/**
	 * Occurrence search page uses SOLR JSON to display results
	 *
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value = {"/cache/refresh"}, method = RequestMethod.GET)
	public @ResponseBody String refreshCache() throws Exception {
        searchDAO.refreshCaches();
		return null;
	}

	/**
	 * Downloads the complete list of values in the supplied facet 
	 * 
	 * ONLY 1 facet should be included in the params.  
	 * 
	 * @param requestParams
	 * @param response
	 * @throws Exception
	 */
	@RequestMapping(value = "/occurrences/facets/download*", method = RequestMethod.GET)
	public void downloadFacet(
        DownloadRequestParams requestParams,
        @RequestParam(value = "count", required = false, defaultValue="false") boolean includeCount,
        @RequestParam(value="lookup" ,required=false, defaultValue="false") boolean lookupName,
        HttpServletResponse response) throws Exception {
        if(requestParams.getFacets().length >0){
            String filename = requestParams.getFile() != null ? requestParams.getFile():requestParams.getFacets()[0];
            response.setHeader("Cache-Control", "must-revalidate");
            response.setHeader("Pragma", "must-revalidate");
            response.setHeader("Content-Disposition", "attachment;filename=" + filename +".csv");
            response.setContentType("text/csv");
            searchDAO.writeFacetToStream(requestParams,includeCount, lookupName, response.getOutputStream());
        }
	}

    /**
     * Webservice to support bulk downloads for a long list of queries for a single field.
     * NOTE: triggered on "Download Records" button
     *
     * @param response
     * @param request
     * @param separator
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/occurrences/batchSearch", method = RequestMethod.POST, params="action=Download")
    public void batchDownload(
            HttpServletResponse response,
            HttpServletRequest request,
            @RequestParam(value="queries", required = true, defaultValue = "") String queries,
            @RequestParam(value="field", required = true, defaultValue = "") String field,
            @RequestParam(value="separator", defaultValue = "\n") String separator,
            @RequestParam(value="title", required=false) String title) throws Exception {

        logger.info("/occurrences/batchSearch with action=Download Records");
        Long qid =  getQidForBatchSearch(queries, field, separator,title);

        if (qid != null) {
            String webservicesRoot = request.getSession().getServletContext().getInitParameter("webservicesRoot");
            response.sendRedirect(webservicesRoot + "/occurrences/download?q=qid:"+qid);
        } else {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST);
        }
    }

    /**
     * Given a list of queries for a single field, return an AJAX response with the qid (cached query id)
     * NOTE: triggered on "Search" button
     *
     * @param response
     * @param queries
     * @param separator
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/occurrences/batchSearch", method = RequestMethod.POST, params="action=Search")
    public void batchSearch(
            HttpServletResponse response,
            @RequestParam(value="redirectBase", required = true, defaultValue = "") String redirectBase,
            @RequestParam(value="queries", required = true, defaultValue = "") String queries,
            @RequestParam(value="field", required = true, defaultValue = "") String field,
            @RequestParam(value="separator", defaultValue = "\n") String separator,
            @RequestParam(value="title", required=false) String title) throws Exception {

        logger.info("/occurrences/batchSearch with action=Search");
        Long qid =  getQidForBatchSearch(queries, field, separator,title);

        if (qid != null && StringUtils.isNotBlank(redirectBase)) {
            response.sendRedirect(redirectBase + "?q=qid:"+qid);
        } else {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "");
        }
    }

    /**
     * Common method for getting a QID for a batch field query
     *
     * @param listOfNames
     * @param separator
     * @return
     * @throws IOException
     * @throws ParamsCacheSizeException
     */
    private Long getQidForBatchSearch(String listOfNames, String field, String separator, String title) throws IOException, ParamsCacheSizeException {
        String[] rawParts = listOfNames.split(separator);
        List<String> parts = new ArrayList<String>();

        for (String part: rawParts) {
            String normalised = StringUtils.trimToNull(part);
            if (normalised != null){
                parts.add(field + ":\"" + normalised + "\"");
            }
        }

        if (parts.isEmpty()){
            return null;
        }

        String q = StringUtils.join(parts.toArray(new String[0]), " OR ");
        title = title == null?q : title;
        long qid = ParamsCache.put(q, title, null, null);
        logger.info("batchSearch: qid = " + qid);

        return qid;
    }
    
    /**
     * Webservice to report the occurrence counts for the supplied list of taxa 
     * 
     */
    @RequestMapping(value="/occurrences/taxaCount", method = {RequestMethod.POST, RequestMethod.GET})    
    public @ResponseBody Map<String, Integer> occurrenceSpeciesCounts(
            HttpServletResponse response,
            HttpServletRequest request,
            @RequestParam (defaultValue = "\n") String separator
            ) throws Exception {
        String listOfGuids = (String) request.getParameter("guids");
        String[] rawGuids = listOfGuids.split(separator);
        
        List<String>guids= new ArrayList<String>();
        for(String guid: rawGuids){
            String normalised = StringUtils.trimToNull(guid);
            if(normalised != null)
                guids.add(normalised);
        }
        return searchDAO.getOccurrenceCountsForTaxa(guids);
    }

	/**
	 * Occurrence search page uses SOLR JSON to display results
     *
     * Please NOTE that the q and fq provided to this URL should be obtained
     * from SearchResultDTO.urlParameters
	 *
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value = "/occurrences/download*", method = RequestMethod.GET)
	public String occurrenceDownload(
			DownloadRequestParams requestParams,
			@RequestParam(value="apiKey", required=false) String apiKey,
			HttpServletResponse response,
            HttpServletRequest request) throws Exception {
       
        String ip = request.getLocalAddr();
        ServletOutputStream out = response.getOutputStream();
        //search params must have a query or formatted query for the downlaod to work
        if (requestParams.getQ().isEmpty() && requestParams.getFormattedQuery().isEmpty()) {
            return null;
        }
        if(apiKey != null){
            return occurrenceSensitiveDownload(requestParams, apiKey, false, response, request);
        }

        writeQueryToStream(requestParams, response, ip, out, false,false);
        return null;
	}
	
	@RequestMapping(value = "/occurrences/index/download*", method = RequestMethod.GET)
	public void occurrenceIndexDownload(DownloadRequestParams requestParams,
            @RequestParam(value="apiKey", required=false) String apiKey,
            HttpServletResponse response,
            HttpServletRequest request) throws Exception{
	    
	    String ip = request.getLocalAddr();
        ServletOutputStream out = response.getOutputStream();
        //search params must have a query or formatted query for the downlaod to work
        if (requestParams.getQ().isEmpty() && requestParams.getFormattedQuery().isEmpty()) {
            return;
        }
        if(apiKey != null){
            occurrenceSensitiveDownload(requestParams, apiKey, true, response, request);
            return;
        }
        try{
        writeQueryToStream(requestParams, response, ip, out, false,true);
        }
        catch(Exception e){
            e.printStackTrace();
        }
	    
	}
	
    //@RequestMapping(value = "/sensitive/occurrences/download*", method = RequestMethod.GET)
    public String occurrenceSensitiveDownload(
            DownloadRequestParams requestParams,
            String apiKey,
            boolean fromIndex,
            HttpServletResponse response,
            HttpServletRequest request) throws Exception {
       
        
        if(shouldPerformOperation(apiKey, response, false)){        
            String ip = request.getLocalAddr();
            ServletOutputStream out = response.getOutputStream();
            //search params must have a query or formatted query for the downlaod to work
            if (requestParams.getQ().isEmpty() && requestParams.getFormattedQuery().isEmpty()) {
                return null;
            }
    
            writeQueryToStream(requestParams, response, ip, out, true,fromIndex);
        }
        return null;
    }	

    private void writeQueryToStream(DownloadRequestParams requestParams, HttpServletResponse response, String ip, ServletOutputStream out, boolean includeSensitive, boolean fromIndex) throws Exception {
        String filename = requestParams.getFile();

        response.setHeader("Cache-Control", "must-revalidate");
        response.setHeader("Pragma", "must-revalidate");
        response.setHeader("Content-Disposition", "attachment;filename=" + filename +".zip");
        response.setContentType("application/zip");

        //Use a zip output stream to include the data and citation together in the download
        ZipOutputStream zop = new ZipOutputStream(out);
        zop.putNextEntry(new java.util.zip.ZipEntry(filename + ".csv"));
        //put the facets
        requestParams.setFacets(new String[]{"assertions", "data_resource_uid"});
        Map<String, Integer> uidStats = null;
        try {
            if(fromIndex)
                uidStats = searchDAO.writeResultsFromIndexToStream(requestParams, zop, includeSensitive);
            else
                uidStats = searchDAO.writeResultsToStream(requestParams, zop, 100, includeSensitive);
        } catch (Exception e){
            logger.error(e);
        }
        zop.closeEntry();
        
        //add the Readme for the data field descriptions
        zop.putNextEntry(new java.util.zip.ZipEntry("README.html"));
        zop.write(("For more information about the fields that are being downloaded please consult <a href='" +dataFieldDescriptionURL+"'>Download Fields</a>.").getBytes());
        
        //Add the data citation to the download
        if (uidStats != null &&!uidStats.isEmpty()) {
            //add the citations for the supplied uids
            zop.putNextEntry(new java.util.zip.ZipEntry("citation.csv"));
            try {
                getCitations(uidStats, zop);
            } catch (Exception e) {
                logger.error(e);
            }
            zop.closeEntry();
        }
        zop.flush();
        zop.close();

        //logger.debug("UID stats : " + uidStats);
        //log the stats to ala logger        
        LogEventVO vo = new LogEventVO(1002,requestParams.getReasonTypeId(), requestParams.getSourceTypeId(), requestParams.getEmail(), requestParams.getReason(), ip,null, uidStats);        
        logger.log(RestLevel.REMOTE, vo);
    }

    /**
	 * get citation info from citation web service and write it into citation.txt file.
	 * 
	 * @param uidStats
	 * @param out
	 * @throws HttpException
	 * @throws IOException
	 */
	private void getCitations(Map<String, Integer> uidStats, OutputStream out) throws IOException{
		if(uidStats == null || uidStats.isEmpty() || out == null){
			throw new NullPointerException("keys and/or out is null!!");
		}
		
        //Object[] citations = restfulClient.restPost(citationServiceUrl, "text/json", uidStats.keySet());
		List<LinkedHashMap<String, Object>> entities = restTemplate.postForObject(citationServiceUrl, uidStats.keySet(), List.class);
		if(entities.size()>0){
		    out.write("\"Data resource ID\",\"Data resource\",\"Citation\",\"Rights\",\"More information\",\"Data generalizations\",\"Information withheld\",\"Download limit\",\"Number of Records in Download\"\n".getBytes());
		    for(Map<String,Object> record : entities){
		        StringBuilder sb = new StringBuilder();
                sb.append("\"").append(record.get("uid")).append("\",");
		        sb.append("\"").append(record.get("name")).append("\",");
		        sb.append("\"").append(record.get("citation")).append("\",");
		        sb.append("\"").append(record.get("rights")).append("\",");
		        sb.append("\"").append(record.get("link")).append("\",");
		        sb.append("\"").append(record.get("dataGeneralizations")).append("\",");
		        sb.append("\"").append(record.get("informationWithheld")).append("\",");
		        sb.append("\"").append(record.get("downloadLimit")).append("\",");
		        String count = uidStats.get(record.get("uid")).toString();
		        sb.append("\"").append(count).append("\"");
		        sb.append("\n");
		        out.write(sb.toString().getBytes());
		    }
		}
	}

	/**
	 * Utility method for retrieving a list of occurrences. Mainly added to help debug
     * webservices for that a developer can retrieve example UUIDs.
     *
	 * @throws Exception
	 */
	@RequestMapping(value = {"/occurrences/nearest"}, method = RequestMethod.GET)
	public @ResponseBody Map<String,Object> nearestOccurrence(SpatialSearchRequestParams requestParams) throws Exception {

        logger.debug(String.format("Received lat: %f, lon:%f, radius:%f", requestParams.getLat(), requestParams.getLon(), requestParams.getRadius()));

        if(requestParams.getLat() == null || requestParams.getLon() == null){
            return new HashMap<String,Object>();
        }
        //requestParams.setRadius(1f);
        requestParams.setDir("asc");
        requestParams.setFacet(false);

        SearchResultDTO searchResult = searchDAO.findByFulltextSpatialQuery(requestParams,null);
        List<OccurrenceIndex> ocs = searchResult.getOccurrences();

        if(!ocs.isEmpty()){
            Map<String,Object> results = new HashMap<String,Object>();
            OccurrenceIndex oc = ocs.get(0);
            Double decimalLatitude = oc.getDecimalLatitude();
            Double decimalLongitude = oc.getDecimalLongitude();
            Double distance = distInMetres(requestParams.getLat().doubleValue(), requestParams.getLon().doubleValue(),
                    decimalLatitude, decimalLongitude);
            results.put("distanceInMeters", distance);
            results.put("occurrence", oc);
            return results;
        } else {
            return new HashMap<String,Object>();
        }
    }

    private Double distInMetres(Double lat1, Double lon1, Double lat2, Double lon2){
        Double R = 6371000d; // km
        Double dLat = Math.toRadians(lat2-lat1);
        Double dLon = Math.toRadians(lon2-lon1);
        Double lat1Rad = Math.toRadians(lat1);
        Double lat2Rad = Math.toRadians(lat2);
        Double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                Math.sin(dLon/2) * Math.sin(dLon/2) * Math.cos(lat1Rad) * Math.cos(lat2Rad);
        Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c;
    }

    /**
     * Dumps the distinct latitudes and longitudes that are used in the
     * connected index (to 4 decimal places)
     */
    @RequestMapping(value="/occurrences/coordinates*")
    public void dumpDistinctLatLongs(SearchRequestParams requestParams,HttpServletResponse response) throws Exception{
         requestParams.setFacets(new String[]{"lat_long"});
         if(requestParams.getQ().length()<1)
             requestParams.setQ("*:*");
         ServletOutputStream out = response.getOutputStream();
         searchDAO.writeCoordinatesToStream(requestParams,out);
    }
	
    /**
     * Occurrence record page
     *
     * When user supplies a uuid that is not found search for a unique record
     * with the supplied occurrenc_id
     *
     * Returns a SearchResultDTO when there is more than 1 record with the supplied UUID
     *
     * @param uuid
     * @throws Exception
     */
    @RequestMapping(value = {"/occurrence/compare/{uuid}.json", "/occurrence/compare/{uuid}"}, method = RequestMethod.GET)
    public @ResponseBody Object showOccurrence(@PathVariable("uuid") String uuid){
        Map values = Store.getComparisonByUuid(uuid);
        if(values.isEmpty())
            values = Store.getComparisonByRowKey(uuid);
        //substitute the values for recordedBy if it is an authenticated user        
        if(values.containsKey("Occurrence")){
            //String recordedBy = values.get("recordedBy").toString();
            List<au.org.ala.util.ProcessedValue> compareList = (List<au.org.ala.util.ProcessedValue>)values.get("Occurrence");
            List<au.org.ala.util.ProcessedValue> newList = new ArrayList<au.org.ala.util.ProcessedValue>();
            for(au.org.ala.util.ProcessedValue pv : compareList){              
                if(pv.getName().equals("recordedBy")){
                    logger.info(pv);
                    String raw = authService.getDisplayNameFor(pv.getRaw());
                    String processed = authService.getDisplayNameFor(pv.getProcessed());                    
                    au.org.ala.util.ProcessedValue newpv = new au.org.ala.util.ProcessedValue("recordedBy", raw, processed);
                    newList.add(newpv);
                }
                else
                    newList.add(pv);
            }
            values.put("Occurrence", newList);
        }
        return values;
    }
    /**
     * Returns a comparison of the occurrence versions.
     * @param uuid
     * @return
     */
    @RequestMapping(value = {"/occurrence/compare*"}, method = RequestMethod.GET)
    public @ResponseBody Object compareOccurrenceVersions(@RequestParam(value = "uuid", required = true) String uuid){
        return showOccurrence(uuid);
    }
    /**
     * Returns the records uuids that have been deleted since the fromDate inclusive.
     * 
     * @param fromDate
     * @param response
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {"/occurrence/deleted"}, method = RequestMethod.GET)
    public @ResponseBody String[] getDeleteOccurrences(@RequestParam(value ="date", required = true) String fromDate,
            HttpServletResponse response) throws Exception{
        try{
            //date must be in a yyyy-MM-dd format
            Date date = org.apache.commons.lang.time.DateUtils.parseDate(fromDate,new String[]{"yyyy-MM-dd"});
            return Store.getDeletedRecords(date);
        }
        catch(Exception e){
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid date format.  Please provide date as yyyy-MM-dd.");
        }
        return null;
    }
    
	/**
	 * Occurrence record page
	 *
	 * When user supplies a uuid that is not found search for a unique record
	 * with the supplied occurrenc_id
	 *
	 * Returns a SearchResultDTO when there is more than 1 record with the supplied UUID
	 *
	 * @param uuid
	 * @param apiKey
	 * @throws Exception
	 */
	@RequestMapping(value = {"/occurrence/{uuid:.+}","/occurrences/{uuid:.+}", "/occurrence/{uuid:.+}.json", "/occurrences/{uuid:.+}.json"}, method = RequestMethod.GET)
	public @ResponseBody Object showOccurrence(@PathVariable("uuid") String uuid,
	        @RequestParam(value="apiKey", required=false) String apiKey,
        HttpServletRequest request, HttpServletResponse response) throws Exception {
	    if(apiKey != null){
	        return showSensitiveOccurrence(uuid, apiKey, request, response);
	    }
		return getOccurrenceInformation(uuid, request, false);
	}
	
	@RequestMapping(value = {"/sensitive/occurrence/{uuid:.+}","/sensitive/occurrences/{uuid:.+}", "/sensitive/occurrence/{uuid:.+}.json", "/senstive/occurrences/{uuid:.+}.json"}, method = RequestMethod.GET)
    public @ResponseBody Object showSensitiveOccurrence(@PathVariable("uuid") String uuid,
            @RequestParam(value="apiKey", required=true) String apiKey,
        HttpServletRequest request,HttpServletResponse response) throws Exception {
	    if(shouldPerformOperation(apiKey, response)){
	        return getOccurrenceInformation(uuid, request, true);
	    }
	    return null;
    }
	
	private Object getOccurrenceInformation(String uuid, HttpServletRequest request, boolean includeSensitive) throws Exception{
	    logger.debug("Retrieving occurrence record with guid: '"+uuid+"'");

        FullRecord[] fullRecord = Store.getAllVersionsByUuid(uuid, includeSensitive);
        if(fullRecord == null){
            //get the rowKey for the supplied uuid in the index
            //This is a workaround.  There seems to be an issue on Cassandra with retrieving uuids that start with e or f
            SpatialSearchRequestParams srp = new SpatialSearchRequestParams();
            srp.setQ("id:"+uuid);
            srp.setPageSize(1);
            srp.setFacets(new String[]{});
            SearchResultDTO results = occurrenceSearch(srp);
            if(results.getTotalRecords()>0)
                fullRecord = Store.getAllVersionsByRowKey(results.getOccurrences().get(0).getRowKey(), includeSensitive);            
        }
        if(fullRecord == null){
            //check to see if we have an occurrence id
            SpatialSearchRequestParams srp = new SpatialSearchRequestParams();
            srp.setQ("occurrence_id:" + uuid);
            SearchResultDTO result = occurrenceSearch(srp);
            if(result.getTotalRecords()>1)
                return result;
            else if(result.getTotalRecords()==0)
                return new OccurrenceDTO();
            else
                fullRecord = Store.getAllVersionsByUuid(result.getOccurrences().get(0).getUuid(), includeSensitive);
        }
        
        
        OccurrenceDTO occ = new OccurrenceDTO(fullRecord);
        // now update the values required for the authService
        if(fullRecord != null){
            //raw record may need recordedBy to be changed 
            fullRecord[0].getOccurrence().setRecordedBy(authService.getDisplayNameFor(fullRecord[0].getOccurrence().getRecordedBy()));            
            //processed record may need recordedBy modified in case it was an email address.
            fullRecord[1].getOccurrence().setRecordedBy(authService.getDisplayNameFor(fullRecord[1].getOccurrence().getRecordedBy()));
            //hide the email addresses in the raw miscProperties
            Map<String,String> miscProps =fullRecord[0].miscProperties();
            for(Map.Entry<String,String> entry: miscProps.entrySet()){
                if(entry.getValue().contains("@"))
                  entry.setValue(authService.getDisplayNameFor(entry.getValue()));
            }
            //if the raw record contains a userId we will need to include the alaUserName in the DTO
            if(fullRecord[0].getOccurrence().getUserId() != null){
                occ.setAlaUserName(authService.getDisplayNameFor(fullRecord[0].getOccurrence().getUserId()));
            }
        }
        String rowKey = occ.getProcessed().getRowKey();
        //assertions are based on the row key not uuid
        occ.setSystemAssertions(Store.getSystemAssertions(rowKey));
        occ.setUserAssertions(Store.getUserAssertions(rowKey));
        

        //TODO retrieve details of the media files
        String[] sounds = occ.getProcessed().getOccurrence().getSounds();
        if(sounds != null && sounds.length > 0){
            List<MediaDTO> soundDtos = new ArrayList<MediaDTO>();
            for(String sound: sounds){
                MediaDTO m = new MediaDTO();
                m.setContentType(MimeType.getForFileExtension(sound).getMimeType());
                m.setFilePath(MediaStore.convertPathToUrl(sound,OccurrenceIndex.biocacheMediaUrl));

                String[] files = Store.getAlternativeFormats(sound);
                for(String fileName: files){
                    String contentType = MimeType.getForFileExtension(fileName).getMimeType();
                    String filePath = MediaStore.convertPathToUrl(fileName,OccurrenceIndex.biocacheMediaUrl);
                    //System.out.println("#########Adding media path: " + m.getFilePath());
                    m.getAlternativeFormats().put(contentType,filePath);
                }
                soundDtos.add(m);
            }
            occ.setSounds(soundDtos);
        }

        //ADD THE DIFFERENT IMAGE FORMATS...thumb,small,large,raw
        setupImageUrls(occ);

        //log the statistics for viewing the record
        String email = null;
        String reason = "Viewing Occurrence Record " + uuid;
        String ip = request.getLocalAddr();
        Map<String, Integer> uidStats = new HashMap<String, Integer>();
        if(occ.getProcessed() != null && occ.getProcessed().getAttribution()!=null){
            if (occ.getProcessed().getAttribution().getCollectionUid() != null) {
                uidStats.put(occ.getProcessed().getAttribution().getCollectionUid(), 1);
            }
            if (occ.getProcessed().getAttribution().getInstitutionUid() != null) {
                uidStats.put(occ.getProcessed().getAttribution().getInstitutionUid(), 1);
            }
            if(occ.getProcessed().getAttribution().getDataProviderUid() != null)
                uidStats.put(occ.getProcessed().getAttribution().getDataProviderUid(), 1);
            if(occ.getProcessed().getAttribution().getDataResourceUid() != null)
                uidStats.put(occ.getProcessed().getAttribution().getDataResourceUid(), 1);
        }

        //fix media store URLs
        MediaStore.convertPathsToUrls(occ.getRaw(), OccurrenceIndex.biocacheMediaUrl);
        MediaStore.convertPathsToUrls(occ.getProcessed(), OccurrenceIndex.biocacheMediaUrl);
            
        LogEventVO vo = new LogEventVO(LogEventType.OCCURRENCE_RECORDS_VIEWED, email, reason, ip, uidStats);
        logger.log(RestLevel.REMOTE, vo);

        return occ;
	}

    private void setupImageUrls(OccurrenceDTO dto){
        String[] images = dto.getProcessed().getOccurrence().getImages();
        if(images != null && images.length > 0){
            List<MediaDTO> ml = new ArrayList<MediaDTO>();
            for(String fileName: images){
                MediaDTO m = new MediaDTO();
                String url =  MediaStore.convertPathToUrl(fileName,OccurrenceIndex.biocacheMediaUrl);
                String extension = url.substring(url.lastIndexOf("."));
                m.getAlternativeFormats().put("thumbnailUrl", url.replace(extension, "__thumb" + extension));
                m.getAlternativeFormats().put("smallImageUrl", url.replace(extension, "__small" + extension));
                m.getAlternativeFormats().put("largeImageUrl", url.replace(extension, "__large" + extension));
                m.getAlternativeFormats().put("imageUrl", url);
                m.setFilePath(fileName);
                ml.add(m);
            }
            dto.setImages(ml);
        }
    }

	/**
     * Create a HashMap for the filter queries
     *
     * @param filterQuery
     * @return
     */
    private Map<String, String> addFacetMap(String[] filterQuery) {
               HashMap<String, String> facetMap = new HashMap<String, String>();

        if (filterQuery != null && filterQuery.length > 0) {
            logger.debug("filterQuery = "+StringUtils.join(filterQuery, "|"));
            for (String fq : filterQuery) {
                if (fq != null && !fq.isEmpty()) {
                    String[] fqBits = StringUtils.split(fq, ":", 2);
                    facetMap.put(fqBits[0], fqBits[1]);
                }
            }
        }
        return facetMap;
    }
    
    /**
     * Calculate the last page number for pagination
     * 
     * @param totalRecords
     * @param pageSize
     * @return
     */
    private Integer calculateLastPage(Long totalRecords, Integer pageSize) {
        Integer lastPage = 0;
        Integer lastRecordNum = totalRecords.intValue();
        
        if (pageSize > 0) {
            lastPage = (lastRecordNum / pageSize) + ((lastRecordNum % pageSize > 0) ? 1 : 0);
        }
        
        return lastPage;
    }

//    /**
//     * Simple check for valid latitude, longitude & radius values
//     *
//     * @param latitude
//     * @param longitude
//     * @param radius
//     * @return
//     */
//    private boolean checkValidSpatialParams(Float latitude, Float longitude, Integer radius) {
//        return (latitude != null && !latitude.isNaN() && longitude != null && !longitude.isNaN() && radius != null && radius > 0);
//    }

	/**
	 * @param hostUrl the hostUrl to set
	 */
	public void setHostUrl(String hostUrl) {
		this.hostUrl = hostUrl;
	}

	/**
	 * @param searchDAO the searchDAO to set
	 */
	public void setSearchDAO(SearchDAO searchDAO) {
		this.searchDAO = searchDAO;
	}

	/**
	 * @param bieBaseUrl the bieBaseUrl to set
	 */
	public void setBieBaseUrl(String bieBaseUrl) {
		this.bieBaseUrl = bieBaseUrl;
	}

	/**
	 * @param collectoryBaseUrl the collectoryBaseUrl to set
	 */
	public void setCollectoryBaseUrl(String collectoryBaseUrl) {
		this.collectoryBaseUrl = collectoryBaseUrl;
	}

	/**
	 * @param searchUtils the searchUtils to set
	 */
	public void setSearchUtils(SearchUtils searchUtils) {
		this.searchUtils = searchUtils;
	}

	public void setCitationServiceUrl(String citationServiceUrl) {
		this.citationServiceUrl = citationServiceUrl;
	}
}
