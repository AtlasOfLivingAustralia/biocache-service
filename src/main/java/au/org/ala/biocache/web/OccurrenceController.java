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

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.biocache.load.MediaStore;
import au.org.ala.biocache.Store;
import au.org.ala.biocache.model.FullRecord;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import au.org.ala.biocache.dto.OccurrenceDTO;
import au.org.ala.biocache.service.AuthService;
import au.org.ala.biocache.service.DownloadService;
import au.org.ala.biocache.service.SpeciesLookupService;
import au.org.ala.biocache.util.*;
import org.ala.client.appender.RestLevel;
import org.ala.client.model.LogEventType;
import org.ala.client.model.LogEventVO;
import org.ala.client.util.RestfulClient;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.validation.ObjectError;
import org.springframework.validation.Validator;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

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
    protected SpeciesLookupService speciesLookupService;
    @Inject
    protected AuthService authService;
    @Inject
    protected ContactUtils contactUtils;
    @Inject
    protected AssertionUtils assertionUtils;
    @Inject 
    protected DownloadService downloadService;
    @Inject
    private AbstractMessageSource messageSource;
    @Autowired
    private Validator validator;

    /** Name of view for site home page */
    private String HOME = "homePage";

    private String VALIDATION_ERROR = "error/validationError";

    @Value("${webservices.root:http://localhost:8080/biocache-service}")
    protected String hostUrl;

    /** The response to be returned for the isAustralian test */
    @Value("${taxon.id.pattern:urn:lsid:biodiversity.org.au[a-zA-Z0-9\\.:-]*}")
    protected String taxonIDPatternString;

    /** Compiled pattern for taxon IDs */
    protected Pattern taxonIDPattern;

    public Pattern getTaxonIDPattern(){
        if(taxonIDPattern == null){
            taxonIDPattern = Pattern.compile(taxonIDPatternString);
        }
        return taxonIDPattern;
    }

    /**
     * Need to initialise the validator to be used otherwise the @Valid annotation will not work
     * @param binder
     */
    @InitBinder  
    protected void initBinder(WebDataBinder binder) {  
        binder.setValidator(validator);  
    }
    
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
    public String homePageHandler(Model model) {
        model.addAttribute("webservicesRoot", hostUrl);
        return HOME;
    }

    /**
     * Custom handler for the welcome view.
     * <p>
     * Note that this handler relies on the RequestToViewNameTranslator to
     * determine the logical view name based on the request URL: "/welcome.do"
     * -&gt; "welcome".
     *
     * @return viewname to render
     */
    @RequestMapping("/oldapi")
    public String oldApiHandler(Model model) {
        model.addAttribute("webservicesRoot", hostUrl);
        return "oldapi";
    }


    @RequestMapping("/active/download/stats")
    public @ResponseBody List<DownloadDetailsDTO> getCurrentDownloads(){
        return downloadService.getCurrentDownloads();
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
            while ((bytesRead = is.read(buffer)) != -1){
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
    @RequestMapping(value={"/australian/taxon/{guid:.+}.json*","/australian/taxon/{guid:.+}*","/native/taxon/{guid:.+}.json*","/native/taxon/{guid:.+}*" })
    public @ResponseBody NativeDTO isAustralian(@PathVariable("guid") String guid) throws Exception {
        //check to see if we have any occurrences on Australia  country:Australia or state != empty
        SpatialSearchRequestParams requestParams = new SpatialSearchRequestParams();
        requestParams.setPageSize(0);
        requestParams.setFacets(new String[]{});
        String query = "lsid:" +guid + " AND " + "(country:Australia OR state:[* TO *]) AND geospatial_kosher:true";
        requestParams.setQ(query);
        NativeDTO adto= new NativeDTO();
        adto.setTaxonGuid(guid);
        SearchResultDTO results = searchDAO.findByFulltextSpatialQuery(requestParams,null);
        adto.setHasOccurrenceRecords(results.getTotalRecords() > 0);
        adto.setIsNSL(getTaxonIDPattern().matcher(guid).matches());
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
     * @param guid
     * @throws Exception
     */
    @RequestMapping(value = "/occurrences/taxon/source/{guid:.+}.json*", method = RequestMethod.GET)
    public @ResponseBody List<OccurrenceSourceDTO> sourceByTaxon(SpatialSearchRequestParams requestParams,
            @PathVariable("guid") String guid) throws Exception {
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
                             "/occurrences/dataResources/{uid}", "/occurrences/dataProviders/{uid}",
            "/occurrences/dataHubs/{uid}"}, method = RequestMethod.GET)
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
    public @ResponseBody SearchResultDTO occurrenceSearchByArea(SpatialSearchRequestParams requestParams,
            Model model) throws Exception {

        SearchResultDTO searchResult = new SearchResultDTO();

        if (StringUtils.isEmpty(requestParams.getQ())) {
            return searchResult;
        }

        //searchUtils.updateSpatial(requestParams);
        searchResult = searchDAO.findByFulltextSpatialQuery(requestParams,null);
        model.addAttribute("searchResult", searchResult);

        if(logger.isDebugEnabled()){
            logger.debug("Returning results set with: " + searchResult.getTotalRecords());
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
        Map<String,String[]> map = request != null ? SearchUtils.getExtraParams(request.getParameterMap()) : null;
        if(map != null){
            map.remove("apiKey");
        }
        logger.debug("occurrence search params= " + requestParams);
        if(apiKey == null)
            return searchDAO.findByFulltextSpatialQuery(requestParams,map);
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
            if(map != null){
                map.remove("apiKey");
            }
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
        @RequestParam(value="synonym", required=false, defaultValue="false") boolean includeSynonyms,
        @RequestParam(value="ip", required=false) String ip,
        HttpServletRequest request,
        HttpServletResponse response) throws Exception {          
        if(requestParams.getFacets().length > 0){
            ip = ip == null?getIPAddress(request):ip;
            DownloadDetailsDTO dd = downloadService.registerDownload(requestParams, ip, DownloadDetailsDTO.DownloadType.FACET);
            try {
                String filename = requestParams.getFile() != null ? requestParams.getFile():requestParams.getFacets()[0];
                response.setHeader("Cache-Control", "must-revalidate");
                response.setHeader("Pragma", "must-revalidate");
                response.setHeader("Content-Disposition", "attachment;filename=" + filename +".csv");
                response.setContentType("text/csv");
                searchDAO.writeFacetToStream(requestParams,includeCount, lookupName,includeSynonyms, response.getOutputStream(),dd);
            } finally {
              downloadService.unregisterDownload(dd);
            }
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
        Long qid = getQidForBatchSearch(queries, field, separator, title);

        if (qid != null) {
            String webservicesRoot = request.getSession().getServletContext().getInitParameter("webservicesRoot");
            response.sendRedirect(webservicesRoot + "/occurrences/download?q=qid:"+qid);
        } else {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST);
        }
    }

    @RequestMapping(value = "/occurrences/download/batchFile", method = RequestMethod.GET)
    public String batchDownload(
            HttpServletResponse response,
            HttpServletRequest request,
            @Valid final DownloadRequestParams params,
            BindingResult result,
            @RequestParam(value="file", required = true) String filepath,
            @RequestParam(value="directory", required = true, defaultValue = "/data/biocache-exports") final String directory,
            @RequestParam(value="ip", required=false) String ip,
            Model model
            ) throws Exception {

        
        if(result.hasErrors()){
            logger.info("validation failed  " + result.getErrorCount() + " checks");
            logger.debug(result.toString());
            model.addAttribute("errorMessage", getValidationErrorMessage(result));
            //response.setStatus(response.SC_INTERNAL_SERVER_ERROR);
            return VALIDATION_ERROR;//result.toString();
        }
        
        final File file = new File(filepath);

        final SpeciesLookupService mySpeciesLookupService = this.speciesLookupService;
        ip = ip == null?getIPAddress(request):ip;
        final DownloadDetailsDTO dd = downloadService.registerDownload(params, ip, DownloadType.RECORDS_INDEX);

        if(file.exists()){
            Thread t = new Thread(){
                @Override
                public void run() {
                    try {
                        //start a thread
                        CSVReader reader  = new CSVReader(new FileReader(file));
                        String[] row = reader.readNext();
                        while(row != null){

                            //get an lsid for the name
                            String lsid = mySpeciesLookupService.getGuidForName(row[0]);
                            if(lsid != null){
                                try {
                                    //download records for this row
                                    String outputFilePath = directory + File.separatorChar + row[0].replace(" ", "_") + ".txt";
                                    String citationFilePath = directory + File.separatorChar + row[0].replace(" ", "_") + "_citations.txt";
                                    logger.debug("Outputting results to:" + outputFilePath + ", with LSID: " + lsid);
                                    FileOutputStream output = new FileOutputStream(outputFilePath);
                                    params.setQ("lsid:\""+lsid+"\"");
                                    Map<String,Integer> uidStats = searchDAO.writeResultsFromIndexToStream(params, output, false, dd,false);
                                    FileOutputStream citationOutput = new FileOutputStream(citationFilePath);
                                    downloadService.getCitations(uidStats, citationOutput);
                                    citationOutput.flush();
                                    citationOutput.close();
                                    output.flush();
                                    output.close();
                                } catch(Exception e){
                                    logger.error(e.getMessage(),e);
                                }
                            } else {
                                logger.error("Unable to match name: " + row[0]);
                            }
                            row = reader.readNext();
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage(),e);
                    } finally {
                        downloadService.unregisterDownload(dd);
                    }
                }
            };
            t.start();
        }
        return null;
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
        long qid = ParamsCache.put(q, title, null, null,null);
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
            @Valid DownloadRequestParams requestParams,
            BindingResult result,
            @RequestParam(value="ip", required=false) String ip,
            @RequestParam(value="apiKey", required=false) String apiKey,
            Model model,
            HttpServletResponse response,
            HttpServletRequest request) throws Exception {
        //org.springframework.validation.BindException errors = new org.springframework.validation.BindException(requestParams,"requestParams");
        //validator.validate(requestParams, errors);
        //check to see if the DownloadRequestParams are valid
        if(result.hasErrors()){
            logger.info("validation failed  " + result.getErrorCount() + " checks");
            logger.debug(result.toString());
            model.addAttribute("errorMessage", getValidationErrorMessage(result));
            //response.setStatus(response.SC_INTERNAL_SERVER_ERROR);
            return VALIDATION_ERROR;//result.toString();
        }
        
        ip = ip == null?getIPAddress(request):ip;//request.getRemoteAddr():ip;
        ServletOutputStream out = response.getOutputStream();
        //search params must have a query or formatted query for the downlaod to work
        if (requestParams.getQ().isEmpty() && requestParams.getFormattedQuery().isEmpty()) {
            return null;
        }
        if(apiKey != null){
            return occurrenceSensitiveDownload(requestParams, apiKey, ip, false, response, request);
        }

        downloadService.writeQueryToStream(requestParams, response, ip, out, false,false);
        return null;
    }
    
    @RequestMapping(value = "/occurrences/index/download*", method = RequestMethod.GET)
    public String occurrenceIndexDownload(@Valid DownloadRequestParams requestParams,
            BindingResult result,
            @RequestParam(value="apiKey", required=false) String apiKey,
            @RequestParam(value="ip", required=false) String ip,
            Model model,
            HttpServletResponse response,
            HttpServletRequest request) throws Exception{
        
        if(result.hasErrors()){
            logger.info("validation failed  " + result.getErrorCount() + " checks");
            logger.debug(result.toString());
            model.addAttribute("errorMessage", getValidationErrorMessage(result));
            //response.setStatus(response.SC_INTERNAL_SERVER_ERROR);
            return VALIDATION_ERROR;//result.toString();
        }
        
        ip = ip == null ? getIPAddress(request) : ip;
        ServletOutputStream out = response.getOutputStream();
        //search params must have a query or formatted query for the downlaod to work
        if (requestParams.getQ().isEmpty() && requestParams.getFormattedQuery().isEmpty()) {
            return null;
        }
        if(apiKey != null){
            occurrenceSensitiveDownload(requestParams, apiKey, ip, true, response, request);
            return null;
        }
        try {
           downloadService.writeQueryToStream(requestParams, response, ip, out, false,true);
        } catch(Exception e){
            logger.error(e.getMessage(), e);
        }
        return null;
    }
    
    //@RequestMapping(value = "/sensitive/occurrences/download*", method = RequestMethod.GET)
    public String occurrenceSensitiveDownload(
            DownloadRequestParams requestParams,
            String apiKey,
            String ip,
            boolean fromIndex,
            HttpServletResponse response,
            HttpServletRequest request) throws Exception {
       
        
        if(shouldPerformOperation(apiKey, response, false)){        
            ip = ip == null?getIPAddress(request):ip;
            ServletOutputStream out = response.getOutputStream();
            //search params must have a query or formatted query for the downlaod to work
            if (requestParams.getQ().isEmpty() && requestParams.getFormattedQuery().isEmpty()) {
                return null;
            }
    
            downloadService.writeQueryToStream(requestParams, response, ip, out, true,fromIndex);
        }
        return null;
    }    
    /**
     * Returns the IP address for the supplied request. It will look for the existence of 
     * an X-Forwarded-For Header before extracting it from the request.
     * @param request
     * @return IP Address of the request
     */
    private String getIPAddress(HttpServletRequest request){
        //check to see if proxied.
        String forwardedFor=request.getHeader("X-Forwarded-For");
        return forwardedFor == null ? request.getRemoteAddr(): forwardedFor;
    }

    /**
     * Utility method for retrieving a list of occurrences. Mainly added to help debug
     * web services for that a developer can retrieve example UUIDs.
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
     * TODO move to service layer
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
            List<au.org.ala.biocache.parser.ProcessedValue> compareList = (List<au.org.ala.biocache.parser.ProcessedValue>)values.get("Occurrence");
            List<au.org.ala.biocache.parser.ProcessedValue> newList = new ArrayList<au.org.ala.biocache.parser.ProcessedValue>();
            for(au.org.ala.biocache.parser.ProcessedValue pv : compareList){
                if(pv.getName().equals("recordedBy")){
                    logger.info(pv);
                    String raw = authService.substituteEmailAddress(pv.getRaw());
                    String processed = authService.substituteEmailAddress(pv.getProcessed());                    
                    au.org.ala.biocache.parser.ProcessedValue newpv = new au.org.ala.biocache.parser.ProcessedValue("recordedBy", raw, processed);
                    newList.add(newpv);
                } else {
                    newList.add(pv);
                }
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
        try {
            //date must be in a yyyy-MM-dd format
            Date date = org.apache.commons.lang.time.DateUtils.parseDate(fromDate,new String[]{"yyyy-MM-dd"});
            return Store.getDeletedRecords(date);
        } catch(Exception e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid date format.  Please provide date as yyyy-MM-dd.");
        }
        return null;
    }
    
    /**
     * Occurrence record page
     *
     * When user supplies a uuid that is not found search for a unique record
     * with the supplied occurrence_id
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
            @RequestParam(value="ip", required=false) String ip,
        HttpServletRequest request, HttpServletResponse response) throws Exception {
        ip = ip == null?getIPAddress(request):ip;
        if(apiKey != null){
            return showSensitiveOccurrence(uuid, apiKey, ip, request, response);
        }
        return getOccurrenceInformation(uuid,ip, request, false);
    }
    
    @RequestMapping(value = {"/sensitive/occurrence/{uuid:.+}","/sensitive/occurrences/{uuid:.+}", "/sensitive/occurrence/{uuid:.+}.json", "/senstive/occurrences/{uuid:.+}.json"}, method = RequestMethod.GET)
    public @ResponseBody Object showSensitiveOccurrence(@PathVariable("uuid") String uuid,
            @RequestParam(value="apiKey", required=true) String apiKey,
            @RequestParam(value="ip", required=false) String ip,
        HttpServletRequest request,HttpServletResponse response) throws Exception {
        ip = ip == null?getIPAddress(request):ip;
        if(shouldPerformOperation(apiKey, response)){
            return getOccurrenceInformation(uuid, ip, request, true);
        }
        return null;
    }
    
    private Object getOccurrenceInformation(String uuid, String ip, HttpServletRequest request, boolean includeSensitive) throws Exception{
        logger.debug("Retrieving occurrence record with guid: '" + uuid + "'");

        FullRecord[] fullRecord = Store.getAllVersionsByUuid(uuid, includeSensitive);
        if(fullRecord == null){
            //get the rowKey for the supplied uuid in the index
            //This is a workaround.  There seems to be an issue on Cassandra with retrieving uuids that start with e or f
            SpatialSearchRequestParams srp = new SpatialSearchRequestParams();
            srp.setQ("id:" + uuid);
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
            if(result.getTotalRecords() > 1)
                return result;
            else if(result.getTotalRecords() == 0)
                return new OccurrenceDTO();
            else
                fullRecord = Store.getAllVersionsByUuid(result.getOccurrences().get(0).getUuid(), includeSensitive);
        }

        OccurrenceDTO occ = new OccurrenceDTO(fullRecord);
        // now update the values required for the authService
        if(fullRecord != null){
            //TODO - move this logic to service layer
            //raw record may need recordedBy to be changed
            //NC 2013-06-26: The substitution was removed in favour of email obscuring due to numeric id's being used for non-ALA data resources 
            fullRecord[0].getOccurrence().setRecordedBy(authService.substituteEmailAddress(fullRecord[0].getOccurrence().getRecordedBy()));
            //processed record may need recordedBy modified in case it was an email address.
            fullRecord[1].getOccurrence().setRecordedBy(authService.substituteEmailAddress(fullRecord[1].getOccurrence().getRecordedBy()));
            //hide the email addresses in the raw miscProperties
            Map<String,String> miscProps = fullRecord[0].miscProperties();
            for(Map.Entry<String,String> entry: miscProps.entrySet()){
                if(entry.getValue().contains("@"))
                  entry.setValue(authService.substituteEmailAddress(entry.getValue()));
            }
            //if the raw record contains a userId we will need to include the alaUserName in the DTO
            if(fullRecord[0].getOccurrence().getUserId() != null){
                occ.setAlaUserName(authService.getDisplayNameFor(fullRecord[0].getOccurrence().getUserId()));
            }
        }

        String rowKey = occ.getProcessed().getRowKey();

        //assertions are based on the row key not uuid
        occ.setSystemAssertions(Store.getAllSystemAssertions(rowKey));

        occ.setUserAssertions(assertionUtils.getUserAssertions(occ));

        //retrieve details of the media files
        List<MediaDTO> soundDtos = getSoundDtos(occ);
        if(!soundDtos.isEmpty()){
            occ.setSounds(soundDtos);
        }

        //ADD THE DIFFERENT IMAGE FORMATS...thumb,small,large,raw
        setupImageUrls(occ);

        //fix media store URLs
        MediaStore.convertPathsToUrls(occ.getRaw(), OccurrenceIndex.biocacheMediaUrl);
        MediaStore.convertPathsToUrls(occ.getProcessed(), OccurrenceIndex.biocacheMediaUrl);

        //log the statistics for viewing the record
        logViewEvent(ip, occ, null, "Viewing Occurrence Record " + uuid);

        return occ;
    }

    private void logViewEvent(String ip, OccurrenceDTO occ, String email, String reason) {
        //String ip = request.getLocalAddr();
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

        LogEventVO vo = new LogEventVO(LogEventType.OCCURRENCE_RECORDS_VIEWED, email, reason, ip, uidStats);
        logger.log(RestLevel.REMOTE, vo);
    }
    /**
     * Constructs an error message to be displayed. The error message is based on validation checks that 
     * were performed and stored in the supplied result.
     * 
     * TODO: If we decide to perform more detailed validations elsewhere it maybe worth providing this in a
     * util or service class.
     * 
     * @param result The result from the validation.
     * @return A string representation that can be displayed in a browser.
     */
    private String getValidationErrorMessage(BindingResult result){
        StringBuilder sb = new StringBuilder();
        List<ObjectError> errors =result.getAllErrors();
        for(ObjectError error :errors){
            logger.debug("Code: " + error.getCode());
            logger.debug(StringUtils.join(error.getCodes(),"@#$^"));
            String code = (error.getCodes() != null && error.getCodes().length>0)? error.getCodes()[0]:null;
            logger.debug("The code in use:" + code);
            sb.append(messageSource.getMessage(code, null, error.getDefaultMessage(),null)).append("<br/>");
        }
        return sb.toString();
    }

    private List<MediaDTO> getSoundDtos(OccurrenceDTO occ) {
        String[] sounds = occ.getProcessed().getOccurrence().getSounds();
        List<MediaDTO> soundDtos = new ArrayList<MediaDTO>();
        if(sounds != null && sounds.length > 0){
            for(String sound: sounds){
                MediaDTO m = new MediaDTO();
                m.setContentType(MimeType.getForFileExtension(sound).getMimeType());
                m.setFilePath(MediaStore.convertPathToUrl(sound, OccurrenceIndex.biocacheMediaUrl));

                String[] files = Store.getAlternativeFormats(sound);
                for(String fileName: files){
                    String contentType = MimeType.getForFileExtension(fileName).getMimeType();
                    String filePath = MediaStore.convertPathToUrl(fileName, OccurrenceIndex.biocacheMediaUrl);
                    //System.out.println("#########Adding media path: " + m.getFilePath());
                    m.getAlternativeFormats().put(contentType,filePath);
                }
                soundDtos.add(m);
            }
        }
        return soundDtos;
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
            if(logger.isDebugEnabled()){
                logger.debug("filterQuery = " + StringUtils.join(filterQuery, "|"));
            }
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
     * @param searchUtils the searchUtils to set
     */
    public void setSearchUtils(SearchUtils searchUtils) {
        this.searchUtils = searchUtils;
    }

    public void setSpeciesLookupService(SpeciesLookupService speciesLookupService) {
        this.speciesLookupService = speciesLookupService;
    }

    public void setContactUtils(ContactUtils contactUtils) {
        this.contactUtils = contactUtils;
    }

    public void setAssertionUtils(AssertionUtils assertionUtils) {
        this.assertionUtils = assertionUtils;
    }
}