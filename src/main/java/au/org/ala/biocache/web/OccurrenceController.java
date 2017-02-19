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
import au.org.ala.biocache.Config;
import au.org.ala.biocache.Store;
import au.org.ala.biocache.dao.QidCacheDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import au.org.ala.biocache.model.FullRecord;
import au.org.ala.biocache.service.AuthService;
import au.org.ala.biocache.service.DownloadService;
import au.org.ala.biocache.service.ImageMetadataService;
import au.org.ala.biocache.service.SpeciesLookupService;
import au.org.ala.biocache.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.sf.ehcache.CacheManager;
import org.ala.client.appender.RestLevel;
import org.ala.client.model.LogEventType;
import org.ala.client.model.LogEventVO;
import org.ala.client.util.RestfulClient;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.gbif.utils.file.FileUtils;
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

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Occurrences controller for the biocache web services.
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 * @author "Natasha Carter <Natasha.Carter@csiro.au>"
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
    @Inject
    private ImageMetadataService imageMetadataService;
    @Autowired
    private Validator validator;
    @Inject
    protected QidCacheDAO qidCacheDao;
    @Inject
    private CacheManager cacheManager;
    
    /** Name of view for site home page */
    private String HOME = "homePage";
    
    private String VALIDATION_ERROR = "error/validationError";
    
    @Value("${webservices.root:http://localhost:8080/biocache-service}")
    protected String webservicesRoot;
    
    /** The response to be returned for the isAustralian test */
    @Value("${taxon.id.pattern:urn:lsid:biodiversity.org.au[a-zA-Z0-9\\.:-]*|http://id.biodiversity.org.au/[a-zA-Z0-9/]*}")
    protected String taxonIDPatternString;

    @Value("${native.country:Australia}")
    protected String nativeCountry;

    /** Compiled pattern for taxon IDs */
    protected Pattern taxonIDPattern;

    @Value("${media.url:http://biocache.ala.org.au/biocache-media/}")
    protected String biocacheMediaUrl;

    @Value("${facet.config:/data/biocache/config/facets.json}")
    protected String facetConfig;

    @Value("${facets.max:4}")
    protected Integer facetsMax;

    @Value("${facets.defaultmax:0}")
    protected Integer facetsDefaultMax;

    @Value("${facet.default:true}")
    protected Boolean facetDefault;

    /** Max number of threads available to all online solr download queries */
    @Value("${online.downloadquery.maxthreads:30}")
    protected Integer maxOnlineDownloadThreads = 30;

    private ExecutorService executor;
    
    private final AtomicBoolean initialised = new AtomicBoolean(false);
    
    private final CountDownLatch initialisationLatch = new CountDownLatch(1);
    
    @PostConstruct
    public void init() {
        // Avoid starting multiple copies of the initialisation thread by repeat calls to this method
        if(initialised.compareAndSet(false, true)) {
            String nameFormat = "occurrencecontroller-pool-%d";
            executor = Executors.newFixedThreadPool(maxOnlineDownloadThreads,
                    new ThreadFactoryBuilder().setNameFormat(nameFormat).setPriority(Thread.MIN_PRIORITY).build());
            
            //init on a thread because SOLR may not yet be up and waiting can prevent SOLR from starting
            Thread initialisationThread = new Thread() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            try {
                                Set<IndexFieldDTO> indexedFields = searchDAO.getIndexedFields();
        
                                if (indexedFields != null) {
                                    //init FacetThemes static values
                                    new FacetThemes(facetConfig, indexedFields, facetsMax, facetsDefaultMax, facetDefault);
        
                                    //successful
                                    break;
                                }
                            } catch (Exception e) {
                                logger.error("Failed to update indexedFields. Retrying...", e);
                            }
                            try {
                                //wait before trying again
                                Thread.sleep(10000);
                            } catch (InterruptedException e) {
                                break;
                            }
                        }
                    } finally {
                        initialisationLatch.countDown();
                    }
                }
            };
            // Have been having issues with initialisation, set to maximum priority to test if it has benefit
            initialisationThread.setPriority(Thread.MAX_PRIORITY);
            initialisationThread.setName("biocache-occurrencecontroller-initialisation");
            initialisationThread.start();
        }
    }

    /**
     * Call this method at the start of web service calls that require initialisation to be complete before continuing.
     * This blocks until it is either interrupted or the initialisation thread from {@link #init()} is finished (successful or not).
     */
    private final void afterInitialisation() {
        try {
            initialisationLatch.await();
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
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
    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String homePageHandler(Model model) {
        model.addAttribute("webservicesRoot", webservicesRoot);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("/git.properties");
        if(input !=null){
            try {
                Properties versionProperties = new Properties();
                versionProperties.load(input);
                model.addAttribute("versionInfo", versionProperties);

                StringBuffer sb  = new StringBuffer();
                for (String name : versionProperties.stringPropertyNames()){
                    sb.append(name + " : "  + versionProperties.getProperty(name) + "\n");
                }

                model.addAttribute("versionInfoString", sb.toString());

            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
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
    @RequestMapping(value = "/oldapi", method = RequestMethod.GET)
    public String oldApiHandler(Model model) {
        model.addAttribute("webservicesRoot", webservicesRoot);
        return "oldapi";
    }
    
    
    @RequestMapping(value = "/active/download/stats", method = RequestMethod.GET)
    public @ResponseBody List<DownloadDetailsDTO> getCurrentDownloads(){
        return downloadService.getCurrentDownloads();
    }
    
    /**
     * Returns the default facets that are applied to a search
     * @return
     */
    @RequestMapping(value = "/search/facets", method = RequestMethod.GET)
    public @ResponseBody String[] listAllFacets() {
        afterInitialisation();
        return new SearchRequestParams().getFacets();
    }
    
    /**
     * Returns the default facets grouped by themes that are applied to a search
     * @return
     */
    @RequestMapping(value = "/search/grouped/facets", method = RequestMethod.GET)
    public @ResponseBody List groupFacets() throws IOException {
        afterInitialisation();
        return FacetThemes.getAllThemes();
    }
    
    /**
     * Returns the content of the messages.properties file.
     * Can also return language specific versions, such as
     * messages_fr.properties if requested via qualifier @PathVariable.
     *
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = "/facets/i18n{qualifier:.*}*", method = RequestMethod.GET)
    public void writei18nPropertiesFile(@PathVariable("qualifier") String qualifier,
                                        HttpServletRequest request,
                                        HttpServletResponse response) throws Exception {
        afterInitialisation();
        qualifier = (StringUtils.isNotEmpty(qualifier)) ? qualifier : ".properties";
        logger.debug("qualifier = " + qualifier);
        
        //default to external messages.properties
        File f = new File("/data/biocache/config/messages" + qualifier);
        InputStream is;
        if (f.exists() && f.isFile() && f.canRead()) {
            is = FileUtils.getInputStream(f);
        } else {
            is = request.getSession().getServletContext().getResourceAsStream("/WEB-INF/messages" + qualifier);
        }
        try(OutputStream os = response.getOutputStream();) {
            if (is != null) {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1){
                    os.write(buffer, 0, bytesRead);
                }
            }
            
            //append cl* and el* names as field.{fieldId}={display name}
            try {
                Map<String, String> fields = new LayersStore(Config.layersServiceUrl()).getFieldIdsAndDisplayNames();
                for (String fieldId : fields.keySet()) {
                    os.write(("\nfield." + fieldId + "=" + fields.get(fieldId)).getBytes("UTF-8"));
                    os.write(("\nfacet." + fieldId + "=" + fields.get(fieldId)).getBytes("UTF-8"));
                }
            } catch (Exception e) {
                logger.error("failed to add layer names from url: " + Config.layersServiceUrl(), e);
            }
        
            os.flush();
        } finally {
            if(is != null) {
                is.close();
            }
        }
    }
    
    /**
     * Returns a list with the details of the index field
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "index/fields", method = RequestMethod.GET)
    public @ResponseBody Set<IndexFieldDTO> getIndexedFields(@RequestParam(value="fl", required=false) String fields) throws Exception {
        afterInitialisation();
        if(fields == null) {
            return searchDAO.getIndexedFields();
        } else {
            return searchDAO.getIndexFieldDetails(fields.split(","));
        }
    }

    /**
     * Returns current index version number.
     * 
     * Can force the refresh if an apiKey is also provided. e.g. after a known edit.
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "index/version", method = RequestMethod.GET)
    public @ResponseBody Map getIndexedFields(@RequestParam(value="apiKey", required=false) String apiKey,
                                              @RequestParam(value="force", required=false, defaultValue="false") Boolean force,
                                              HttpServletResponse response) throws Exception{
        afterInitialisation();
        
        Long version;
        if (force && shouldPerformOperation(apiKey, response)) {
            version = searchDAO.getIndexVersion(force);
        } else {
            version = searchDAO.getIndexVersion(false);
        }

        return Collections.singletonMap("version", version);
    }

    /**
     * Returns current index version number.
     *
     * Can force the refresh if an apiKey is also provided. e.g. after a known edit.
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "index/maxBooleanClauses", method = RequestMethod.GET)
    public @ResponseBody Map getIndexedFields() throws Exception{

        int m = searchDAO.getMaxBooleanClauses();

        Map map = new HashMap();
        map.put("maxBooleanClauses", m);

        return map;
    }
    
    /**
     * Returns a facet list including the number of distinct values for a field
     * @param requestParams
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "occurrence/facets", method = RequestMethod.GET)
    public @ResponseBody List<FacetResultDTO> getOccurrenceFacetDetails(SpatialSearchRequestParams requestParams) throws Exception{
        afterInitialisation();
        return searchDAO.getFacetCounts(requestParams);
    }

    /**
     * Returns a group list including the number of distinct values for a field, and occurrences.
     *
     * Requires valid apiKey.
     *
     * @param requestParams
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "occurrence/groups", method = RequestMethod.GET)
    public
    @ResponseBody
    List<GroupFacetResultDTO> getOccurrenceGroupDetails(SpatialSearchRequestParams requestParams,
                                                        @RequestParam(value = "apiKey", required = true) String apiKey,
                                                        HttpServletResponse response) throws Exception {
        afterInitialisation();
        if (isValidKey(apiKey)) {
            return searchDAO.searchGroupedFacets(requestParams);
        }

        response.sendError(HttpServletResponse.SC_FORBIDDEN, "An invalid API Key was provided.");
        return null;
        
    }
    
    /**
     * Returns a list of image urls for the supplied taxon guid.
     * An empty list is returned when no images are available.
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/images/taxon/**", method = RequestMethod.GET)
    public @ResponseBody List<String> getImages(HttpServletRequest request) throws Exception {
        afterInitialisation();
        String guid = searchUtils.getGuidFromPath(request);
        SpatialSearchRequestParams srp = new SpatialSearchRequestParams();
        srp.setQ("lsid:" + guid);
        srp.setPageSize(0);
        srp.setFacets(new String[]{"image_url"});
        SearchResultDTO results = searchDAO.findByFulltextSpatialQuery(srp, null);
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
     * @return
     * @throws Exception
     */
    @RequestMapping(value={"/australian/taxon/**", "/native/taxon/**"}, method = RequestMethod.GET)
    public @ResponseBody NativeDTO isAustralian(HttpServletRequest request) throws Exception {
        //check to see if we have any occurrences on Australia  country:Australia or state != empty
        String guid = searchUtils.getGuidFromPath(request);
        NativeDTO adto = new NativeDTO();

        if (guid != null) {
            adto = getIsAustraliaForGuid(guid);
        }

        return adto;
    }

    /**
     * Checks to see if the supplied GUID list has items that represents an Australian species.
     * @param guids - comma separated list of guids
     * @return
     * @throws Exception
     */
    @RequestMapping(value={"/australian/taxa.json*","/australian/taxa*", "/native/taxa.json*","/native/taxa*" }, method = RequestMethod.GET)
    public @ResponseBody List<NativeDTO> isAustralianForList(@RequestParam(value = "guids", required = true) String guids) throws Exception {
        afterInitialisation();
        List<NativeDTO> nativeDTOs = new ArrayList<NativeDTO>();
        String[] guidArray = StringUtils.split(guids, ',');

        if (guidArray !=null) {
            for (String guid : guidArray) {
                nativeDTOs.add(getIsAustraliaForGuid(guid));
                logger.debug("guid = " + guid);
            }
        }

        return nativeDTOs;
    }

    /**
     * Service to determine if a GUID has native or isAustralian status
     * TODO should be in a service
     *
     * @param guid
     * @return
     */
    private NativeDTO getIsAustraliaForGuid(String guid) {
        SpatialSearchRequestParams requestParams = new SpatialSearchRequestParams();
        requestParams.setPageSize(0);
        requestParams.setFacets(new String[]{});
        String query = "lsid:" +guid + " AND " + "(country:\""+nativeCountry+"\" OR state:[* TO *]) AND geospatial_kosher:true";
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
            adto.setHasCSOnly(results.getTotalRecords() == 0);
        }

        return adto;
    }

    /**
     * Returns the complete list of Occurrences
     */
    @RequestMapping(value = {"/occurrences", "/occurrences/collections", "/occurrences/institutions", "/occurrences/dataResources", "/occurrences/dataProviders", "/occurrences/taxa", "/occurrences/dataHubs"}, method = RequestMethod.GET)
    public @ResponseBody SearchResultDTO listOccurrences(Model model) throws Exception {
        afterInitialisation();
        SpatialSearchRequestParams srp = new SpatialSearchRequestParams();
        srp.setQ("*:*");
        return occurrenceSearch(srp);
    }
    
    /**
     * Occurrence search page uses SOLR JSON to display results
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {"/occurrences/taxon/**","/occurrences/taxon/**","/occurrences/taxa/**"}, method = RequestMethod.GET)
    public @ResponseBody SearchResultDTO occurrenceSearchByTaxon(
                                                                 SpatialSearchRequestParams requestParams,
                                                                 HttpServletRequest request) throws Exception {
        afterInitialisation();
        String guid = searchUtils.getGuidFromPath(request);
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
     * @throws Exception
     */
    @RequestMapping(value = "/occurrences/taxon/source/**", method = RequestMethod.GET)
    public @ResponseBody List<OccurrenceSourceDTO> sourceByTaxon(SpatialSearchRequestParams requestParams,
                                                                 HttpServletRequest request) throws Exception {
        afterInitialisation();
        String guid = searchUtils.getGuidFromPath(request);
        requestParams.setQ("lsid:" + guid);
        Map<String, Integer> sources = searchDAO.getSourcesForQuery(requestParams);
        //now turn them to a list of OccurrenceSourceDTO
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
    public @ResponseBody SearchResultDTO occurrenceSearchForUID(SpatialSearchRequestParams requestParams,
                                                                @PathVariable("uid") String uid,
                                                                Model model) throws Exception {
        afterInitialisation();
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
        afterInitialisation();
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
        return occurrenceSearch(requestParams,null,false,null,null);
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
                                                          @RequestParam(value="im", required=false, defaultValue = "false") Boolean lookupImageMetadata,
                                                          HttpServletRequest request,
                                                          HttpServletResponse response) throws Exception {
        afterInitialisation();
        // handle empty param values, e.g. &sort=&dir=
        SearchUtils.setDefaultParams(requestParams);
        Map<String,String[]> map = request != null ? SearchUtils.getExtraParams(request.getParameterMap()) : null;
        if(map != null){
            map.remove("apiKey");
        }
        logger.debug("occurrence search params = " + requestParams + " extra params = " + map);
        
        SearchResultDTO srtdto = null;
        if(apiKey == null){
            srtdto = searchDAO.findByFulltextSpatialQuery(requestParams, map);
        } else {
            srtdto = occurrenceSearchSensitive(requestParams, apiKey, request, response);
        }

        if(srtdto.getTotalRecords() > 0 && lookupImageMetadata){
            //use the image service API & grab the list of IDs
            List<String> occurrenceIDs = new ArrayList<String>();
            for(OccurrenceIndex oi : srtdto.getOccurrences()){
                occurrenceIDs.add(oi.getUuid());
            }
            
            Map<String, List<Map<String, Object>>> imageMap = imageMetadataService.getImageMetadataForOccurrences(occurrenceIDs);
            
            for(OccurrenceIndex oi : srtdto.getOccurrences()){
                //lookup metadata
                List<Map<String, Object>> imageMetadata = imageMap.get(oi.getUuid());
                oi.setImageMetadata(imageMetadata);
            }
        }
        return srtdto;
    }
    
    public @ResponseBody SearchResultDTO occurrenceSearchSensitive(SpatialSearchRequestParams requestParams,
                                                                   @RequestParam(value="apiKey", required=true) String apiKey,
                                                                   HttpServletRequest request,
                                                                   HttpServletResponse response) throws Exception {
        afterInitialisation();
        // handle empty param values, e.g. &sort=&dir=
        if(shouldPerformOperation(apiKey, response, false)){
            SearchUtils.setDefaultParams(requestParams);
            Map<String,String[]> map = SearchUtils.getExtraParams(request.getParameterMap());
            if(map != null){
                map.remove("apiKey");
            }
            logger.debug("occurrence search params = " + requestParams);
            SearchResultDTO searchResult = searchDAO.findByFulltextSpatialQuery(requestParams, true, map);
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

        //update FacetThemes static values
        new FacetThemes(facetConfig, searchDAO.getIndexedFields(), facetsMax, facetsDefaultMax, facetDefault);

        cacheManager.clearAll();
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
            @RequestParam(value="count", required=false, defaultValue="false") boolean includeCount,
            @RequestParam(value="lookup" ,required=false, defaultValue="false") boolean lookupName,
            @RequestParam(value="synonym", required=false, defaultValue="false") boolean includeSynonyms,
            @RequestParam(value = "lists", required = false, defaultValue = "false") boolean includeLists,
            @RequestParam(value="ip", required=false) String ip,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {
        afterInitialisation();
        if(requestParams.getFacets().length > 0){
            ip = ip == null ? getIPAddress(request) : ip;
            DownloadDetailsDTO dd = downloadService.registerDownload(requestParams, ip, DownloadDetailsDTO.DownloadType.FACET);
            try {
                String filename = requestParams.getFile() != null ? requestParams.getFile() : requestParams.getFacets()[0];
                response.setHeader("Cache-Control", "must-revalidate");
                response.setHeader("Pragma", "must-revalidate");
                response.setHeader("Content-Disposition", "attachment;filename=" + filename + ".csv");
                response.setContentType("text/csv");
                searchDAO.writeFacetToStream(requestParams, includeCount, lookupName, includeSynonyms, includeLists, response.getOutputStream(), dd);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
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
        afterInitialisation();
        
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
                                HttpServletRequest request,
                                @Valid final DownloadRequestParams params,
                                BindingResult result,
                                @RequestParam(value="file", required = true) String filepath,
                                @RequestParam(value="directory", required = true, defaultValue = "/data/biocache-exports") final String directory,
                                @RequestParam(value="ip", required=false) String ip,
                                Model model
                                ) throws Exception {
        afterInitialisation();
        
        if(result.hasErrors()){
            if(logger.isInfoEnabled()) {
                logger.info("validation failed  " + result.getErrorCount() + " checks");
                if(logger.isDebugEnabled()) {
                    logger.debug(result.toString());
                }
            }
            model.addAttribute("errorMessage", getValidationErrorMessage(result));
            //response.setStatus(response.SC_INTERNAL_SERVER_ERROR);
            return VALIDATION_ERROR;//result.toString();
        }
        
        final File file = new File(filepath);
        
        final SpeciesLookupService mySpeciesLookupService = this.speciesLookupService;
        ip = ip == null?getIPAddress(request):ip;
        final DownloadDetailsDTO dd = downloadService.registerDownload(params, ip, DownloadType.RECORDS_INDEX);
        
        if(file.exists()){
            Runnable t = new Runnable(){
                @Override
                public void run() {
                    long executionDelay = 10 + Math.round(Math.random() * 50);
                    try(CSVReader reader  = new CSVReader(new FileReader(file));){
                        String[] row = reader.readNext();
                        while(row != null){
                            // Reduce congestion on db/index by artificially sleeping
                            // for a random amount of time between rows in the batch file
                            Thread.sleep(executionDelay);
                            
                            //get an lsid for the name
                            String lsid = mySpeciesLookupService.getGuidForName(row[0]);
                            if(lsid != null){
                                try {
                                    //download records for this row
                                    String outputFilePath = directory + File.separatorChar + row[0].replace(" ", "_") + ".txt";
                                    String citationFilePath = directory + File.separatorChar + row[0].replace(" ", "_") + "_citations.txt";
                                    if(logger.isDebugEnabled()) {
                                        logger.debug("Outputting results to:" + outputFilePath + ", with LSID: " + lsid);
                                    }
                                    try(FileOutputStream output = new FileOutputStream(outputFilePath);) {
                                        params.setQ("lsid:\""+lsid+"\"");
                                        ConcurrentMap<String, AtomicInteger> uidStats = searchDAO.writeResultsFromIndexToStream(params, new CloseShieldOutputStream(output), false, dd,false);
                                        output.flush();
                                        try(FileOutputStream citationOutput = new FileOutputStream(citationFilePath);) {
                                            downloadService.getCitations(uidStats, citationOutput, params.getSep(), params.getEsc(), null);
                                            citationOutput.flush();
                                        }
                                    }
                                } catch (Exception e){
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
            executor.submit(t);
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
        afterInitialisation();
        
        logger.info("/occurrences/batchSearch with action=Search");
        Long qid =  getQidForBatchSearch(queries, field, separator, title);
        
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
     * @throws QidSizeException
     */
    private Long getQidForBatchSearch(String listOfNames, String field, String separator, String title) throws IOException, QidSizeException {
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
        String qid = qidCacheDao.put(q, title, null, null, null, -1, null);
        logger.info("batchSearch: qid = " + qid);
        
        return Long.parseLong(qid);
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
        afterInitialisation();
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
                                     @RequestParam(value="zip", required=false, defaultValue="true") Boolean zip,
                                     Model model,
                                     HttpServletResponse response,
                                     HttpServletRequest request) throws Exception {
        afterInitialisation();

        //check to see if the DownloadRequestParams are valid
        if(result.hasErrors()){
            logger.info("validation failed  " + result.getErrorCount() + " checks");
            logger.debug(result.toString());
            model.addAttribute("errorMessage", getValidationErrorMessage(result));
            //response.setStatus(response.SC_INTERNAL_SERVER_ERROR);
            return VALIDATION_ERROR;//result.toString();
        }
        
        ip = ip == null?getIPAddress(request):ip;//request.getRemoteAddr():ip;
        //search params must have a query or formatted query for the downlaod to work
        if (requestParams.getQ().isEmpty() && requestParams.getFormattedQuery().isEmpty()) {
            return null;
        }
        if(apiKey != null){
            return occurrenceSensitiveDownload(requestParams, apiKey, ip, false, zip, response, request);
        }

        try {
            ServletOutputStream out = response.getOutputStream();
            downloadService.writeQueryToStream(requestParams, response, ip, new CloseShieldOutputStream(out), false, false, zip, executor);
        } catch (Exception e){
            logger.error(e.getMessage(),e);
        }
        return null;
    }
    
    @RequestMapping(value = "/occurrences/index/download*", method = RequestMethod.GET)
    public String occurrenceIndexDownload(@Valid DownloadRequestParams requestParams,
                                          BindingResult result,
                                          @RequestParam(value="apiKey", required=false) String apiKey,
                                          @RequestParam(value="ip", required=false) String ip,
                                          @RequestParam(value="zip", required=false, defaultValue="true") Boolean zip,
                                          Model model,
                                          HttpServletResponse response,
                                          HttpServletRequest request) throws Exception{
        afterInitialisation();
        
        if(result.hasErrors()){
            logger.info("validation failed  " + result.getErrorCount() + " checks");
            logger.debug(result.toString());
            model.addAttribute("errorMessage", getValidationErrorMessage(result));
            //response.setStatus(response.SC_INTERNAL_SERVER_ERROR);
            return VALIDATION_ERROR;//result.toString();
        }
        
        ip = ip == null ? getIPAddress(request) : ip;

        //search params must have a query or formatted query for the download to work
        if (requestParams.getQ().isEmpty() && requestParams.getFormattedQuery().isEmpty()) {
            return null;
        }
        if(apiKey != null){
            occurrenceSensitiveDownload(requestParams, apiKey, ip, true, zip, response, request);
            return null;
        }
        try {
            ServletOutputStream out = response.getOutputStream();
            downloadService.writeQueryToStream(requestParams, response, ip, new CloseShieldOutputStream(out), false, true, zip, executor);
        } catch(Exception e){
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    public String occurrenceSensitiveDownload(
                                              DownloadRequestParams requestParams,
                                              String apiKey,
                                              String ip,
                                              boolean fromIndex,
                                              boolean zip,
                                              HttpServletResponse response,
                                              HttpServletRequest request) throws Exception {
        afterInitialisation();
        
        if(shouldPerformOperation(apiKey, response, false)){
            ip = ip == null?getIPAddress(request):ip;

            //search params must have a query or formatted query for the downlaod to work
            if (requestParams.getQ().isEmpty() && requestParams.getFormattedQuery().isEmpty()) {
                return null;
            }

            try {
                ServletOutputStream out = response.getOutputStream();
                downloadService.writeQueryToStream(requestParams, response, ip, new CloseShieldOutputStream(out), true, fromIndex, zip, executor);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }

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
        afterInitialisation();
        
        logger.debug(String.format("Received lat: %f, lon:%f, radius:%f", requestParams.getLat(),
                requestParams.getLon(), requestParams.getRadius()));
        
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
        afterInitialisation();
        requestParams.setFacets(new String[]{"lat_long"});
        if(requestParams.getQ().length()<1)
            requestParams.setQ("*:*");
        try {
            ServletOutputStream out = response.getOutputStream();
            searchDAO.writeCoordinatesToStream(requestParams,out);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
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
        afterInitialisation();
        Map values = OccurrenceUtils.getComparisonByUuid(uuid);
        if(values.isEmpty()) {
            // Try again, better the second time around?
            values = OccurrenceUtils.getComparisonByUuid(uuid);
        }
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
        afterInitialisation();
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
                                                       HttpServletResponse response) throws Exception {
        afterInitialisation();

        String[] deletedRecords = new String[0];
        try {
            //date must be in a yyyy-MM-dd format
            Date date = org.apache.commons.lang.time.DateUtils.parseDate(fromDate,new String[]{"yyyy-MM-dd"});
            deletedRecords = Store.getDeletedRecords(date);
            if(deletedRecords == null) {
                deletedRecords = new String[0];
            }
        } catch(java.text.ParseException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid date format.  Please provide date as yyyy-MM-dd.");
        } catch(Exception e) {
            logger.error(e.getMessage(), e);
            response.sendError(500, "Problem retrieving details of deleted records.");
        }
        return deletedRecords;
    }

    /**
     * API method for submitting a single occurrence record for a data resource.
     * This method should __not__ be used for bulk data loading.
     *
     * @param dataResourceUid
     * @param request
     * @param response
     * @return
     * @throws Exception
     */
    @RequestMapping(value={"/occurrence/{dataResourceUid}"}, method = RequestMethod.POST)
    public @ResponseBody Object uploadSingleRecord(
            @PathVariable String dataResourceUid,
            @RequestParam(value = "apiKey", required = true) String apiKey,
            @RequestParam(value = "index", required = true, defaultValue = "true") Boolean index,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {
        afterInitialisation();

        //auth check
        boolean apiKeyValid = shouldPerformOperation(request, response);
        if(!apiKeyValid){
            return null;
        }

        //create record
        try {
            ObjectMapper om = new ObjectMapper();

            Map<String, Object> properties = om.readValue(request.getInputStream(), Map.class);
            List<Map<String, String>> multimedia = (List<Map<String, String>>) properties.remove("multimedia");

            Map<String, String> darwinCore  = new HashMap<String,String>();
            for(Map.Entry<String, Object> entry : properties.entrySet()){
                darwinCore.put(entry.getKey(), entry.getValue().toString());
            }

            FullRecord occurrence = Store.upsertRecord(dataResourceUid, darwinCore, multimedia, index);
            response.setContentType("application/json");
            response.setHeader("Location", webservicesRoot + "/occurrence/" + occurrence.getUuid());
            response.setStatus(HttpServletResponse.SC_CREATED);

            Map<String, Object> map = new HashMap<String, Object>();
            map.put("occurrenceID", occurrence.getUuid());
            map.put("images", occurrence.getOccurrence().getImages());
            return map;

        } catch (Exception e){
            logger.error(e.getMessage(), e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
            return null;
        }
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
                                               @RequestParam(value="im", required=false) String im,
                                               HttpServletRequest request, HttpServletResponse response) throws Exception {
        afterInitialisation();
        ip = ip == null?getIPAddress(request):ip;
        if(apiKey != null){
            return showSensitiveOccurrence(uuid, apiKey, ip, im, request, response);
        }
        return getOccurrenceInformation(uuid, ip, im, request, false);
    }
    
    @RequestMapping(value = {"/sensitive/occurrence/{uuid:.+}","/sensitive/occurrences/{uuid:.+}", "/sensitive/occurrence/{uuid:.+}.json", "/senstive/occurrences/{uuid:.+}.json"}, method = RequestMethod.GET)
    public @ResponseBody Object showSensitiveOccurrence(@PathVariable("uuid") String uuid,
                                                        @RequestParam(value="apiKey", required=true) String apiKey,
                                                        @RequestParam(value="ip", required=false) String ip,
                                                        @RequestParam(value="im", required=false) String im,
                                                        HttpServletRequest request, HttpServletResponse response) throws Exception {
        afterInitialisation();
        ip = ip == null ? getIPAddress(request) : ip;
        if(shouldPerformOperation(apiKey, response)){
            return getOccurrenceInformation(uuid, ip, im, request, true);
        }
        return null;
    }
    
    private Object getOccurrenceInformation(String uuid, String ip, String im, HttpServletRequest request, boolean includeSensitive) throws Exception{
        logger.debug("Retrieving occurrence record with guid: '" + uuid + "'");
        
        FullRecord[] fullRecord = OccurrenceUtils.getAllVersionsByUuid(uuid, includeSensitive);
        if(fullRecord == null){
            //get the rowKey for the supplied uuid in the index
            //This is a workaround.  There seems to be an issue on Cassandra with retrieving uuids that start with e or f
            SpatialSearchRequestParams srp = new SpatialSearchRequestParams();
            srp.setQ("id:" + uuid);
            srp.setPageSize(1);
            srp.setFacets(new String[]{});
            SearchResultDTO results = occurrenceSearch(srp);
            if(results.getTotalRecords()>0) {
                fullRecord = OccurrenceUtils.getAllVersionsByUuid(results.getOccurrences().get(0).getUuid(), includeSensitive);
            }
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
                fullRecord = OccurrenceUtils.getAllVersionsByUuid(result.getOccurrences().get(0).getUuid(), includeSensitive);
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
            } else if(fullRecord[1].getOccurrence().getUserId() != null){
                occ.setAlaUserName(authService.getDisplayNameFor(fullRecord[1].getOccurrence().getUserId()));
            }
        }
        
        //assertions are based on the row key not uuid
        occ.setSystemAssertions(Store.getAllSystemAssertions(occ.getRaw().getUuid()));
        
        occ.setUserAssertions(assertionUtils.getUserAssertions(occ));
        
        //retrieve details of the media files
        List<MediaDTO> soundDtos = getSoundDtos(occ);
        if(!soundDtos.isEmpty()){
            occ.setSounds(soundDtos);
        }
        
        //ADD THE DIFFERENT IMAGE FORMATS...thumb,small,large,raw
        //default lookupImageMetadata to "true"
        setupImageUrls(occ, im == null || !im.equalsIgnoreCase("false"));
        
        //fix media store URLs
        Config.mediaStore().convertPathsToUrls(occ.getRaw(), biocacheMediaUrl);
        Config.mediaStore().convertPathsToUrls(occ.getProcessed(), biocacheMediaUrl);
        
        //log the statistics for viewing the record
        logViewEvent(ip, occ, null, "Viewing Occurrence Record " + uuid);
        
        return occ;
    }
    
    private void logViewEvent(String ip, OccurrenceDTO occ, String email, String reason) {
        //String ip = request.getLocalAddr();
        ConcurrentMap<String, AtomicInteger> uidStats = new ConcurrentHashMap<>();
        if(occ.getProcessed() != null && occ.getProcessed().getAttribution()!=null){
            if (occ.getProcessed().getAttribution().getCollectionUid() != null) {
                uidStats.put(occ.getProcessed().getAttribution().getCollectionUid(), new AtomicInteger(1));
            }
            if (occ.getProcessed().getAttribution().getInstitutionUid() != null) {
                uidStats.put(occ.getProcessed().getAttribution().getInstitutionUid(), new AtomicInteger(1));
            }
            if(occ.getProcessed().getAttribution().getDataProviderUid() != null) {
                uidStats.put(occ.getProcessed().getAttribution().getDataProviderUid(), new AtomicInteger(1));
            }
            if(occ.getProcessed().getAttribution().getDataResourceUid() != null) {
                uidStats.put(occ.getProcessed().getAttribution().getDataResourceUid(), new AtomicInteger(1));
            }
        }

        //remove header entries from uidStats
        if (uidStats != null) {
            List<String> toRemove = new ArrayList<String>();
            for (String key : uidStats.keySet()) {
                if (uidStats.get(key).get() < 0) {
                    toRemove.add(key);
                }
            }
            for (String key : toRemove) {
                uidStats.remove(key);
            }
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
            for(String soundFile: sounds){
                MediaDTO m = new MediaDTO();
                Map<String,String> mimeToUrl = Config.mediaStore().getSoundFormats(soundFile);
                for(String mimeType: mimeToUrl.keySet()){
                    m.getAlternativeFormats().put(mimeType, mimeToUrl.get(mimeType));
                }
                soundDtos.add(m);
            }
        }
        return soundDtos;
    }
    
    private void setupImageUrls(OccurrenceDTO dto, boolean lookupImageMetadata) {
        String[] images = dto.getProcessed().getOccurrence().getImages();
        if(images != null && images.length > 0){
            List<MediaDTO> ml = new ArrayList<MediaDTO>();

            Map<String, Map> metadata = new HashMap();
            if (lookupImageMetadata) {
                try {
                    String uuid = dto.getProcessed().getUuid();
                    List<Map<String, Object>> list = imageMetadataService.getImageMetadataForOccurrences(Arrays.asList(new String[]{uuid})).get(uuid);
                    if (list != null) {
                        for (Map m : list) {
                            metadata.put(String.valueOf(m.get("imageId")), m);
                        }
                    }
                } catch (Exception e) {
                }
            }

            for(String fileNameOrID: images){
                try {
                    MediaDTO m = new MediaDTO();
                    Map<String, String> urls = Config.mediaStore().getImageFormats(fileNameOrID);
                    m.getAlternativeFormats().put("thumbnailUrl", urls.get("thumb"));
                    m.getAlternativeFormats().put("smallImageUrl", urls.get("small"));
                    m.getAlternativeFormats().put("largeImageUrl", urls.get("large"));
                    m.getAlternativeFormats().put("imageUrl", urls.get("raw"));
                    m.setFilePath(fileNameOrID);
                    m.setMetadataUrl(imageMetadataService.getUrlFor(fileNameOrID));

                    if (metadata != null && metadata.get(fileNameOrID) != null) {
                        m.setMetadata(metadata.get(fileNameOrID));
                    }
                    ml.add(m);
                } catch (Exception ex) {
                    logger.warn("Unable to get image data for " + fileNameOrID + ": " + ex.getMessage());
                }
            }
            dto.setImages(ml);
        }
    }

    /**
     * Perform one pivot facet query.
     * <p/>
     * Requires valid apiKey.
     * <p/>
     * facets is the pivot facet list
     */
    @RequestMapping("occurrence/pivot")
    public
    @ResponseBody
    List<FacetPivotResultDTO> searchPivot(SpatialSearchRequestParams searchParams,
                                          @RequestParam(value = "apiKey", required = true) String apiKey,
                                          HttpServletResponse response) throws Exception {
        afterInitialisation();
        if (isValidKey(apiKey)) {
            return searchDAO.searchPivot(searchParams);
        }

        response.sendError(HttpServletResponse.SC_FORBIDDEN, "An invalid API Key was provided.");
        return null;
    }
}