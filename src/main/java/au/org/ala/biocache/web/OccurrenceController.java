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
import au.com.bytecode.opencsv.CSVWriter;
import au.org.ala.biocache.dao.IndexDAO;
import au.org.ala.biocache.dao.QidCacheDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dao.SearchDAOImpl;
import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import au.org.ala.biocache.service.*;
import au.org.ala.biocache.util.OccurrenceUtils;
import au.org.ala.biocache.util.QidSizeException;
import au.org.ala.biocache.util.SearchUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.sf.json.JSONArray;
import org.ala.client.model.LogEventType;
import org.ala.client.model.LogEventVO;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.utils.file.FileUtils;
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
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static au.org.ala.biocache.dto.DuplicateRecordDetails.ASSOCIATED;
import static au.org.ala.biocache.dto.DuplicateRecordDetails.REPRESENTATIVE;
import static au.org.ala.biocache.dto.OccurrenceIndex.*;

/**
 * Occurrences controller for the biocache web services.
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 * @author "Natasha Carter <Natasha.Carter@csiro.au>"
 */
@Controller
public class OccurrenceController extends AbstractSecureController {
    /**
     * Logger initialisation
     */
    private static final Logger logger = Logger.getLogger(OccurrenceController.class);
    public static final String LEGACY_REPRESENTATIVE_RECORD_VALUE = "R";
    public static final String LEGACY_DUPLICATE_RECORD_VALUE = "D";
    public static final String RAW_FIELD_PREFIX = "raw_";
    public static final String DYNAMIC_PROPERTIES_PREFIX = "dynamicProperties_";

    /**
     * Fulltext search DAO
     */
    @Inject
    protected SearchDAO searchDAO;

    @Inject
    protected IndexDAO indexDao;

    /**
     * Data Resource DAO
     */
    @Inject
    protected SearchUtils searchUtils;

    @Inject
    protected SpeciesLookupService speciesLookupService;
    @Inject
    protected AuthService authService;
    @Inject
    protected OccurrenceUtils occurrenceUtils;
    @Inject
    protected DownloadService downloadService;
    @Inject
    protected LoggerService loggerService;
    @Inject
    private AbstractMessageSource messageSource;
    @Inject
    private ImageMetadataService imageMetadataService;
    @Inject
    protected Validator validator;
    @Inject
    protected QidCacheDAO qidCacheDao;
    @Inject
    private LayersService layersService;

    @Inject
    private AssertionService assertionService;

    /**
     * Name of view for site home page
     */
    private final String HOME = "homePage";

    private final String VALIDATION_ERROR = "error/validationError";

    @Value("${webservices.root:http://localhost:8080/biocache-service}")
    protected String webservicesRoot;

    @Value("${media.store.url:}")
    private String remoteMediaStoreUrl;

    /**
     * The response to be returned for the isAustralian test
     */
    @Value("${taxon.id.pattern:urn:lsid:biodiversity.org.au[a-zA-Z0-9\\.:-]*|http://id.biodiversity.org.au/[a-zA-Z0-9/]*}")
    protected String taxonIDPatternString;

    @Value("${native.country:Australia}")
    protected String nativeCountry;

    /**
     * Compiled pattern for taxon IDs
     */
    protected Pattern taxonIDPattern;

    @Value("${media.url:https://biocache.ala.org.au/biocache-media/}")
    protected String biocacheMediaUrl;

    @Value("${facet.config:/data/biocache/config/facets.json}")
    protected String facetConfig;

    @Value("${facets.max:4}")
    public Integer facetsMax;

    @Value("${facets.defaultmax:0}")
    public Integer facetsDefaultMax;

    @Value("${facet.default:true}")
    public Boolean facetDefault;

    /**
     * Max number of threads available to all online solr download queries
     */
    @Value("${online.downloadquery.maxthreads:30}")
    protected Integer maxOnlineDownloadThreads = 30;

    /**
     * The public or private value to use in the Cache-Control HTTP header for aggregated results.
     * Defaults to public
     */
    @Value("${occurrence.cache.cachecontrol.publicorprivate:public}")
    private String occurrenceCacheControlHeaderPublicOrPrivate;

    /**
     * The max-age value to use in the Cache-Control HTTP header for aggregated results. Defaults to
     * 86400, equivalent to 1 day
     */
    @Value("${occurrence.cache.cachecontrol.maxage:86400}")
    private String occurrenceCacheControlHeaderMaxAge;

    @Value("${occurrence.log.enabled:true}")
    private boolean occurrenceLogEnabled = true;

    private final AtomicReference<String> occurrenceETag = new AtomicReference<>(UUID.randomUUID().toString());

    private ExecutorService executor;

    @PostConstruct
    public void init() throws Exception {

        logger.debug("Initialising OccurrenceController");

        String nameFormat = "occurrencecontroller-pool-%d";
        executor = Executors.newFixedThreadPool(maxOnlineDownloadThreads,
                new ThreadFactoryBuilder().setNameFormat(nameFormat).setPriority(Thread.MIN_PRIORITY).build());

        Set<IndexFieldDTO> indexedFields = indexDao.getIndexedFields();

        if (indexedFields != null) {
            //init FacetThemes static values
            new FacetThemes(facetConfig, indexedFields, facetsMax, facetsDefaultMax, facetDefault);
        }
    }

    public Pattern getTaxonIDPattern() {
        if (taxonIDPattern == null) {
            taxonIDPattern = Pattern.compile(taxonIDPatternString);
        }
        return taxonIDPattern;
    }

    /**
     * Need to initialise the validator to be used otherwise the @Valid annotation will not work
     *
     * @param binder
     */
    @InitBinder
    protected void initBinder(WebDataBinder binder) {
        binder.setValidator(validator);
    }

    /**
     * Custom handler for the welcome view.
     *
     * <p>Note that this handler relies on the RequestToViewNameTranslator to determine the logical
     * view name based on the request URL: "/welcome.do" -&gt; "welcome".
     *
     * @return viewname to render
     */
    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String homePageHandler(Model model) {
        model.addAttribute("webservicesRoot", webservicesRoot);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("/git.properties");
        if (input != null) {
            try {
                Properties versionProperties = new Properties();
                versionProperties.load(input);
                model.addAttribute("versionInfo", versionProperties);

                StringBuffer sb = new StringBuffer();
                for (String name : versionProperties.stringPropertyNames()) {
                    sb.append(name + " : " + versionProperties.getProperty(name) + "\n");
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
     *
     * <p>Note that this handler relies on the RequestToViewNameTranslator to determine the logical
     * view name based on the request URL: "/welcome.do" -&gt; "welcome".
     *
     * @return viewname to render
     */
    @RequestMapping(value = "/oldapi", method = RequestMethod.GET)
    public String oldApiHandler(Model model) {
        model.addAttribute("webservicesRoot", webservicesRoot);
        return "oldapi";
    }

    @RequestMapping(value = {"/upload/dynamicFacets", "/upload/dynamicFacets.json" }, method = RequestMethod.GET)
    public @ResponseBody
    Map emptyJson() {
        Map<String, String> map = new HashMap();
        return map;
    }

    @RequestMapping(value = { "/active/download/stats", "active/download/stats.json" }, method = RequestMethod.GET)
    public @ResponseBody
    List<DownloadDetailsDTO> getCurrentDownloads() {
        return downloadService.getCurrentDownloads();
    }

    /**
     * Returns the default facets that are applied to a search
     *
     * @return
     */
    @RequestMapping(value = { "/search/facets", "/search/facets.json" }, method = RequestMethod.GET)
    public @ResponseBody
    String[] listAllFacets() {
        return new SearchRequestParams().getFacets();
    }

    /**
     * Returns the default facets grouped by themes that are applied to a search
     *
     * @return
     */
    @RequestMapping(value = { "/search/grouped/facets", "/search/grouped/facets.json" }, method = RequestMethod.GET)
    public @ResponseBody
    List groupFacets() throws IOException {
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
        response.setHeader("Content-Type", "text/plain; charset=UTF-8");
        response.setCharacterEncoding("UTF-8");
        response.setHeader("Cache-Control", occurrenceCacheControlHeaderPublicOrPrivate + ", max-age=" + occurrenceCacheControlHeaderMaxAge);
        response.setHeader("ETag", occurrenceETag.get());

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
        try (OutputStream os = response.getOutputStream();) {
            if (is != null) {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                    os.write(buffer, 0, bytesRead);
                }
            }

            //append cl* and el* names as field.{fieldId}={display name}
            if (StringUtils.isNotEmpty(layersService.getLayersServiceUrl())) {
                try {
                    Map<String, String> fields = layersService.getLayerNameMap();
                    for (Map.Entry<String, String> field : fields.entrySet()) {
                        os.write(("\nfield." + field.getKey() + "=" + field.getValue()).getBytes(StandardCharsets.UTF_8));
                        os.write(("\nfacet." + field.getKey() + "=" + field.getValue()).getBytes(StandardCharsets.UTF_8));
                    }
                } catch (Exception e) {
                    logger.error(
                            "failed to add layer names from url: " + layersService.getLayersServiceUrl(), e);
                }
            }
            os.flush();
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    /**
     * Returns a list with the details of the index field
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = { "index/fields", "index/fields.json" }, method = RequestMethod.GET)
    public @ResponseBody
    Collection<IndexFieldDTO> getIndexedFields(
            @RequestParam(value = "fl", required = false) String fields,
            @RequestParam(value = "indexed", required = false) Boolean indexed,
            @RequestParam(value = "stored", required = false) Boolean stored,
            @RequestParam(value = "multivalue", required = false) Boolean multivalue,
            @RequestParam(value = "dataType", required = false) String dataType,
            @RequestParam(value = "classs", required = false) String classs,
            @RequestParam(value = "isMisc", required = false, defaultValue = "false") Boolean isMisc,
            @RequestParam(value = "deprecated", required = false, defaultValue = "false") Boolean isDeprecated,
            @RequestParam(value = "isDwc", required = false) Boolean isDwc) throws Exception {

        Set<IndexFieldDTO> result;
        if (fields == null) {
            result = indexDao.getIndexedFields();
        } else {
            result = indexDao.getIndexFieldDetails(fields.split(","));
        }

        if (indexed != null || stored != null || multivalue != null || dataType != null || classs != null || !isMisc) {
            Set<IndexFieldDTO> filtered = new HashSet();
            Set<String> dataTypes = dataType == null ? null : new HashSet(Arrays.asList(dataType.split(",")));
            Set<String> classss = classs == null ? null : new HashSet(Arrays.asList(classs.split(",")));
            for (IndexFieldDTO i : result) {
                if ((indexed == null || i.isIndexed() == indexed)
                        && (stored == null || i.isStored() == stored)
                        && (multivalue == null || i.isMultivalue() == multivalue)
                        && (dataType == null || dataTypes.contains(i.getDataType()))
                        && (classs == null || classss.contains(i.getClasss()))
                        && (isMisc || !i.getName().startsWith("dynamicProperties_"))
                        && (isDeprecated || !i.isDeprecated())
                        && (isDwc == null || isDwc == StringUtils.isNotEmpty(i.getDwcTerm()))) {

                    filtered.add(i);
                }
            }
            result = filtered;
        }

        List myList = new ArrayList(result);
        Collections.sort(myList, new Comparator<IndexFieldDTO>() {
            @Override
            public int compare(IndexFieldDTO o1, IndexFieldDTO o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });

        return myList;
    }

    /**
     * Returns a list with the details of the index field
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "index/fields.csv", method = RequestMethod.GET)
    public void getIndexedFields(
            @RequestParam(value = "fl", required = false) String fields,
            @RequestParam(value = "indexed", required = false) Boolean indexed,
            @RequestParam(value = "stored", required = false) Boolean stored,
            @RequestParam(value = "multivalue", required = false) Boolean multivalue,
            @RequestParam(value = "dataType", required = false) String dataType,
            @RequestParam(value = "classs", required = false) String classs,
            @RequestParam(value = "isMisc", required = false, defaultValue = "false") Boolean isMisc,
            @RequestParam(value = "deprecated", required = false, defaultValue = "false") Boolean isDeprecated,
            @RequestParam(value = "isDwc", required = false) Boolean isDwc,
            HttpServletResponse response
    ) throws Exception {

        Collection<IndexFieldDTO> indexedFields = getIndexedFields(fields,
                indexed,
                stored,
                multivalue,
                dataType,
                classs,
                isMisc,
                isDeprecated,
                isDwc);

        response.setHeader("Cache-Control", "must-revalidate");
        response.setHeader("Pragma", "must-revalidate");
        response.setHeader("Content-Disposition", "attachment;filename=index-fields.csv");
        response.setContentType("text/csv");

        CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(response.getOutputStream()));

        csvWriter.writeNext(new String[]{
                "name",
                "dwc term",
                "dwc category",
                "info",
                "dataType",
                "description",
                "download name",
                "download description",
                "json name",
                "stored",
                "indexed",
                "multivalue",
                "deprecated",
                "newFieldName"
        });

        Iterator<IndexFieldDTO> iter = indexedFields.iterator();
        while (iter.hasNext()) {
            IndexFieldDTO indexField = iter.next();
            csvWriter.writeNext(new String[]{
                    indexField.getName(),
                    indexField.getDwcTerm(),
                    indexField.getClasss(),
                    indexField.getInfo(),
                    indexField.getDataType(),
                    indexField.getDescription(),
                    indexField.getDownloadName(),
                    indexField.getDownloadDescription(),
                    indexField.getJsonName(),
                    Boolean.toString(indexField.isStored()),
                    Boolean.toString(indexField.isIndexed()),
                    Boolean.toString(indexField.isMultivalue()),
                    Boolean.toString((indexField.isDeprecated())),
                    indexField.getNewFieldName()
            });
        }

        csvWriter.flush();
        csvWriter.close();
    }

    /**
     * Returns current index version number.
     * <p>
     * Can force the refresh if an apiKey is also provided. e.g. after a known edit.
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = { "index/version", "index/version.json" }, method = RequestMethod.GET)
    public @ResponseBody
    Map getIndexedFields(@RequestParam(value = "apiKey", required = false) String apiKey,
                         @RequestParam(value = "force", required = false, defaultValue = "false") Boolean force,
                         HttpServletResponse response) throws Exception {
        Long version;
        if (force && shouldPerformOperation(apiKey, response)) {
            version = indexDao.getIndexVersion(force);
        } else {
            version = indexDao.getIndexVersion(false);
        }

        return Collections.singletonMap("version", version);
    }

    /**
     * Returns current maxBooleanClauses
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = { "index/maxBooleanClauses", "index/maxBooleanClauses.json" }, method = RequestMethod.GET)
    public @ResponseBody
    Map getIndexedFields() throws Exception {

        int m = searchDAO.getMaxBooleanClauses();

        Map map = new HashMap();
        map.put("maxBooleanClauses", m);

        return map;
    }

    /**
     * Public service that reports limits and other useful config for clients.
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = { "config", "config.json" }, method = RequestMethod.GET)
    public @ResponseBody
    Map getConfig() {
        Map map = new HashMap();

        map.put("maxBooleanClauses", searchDAO.getMaxBooleanClauses());
        map.put("facet.default", facetDefault);
        map.put("facets.defaultmax", facetsDefaultMax);
        map.put("facets.max", facetsMax);

        if (searchDAO instanceof SearchDAOImpl) {
            SearchDAOImpl dao = (SearchDAOImpl) searchDAO;
            map.put("download.max", dao.MAX_DOWNLOAD_SIZE);
            map.put("download.unzipped.limit", dao.unzippedLimit);
        }

        map.put("citations.enabled", downloadService.citationsEnabled);
        map.put("headings.enabled", downloadService.headingsEnabled);
        map.put("zip.file.size.mb.max", downloadService.maxMB);
        map.put("download.offline.max.size", downloadService.dowloadOfflineMaxSize);

        return map;
    }


    /**
     * Returns a facet list including the number of distinct values for a field
     *
     * @param requestParams
     * @return
     * @throws Exception
     */
    @RequestMapping(value = { "occurrence/facets", "occurrence/facets.json" }, method = RequestMethod.GET)
    public @ResponseBody
    List<FacetResultDTO> getOccurrenceFacetDetails(
            SpatialSearchRequestParams requestParams,
            HttpServletResponse response
    ) throws Exception {
        return searchDAO.getFacetCounts(requestParams);
    }

    /**
     * Returns a list of image urls for the supplied taxon guid.
     * An empty list is returned when no images are available.
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/images/taxon/**", method = RequestMethod.GET)
    public @ResponseBody
    List<String> getImages(HttpServletRequest request) throws Exception {
        String guid = searchUtils.getGuidFromPath(request);
        SpatialSearchRequestParams srp = new SpatialSearchRequestParams();
        srp.setQ("lsid:" + guid);
        srp.setPageSize(0);
        srp.setFacets(new String[]{OccurrenceIndex.IMAGE_URL});
        SearchResultDTO results = searchDAO.findByFulltextSpatialQuery(srp, false, null);
        if (results.getFacetResults().size() > 0) {
            List<FieldResultDTO> fieldResults = results.getFacetResults().iterator().next().getFieldResult();
            ArrayList<String> images = new ArrayList<String>(fieldResults.size());
            for (FieldResultDTO fr : fieldResults)
                images.add(fr.getLabel());
            return images;
        }
        return Collections.EMPTY_LIST;
    }

    /**
     * Checks to see if the supplied GUID represents an Australian species.
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {"/australian/taxon/**", "/native/taxon/**"}, method = RequestMethod.GET)
    public @ResponseBody
    NativeDTO isAustralian(HttpServletRequest request) throws Exception {
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
     *
     * @param guids - comma separated list of guids
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {"/australian/taxa.json*", "/australian/taxa*", "/native/taxa.json*", "/native/taxa*"}, method = RequestMethod.GET)
    public @ResponseBody
    List<NativeDTO> isAustralianForList(@RequestParam(value = "guids", required = true) String guids) throws Exception {
        List<NativeDTO> nativeDTOs = new ArrayList<NativeDTO>();
        String[] guidArray = StringUtils.split(guids, ',');

        if (guidArray != null) {
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
    private NativeDTO getIsAustraliaForGuid(String guid) throws Exception {
        SpatialSearchRequestParams requestParams = new SpatialSearchRequestParams();
        requestParams.setPageSize(0);
        requestParams.setFacets(new String[]{});
        String query =
                "lsid:"
                        + guid
                        + " AND "
                        + "(" + COUNTRY + ":\""
                        + nativeCountry
                        + "\" OR " + STATE + ":[* TO *])";
        requestParams.setQ(query);
        NativeDTO adto = new NativeDTO();
        adto.setTaxonGuid(guid);
        SearchResultDTO results = searchDAO.findByFulltextSpatialQuery(requestParams, false, null);
        adto.setHasOccurrenceRecords(results.getTotalRecords() > 0);
        adto.setIsNSL(getTaxonIDPattern().matcher(guid).matches());
        if (adto.isHasOccurrences()) {
            //check to see if the records have only been provided by citizen science
            //TODO change this to a confidence setting after it has been included in the index
            requestParams.setQ("lsid:" + guid + " AND (" + PROVENANCE + ":\"Published dataset\")");
            results = searchDAO.findByFulltextSpatialQuery(requestParams, false, null);
            adto.setHasCSOnly(results.getTotalRecords() == 0);
        }

        return adto;
    }

    /**
     * Returns the complete list of Occurrences
     */
    @RequestMapping(value = {
            "/occurrences",
            "/occurrences.json",
            "/occurrences/collections",
            "/occurrences/collections.json",
            "/occurrences/institutions",
            "/occurrences/institutions.json",
            "/occurrences/dataResources",
            "/occurrences/dataResources.json",
            "/occurrences/dataProviders",
            "/occurrences/dataProviders.json",
            "/occurrences/taxa",
            "/occurrences/taxa.json",
            "/occurrences/dataHubs",
            "/occurrences/dataHubs.json" },
            method = RequestMethod.GET)
    public @ResponseBody
    SearchResultDTO listOccurrences(Model model) throws Exception {
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
    @RequestMapping(value = {"/occurrences/taxon/**", "/occurrences/taxon/**", "/occurrences/taxa/**"}, method = RequestMethod.GET)
    public @ResponseBody
    SearchResultDTO occurrenceSearchByTaxon(
            SpatialSearchRequestParams requestParams,
            HttpServletRequest request) throws Exception {
        String guid = searchUtils.getGuidFromPath(request);
        requestParams.setQ("lsid:" + guid);
        SearchUtils.setDefaultParams(requestParams);
        return occurrenceSearch(requestParams);
    }

    /**
     * Obtains a list of the sources for the supplied guid.
     * <p>
     * I don't think that this should be necessary. We should be able to
     * configure the requestParams facets to contain the collectino_uid, institution_uid
     * data_resource_uid and data_provider_uid
     * <p>
     * It also handle's the logging for the BIE.
     * //TODO Work out what to do with this
     *
     * @throws Exception
     */
    @RequestMapping(value = "/occurrences/taxon/source/**", method = RequestMethod.GET)
    public @ResponseBody
    List<OccurrenceSourceDTO> sourceByTaxon(SpatialSearchRequestParams requestParams,
                                            HttpServletRequest request) throws Exception {
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
     * @param uid           The uid for collection, institution, data_resource or data_provider
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {
            "/occurrences/collections/{uid}",
            "/occurrences/collections/{uid}.json",
            "/occurrences/institutions/{uid}",
            "/occurrences/institutions/{uid}.json",
            "/occurrences/dataResources/{uid}",
            "/occurrences/dataResources/{uid}.json",
            "/occurrences/dataProviders/{uid}",
            "/occurrences/dataProviders/{uid}.json",
            "/occurrences/dataHubs/{uid}",
            "/occurrences/dataHubs/{uid}.json" }, method = RequestMethod.GET)
    public @ResponseBody
    SearchResultDTO occurrenceSearchForUID(SpatialSearchRequestParams requestParams,
                                           @PathVariable("uid") String uid) throws Exception {
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
     *
     * @param model
     * @return
     * @throws Exception
     * @deprecated use {@link #occurrenceSearch(SpatialSearchRequestParams)}
     */
    @RequestMapping(value = { "/occurrences/searchByArea*", "/occurrences/searchByArea.json*"}, method = RequestMethod.GET)
    @Deprecated
    public @ResponseBody
    SearchResultDTO occurrenceSearchByArea(SpatialSearchRequestParams requestParams,
                                           Model model) throws Exception {
        SearchResultDTO searchResult = new SearchResultDTO();

        if (StringUtils.isEmpty(requestParams.getQ())) {
            return searchResult;
        }

        //searchUtils.updateSpatial(requestParams);
        searchResult = searchDAO.findByFulltextSpatialQuery(requestParams, false, null);
        model.addAttribute("searchResult", searchResult);

        if (logger.isDebugEnabled()) {
            logger.debug("Returning results set with: " + searchResult.getTotalRecords());
        }

        return searchResult;
    }

    private SearchResultDTO occurrenceSearch(SpatialSearchRequestParams requestParams) throws Exception {
        return occurrenceSearch(requestParams, null, false, null, null);
    }

    /**
     * Occurrence search page uses SOLR JSON to display results
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {"/occurrences/search.json*", "/occurrences/search*", "/occurrence/search*"}, method = RequestMethod.GET)
    public @ResponseBody
    SearchResultDTO occurrenceSearch(SpatialSearchRequestParams requestParams,
                                     @RequestParam(value = "apiKey", required = false) String apiKey,
                                     @RequestParam(value = "im", required = false, defaultValue = "false") Boolean lookupImageMetadata,
                                     HttpServletRequest request,
                                     HttpServletResponse response) throws Exception {
            // handle empty param values, e.g. &sort=&dir=
         SearchUtils.setDefaultParams(requestParams);
         Map<String, String[]> map = request != null ? SearchUtils.getExtraParams(request.getParameterMap()) : null;
         if (map != null) {
             map.remove("apiKey");
         }

         if (logger.isDebugEnabled()) {
             logger.debug("occurrence search params = " + requestParams + " extra params = " + map);
         }

         SearchResultDTO srtdto = null;
         if (apiKey == null) {
             srtdto = searchDAO.findByFulltextSpatialQuery(requestParams, false, map);
         } else {
             srtdto = occurrenceSearchSensitive(requestParams, apiKey, request, response);
         }

         if (srtdto.getTotalRecords() > 0 && lookupImageMetadata) {
             //use the image service API & grab the list of IDs
             List<String> occurrenceIDs = new ArrayList<String>();
             for (OccurrenceIndex oi : srtdto.getOccurrences()) {
                 occurrenceIDs.add(oi.getUuid());
             }

             Map<String, List<Map<String, Object>>> imageMap = imageMetadataService.getImageMetadataForOccurrences(occurrenceIDs);

             for (OccurrenceIndex oi : srtdto.getOccurrences()) {
                 //lookup metadata
                 List<Map<String, Object>> imageMetadata = imageMap.get(oi.getUuid());
                 oi.setImageMetadata(imageMetadata);
             }
         }
         return srtdto;
    }

    public @ResponseBody
    SearchResultDTO occurrenceSearchSensitive(SpatialSearchRequestParams requestParams,
                                              @RequestParam(value = "apiKey", required = true) String apiKey,
                                              HttpServletRequest request,
                                              HttpServletResponse response) throws Exception {
        // handle empty param values, e.g. &sort=&dir=
        if (shouldPerformOperation(apiKey, response, false)) {
            SearchUtils.setDefaultParams(requestParams);
            Map<String, String[]> map = SearchUtils.getExtraParams(request.getParameterMap());
            if (map != null) {
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
    public @ResponseBody
    String refreshCache() throws Exception {
        searchDAO.refreshCaches();

        //update FacetThemes static values
        new FacetThemes(
                facetConfig, indexDao.getIndexedFields(), facetsMax, facetsDefaultMax, facetDefault);

        cacheManager.getCacheNames().forEach((String cacheName) -> cacheManager.getCache(cacheName).clear());

        regenerateETag();
        return null;
    }

    /**
     * Regenerate the ETag after clearing the cache so that cached responses are identified as out of date
     */
    private void regenerateETag() {
        occurrenceETag.set(UUID.randomUUID().toString());
    }

    /**
     * Downloads the complete list of values in the supplied facet
     * <p>
     * ONLY 1 facet should be included in the params.
     *
     * @param requestParams
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = "/occurrences/facets/download*", method = RequestMethod.GET)
    public void downloadFacet(
            DownloadRequestParams requestParams,
            @RequestParam(value = "count", required = false, defaultValue = "false") boolean includeCount,
            @RequestParam(value = "lookup", required = false, defaultValue = "false") boolean lookupName,
            @RequestParam(value = "synonym", required = false, defaultValue = "false") boolean includeSynonyms,
            @RequestParam(value = "lists", required = false, defaultValue = "false") boolean includeLists,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {
        if (requestParams.getFacets().length > 0) {
            DownloadDetailsDTO dd = downloadService.registerDownload(requestParams, getIPAddress(request), getUserAgent(request), DownloadDetailsDTO.DownloadType.FACET);
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
    @RequestMapping(value = "/occurrences/batchSearch", method = RequestMethod.POST, params = "action=Download")
    public void batchDownload(
            HttpServletResponse response,
            HttpServletRequest request,
            DownloadRequestParams downloadRequestParams,
            @RequestParam(value = "queries", required = true, defaultValue = "") String queries,
            @RequestParam(value = "field", required = true, defaultValue = "") String field,
            @RequestParam(value = "separator", defaultValue = "\n") String separator,
            @RequestParam(value = "title", required = false) String title) throws Exception {
        logger.info("/occurrences/batchSearch with action=Download Records");
        Long qid = getQidForBatchSearch(queries, field, separator, title);

        if (qid != null) {
            if ("*:*".equals(downloadRequestParams.getQ())) {
                downloadRequestParams.setQ("qid:" + qid);
            } else {
                downloadRequestParams.setQ("(" + downloadRequestParams.getQ() + ") AND qid:" + qid);
            }
            String webservicesRoot = request.getSession().getServletContext().getInitParameter("webservicesRoot");
            response.sendRedirect(webservicesRoot + "/occurrences/download?" + downloadRequestParams.getEncodedParams());
        } else {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST);
        }
    }

    @RequestMapping(value = "/occurrences/download/batchFile", method = RequestMethod.GET)
    public String batchDownload(
            HttpServletRequest request,
            @Valid final DownloadRequestParams params,
            BindingResult result,
            @RequestParam(value = "file", required = true) String filepath,
            @RequestParam(value = "directory", required = true, defaultValue = "/data/biocache-exports") final String directory,
            Model model) {
        if (result.hasErrors()) {
            if (logger.isInfoEnabled()) {
                logger.info("validation failed  " + result.getErrorCount() + " checks");
                if (logger.isDebugEnabled()) {
                    logger.debug(result.toString());
                }
            }
            model.addAttribute("errorMessage", getValidationErrorMessage(result));
            return VALIDATION_ERROR;
        }

        final File file = new File(filepath);

        final SpeciesLookupService mySpeciesLookupService = this.speciesLookupService;
        final DownloadDetailsDTO dd = downloadService.registerDownload(params, getIPAddress(request), getUserAgent(request), DownloadType.RECORDS_INDEX);

        if (file.exists()) {
            Runnable t = new Runnable() {
                @Override
                public void run() {
                    long executionDelay = 10 + Math.round(Math.random() * 50);
                    try (CSVReader reader = new CSVReader(new FileReader(file));) {
                        String[] row = reader.readNext();
                        while (row != null) {
                            // Reduce congestion on db/index by artificially sleeping
                            // for a random amount of time between rows in the batch file
                            Thread.sleep(executionDelay);

                            //get an lsid for the name
                            String lsid = mySpeciesLookupService.getGuidForName(row[0]);
                            if (lsid != null) {
                                try {
                                    //download records for this row
                                    String outputFilePath = directory + File.separatorChar + row[0].replace(" ", "_") + ".txt";
                                    String citationFilePath = directory + File.separatorChar + row[0].replace(" ", "_") + "_citations.txt";
                                    if (logger.isDebugEnabled()) {
                                        logger.debug("Outputting results to:" + outputFilePath + ", with LSID: " + lsid);
                                    }
                                    try (FileOutputStream output = new FileOutputStream(outputFilePath);) {
                                        params.setQ("lsid:\"" + lsid + "\"");
                                        ConcurrentMap<String, AtomicInteger> uidStats = new ConcurrentHashMap<>();
                                        searchDAO.writeResultsFromIndexToStream(params, new CloseShieldOutputStream(output), uidStats,false, dd, false, null);
                                        output.flush();
                                        try (FileOutputStream citationOutput = new FileOutputStream(citationFilePath);) {
                                            downloadService.getCitations(uidStats, citationOutput, params.getSep(), params.getEsc(), null, null);
                                            citationOutput.flush();
                                        }
                                    }
                                } catch (Exception e) {
                                    logger.error(e.getMessage(), e);
                                }
                            } else {
                                logger.error("Unable to match name: " + row[0]);
                            }
                            row = reader.readNext();
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
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
    @RequestMapping(value = "/occurrences/batchSearch", method = RequestMethod.POST, params = "action=Search")
    public void batchSearch(
            HttpServletResponse response,
            @RequestParam(value = "redirectBase", required = true, defaultValue = "") String redirectBase,
            @RequestParam(value = "queries", required = true, defaultValue = "") String queries,
            @RequestParam(value = "field", required = true, defaultValue = "") String field,
            @RequestParam(value = "separator", defaultValue = "\n") String separator,
            @RequestParam(value = "title", required = false) String title) throws Exception {
        logger.info("/occurrences/batchSearch with action=Search");
        Long qid = getQidForBatchSearch(queries, field, separator, title);

        if (qid != null && StringUtils.isNotBlank(redirectBase)) {
            response.sendRedirect(redirectBase + "?q=qid:" + qid);
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
        StringBuilder sb = new StringBuilder();
        int terms = 0;

        for (String part : rawParts) {
            String normalised = StringUtils.trimToNull(part);
            if (normalised != null) {
                if (terms == 0) {
                    if (sb.length() > 0) {
                        sb.append(" OR ");
                    }
                    sb.append("(");
                } else {
                    sb.append(" OR ");
                }
                String[] fields = field.split(",");
                List<String> fieldQ = new ArrayList<>();
                for (String f : fields) {
                    fieldQ.add(f + ":\"" + normalised + "\"");
                }

                sb.append(String.join(" OR ", fieldQ));
                terms++;

                if (terms >= searchDAO.getMaxBooleanClauses()) {
                    sb.append(")");
                    terms = 0;
                }
            }
        }
        if (terms > 0) {
            sb.append(")");
        }

        String q = sb.toString();
        title = title == null ? q : title;
        String qid = qidCacheDao.put(q, title, null, null, null, -1, null);
        logger.info("batchSearch: qid = " + qid);

        return Long.parseLong(qid);
    }

    /**
     * Webservice to report the occurrence counts for the supplied list of taxa
     */
    @RequestMapping(value = { "/occurrences/taxaCount", "/occurrences/taxaCount.json" }, method = {RequestMethod.POST, RequestMethod.GET})
    public @ResponseBody
    Map<String, Integer> occurrenceSpeciesCounts(
            HttpServletResponse response,
            HttpServletRequest request,
            @RequestParam(defaultValue = "\n") String separator
    ) throws Exception {
        String listOfGuids = request.getParameter("guids");
        String[] filterQueries = request.getParameterValues("fq");

        if (StringUtils.isBlank(listOfGuids)) {
            response.sendError(400, "Provide a non-empty guids parameter");
            return null;
        }

        String[] rawGuids = listOfGuids.split(separator);

        List<String> guids = new ArrayList<String>();
        for (String guid : rawGuids) {
            String normalised = StringUtils.trimToNull(guid);
            if (normalised != null) {
                guids.add(normalised);
            }
        }
        response.setHeader("Cache-Control", occurrenceCacheControlHeaderPublicOrPrivate + ", max-age=" + occurrenceCacheControlHeaderMaxAge);
        response.setHeader("ETag", occurrenceETag.get());

        return searchDAO.getOccurrenceCountsForTaxa(guids, filterQueries);
    }

    /**
     * Occurrence search page uses SOLR JSON to display results
     * <p>
     * Please NOTE that the q and fq provided to this URL should be obtained
     * from SearchResultDTO.urlParameters
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {
            "/occurrences/index/download*",
            "/occurrences/index/download.json*",
            "/occurrences/download*",
            "/occurrences/download.json*" }, method = RequestMethod.GET)
    public String occurrenceIndexDownload(@Valid DownloadRequestParams requestParams,
                                          BindingResult result,
                                          @RequestParam(value = "email", required = false) String email,
                                          @RequestParam(value = "apiKey", required = false) String apiKey,
                                          @RequestParam(value = "zip", required = false, defaultValue = "true") Boolean zip,
                                          Model model,
                                          HttpServletResponse response,
                                          HttpServletRequest request) throws Exception {
        if (result.hasErrors()) {
            logger.info("validation failed  " + result.getErrorCount() + " checks");
            logger.debug(result.toString());
            model.addAttribute("errorMessage", getValidationErrorMessage(result));
            return VALIDATION_ERROR;
        }

        boolean validEmail = false;
        if (email != null) {

            try {
                new InternetAddress(email).validate();
                validEmail = true;
            } catch (AddressException e) {
            }
        }

        if (apiKey == null && !validEmail && rateLimitRequest(request, response)) {

            response.sendError(HttpServletResponse.SC_FORBIDDEN, "API Key or email required, please contact 'support@ala.org.au'");
            return null;
        }

        //search params must have a query or formatted query for the download to work
        if (requestParams.getQ().isEmpty() && requestParams.getFormattedQuery().isEmpty()) {
            return null;
        }
        if (apiKey != null) {
            occurrenceSensitiveDownload(requestParams, apiKey, zip, response, request);
            return null;
        }
        try {
            ServletOutputStream out = response.getOutputStream();
            downloadService.writeQueryToStream(requestParams, response, getIPAddress(request), getUserAgent(request), new CloseShieldOutputStream(out), true, zip, executor);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    public String occurrenceSensitiveDownload(
            DownloadRequestParams requestParams,
            String apiKey,
            boolean zip,
            HttpServletResponse response,
            HttpServletRequest request) throws Exception {
        if (shouldPerformOperation(apiKey, response, false)) {

            //search params must have a query or formatted query for the downlaod to work
            if (requestParams.getQ().isEmpty() && requestParams.getFormattedQuery().isEmpty()) {
                return null;
            }

            try {
                ServletOutputStream out = response.getOutputStream();
                downloadService.writeQueryToStream(requestParams, response, getIPAddress(request), getUserAgent(request), new CloseShieldOutputStream(out), true, zip, executor);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }

        }
        return null;
    }

    /**
     * Utility method for retrieving a list of occurrences. Mainly added to help debug
     * web services for that a developer can retrieve example UUIDs.
     *
     * @throws Exception
     */
    @RequestMapping(value = {"/occurrences/nearest", "/occurrences/nearest.json" }, method = RequestMethod.GET)
    public @ResponseBody
    Map<String, Object> nearestOccurrence(SpatialSearchRequestParams requestParams) throws Exception {

        logger.debug(String.format("Received lat: %f, lon:%f, radius:%f", requestParams.getLat(),
                requestParams.getLon(), requestParams.getRadius()));

        if (requestParams.getLat() == null || requestParams.getLon() == null) {
            return new HashMap<String, Object>();
        }
        //requestParams.setRadius(1f);
        requestParams.setDir("asc");
        requestParams.setFacet(false);

        SearchResultDTO searchResult = searchDAO.findByFulltextSpatialQuery(requestParams, false, null);
        List<OccurrenceIndex> ocs = searchResult.getOccurrences();

        if (!ocs.isEmpty()) {
            Map<String, Object> results = new HashMap<String, Object>();
            OccurrenceIndex oc = ocs.get(0);
            Double decimalLatitude = oc.getDecimalLatitude();
            Double decimalLongitude = oc.getDecimalLongitude();
            Double distance = distInMetres(requestParams.getLat().doubleValue(), requestParams.getLon().doubleValue(),
                    decimalLatitude, decimalLongitude);
            results.put("distanceInMeters", distance);
            results.put("occurrence", oc);
            return results;
        } else {
            return new HashMap<String, Object>();
        }
    }

    private Double distInMetres(Double lat1, Double lon1, Double lat2, Double lon2) {
        Double R = 6371000d; // km
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double lat1Rad = Math.toRadians(lat1);
        double lat2Rad = Math.toRadians(lat2);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(lat1Rad) * Math.cos(lat2Rad);
        Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }

    /**
     * Dumps the distinct latitudes and longitudes that are used in the
     * connected index (to 4 decimal places)
     */
    @RequestMapping(value = { "/occurrences/coordinates*", "/occurrences/coordinates.json*" })
    public void dumpDistinctLatLongs(SpatialSearchRequestParams requestParams, HttpServletResponse response) throws Exception {
        requestParams.setFacets(new String[]{OccurrenceIndex.LAT_LNG});
        requestParams.setFacet(true);
        if (requestParams.getQ().length() < 1)
            requestParams.setQ("*:*");
        try {
            ServletOutputStream out = response.getOutputStream();
            searchDAO.writeCoordinatesToStream(requestParams, out);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Occurrence record page
     * <p>
     * When user supplies a uuid that is not found search for a unique record
     * with the supplied occurrenc_id
     * <p>
     * Returns a SearchResultDTO when there is more than 1 record with the supplied UUID
     *
     * @param uuid
     * @throws Exception
     */
    @Deprecated // Remove when support for version 1.0 is no longer required.
    @RequestMapping(
            value = {"/occurrence/compare/{uuid}.json", "/occurrence/compare/{uuid}"},
            method = RequestMethod.GET)
    public @ResponseBody
    Object showOccurrence(@PathVariable("uuid") String uuid, HttpServletResponse response) throws Exception {

        SpatialSearchRequestParams idRequest = new SpatialSearchRequestParams();
        idRequest.setQ(OccurrenceIndex.ID + ":\"" + uuid + "\"");
        idRequest.setFl(StringUtils.join(indexDao.getIndexedFieldsMap().keySet(), ","));
        idRequest.setFacet(false);

        SolrDocumentList sdl = searchDAO.findByFulltext(idRequest);

        if (sdl == null || sdl.isEmpty() || sdl.get(0).get("id") == null) {
            response.sendError(404, "Unrecognised ID");
            return null;
        }

        SolrDocument sd = sdl.get(0);

        Map fullRecord = mapAsFullRecord(sd, false, false);
        Map rawGroups = (Map) ((Map) fullRecord.get("raw"));
        Map processedGroups = (Map) ((Map) fullRecord.get("processed"));

        Set groupKeys = new HashSet();
        groupKeys.addAll(rawGroups.keySet());
        groupKeys.addAll(processedGroups.keySet());

        // remove keys not required
        groupKeys.remove("el");
        groupKeys.remove("cl");
        groupKeys.remove("queryAssertions");
        groupKeys.remove("miscProperties");

        Map groups = new HashMap();
        for (Object groupKey : groupKeys) {

            Object raw = rawGroups.get(groupKey);
            Object processed = processedGroups.get(groupKey);

            if (raw == null || processed == null || !(raw instanceof Map) || !(processed instanceof Map)) {
                continue;
            }

            Map rawMap = (Map) raw;
            Map processedMap = (Map) processed;

            Set keys = new HashSet();
            keys.addAll(rawMap.keySet());
            keys.addAll(processedMap.keySet());

            //substitute the values for recordedBy if it is an authenticated user
            List<Comparison> comparison = new ArrayList<>();
            for (Object key : keys) {
                Object r = rawMap.get(key);
                Object p = processedMap.get(key);

                if (r != null || p != null) {
                    Comparison c = new Comparison();
                    c.setName((String) key);

                    if (r instanceof String && !StringUtils.isEmpty((String) r)) {
                        c.setRaw(authService.substituteEmailAddress((String) r));
                    } else if (r != null) {
                        c.setRaw(r.toString());
                    } else {
                        c.setRaw("");
                    }

                    if (p instanceof String && !StringUtils.isEmpty((String) p)) {
                        c.setProcessed(authService.substituteEmailAddress((String) p));
                    } else if (p != null) {
                        c.setProcessed(p.toString());
                    } else {
                        c.setProcessed("");
                    }

                    comparison.add(c);
                }
            }

            groups.put(StringUtils.capitalize((String) groupKey), comparison);
        }
        return groups;
    }

    /**
     * Returns a comparison of the occurrence versions.
     *
     * @param uuid
     * @return
     */
    @RequestMapping(value = {"/occurrence/compare*", "/occurrence/compare.json*" }, method = RequestMethod.GET)
    public @ResponseBody
    Object compareOccurrenceVersions(@RequestParam(value = "uuid", required = true) String uuid,
                                     HttpServletResponse response) throws Exception {
        return showOccurrence(uuid, response);
    }

    private void sendCustomJSONResponse(HttpServletResponse response, int statusCode, Map<String, String> content) throws IOException {
        response.resetBuffer();
        response.setStatus(statusCode);
        response.setHeader("Content-Type", "application/json");
        response.getOutputStream().print(new ObjectMapper().writeValueAsString(content));
        response.flushBuffer();
    }
    /**
     * Occurrence record page
     * <p>
     * When user supplies a uuid that is not found search for a unique record
     * with the supplied occurrence_id
     * <p>
     * Returns a SearchResultDTO when there is more than 1 record with the supplied UUID
     *
     * @param uuid
     * @param apiKey
     * @throws Exception
     */
    @RequestMapping(value = {"/occurrence/{uuid:.+}", "/occurrences/{uuid:.+}", "/occurrence/{uuid:.+}.json", "/occurrences/{uuid:.+}.json"}, method = RequestMethod.GET)
    public @ResponseBody
    Object showOccurrence(@PathVariable("uuid") String uuid,
                          @RequestParam(value = "apiKey", required = false) String apiKey,
                          @RequestParam(value = "im", required = false) String im,
                          HttpServletRequest request, HttpServletResponse response) throws Exception {
        Object responseObject;
        if (apiKey != null) {
            responseObject = showSensitiveOccurrence(uuid, apiKey, im, request, response);
        } else {
            responseObject = getOccurrenceInformation(uuid, im, request, false);
        }
        if (responseObject == null) {
            sendCustomJSONResponse(response, HttpServletResponse.SC_NOT_FOUND, new HashMap<String, String>() {{put("message", "Unrecognised UID");}});
        }
        return responseObject;
    }

    @RequestMapping(value = {"/sensitive/occurrence/{uuid:.+}", "/sensitive/occurrences/{uuid:.+}", "/sensitive/occurrence/{uuid:.+}.json", "/senstive/occurrences/{uuid:.+}.json"}, method = RequestMethod.GET)
    public @ResponseBody
    Object showSensitiveOccurrence(@PathVariable("uuid") String uuid,
                                   @RequestParam(value = "apiKey", required = true) String apiKey,
                                   @RequestParam(value = "im", required = false) String im,
                                   HttpServletRequest request, HttpServletResponse response) throws Exception {
        if (shouldPerformOperation(apiKey, response)) {
            return getOccurrenceInformation(uuid, im, request, true);
        }
        return null;
    }

    private Object getOccurrenceInformation(String uuid, String im, HttpServletRequest request,
                                            boolean includeSensitive) throws Exception {
        logger.debug("Retrieving occurrence record with guid: '" + uuid + "'");

        String ip = getIPAddress(request);

        SpatialSearchRequestParams idRequest = new SpatialSearchRequestParams();
        idRequest.setQ(OccurrenceIndex.ID + ":\"" + uuid + "\"");
        idRequest.setFacet(false);
        idRequest.setFl("*");

        SolrDocumentList sdl = searchDAO.findByFulltext(idRequest);
        if (sdl == null || sdl.isEmpty()) {
            return null;
        }

        SolrDocument sd = sdl.get(0);

        // obscure email addresses, or anything else containing @
        Set<String> keys = new HashSet<>();
        keys.addAll(sd.keySet());
        for (String key : keys) {
            Object value = sd.getFieldValue(key);
            if (value != null) {
                // obscure emails
                boolean notJson = true;
                if (value instanceof String && ((String) value).startsWith("[")) {
                    notJson = false;
                    try {
                        String[] list = (String[]) JSONArray.fromObject(value).toArray(new String[0]);
                        boolean changed = false;
                        for (int i = 0; i < list.length; i++) {
                            String v = list[i];
                            if (value.toString().contains("@")) {
                                //multivalue fields; collector_text, collectors
                                list[i] = authService.substituteEmailAddress(value.toString());
                                changed = true;
                            } else if (key.contains("user_id")) {
                                //multivalue fields; assertion_user_id
                                list[i] = authService.getDisplayNameFor(v);
                                changed = true;
                            }
                        }
                        if (changed) {
                            sd.setField(key, JSONArray.fromObject(list).toString());
                        }
                    } catch (Exception e) {
                        notJson = true;
                    }
                }
                if (notJson) {
                    if (value.toString().contains("@")
                            && (key.contains("recorded")
                            || key.startsWith("_")
                            || key.contains("collector"))) {
                        sd.setField(key, authService.substituteEmailAddress(value.toString()));
                    } else if (value instanceof String && key.contains("user_id")) {
                        sd.setField(key, authService.getDisplayNameFor(value.toString()));
                    }
                }
            }
        }

        if (occurrenceLogEnabled) {
            //log the statistics for viewing the record
            logViewEvent(ip, sd, getUserAgent(request), null, "Viewing Occurrence Record " + uuid);
        }

        boolean includeImageMetadata = (im == null || !im.equalsIgnoreCase("false"));
        return mapAsFullRecord(sd, includeImageMetadata, includeSensitive);
    }

    private boolean isSensitive(SolrDocument doc) {
        String sensitiveValue = (String) doc.getFieldValue("sensitive");
        return sensitiveValue != null && (sensitiveValue.equals("generalised") || sensitiveValue.equals("alreadyGeneralised"));
    }

    /**
     * @param sd
     * @return
     */
    private Map mapAsFullRecord(SolrDocument sd, Boolean includeImageMetadata, Boolean includeSensitive) throws Exception {

        Set<String> schemaFields = indexDao.getSchemaFields();

        Map map = new LinkedHashMap();
        Map raw = fullRecord(sd, (String fieldName) -> schemaFields.contains(RAW_FIELD_PREFIX + fieldName) ? (RAW_FIELD_PREFIX + fieldName) : fieldName);
        Map processed = fullRecord(sd, (String fieldName) -> schemaFields.contains(RAW_FIELD_PREFIX + fieldName) ? fieldName : null);

        // add lastModifiedTime
        addInstant(sd, raw, "lastModifiedTime", "lastLoadDate");
        addInstant(sd, processed, "lastModifiedTime", "lastProcessedDate");

        // add extra processed fields
        processed.put("el", extractLayerValues(sd, "el"));
        processed.put("cl", extractLayerValues(sd, "cl"));

        Map location = (Map) processed.get("location");
        location.put("marine", "Marine".equalsIgnoreCase(String.valueOf(sd.getFieldValue("biome"))));
        location.put("terrestrial", "Terrestrial".equalsIgnoreCase(String.valueOf(sd.getFieldValue("biome"))));

        Map occurrence = (Map) processed.get("occurrence");

        // duplicate status
        String duplicateStatus = (String) sd.getFieldValue("duplicateStatus");
        occurrence.put("duplicateStatus", duplicateStatus);
        if (REPRESENTATIVE.equals(duplicateStatus)) {
            occurrence.put("duplicationStatus", LEGACY_REPRESENTATIVE_RECORD_VALUE);  //backwards compatibility
        } else if (ASSOCIATED.equals(duplicateStatus)) {
            occurrence.put("duplicationStatus", LEGACY_DUPLICATE_RECORD_VALUE);  //backwards compatibility
        }

        Collection<String> outlierLayer = (Collection) sd.getFieldValues("outlierLayer");
        if (outlierLayer != null) {
            occurrence.put("outlierForLayers", outlierLayer);
        }

        raw.put("miscProperties", extractMiscProperties(sd));

        map.put("raw", raw);
        map.put("processed", processed);

        map.put("systemAssertions", systemAssertions(sd));
        map.put("userAssertions", userAssertions(sd));

        map.put("alaUserId", sd.getFieldValue(OccurrenceIndex.ALA_USER_ID));

        boolean bSensitive = isSensitive(sd);
        map.put("sensitive", bSensitive);
        if (includeSensitive && bSensitive) {
            Map rawLocation = (Map) raw.get("location");
            Map rawEvent = (Map) raw.get("event");

            addField(sd, rawLocation, "dataGeneralizations", "sensitive_dataGeneralizations");
            addField(sd, rawLocation, "decimalLatitude", "sensitive_decimalLatitude");
            addField(sd, rawLocation, "decimalLongitude", "sensitive_decimalLongitude");
            addField(sd, rawLocation, "footprintWKT", "sensitive_footprintWKT");
            addField(sd, rawLocation, "locality", "sensitive_locality");
            addField(sd, rawLocation, "locationRemarks", "sensitive_locationRemarks");
            addField(sd, rawLocation, "verbatimCoordinates", "sensitive_verbatimCoordinates");
            addField(sd, rawLocation, "verbatimLatitude", "sensitive_verbatimLatitude");
            addField(sd, rawLocation, "verbatimLocality", "sensitive_verbatimLocality");
            addField(sd, rawLocation, "verbatimLongitude", "sensitive_verbatimLongitude");

            addField(sd, rawEvent, "day", "sensitive_day");
            addField(sd, rawEvent, "eventDate", "sensitive_eventDate");
            addField(sd, rawEvent, "eventDate", "sensitive_eventDate");
            addField(sd, rawEvent, "eventID", "sensitive_eventID");
            addField(sd, rawEvent, "eventTime", "sensitive_eventTime");
            addField(sd, rawEvent, "month", "sensitive_month");
            addField(sd, rawEvent, "verbatimEventDate", "sensitive_verbatimEventDate");
        }

        // add multimedia links
        addImages(sd, map, "imageIDs", "images", includeImageMetadata);
        addSounds(sd, map, "soundIDs", "sounds");

        return map;
    }

    /**
     * Pull miscProperties out of the JSON blob dynamicProperties, and merges
     * with any properties indexed with `dynamicProperties_` prefix.
     *
     * @param sd
     * @return map of miscProperties
     */
    private Map<String, Object> extractMiscProperties(SolrDocument sd) {

        // add misc properties
        Map<String, Object> miscProperties = new HashMap<>();
        try {
            ObjectMapper om = new ObjectMapper();
            Map<String, Object> jsonProperties = om.readValue((String) sd.getFieldValue(DwcTerm.dynamicProperties.simpleName()), Map.class);
            miscProperties.putAll(jsonProperties);
        } catch (Exception e) {
            // best effort service - dynamic properties may not be in JSON format, dependent on data publisher
        }

        // retrieve a list of dynamicProperties_ fields
        List<String> dynamicProperties = sd.getFieldNames()
                .stream()
                .filter(fieldName -> fieldName.startsWith(DYNAMIC_PROPERTIES_PREFIX))
                .collect(Collectors.toList());

        for (String property : dynamicProperties) {
            //Strip off prefix
            String fieldName = property.replaceAll(DYNAMIC_PROPERTIES_PREFIX, "");
            miscProperties.put(fieldName, sd.get(property));
        }
        return miscProperties;
    }

    private Map extractLayerValues(SolrDocument sd, String key) {
        Map map = new HashMap();
        String regex = "^" + key + "[0-9]+$";
        for (Map.Entry<String, Object> es : sd.entrySet()) {
            if (es.getKey().matches(regex)) {
                map.put(es.getKey(), es.getValue());
            }
        }
        return map;
    }

    private void addField(SolrDocument sd, Map map, String fieldName, Function<String, String> getFieldName) {
        addField(sd, map, fieldName, getFieldName.apply(fieldName));
    }

    private void addField(SolrDocument sd, Map map, String fieldNameToUser, String fieldName) {
        map.put(fieldNameToUser, sd.getFieldValue(fieldName));
    }

    private void addAll(SolrDocument sd, Map map, String fieldName, Function<String, String> getFieldName) {
        map.put(fieldName, StringUtils.join(sd.getFieldValues(getFieldName.apply(fieldName)), "|"));
    }

    private void addLocalDate(SolrDocument sd, Map map, String fieldName, Function<String, String> getFieldName) {
        addLocalDate(sd, map, fieldName, getFieldName.apply(fieldName));
    }

    private void addLocalDate(SolrDocument sd, Map map, String fieldNameToUse, String fieldName) {

        Object value = sd.getFieldValue(fieldName);

        if (value != null && value instanceof Date) {

            LocalDate localDate = ((Date) value).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            map.put(fieldNameToUse, localDate.toString());

        } else if (value != null) {

            // If the value is not a date add without parsing to date.
            // Note: raw (unprocessed) fields are stored as string values.
            addField(sd, map, fieldNameToUse, fieldName);
        }
    }

    private void addInstant(SolrDocument sd, Map map, String fieldNameToUse, String fieldName) {

        Object value = sd.getFieldValue(fieldName);

        if (value != null && value instanceof Date) {

            Instant instant = ((Date) value).toInstant();
            map.put(fieldNameToUse, instant.toString());
        }
    }


    private void addSounds(SolrDocument sd, Map map, String srcKey, String dstKey) {
        Collection<Object> value = sd.getFieldValues(srcKey);

        if (value != null && !value.isEmpty()) {
            List<String> strings = value.stream()
                    .map(object -> Objects.toString(object, null))
                    .collect(Collectors.toList());
            if (value != null) {
                List<MediaDTO> mediaDTOs = new ArrayList<>();
                for (String soundID : strings) {
                    MediaDTO m = new MediaDTO();
                    m.getAlternativeFormats()
                            .put("audio/mpeg", remoteMediaStoreUrl + "/image/proxyImage?imageId=" + soundID);
                    mediaDTOs.add(m);
                }
                map.put(dstKey, mediaDTOs);
            }
        }
    }

    private void addImageUrls(SolrDocument sd, Map map, String srcKey, String dstKey) {
        Collection<Object> value = sd.getFieldValues(srcKey);

        if (value != null && !value.isEmpty()) {
            List<String> imageIds = value.stream()
                    .map(object -> Objects.toString(object, null))
                    .filter(str -> str != null)
                    .map(imageId -> remoteMediaStoreUrl + "/image/proxyImage?imageId=" + imageId)
                    .collect(Collectors.toList());
            map.put(dstKey, imageIds);
        }
    }

    private void addImages(SolrDocument sd, Map map, String srcKey, String dstKey, boolean lookupImageMetadata) {
        Collection<Object> value = sd.getFieldValues(srcKey);

        if (value != null && !value.isEmpty()) {
            List<String> strings = value.stream()
                    .map(object -> Objects.toString(object, null))
                    .collect(Collectors.toList());
            if (value != null) {
                String id = (String) sd.getFieldValue(OccurrenceIndex.ID);
                List<MediaDTO> mediaDTOs = setupImageUrls(id, strings, lookupImageMetadata);
                map.put(dstKey, mediaDTOs);
            }
        }
    }

    private List<Map<String, Object>> userAssertions(SolrDocument sd) throws IOException {

        String occurrenceId = (String) sd.getFieldValue(ID);
        UserAssertions userAssertions = assertionService.getAssertions(occurrenceId);

        if (userAssertions == null) {
            return new ArrayList<>();
        }

        return userAssertions.stream()
                .map((QualityAssertion qa) -> {

                    Map<String, Object> userAssertion = new HashMap() {{
                        put("uuid", qa.getUuid());
                        put("referenceRowKey", qa.getReferenceRowKey());
                        put("name", qa.getName());
                        put("code", qa.getCode());
                        put("problemAsserted", qa.getProblemAsserted());
                        put("relatedUuid", qa.getRelatedUuid());
                        put("qaStatus", qa.getQaStatus());
                        put("comment", qa.getComment());
                        put("userId", qa.getUserId());
                        put("userEmail", qa.getUserEmail());
                        put("userDisplayName", qa.getUserDisplayName());
                        put("created", qa.getCreated());
                    }};

                    if (qa.getUserId().contains("@")) {
                        String email = qa.getUserId();
                        String userId = authService.getMapOfEmailToId().get(email);
                        userAssertion.put("userId", userId);
                        userAssertion.put("userEmail", authService.substituteEmailAddress(email));
                        userAssertion.put("userDisplayName", authService.getDisplayNameFor(userId));
                    }

                    return userAssertion;
                })
                .collect(Collectors.toList());
    }

    /**
     * Build a FullRecord, raw or processed, from a SolrDocument.
     * <p>
     * Only for use with Version 1.0 requests.
     *
     * @param sd           SolrDocument
     * @param getFieldName function to resolve the solr field name from the
     * @return
     */
    Map fullRecord(SolrDocument sd, Function<String, String> getFieldName) {
        Map fullRecord = new LinkedHashMap();
        fullRecord.put("rowKey", sd.getFieldValue(ID)); // Use the processed ID for compatability
        fullRecord.put("uuid", sd.getFieldValue(ID));
        // au.org.ala.biocache.model.Occurrence
        Map occurrence = new HashMap();
        fullRecord.put("occurrence", occurrence);
        addField(sd, occurrence, "occurrenceID", getFieldName);
        addField(sd, occurrence, "accessRights", getFieldName);
        addField(sd, occurrence, "associatedMedia", getFieldName);
        addField(sd, occurrence, "associatedOccurrences", getFieldName);
        addField(sd, occurrence, "associatedOrganisms", getFieldName);
        addField(sd, occurrence, "associatedReferences", getFieldName);
        addField(sd, occurrence, "associatedSequences", getFieldName);
        addField(sd, occurrence, "associatedTaxa", getFieldName);
        addField(sd, occurrence, "basisOfRecord", getFieldName);
        addField(sd, occurrence, "behavior", getFieldName);
        addField(sd, occurrence, "bibliographicCitation", getFieldName);
        addField(sd, occurrence, "catalogNumber", getFieldName);
        addField(sd, occurrence, "collectionCode", getFieldName);
        addField(sd, occurrence, "collectionID", getFieldName);
        addField(sd, occurrence, "dataGeneralizations", getFieldName);        //used for sensitive data information
        addField(sd, occurrence, "datasetID", getFieldName);
        addField(sd, occurrence, "datasetName", getFieldName);
        addField(sd, occurrence, "disposition", getFieldName);
//        addField(sd, occurrence, "dynamicProperties", getFieldName);
        addField(sd, occurrence, "establishmentMeans", getFieldName);
        addField(sd, occurrence, "fieldNotes", getFieldName);
        addField(sd, occurrence, "fieldNumber", getFieldName);
        addField(sd, occurrence, "individualCount", getFieldName);
        addField(sd, occurrence, "informationWithheld", getFieldName);   //used for sensitive data information
        addField(sd, occurrence, "institutionCode", getFieldName);
        addField(sd, occurrence, "institutionID", getFieldName);
        addField(sd, occurrence, "language", getFieldName);
        addField(sd, occurrence, "lifeStage", getFieldName);
        addField(sd, occurrence, "modified", getFieldName);
        addField(sd, occurrence, "occurrenceAttributes", getFieldName);
        addField(sd, occurrence, "occurrenceDetails", getFieldName);
        addField(sd, occurrence, "occurrenceRemarks", getFieldName);
        addField(sd, occurrence, "occurrenceStatus", getFieldName);
        addField(sd, occurrence, "organismName", getFieldName);
        addField(sd, occurrence, "organismQuantity", getFieldName);
        addField(sd, occurrence, "organismQuantityType", getFieldName);
        addField(sd, occurrence, "organismRemarks", getFieldName);
        addField(sd, occurrence, "organismScope", getFieldName);
        addField(sd, occurrence, "organismID", getFieldName);
        addField(sd, occurrence, "otherCatalogNumbers", getFieldName);
        addField(sd, occurrence, "ownerInstitutionCode", getFieldName);
        addAll(sd, occurrence, "preparations", getFieldName);
        addField(sd, occurrence, "previousIdentifications", getFieldName);
        addField(sd, occurrence, "recordNumber", getFieldName);
        addField(sd, occurrence, "relatedResourceID", getFieldName);
        addField(sd, occurrence, "relationshipAccordingTo", getFieldName);
        addField(sd, occurrence, "relationshipEstablishedDate", getFieldName);
        addField(sd, occurrence, "relationshipOfResource", getFieldName);
        addField(sd, occurrence, "relationshipRemarks", getFieldName);
        addField(sd, occurrence, "reproductiveCondition", getFieldName);
        addField(sd, occurrence, "resourceID", getFieldName);
        addField(sd, occurrence, "resourceRelationshipID", getFieldName);
        addField(sd, occurrence, "rights", getFieldName);
        addField(sd, occurrence, "rightsHolder", getFieldName);
        addField(sd, occurrence, "samplingProtocol", getFieldName);
        addField(sd, occurrence, "samplingEffort", getFieldName);
        addField(sd, occurrence, "sex", getFieldName);
        addField(sd, occurrence, "source", getFieldName);
        addField(sd, occurrence, "userId", getFieldName);  //this is the ALA ID for the user

        //Additional fields for HISPID support
        addField(sd, occurrence, "collectorFieldNumber", getFieldName);  //This value now maps to the correct DWC field http://rs.tdwg.org/dwc/terms/fieldNumber
        addField(sd, occurrence, "cultivated", getFieldName); //http://www.chah.org.au/hispid/terms/cultivatedOccurrence
        addField(sd, occurrence, "duplicates", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0711
        addField(sd, occurrence, "duplicatesOriginalInstitutionID", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0580
        addField(sd, occurrence, "duplicatesOriginalUnitID", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0579
        addField(sd, occurrence, "loanIdentifier", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0712
        addField(sd, occurrence, "loanSequenceNumber", getFieldName);  //this one would be http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0713 but not in current archive
        addField(sd, occurrence, "loanDestination", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0714
        addField(sd, occurrence, "loanForBotanist", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0715
        addField(sd, occurrence, "loanDate", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0717
        addField(sd, occurrence, "loanReturnDate", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0718
        addField(sd, occurrence, "phenology", getFieldName); //http://www.chah.org.au/hispid/terms/phenology
        addField(sd, occurrence, "preferredFlag", getFieldName);
        addField(sd, occurrence, "secondaryCollectors", getFieldName); //http://www.chah.org.au/hispid/terms/secondaryCollectors
        addField(sd, occurrence, "naturalOccurrence", getFieldName); //http://www.chah.org.au/hispid/terms/naturalOccurrence
        //this property is in use in flickr tagging - currently no equivalent in DwC
        addField(sd, occurrence, "validDistribution", getFieldName);
        //custom fields
        addField(sd, occurrence, "sounds", "soundIDs");
        addField(sd, occurrence, "videos", "videoIDs");
        addImageUrls(sd, occurrence, "imageIDs", "images");
        //custom fields
        addField(sd, occurrence, "interactions", getFieldName);
        //Store the conservation status
        addField(sd, occurrence, "countryConservation", getFieldName);
        addField(sd, occurrence, "stateConservation", getFieldName);
        addField(sd, occurrence, "globalConservation", getFieldName);
        addField(sd, occurrence, "photographer", getFieldName);
        addField(sd, occurrence, "stateInvasive", getFieldName);
        addField(sd, occurrence, "countryInvasive", getFieldName);

        // concatenate all recordedBy values for hubs
        addField(sd, occurrence, "recordedBy", getFieldName);
        addAll(sd, occurrence, "recordedByID", getFieldName);
        addAll(sd, occurrence, "identifiedByID", getFieldName);

        // au.org.ala.biocache.model.Classification
        Map classification = new HashMap();
        fullRecord.put("classification", classification);

        addField(sd, classification, "scientificName", getFieldName);
        addField(sd, classification, "scientificNameAuthorship", getFieldName);
        addField(sd, classification, "scientificNameID", getFieldName);
        addField(sd, classification, "taxonConceptID", getFieldName);
        addField(sd, classification, "taxonID", getFieldName);
        addField(sd, classification, "kingdom", getFieldName);
        addField(sd, classification, "phylum", getFieldName);
        addField(sd, classification, "classs", getFieldName.apply("class"));
        addField(sd, classification, "order", getFieldName);
        addField(sd, classification, "superfamily", getFieldName);    //an addition to darwin core
        addField(sd, classification, "family", getFieldName);
        addField(sd, classification, "subfamily", getFieldName); //an addition to darwin core
        addField(sd, classification, "genus", getFieldName);
        addField(sd, classification, "subgenus", getFieldName);
        addField(sd, classification, "species", getFieldName);
        addField(sd, classification, "specificEpithet", getFieldName);
        addField(sd, classification, "subspecies", getFieldName);
        addField(sd, classification, "infraspecificEpithet", getFieldName);
        addField(sd, classification, "infraspecificMarker", getFieldName);
        addField(sd, classification, "cultivarName", getFieldName); //an addition to darwin core for http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0315
        addField(sd, classification, "higherClassification", getFieldName);
        addField(sd, classification, "parentNameUsage", getFieldName);
        addField(sd, classification, "parentNameUsageID", getFieldName);
        addField(sd, classification, "acceptedNameUsage", getFieldName);
        addField(sd, classification, "acceptedNameUsageID", getFieldName);
        addField(sd, classification, "originalNameUsage", getFieldName);
        addField(sd, classification, "originalNameUsageID", getFieldName);
        addField(sd, classification, "taxonRank", getFieldName);
        addField(sd, classification, "taxonomicStatus", getFieldName);
        addField(sd, classification, "taxonRemarks", getFieldName);
        addField(sd, classification, "verbatimTaxonRank", getFieldName);
        addField(sd, classification, "vernacularName", getFieldName);
        addField(sd, classification, "nameAccordingTo", getFieldName);
        addField(sd, classification, "nameAccordingToID", getFieldName);
        addField(sd, classification, "namePublishedIn", getFieldName);
        addField(sd, classification, "namePublishedInYear", getFieldName);
        addField(sd, classification, "namePublishedInID", getFieldName);
        addField(sd, classification, "nomenclaturalCode", getFieldName);
        addField(sd, classification, "nomenclaturalStatus", getFieldName);

        //additional fields for HISPID support
        addField(sd, classification, "scientificNameWithoutAuthor", getFieldName);
        addField(sd, classification, "scientificNameAddendum", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0334
        //custom additional fields
        addField(sd, classification, "taxonRankID", getFieldName);
        addField(sd, classification, "kingdomID", getFieldName);
        addField(sd, classification, "phylumID", getFieldName);
        addField(sd, classification, "classID", getFieldName);
        addField(sd, classification, "orderID", getFieldName);
        addField(sd, classification, "familyID", getFieldName);
        addField(sd, classification, "genusID", getFieldName);
        addField(sd, classification, "subgenusID", getFieldName);
        addField(sd, classification, "speciesID", getFieldName);
        addField(sd, classification, "subspeciesID", getFieldName);

        addField(sd, classification, "left", getFieldName.apply("lft"));
        addField(sd, classification, "right", getFieldName.apply("rgt"));
        addField(sd, classification, "speciesGroups", getFieldName);
        addField(sd, classification, "matchType", getFieldName); //stores the type of name match that was performed
        addField(sd, classification, "taxonomicIssues", getFieldName); //stores if no issue, questionableSpecies, conferSpecies or affinitySpecies
        addField(sd, classification, "nameType", getFieldName);


        // au.org.ala.biocache.model.Location
        Map location = new HashMap();
        fullRecord.put("location", location);
        //dwc terms
        addField(sd, location, "continent", getFieldName);
        addField(sd, location, "coordinatePrecision", getFieldName);
        addField(sd, location, "coordinateUncertaintyInMeters", getFieldName);
        addField(sd, location, "country", getFieldName);
        addField(sd, location, "countryCode", getFieldName);
        addField(sd, location, "county", getFieldName);
        addField(sd, location, "decimalLatitude", getFieldName);
        addField(sd, location, "decimalLongitude", getFieldName);
        addField(sd, location, "footprintSpatialFit", getFieldName);
        addField(sd, location, "footprintWKT", getFieldName);
        addField(sd, location, "footprintSRS", getFieldName);
        addField(sd, location, "geodeticDatum", getFieldName);
        addField(sd, location, "georeferencedBy", getFieldName);
        addField(sd, location, "georeferencedDate", getFieldName);
        addField(sd, location, "georeferenceProtocol", getFieldName);
        addField(sd, location, "georeferenceRemarks", getFieldName);
        addField(sd, location, "georeferenceSources", getFieldName);
        addField(sd, location, "georeferenceVerificationStatus", getFieldName);
        addField(sd, location, "habitat", getFieldName);
        addField(sd, location, "biome", getFieldName);
        addField(sd, location, "higherGeography", getFieldName);
        addField(sd, location, "higherGeographyID", getFieldName);
        addField(sd, location, "island", getFieldName);
        addField(sd, location, "islandGroup", getFieldName);
        addField(sd, location, "locality", getFieldName);
        addField(sd, location, "locationAccordingTo", getFieldName);
        addField(sd, location, "locationAttributes", getFieldName);
        addField(sd, location, "locationID", getFieldName);
        addField(sd, location, "locationRemarks", getFieldName);
        addField(sd, location, "maximumDepthInMeters", getFieldName);
        addField(sd, location, "maximumDistanceAboveSurfaceInMeters", getFieldName);
        addField(sd, location, "maximumElevationInMeters", getFieldName);
        addField(sd, location, "minimumDepthInMeters", getFieldName);
        addField(sd, location, "minimumDistanceAboveSurfaceInMeters", getFieldName);
        addField(sd, location, "minimumElevationInMeters", getFieldName);
        addField(sd, location, "municipality", getFieldName);
        addField(sd, location, "pointRadiusSpatialFit", getFieldName);
        addField(sd, location, "stateProvince", getFieldName);
        addField(sd, location, "verbatimCoordinates", getFieldName);
        addField(sd, location, "verbatimCoordinateSystem", getFieldName);
        addField(sd, location, "verbatimDepth", getFieldName);
        addField(sd, location, "verbatimElevation", getFieldName);
        addField(sd, location, "verbatimLatitude", getFieldName);
        addField(sd, location, "verbatimLocality", getFieldName);
        addField(sd, location, "verbatimLongitude", getFieldName);
        addField(sd, location, "verbatimSRS", getFieldName);
        addField(sd, location, "waterBody", getFieldName);
        // custom additional fields not in darwin core
        addField(sd, location, "lga", getFieldName);
        // AVH additions
        addField(sd, location, "generalisedLocality", getFieldName); ///http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0977
        addField(sd, location, "nearNamedPlaceRelationTo", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0980
        addField(sd, location, "australianHerbariumRegion", getFieldName); //http://www.chah.org.au/hispid/terms/australianHerbariumRegion
        // For occurrences found to be outside the expert distribution range for the associated speces.
        addField(sd, location, "distanceOutsideExpertRange", getFieldName);
        addField(sd, location, "easting", getFieldName);
        addField(sd, location, "northing", getFieldName);
        addField(sd, location, "zone", getFieldName);
        addField(sd, location, "gridReference", getFieldName);
        addField(sd, location, "bbox", getFieldName); //stored in minX,minY,maxX,maxy format (not in JSON)
        location.put("marine", "Marine".equalsIgnoreCase(String.valueOf(sd.getFieldValue("biome"))));
        location.put("terrestrial", "Terrestrial".equalsIgnoreCase(String.valueOf(sd.getFieldValue("biome"))));

        // au.org.ala.biocache.model.Event
        Map event = new HashMap();
        fullRecord.put("event", event);
        addField(sd, event, "eventID", getFieldName);
        addField(sd, event, "parentEventID", getFieldName);
        addField(sd, event, "datasetName", getFieldName);
        addField(sd, event, "day", getFieldName);
        addField(sd, event, "endDayOfYear", getFieldName);
        addField(sd, event, "eventAttributes", getFieldName);
        addLocalDate(sd, event, "eventDate", getFieldName);
        addLocalDate(sd, event, "eventDateEnd", getFieldName);
        addField(sd, event, "eventRemarks", getFieldName);
        addField(sd, event, "eventTime", getFieldName);
        addField(sd, event, "verbatimEventDate", getFieldName);
        addField(sd, event, "year", getFieldName);
        addField(sd, event, "month", getFieldName);
        addField(sd, event, "startDayOfYear", getFieldName);
        //custom date range fields in biocache-store
        addField(sd, event, "startYear", getFieldName);
        addField(sd, event, "endYear", getFieldName);
        addField(sd, event, "datePrecision", getFieldName);

        // au.org.ala.biocache.model.Attribution
        Map attribution = new HashMap();
        fullRecord.put("attribution", attribution);
        addField(sd, attribution, "dataResourceName", getFieldName);
        // special case for dataResourceUid which needs to appear on raw and processed views
        addField(sd, attribution, "dataResourceUid", "dataResourceUid");
        addField(sd, attribution, "dataProviderUid", getFieldName);
        addField(sd, attribution, "dataProviderName", getFieldName);
        addField(sd, attribution, "collectionUid", getFieldName);
        addField(sd, attribution, "institutionUid", getFieldName);
        addField(sd, attribution, "dataHubUid", getFieldName);
        addField(sd, attribution, "dataHubName", getFieldName);
        addField(sd, attribution, "institutionName", getFieldName);
        addField(sd, attribution, "collectionName", getFieldName);
        addField(sd, attribution, "citation", getFieldName);
        addField(sd, attribution, "provenance", getFieldName);
        addField(sd, attribution, "license", getFieldName);

        // au.org.ala.biocache.model.Identification
        Map identification = new HashMap();
        fullRecord.put("identification", identification);
        addField(sd, identification, "dateIdentified", getFieldName);
        addField(sd, identification, "identificationAttributes", getFieldName);
        addField(sd, identification, "identificationID", getFieldName);
        addField(sd, identification, "identificationQualifier", getFieldName);
        addField(sd, identification, "identificationReferences", getFieldName);
        addField(sd, identification, "identificationRemarks", getFieldName);
        addField(sd, identification, "identificationVerificationStatus", getFieldName);
        addField(sd, identification, "identifiedBy", getFieldName);
        addField(sd, identification, "identifierRole", getFieldName); //HISPID addition http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0376
        addField(sd, identification, "typeStatus", getFieldName);
        /* AVH addition */
        addField(sd, identification, "abcdTypeStatus", getFieldName); //ABCD addition for AVH http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0645
        addField(sd, identification, "typeStatusQualifier", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0647
        addField(sd, identification, "typifiedName", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0604
        addField(sd, identification, "verbatimDateIdentified", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0383
        addField(sd, identification, "verifier", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0649
        addField(sd, identification, "verificationDate", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0657
        addField(sd, identification, "verificationNotes", getFieldName); //http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0658
        addField(sd, identification, "abcdIdentificationQualifier", getFieldName); //ABCD addition for AVH http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0332
        addField(sd, identification, "abcdIdentificationQualifierInsertionPoint", getFieldName); //ABCD addition for AVH http://wiki.tdwg.org/twiki/bin/view/ABCD/AbcdConcept0333

        // au.org.ala.biocache.model.Measurement
        Map measurement = new HashMap();
        fullRecord.put("measurement", measurement);
        addField(sd, measurement, "measurementAccuracy", getFieldName);
        addField(sd, measurement, "measurementDeterminedBy", getFieldName);
        addField(sd, measurement, "measurementDeterminedDate", getFieldName);
        addField(sd, measurement, "measurementID", getFieldName);
        addField(sd, measurement, "measurementMethod", getFieldName);
        addField(sd, measurement, "measurementRemarks", getFieldName);
        addField(sd, measurement, "measurementType", getFieldName);
        addField(sd, measurement, "measurementUnit", getFieldName);
        addField(sd, measurement, "measurementValue", getFieldName);

        addField(sd, fullRecord, "assertions", getFieldName);

        Map miscProperties = new HashMap();
        fullRecord.put("miscProperties", miscProperties);
        addField(sd, miscProperties, "references", getFieldName);
        addField(sd, miscProperties, "numIdentificationAgreements", getFieldName);

        Map queryAssertions = new HashMap();
        fullRecord.put("queryAssertions", queryAssertions);

        addField(sd, fullRecord, "userQualityAssertion", getFieldName);
        addField(sd, fullRecord, "hasUserAssertions", getFieldName);
        addField(sd, fullRecord, "userAssertionStatus", getFieldName.apply("userAssertions"));
        addField(sd, fullRecord, "assertionUserId", getFieldName);
        addField(sd, fullRecord, "locationDetermined", getFieldName);
        addField(sd, fullRecord, "defaultValuesUsed", getFieldName);
        addField(sd, fullRecord, "spatiallyValid", getFieldName);

        //keep for backwards compatibility
        fullRecord.put("geospatiallyKosher", (Boolean) sd.getFieldValue("spatiallyValid"));

        fullRecord.put("taxonomicallyKosher", "");

        fullRecord.put("deleted", false); // no deletion flags in use
        addField(sd, fullRecord, "userVerified", getFieldName); // same value for both raw and processed

        addInstant(sd, fullRecord, "firstLoaded", getFieldName.apply("firstLoadedDate"));
        addInstant(sd, fullRecord, "lastUserAssertionDate", getFieldName.apply("lastAssertionDate"));

        // no deletion flags in use in pipelines - field left in for backwards compatibility
        fullRecord.put("dateDeleted", "");

        return fullRecord;
    }

    private Map systemAssertions(SolrDocument sd) {

        List assertions = (List) sd.getFieldValue("assertions");

        List<ErrorCode> unchecked = new ArrayList();
        List<ErrorCode> warning = new ArrayList();
        List<ErrorCode> missing = new ArrayList();
        List<ErrorCode> passed = new ArrayList();
        List<ErrorCode> allErrorCodes = new ArrayList(Arrays.asList(AssertionCodes.getAll()));

        List<ErrorCode> flaggedErrors = new ArrayList<>();
        if (assertions != null) {
            for (Object assertion : assertions) {
                ErrorCode ec = AssertionCodes.getByName((String) assertion);
                if (ec != null) {
                    flaggedErrors.add(ec);
                    if (ErrorCode.Category.Missing.toString().equalsIgnoreCase(ec.getCategory()) || ec.getName().toLowerCase(Locale.ROOT).startsWith("missing")) {
                        missing.add(ec);
                    } else {
                        warning.add(ec);
                    }
                }
            }
        }

        // add the remaining to passes
        for (ErrorCode errorCode : allErrorCodes) {
            if (!flaggedErrors.contains(errorCode)) {
                // do we have the populated terms for this errorCode
                if (hasRequiredTerms(errorCode, sd)) {
                    passed.add(errorCode);
                } else {
                    unchecked.add(errorCode);
                }
            }
        }

        // sort alphabetically
        passed = passed.stream().sorted(new Comparator<ErrorCode>() {
            @Override
            public int compare(ErrorCode o1, ErrorCode o2) {
                return o1.getName().compareTo(o2.getName());
            }
        }).collect(Collectors.toList());

        warning = warning.stream().sorted(new Comparator<ErrorCode>() {
            @Override
            public int compare(ErrorCode o1, ErrorCode o2) {
                return o1.getName().compareTo(o2.getName());
            }
        }).collect(Collectors.toList());

        missing = missing.stream().sorted(new Comparator<ErrorCode>() {
            @Override
            public int compare(ErrorCode o1, ErrorCode o2) {
                return o1.getName().compareTo(o2.getName());
            }
        }).collect(Collectors.toList());

        Map systemAssertions = new HashMap();
        systemAssertions.put("missing", missing);
        systemAssertions.put("passed", passed); // no longer available
        systemAssertions.put("warning", warning);
        systemAssertions.put("unchecked", unchecked); // no longer available

        return systemAssertions;
    }

    /**
     * Will return true if one or more of the required terms is populated
     *
     * @param errorCode
     * @param sd
     * @return
     */
    boolean hasRequiredTerms(ErrorCode errorCode, SolrDocument sd) {

        if (errorCode.getTermsRequiredToTest().isEmpty()) {
            // missing the definition - assume we have the required terms
            return true;
        }

        for (String term : errorCode.getTermsRequiredToTest()) {
            Object termValue = sd.getFieldValue(term);
            Object rawTermValue = sd.getFieldValue(RAW_FIELD_PREFIX + term);
            if (Objects.nonNull(termValue)) {
                if (termValue instanceof String) {
                    if (StringUtils.isNotBlank((String) termValue)) {
                        return true;
                    } else {
                        return true;
                    }
                }
            }
            if (Objects.nonNull(rawTermValue)) {
                if (rawTermValue instanceof String) {
                    if (StringUtils.isNotBlank((String) rawTermValue)) {
                        return true;
                    } else {
                        return true;
                    }
                }
            }

        }
        return false;
    }

    private void logViewEvent(String ip, SolrDocument occ, String userAgent, String email, String reason) {
        //String ip = request.getLocalAddr();
        ConcurrentMap<String, AtomicInteger> uidStats = new ConcurrentHashMap<>();

        String collectionUid;
        String institutionUid;
        String dataProviderUid;
        String dataResourceUid;

        collectionUid = (String) occ.getFieldValue(OccurrenceIndex.COLLECTION_UID);
        institutionUid = (String) occ.getFieldValue(OccurrenceIndex.INSTITUTION_UID);
        dataProviderUid = (String) occ.getFieldValue(OccurrenceIndex.DATA_PROVIDER_UID);
        dataResourceUid = (String) occ.getFieldValue(OccurrenceIndex.DATA_RESOURCE_UID);

        if (StringUtils.isNotEmpty(collectionUid)) {
            uidStats.put(collectionUid, new AtomicInteger(1));
        }
        if (StringUtils.isNotEmpty(institutionUid)) {
            uidStats.put(institutionUid, new AtomicInteger(1));
        }
        if (StringUtils.isNotEmpty(dataProviderUid)) {
            uidStats.put(dataProviderUid, new AtomicInteger(1));
        }
        if (StringUtils.isNotEmpty(dataResourceUid)) {
            uidStats.put(dataResourceUid, new AtomicInteger(1));
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
        vo.setUserAgent(userAgent);

        loggerService.logEvent(vo);
    }

    /**
     * Constructs an error message to be displayed. The error message is based on validation checks that
     * were performed and stored in the supplied result.
     * <p>
     * TODO: If we decide to perform more detailed validations elsewhere it maybe worth providing this in a
     * util or service class.
     *
     * @param result The result from the validation.
     * @return A string representation that can be displayed in a browser.
     */
    private String getValidationErrorMessage(BindingResult result) {
        StringBuilder sb = new StringBuilder();
        List<ObjectError> errors = result.getAllErrors();
        for (ObjectError error : errors) {
            logger.debug("Code: " + error.getCode());
            logger.debug(StringUtils.join(error.getCodes(), "@#$^"));
            String code = (error.getCodes() != null && error.getCodes().length > 0) ? error.getCodes()[0] : null;
            logger.debug("The code in use:" + code);
            sb.append(messageSource.getMessage(code, null, error.getDefaultMessage(), null)).append("<br/>");
        }
        return sb.toString();
    }


    private List<MediaDTO> setupImageUrls(String uuid, List<String> imageIDs, boolean lookupImageMetadata) {
        if (imageIDs != null && !imageIDs.isEmpty()) {

            List<MediaDTO> ml = new ArrayList<>();
            Map<String, Map> metadata = new HashMap();

            if (lookupImageMetadata) {
                try {
                    List<Map<String, Object>> list = imageMetadataService.getImageMetadataForOccurrences(Arrays.asList(new String[]{uuid})).get(uuid);
                    if (list != null) {
                        for (Map m : list) {
                            metadata.put(String.valueOf(m.get("imageId")), m);
                        }
                    }
                } catch (Exception e) {
                }
            }

            for (String fileNameOrID : imageIDs) {
                try {
                    MediaDTO m = new MediaDTO();
                    Map<String, String> urls = occurrenceUtils.getImageFormats(fileNameOrID);
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
            return ml;
        }
        return null;
    }

    /**
     * Perform one pivot facet query.
     * <p/>
     * Requires valid apiKey.
     * <p/>
     * facets is the pivot facet list
     */
    @Deprecated
    @RequestMapping("occurrence/pivot")
    public
    @ResponseBody
    List<FacetPivotResultDTO> searchPivot(SpatialSearchRequestParams searchParams,
                                          @RequestParam(value = "apiKey", required = true) String apiKey,
                                          HttpServletResponse response) throws Exception {
        if (isValidKey(apiKey)) {
            return searchDAO.searchPivot(searchParams);
        }

        response.sendError(HttpServletResponse.SC_FORBIDDEN, "An invalid API Key was provided.");
        return null;
    }

    /**
     * List all facets available for a query.
     * <p/>
     * Requires valid apiKey because it is very slow.
     */
    @Deprecated
    @RequestMapping("occurrences/facets/available")
    public
    @ResponseBody
    List<String> listFacets(SpatialSearchRequestParams searchParams,
                            @RequestParam(value = "apiKey", required = true) String apiKey,
                            HttpServletResponse response) throws Exception {
        if (isValidKey(apiKey)) {
            return searchDAO.listFacets(searchParams);
        }

        response.sendError(HttpServletResponse.SC_FORBIDDEN, "An invalid API Key was provided.");
        return null;
    }
}
