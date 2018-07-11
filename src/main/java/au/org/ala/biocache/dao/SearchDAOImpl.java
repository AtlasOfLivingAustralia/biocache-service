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
package au.org.ala.biocache.dao;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import au.org.ala.biocache.Config;
import au.org.ala.biocache.RecordWriter;
import au.org.ala.biocache.Store;
import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.service.*;
import au.org.ala.biocache.stream.OptionalZipOutputStream;
import au.org.ala.biocache.util.*;
import au.org.ala.biocache.util.thread.EndemicCallable;
import au.org.ala.biocache.vocab.ErrorCode;
import au.org.ala.biocache.writer.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.googlecode.ehcache.annotations.Cacheable;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.RangeFacet.Numeric;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;

/**
 * SOLR implementation of SearchDao.
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 * @see au.org.ala.biocache.dao.SearchDAO
 */

@Component("searchDao")
public class SearchDAOImpl implements SearchDAO {

    /**
     * log4j logger
     */
    private static final Logger logger = Logger.getLogger(SearchDAOImpl.class);

    public static final String DECADE_FACET_START_DATE = "1850-01-01T00:00:00Z";
    public static final String DECADE_PRE_1850_LABEL = "before";
    public static final String SOLR_DATE_FORMAT = "yyyy-MM-dd'T'hh:mm:ss'Z'";
    public static final String OCCURRENCE_YEAR_INDEX_FIELD = "occurrence_year";

    //sensitive fields and their non-sensitive replacements
    private static final String[] sensitiveCassandraHdr = {"decimalLongitude", "decimalLatitude", "verbatimLocality"};
    private static final String[] sensitiveSOLRHdr = {"sensitive_longitude", "sensitive_latitude", "sensitive_locality", "sensitive_event_date", "sensitive_event_date_end", "sensitive_grid_reference"};
    private static final String[] notSensitiveCassandraHdr = {"decimalLongitude_p", "decimalLatitude_p", "locality"};
    private static final String[] notSensitiveSOLRHdr = {"longitude", "latitude", "locality"};

    /**
     * SOLR client instance
     */
    @Inject
    protected SolrClient solrClient;

    protected SolrRequest.METHOD queryMethod;
    /**
     * Limit search results - for performance reasons
     */
    @Value("${download.max:500000}")
    public Integer MAX_DOWNLOAD_SIZE = 500000;
    /**
     * Throttle value used to split up large downloads from Solr.
     * Randomly set to a range of 100% up to 200% of the value given here in each case.
     **/
    @Value("${download.throttle.ms:50}")
    protected Integer throttle = 50;
    /**
     * Batch size for a download
     */
    @Value("${download.batch.size:500}")
    protected Integer downloadBatchSize = 500;
    /**
     * The size of an internal fixed length blocking queue used to parallelise
     * reading from Solr using 'solr.downloadquery.maxthreads' producers before
     * writing from the queue using a single consumer thread.
     * <br> This should be set large enough so that writing to the output stream
     * is the limiting factor, but not so large as to allow OutOfMemoryError's to
     * occur due to its memory usage.
     **/
    @Value("${download.internal.queue.size:100}")
    protected Integer resultsQueueLength;
    /**
     * Maximum total time for downloads to be execute. Defaults to 1 week (604,800,000ms)
     */
    @Value("${download.max.execute.time:604800000}")
    protected Long downloadMaxTime = 604800000L;
    /**
     * Maximum total time for downloads to be allowed to normally complete before they are aborted,
     * once all of the Solr/etc. queries have been completed or aborted and the RecordWriter is reading the remaining download.internal.queue.size items off the queue.
     * Defaults to 5 minutes (300,000ms)
     */
    @Value("${download.max.completion.time:300000}")
    protected Long downloadMaxCompletionTime = 300000L;
    public static final String NAMES_AND_LSID = "names_and_lsid";
    public static final String COMMON_NAME_AND_LSID = "common_name_and_lsid";
    protected static final String DECADE_FACET_NAME = "decade";
    protected static final Integer FACET_PAGE_SIZE = 1000;
    protected static final String RANGE_SUFFIX = "_RNG";

    private String spatialField = "geohash";

    protected Pattern layersPattern = Pattern.compile("(el|cl)[0-9abc]+");
    protected Pattern clpField = Pattern.compile("(,|^)cl.p(,|$)");
    protected Pattern elpField = Pattern.compile("(,|^)el.p(,|$)");
    protected Pattern allDwcField = Pattern.compile("(,|^)allDwc(,|$)");

    /**
     * solr connection retry limit
     **/
    @Value("${solr.server.retry.max:6}")
    protected int maxRetries = 6;
    /**
     * solr connection wait time between retries in ms
     **/
    @Value("${solr.server.retry.wait:50}")
    protected long retryWait = 50;
    /**
     * solr index version refresh time in ms, 5*60*1000
     **/
    @Value("${solr.server.indexVersion.refresh:300000}")
    protected int solrIndexVersionRefreshTime = 300000;

    @Value("${shapefile.tmp.dir:/data/biocache-download/tmp}")
    protected String tmpShapefileDir;

    @Value("${download.unzipped.limit:10000}")
    public Integer unzippedLimit;

    /**
     * Download properties
     */
    protected DownloadFields downloadFields;

    @Inject
    protected SearchUtils searchUtils;

    @Inject
    protected QueryFormatUtils queryFormatUtils;

    @Inject
    protected CollectionsCache collectionCache;

    @Inject
    protected AbstractMessageSource messageSource;

    @Inject
    protected SpeciesLookupService speciesLookupService;

    @Inject
    protected AuthService authService;

    @Inject
    protected LayersService layersService;

    @Inject
    protected QidCacheDAO qidCacheDao;

    @Inject
    protected RangeBasedFacets rangeBasedFacets;

    @Inject
    protected SpeciesCountsService speciesCountsService;

    @Inject
    protected SpeciesImageService speciesImageService;

    @Inject
    protected ListsService listsService;

    @Inject
    protected DownloadService downloadService;

    @Value("${media.store.local:true}")
    protected Boolean usingLocalMediaRepo = true;

    /**
     * Max number of threads to use in parallel for endemic queries
     */
    @Value("${endemic.query.maxthreads:30}")
    protected Integer maxEndemicQueryThreads = 30;

    /**
     * Max number of threads to use in parallel for large online solr download queries
     */
    @Value("${solr.downloadquery.maxthreads:30}")
    protected Integer maxSolrDownloadThreads = 30;

    /**
     * The time (ms) to wait for the blocking queue to have new capacity between thread interruption checks.
     */
    @Value("${solr.downloadquery.writertimeout:60000}")
    protected Long writerTimeoutWaitMillis = 60000L;

    /**
     * The time (ms) to wait between checking if interrupts have occurred or all of the download tasks have completed.
     */
    @Value("${solr.downloadquery.busywaitsleep:100}")
    protected Long downloadCheckBusyWaitSleep = 100L;

    /**
     * thread pool for multipart endemic queries
     */
    private volatile ExecutorService endemicExecutor = null;

    /**
     * thread pool for faceted solr queries
     */
    private volatile ExecutorService solrOnlineExecutor = null;

    /**
     * should we check download limits
     */
    @Value("${check.download.limits:false}")
    protected boolean checkDownloadLimits = false;

    @Value("${term.query.limit:1000}")
    protected Integer termQueryLimit = 1000;

    @Value("${media.url:http://biocache.ala.org.au/biocache-media/}")
    public String biocacheMediaUrl = "http://biocache.ala.org.au/biocache-media/";

    @Value("${media.dir:/data/biocache-media/}")
    public String biocacheMediaDir = "/data/biocache-media/";

    @Value("${default.download.fields:id,data_resource_uid,data_resource,license,catalogue_number,taxon_concept_lsid,raw_taxon_name,raw_common_name,taxon_name,rank,common_name,kingdom,phylum,class,order,family,genus,species,subspecies,institution_code,collection_code,locality,raw_latitude,raw_longitude,raw_datum,latitude,longitude,coordinate_precision,coordinate_uncertainty,country,state,cl959,min_elevation_d,max_elevation_d,min_depth_d,max_depth_d,individual_count,recorded_by,year,month,day,verbatim_event_date,basis_of_record,raw_basis_of_record,sex,preparations,information_withheld,data_generalizations,outlier_layer,taxonomic_kosher,geospatial_kosher}")
    protected String defaultDownloadFields;

    /**
     * A list of fields that are left in the index for legacy reasons, but are removed from the public API to avoid confusion.
     */
    @Value("${index.fields.tohide:collector_text,location_determined,row_key,matched_name,decimal_latitudelatitude,collectors,default_values_used,generalisation_to_apply_in_metres,geohash,ibra_subregion,identifier_by,occurrence_details,text,photo_page_url,photographer,places,portal_id,quad,rem_text,occurrence_status_s,identification_qualifier_s}")
    protected String indexFieldsToHide;

    private volatile Set<IndexFieldDTO> indexFields = new ConcurrentHashSet<IndexFieldDTO>(); //RestartDataService.get(this, "indexFields", new TypeReference<TreeSet<IndexFieldDTO>>(){}, TreeSet.class);
    private volatile Map<String, IndexFieldDTO> indexFieldMap = RestartDataService.get(this, "indexFieldMap", new TypeReference<HashMap<String, IndexFieldDTO>>(){}, HashMap.class);
    private final Map<String, StatsIndexFieldDTO> rangeFieldCache = new HashMap<String, StatsIndexFieldDTO>();

    /**
     * SOLR index version for client app caching use.
     */
    private volatile long solrIndexVersion = 0;
    /**
     * last time SOLR index version was refreshed
     */
    private volatile long solrIndexVersionTime = 0;
    /**
     * Lock object used to synchronize updates to the solr index version
     */
    private final Object solrIndexVersionLock = new Object();

    @Value("${wms.colour:0x00000000}")
    protected int DEFAULT_COLOUR;

    @Value("${dwc.url:http://rs.tdwg.org/dwc/terms/}")
    protected String dwcUrl = "http://rs.tdwg.org/dwc/terms/";

    /**
     * max.boolean.clauses is automatically determined when set to 0
     */
    @Value("${max.boolean.clauses:1024}")
    private int maxBooleanClauses;

    @Value("${layers.service.url:http://spatial.ala.org.au/ws}")
    protected String layersServiceUrl;

    /**
     * Initialise the SOLR server instance
     */
    public SearchDAOImpl() {
    }

    @PostConstruct
    public void init() throws Exception {

        logger.debug("Initialising SearchDAOImpl");

        queryMethod = solrClient instanceof EmbeddedSolrServer ? SolrRequest.METHOD.GET : SolrRequest.METHOD.POST;

        // TODO: There was a note about possible issues with the following two lines
        Set<IndexFieldDTO> indexedFields = getIndexedFields();
        if (downloadFields == null) {
            downloadFields = new DownloadFields(indexedFields, messageSource, layersService);
        } else {
            downloadFields.update(indexedFields);
        }

        getMaxBooleanClauses();
    }

    public void refreshCaches() {

        try {
            init(); // In the past the call internally logged the exception but the caller was unaware of any issues
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        collectionCache.updateCache();
        //empties the range cache to allow the settings to be recalculated.
        rangeFieldCache.clear();
        try {
            //update indexed fields
            downloadFields.update(getIndexedFields(true));
        } catch (Exception e) {
            logger.error("Unable to refresh cache.", e);
        }
        speciesImageService.resetCache();
        speciesCountsService.resetCache();

        listsService.refreshCache();
        layersService.refreshCache();
    }

    /**
     * Returns a list of species that are endemic to the supplied region. Values are cached
     * due to the "expensive" operation.
     */
    @Cacheable(cacheName = "endemicCache")
    public List<FieldResultDTO> getEndemicSpecies(SpatialSearchRequestParams requestParams) throws Exception {
        ExecutorService nextExecutor = getEndemicThreadPoolExecutor();
        // 1)get a list of species that are in the WKT
        if (logger.isDebugEnabled()) {
            logger.debug("Starting to get Endemic Species...");
        }
        List<FieldResultDTO> list1 = getValuesForFacet(requestParams);//new ArrayList(Arrays.asList(getValuesForFacets(requestParams)));
        if (logger.isDebugEnabled()) {
            logger.debug("Retrieved species within area...(" + list1.size() + ")");
        }
        // 2)get a list of species that occur in the inverse WKT

        String reverseQuery = SpatialUtils.getWKTQuery(spatialField, requestParams.getWkt(), true);//"-geohash:\"Intersects(" +wkt + ")\"";

        if (logger.isDebugEnabled()) {
            logger.debug("The reverse query:" + reverseQuery);
        }

        requestParams.setWkt(null);

        int i = 0, localterms = 0;

        String facet = requestParams.getFacets()[0];
        String[] originalFqs = requestParams.getFq();
        //add the negated WKT query to the fq
        originalFqs = (String[]) ArrayUtils.add(originalFqs, reverseQuery);
        List<Future<List<FieldResultDTO>>> threads = new ArrayList<Future<List<FieldResultDTO>>>();
        //batch up the rest of the world query so that we have fqs based on species we want to test for. This should improve the performance of the endemic services.
        while (i < list1.size()) {
            StringBuffer sb = new StringBuffer();
            while ((localterms == 0 || localterms % termQueryLimit != 0) && i < list1.size()) {
                if (localterms > 0) {
                    sb.append(" OR ");
                }
                sb.append(facet).append(":").append(ClientUtils.escapeQueryChars(list1.get(i).getFieldValue()));
                i++;
                localterms++;
            }
            String newfq = sb.toString();
            if (localterms == 1)
                newfq = newfq + " OR " + newfq; //cater for the situation where there is only one term.  We don't want the term to be escaped again
            localterms = 0;
            //System.out.println("FQ = " + newfq);
            SpatialSearchRequestParams srp = new SpatialSearchRequestParams();
            BeanUtils.copyProperties(requestParams, srp);
            srp.setFq((String[]) ArrayUtils.add(originalFqs, newfq));
            int batch = i / termQueryLimit;
            EndemicCallable callable = new EndemicCallable(srp, batch, this);
            threads.add(nextExecutor.submit(callable));
        }
        for (Future<List<FieldResultDTO>> future : threads) {
            List<FieldResultDTO> list = future.get();
            if (list != null) {
                list1.removeAll(list);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Determined final endemic list (" + list1.size() + ")...");
        }
        return list1;
    }

    /**
     * @return An instance of ExecutorService used to concurrently execute multiple endemic queries.
     */
    private ExecutorService getEndemicThreadPoolExecutor() {
        ExecutorService nextExecutor = endemicExecutor;
        if (nextExecutor == null) {
            synchronized (this) {
                nextExecutor = endemicExecutor;
                if (nextExecutor == null) {
                    nextExecutor = endemicExecutor = Executors.newFixedThreadPool(
                            getMaxEndemicQueryThreads(),
                            new ThreadFactoryBuilder().setNameFormat("biocache-endemic-%d")
                                    .setPriority(Thread.MIN_PRIORITY).build());
                }
            }
        }
        return nextExecutor;
    }

    /**
     * @return An instance of ExecutorService used to concurrently execute multiple solr queries for online downloads.
     */
    private ExecutorService getSolrOnlineThreadPoolExecutor() {
        ExecutorService nextExecutor = solrOnlineExecutor;
        if (nextExecutor == null) {
            synchronized (this) {
                nextExecutor = solrOnlineExecutor;
                if (nextExecutor == null) {
                    nextExecutor = solrOnlineExecutor = Executors.newFixedThreadPool(
                            getMaxSolrOnlineDownloadThreads(),
                            new ThreadFactoryBuilder().setNameFormat("biocache-solr-online-%d")
                                    .setPriority(Thread.MIN_PRIORITY).build());
                }
            }
        }
        return nextExecutor;
    }

    /**
     * (Endemic)
     * <p>
     * Returns a list of species that are only within a subQuery.
     * <p>
     * The subQuery is a subset of parentQuery.
     */
    public List<FieldResultDTO> getSubquerySpeciesOnly(SpatialSearchRequestParams subQuery, SpatialSearchRequestParams parentQuery) throws Exception {
        ExecutorService nextExecutor = getEndemicThreadPoolExecutor();
        // 1)get a list of species that are in the WKT
        if (logger.isDebugEnabled()) {
            logger.debug("Starting to get Endemic Species...");
        }
        subQuery.setFacet(true);
        subQuery.setFacets(parentQuery.getFacets());
        List<FieldResultDTO> list1 = getValuesForFacet(subQuery);
        if (logger.isDebugEnabled()) {
            logger.debug("Retrieved species within area...(" + list1.size() + ")");
        }

        int i = 0, localterms = 0;

        String facet = parentQuery.getFacets()[0];
        String[] originalFqs = parentQuery.getFq();
        List<Future<List<FieldResultDTO>>> futures = new ArrayList<Future<List<FieldResultDTO>>>();
        //batch up the rest of the world query so that we have fqs based on species we want to test for.
        // This should improve the performance of the endemic services.
        while (i < list1.size()) {
            StringBuffer sb = new StringBuffer();
            while ((localterms == 0 || localterms % termQueryLimit != 0) && i < list1.size()) {
                if (localterms > 0) {
                    sb.append(" OR ");
                }
                String value = list1.get(i).getFieldValue();
                if (facet.equals(NAMES_AND_LSID)) {
                    if (value.startsWith("\"") && value.endsWith("\"")) {
                        value = value.substring(1, value.length() - 1);
                    }
                    value = "\"" + ClientUtils.escapeQueryChars(value) + "\"";
                } else {
                    value = ClientUtils.escapeQueryChars(value);
                }
                sb.append(facet).append(":").append(value);
                i++;
                localterms++;
            }
            String newfq = sb.toString();
            if (localterms == 1)
                newfq = newfq + " OR " + newfq; //cater for the situation where there is only one term.  We don't want the term to be escaped again
            localterms = 0;
            SpatialSearchRequestParams srp = new SpatialSearchRequestParams();
            BeanUtils.copyProperties(parentQuery, srp);
            srp.setFq((String[]) ArrayUtils.add(originalFqs, newfq));
            int batch = i / termQueryLimit;
            EndemicCallable callable = new EndemicCallable(srp, batch, this);
            futures.add(nextExecutor.submit(callable));
        }

        Collections.sort(list1);
        for (Future<List<FieldResultDTO>> future : futures) {
            List<FieldResultDTO> list = future.get();
            if (list != null) {
                for (FieldResultDTO find : list) {
                    int idx = Collections.binarySearch(list1, find);
                    //remove if sub query count < parent query count
                    if (idx >= 0 && list1.get(idx).getCount() < find.getCount()) {
                        list1.remove(idx);
                    }
                }
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Determined final endemic list (" + list1.size() + ")...");
        }
        return list1;
    }

    public void writeEndemicFacetToStream(SpatialSearchRequestParams subQuery, SpatialSearchRequestParams parentQuery, boolean includeCount, boolean lookupName, boolean includeSynonyms, boolean includeLists, OutputStream out) throws Exception {
        List<FieldResultDTO> list = getSubquerySpeciesOnly(subQuery, parentQuery);
        String facet = parentQuery.getFacets()[0];

        boolean shouldLookup = lookupName && (facet.contains("_guid") || facet.contains("_lsid"));

        String[] header = new String[]{facet};
        if (shouldLookup) {
            header = speciesLookupService.getHeaderDetails(facet, includeCount, includeSynonyms);
        } else if (includeCount) {
            header = (String[]) ArrayUtils.add(header, "count");
        }
        if (includeLists) {
            header = (String[]) ArrayUtils.addAll(header, listsService.getTypes().toArray(new String[]{}));
        }
        CSVRecordWriter writer = new CSVRecordWriter(out, header);
        try {
            writer.initialise();
    
            boolean addedNullFacet = false;
    
            List<String> guids = new ArrayList<String>();
            List<Long> counts = new ArrayList<Long>();
    
            for (FieldResultDTO ff : list) {
                //only add null facet once
                if (ff.getLabel() == null) addedNullFacet = true;
                if (ff.getCount() == 0 || (ff.getLabel() == null && addedNullFacet)) continue;
    
                //process the "species_guid_ facet by looking up the list of guids
                if (shouldLookup) {
                    guids.add(ff.getLabel());
                    if (includeCount) {
                        counts.add(ff.getCount());
                    }
    
                    //Only want to send a sub set of the list so that the URI is not too long for BIE
                    if (guids.size() == 30) {
                        //now get the list of species from the web service TODO may need to move this code
                        //handle null values being returned from the service...
                        writeTaxonDetailsToStream(guids, counts, includeCount, includeSynonyms, includeLists, writer);
                        guids.clear();
                        counts.clear();
                    }
                } else {
                    //default processing of facets
                    String name = ff.getLabel() != null ? ff.getLabel() : "";
                    String[] row = includeCount ? new String[]{name, Long.toString(ff.getCount())} : new String[]{name};
                    writer.write(row);
                }
            }
    
            if (shouldLookup) {
                //now write any guids that remain at the end of the looping
                writeTaxonDetailsToStream(guids, counts, includeCount, includeSynonyms, includeLists, writer);
            }
        } finally {
            writer.finalise();
        }
    }

    /**
     * Returns the values and counts for a single facet field.
     */
    public List<FieldResultDTO> getValuesForFacet(SpatialSearchRequestParams requestParams) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        writeFacetToStream(requestParams, true, false, false, false, outputStream, null);
        outputStream.flush();
        outputStream.close();
        String includedValues = outputStream.toString(StandardCharsets.UTF_8);
        includedValues = includedValues == null ? "" : includedValues;
        String[] values = includedValues.split("\n");
        List<FieldResultDTO> list = new ArrayList<FieldResultDTO>();
        boolean first = true;
        for (String value : values) {
            if (first) {
                first = false;
            } else {
                int idx = value.lastIndexOf(",");
                //handle values enclosed in "
                String name = value.substring(0, idx);
                list.add(
                    new FieldResultDTO(
                        name,
                        name,
                        Long.parseLong(value.substring(idx + 1).replace("\"", ""))
                    )
                );
            }
        }
        return list;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#findByFulltextSpatialQuery
     */
    @Override
    public SearchResultDTO findByFulltextSpatialQuery(SpatialSearchRequestParams searchParams, Map<String, String[]> extraParams) {
        return findByFulltextSpatialQuery(searchParams, false, extraParams);
    }

    /**
     * Main search query method.
     *
     * @param searchParams
     * @param includeSensitive
     * @param extraParams
     * @return
     */
    @Override
    public SearchResultDTO findByFulltextSpatialQuery(SpatialSearchRequestParams searchParams, boolean includeSensitive, Map<String, String[]> extraParams) {
        SearchResultDTO searchResults = new SearchResultDTO();
        SpatialSearchRequestParams original = new SpatialSearchRequestParams();
        BeanUtils.copyProperties(searchParams, original);
        try {
            Map<String, Facet> activeFacetMap = queryFormatUtils.formatSearchQuery(searchParams, true);
            String queryString = searchParams.getFormattedQuery();
            SolrQuery solrQuery = initSolrQuery(searchParams, true, extraParams); // general search settings
            solrQuery.setQuery(queryString);

            QueryResponse qr = runSolrQuery(solrQuery, searchParams);
            //need to set the original q to the processed value so that we remove the wkt etc that is added from paramcache object
            Class resultClass = includeSensitive ? au.org.ala.biocache.dto.SensitiveOccurrenceIndex.class : OccurrenceIndex.class;
            searchResults = processSolrResponse(original, qr, solrQuery, resultClass);
            searchResults.setQueryTitle(searchParams.getDisplayString());
            searchResults.setUrlParameters(original.getUrlParams());

            //now update the fq display map...
            searchResults.setActiveFacetMap(activeFacetMap);

            if (logger.isInfoEnabled()) {
                logger.info("spatial search query: " + queryString);
            }
        } catch (Exception ex) {
            logger.error("Error executing query with requestParams: " + searchParams.toString(), ex);
            searchResults.setStatus("ERROR"); // TODO also set a message field on this bean with the error message(?)
            searchResults.setErrorMessage(ex.getMessage());
        }

        return searchResults;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#writeSpeciesCountByCircleToStream(au.org.ala.biocache.dto.SpatialSearchRequestParams, String, javax.servlet.ServletOutputStream)
     */
    public int writeSpeciesCountByCircleToStream(SpatialSearchRequestParams searchParams, String speciesGroup, ServletOutputStream out) throws Exception {

        //get the species counts:
        if (logger.isDebugEnabled()) {
            logger.debug("Writing CSV file for species count by circle");
        }
        searchParams.setPageSize(-1);
        List<TaxaCountDTO> species = findAllSpeciesByCircleAreaAndHigherTaxa(searchParams, speciesGroup);
        if (logger.isDebugEnabled()) {
            logger.debug("There are " + species.size() + "records being downloaded");
        }
        try (CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(new CloseShieldOutputStream(out), StandardCharsets.UTF_8), '\t', '"');) {
            csvWriter.writeNext(new String[]{
                    "Taxon ID",
                    "Kingdom",
                    "Family",
                    "Scientific name",
                    "Common name",
                    "Record count",});
            int count = 0;
            for (TaxaCountDTO item : species) {

                String[] record = new String[]{
                        item.getGuid(),
                        item.getKingdom(),
                        item.getFamily(),
                        item.getName(),
                        item.getCommonName(),
                        item.getCount().toString()
                };

                csvWriter.writeNext(record);
                count++;
            }
            csvWriter.flush();
            return count;
        }
    }

    /**
     * Writes the values for the first supplied facet to output stream
     *
     * @param includeCount true when the count should be included in the download
     * @param lookupName   true when a name lsid should be looked up in the bie
     */
    public void writeFacetToStream(SpatialSearchRequestParams searchParams, boolean includeCount, boolean lookupName, boolean includeSynonyms, boolean includeLists, OutputStream out, DownloadDetailsDTO dd) throws Exception {
        //set to unlimited facets
        searchParams.setFlimit(-1);
        queryFormatUtils.formatSearchQuery(searchParams);
        String queryString = searchParams.getFormattedQuery();
        SolrQuery solrQuery = initSolrQuery(searchParams, false, null);
        solrQuery.setQuery(queryString);

        //don't want any results returned
        solrQuery.setRows(0);
        searchParams.setPageSize(0);
        solrQuery.setFacetLimit(FACET_PAGE_SIZE);
        int offset = 0;
        boolean shouldLookupTaxon = lookupName && (searchParams.getFacets()[0].contains("_guid") || searchParams.getFacets()[0].contains("_lsid"));
        boolean shouldLookupAttribution = lookupName && searchParams.getFacets()[0].contains("_uid");

        if (dd != null) {
            dd.resetCounts();
        }

        QueryResponse qr = runSolrQuery(solrQuery, searchParams);
        if (logger.isDebugEnabled()) {
            logger.debug("Retrieved facet results from server...");
        }
        if (qr.getResults().getNumFound() > 0) {
            FacetField ff = qr.getFacetField(searchParams.getFacets()[0]);

            //write the header line
            if (ff != null) {
                String[] header = new String[]{ff.getName()};
                if (shouldLookupTaxon) {
                    header = speciesLookupService.getHeaderDetails(ff.getName(), includeCount, includeSynonyms);
                } else if (shouldLookupAttribution){
                    header = (String[]) ArrayUtils.addAll(header, new String[]{"name", "count"});
                } else if (includeCount) {
                    header = (String[]) ArrayUtils.add(header, "count");
                }
                if (includeLists) {
                    header = (String[]) ArrayUtils.addAll(header, listsService.getTypes().toArray(new String[]{}));
                }

                CSVRecordWriter writer = new CSVRecordWriter(new CloseShieldOutputStream(out), header);
                try {
                    writer.initialise();
                    boolean addedNullFacet = false;

                    //PAGE through the facets until we reach the end.
                    //do not continue when null facet is already added and the next facet is only null
                    while (ff.getValueCount() > 1 || !addedNullFacet || (ff.getValueCount() == 1 && ff.getValues().get(0).getName() != null)) {
                        //process the "species_guid_ facet by looking up the list of guids
                        if (shouldLookupTaxon) {
                            List<String> guids = new ArrayList<String>();
                            List<Long> counts = new ArrayList<Long>();
                            if (logger.isDebugEnabled()) {
                                logger.debug("Downloading " + ff.getValueCount() + " species guids");
                            }
                            for (FacetField.Count value : ff.getValues()) {
                                //only add null facet once
                                if (value.getName() == null) addedNullFacet = true;
                                if (value.getCount() == 0 || (value.getName() == null && addedNullFacet)) continue;

                                guids.add(value.getName());
                                if (includeCount) {
                                    counts.add(value.getCount());
                                }

                                //Only want to send a sub set of the list so that the URI is not too long for BIE
                                if (guids.size() == 30) {
                                    //now get the list of species from the web service TODO may need to move this code
                                    //handle null values being returned from the service...
                                    writeTaxonDetailsToStream(guids, counts, includeCount, includeSynonyms, includeLists, writer);
                                    guids.clear();
                                    counts.clear();
                                }
                            }
                            //now write any guids that remain at the end of the looping
                            writeTaxonDetailsToStream(guids, counts, includeCount, includeSynonyms, includeLists, writer);
                        } else {
                            //default processing of facets
                            for (FacetField.Count value : ff.getValues()) {
                                //only add null facet once
                                if (value.getName() == null) addedNullFacet = true;
                                if (value.getCount() == 0 || (value.getName() == null && addedNullFacet)) continue;

                                String name = value.getName() != null ? value.getName() : "";
                                if (shouldLookupAttribution) {
                                    writer.write(includeCount ? new String[]{name, collectionCache.getNameForCode(name), Long.toString(value.getCount())} : new String[]{name});
                                } else {
                                    writer.write(includeCount ? new String[]{name, Long.toString(value.getCount())} : new String[]{name});
                                }
                            }
                        }
                        offset += FACET_PAGE_SIZE;
                        if (dd != null) {
                            dd.updateCounts(FACET_PAGE_SIZE);
                        }

                        //get the next values
                        solrQuery.remove("facet.offset");
                        solrQuery.add("facet.offset", Integer.toString(offset));
                        qr = runSolrQuery(solrQuery, searchParams);
                        ff = qr.getFacetField(searchParams.getFacets()[0]);
                    }
                } finally {
                    writer.finalise();
                }
            }
        }
    }

    /**
     * Writes additional taxon information to the stream. It performs bulk lookups to the
     * BIE in order to obtain extra classification information
     *
     * @param guids           The guids to lookup
     * @param counts          The occurrence counts for each guid if "includeCounts = true"
     * @param includeCounts   Whether or not to include the occurrence counts in the download
     * @param includeSynonyms whether or not to include the synonyms in the download - when
     *                        true this will perform additional lookups in the BIE
     * @param writer          The CSV writer to write to.
     * @throws Exception
     */
    private void writeTaxonDetailsToStream(List<String> guids, List<Long> counts, boolean includeCounts, boolean includeSynonyms, boolean includeLists, CSVRecordWriter writer) throws Exception {
        List<String[]> values = speciesLookupService.getSpeciesDetails(guids, counts, includeCounts, includeSynonyms, includeLists);
        for (String[] value : values) {
            writer.write(value);
        }
    }

    /**
     * Writes all the distinct latitude and longitude in the index to the supplied
     * output stream.
     *
     * @param out
     * @throws Exception
     */
    public void writeCoordinatesToStream(SearchRequestParams searchParams, OutputStream out) throws Exception {
        //generate the query to obtain the lat,long as a facet
        SearchRequestParams srp = new SearchRequestParams();
        SearchUtils.setDefaultParams(srp);
        srp.setFacets(searchParams.getFacets());

        SolrQuery solrQuery = initSolrQuery(srp, false, null);
        //We want all the facets so we can dump all the coordinates
        solrQuery.setFacetLimit(-1);
        solrQuery.setFacetSort("count");
        solrQuery.setRows(0);
        solrQuery.setQuery(searchParams.getQ());

        QueryResponse qr = runSolrQuery(solrQuery, srp);
        if (qr.getResults().size() > 0) {
            FacetField ff = qr.getFacetField(searchParams.getFacets()[0]);
            if (ff != null && ff.getValueCount() > 0) {
                out.write("latitude,longitude\n".getBytes(StandardCharsets.UTF_8));
                //write the facets to file
                for (FacetField.Count value : ff.getValues()) {
                    //String[] slatlon = value.getName().split(",");
                    if (value.getName() != null && value.getCount() > 0) {
                        out.write(value.getName().getBytes(StandardCharsets.UTF_8));
                        out.write("\n".getBytes(StandardCharsets.UTF_8));
                    }
                }
            }
        }
    }

    /**
     * Writes the index fields to the supplied output stream in CSV format.
     * <p>
     * DM: refactored to split the query by month to improve performance.
     * Further enhancements possible:
     * 1) Multi threaded
     * 2) More filtering, by year or decade..
     *
     * @param downloadParams
     * @param out
     * @param includeSensitive
     * @param dd               The details of the download
     * @param checkLimit
     * @throws Exception
     */
    @Override
    public ConcurrentMap<String, AtomicInteger> writeResultsFromIndexToStream(final DownloadRequestParams downloadParams,
                                                                              final OutputStream out,
                                                                              final boolean includeSensitive,
                                                                              final DownloadDetailsDTO dd,
                                                                              final boolean checkLimit) throws Exception {
        ExecutorService nextExecutor = getSolrOnlineThreadPoolExecutor();
        return writeResultsFromIndexToStream(downloadParams, out, includeSensitive, dd, checkLimit, nextExecutor);
    }

    /**
     * Writes the index fields to the supplied output stream in CSV format.
     * <p>
     * DM: refactored to split the query by month to improve performance.
     * Further enhancements possible:
     * 1) Multi threaded
     * 2) More filtering, by year or decade..
     *
     * @param downloadParams
     * @param out
     * @param includeSensitive
     * @param dd               The details of the download
     * @param checkLimit
     * @param nextExecutor     The ExecutorService to use to process results on different threads
     * @throws Exception
     */
    @Override
    public ConcurrentMap<String, AtomicInteger> writeResultsFromIndexToStream(final DownloadRequestParams downloadParams,
                                                                              final OutputStream out,
                                                                              final boolean includeSensitive,
                                                                              final DownloadDetailsDTO dd,
                                                                              boolean checkLimit,
                                                                              final ExecutorService nextExecutor) throws Exception {
        expandRequestedFields(downloadParams, true);

        if (dd != null) {
            dd.resetCounts();
        }

        long start = System.currentTimeMillis();
        final ConcurrentMap<String, AtomicInteger> uidStats = new ConcurrentHashMap<>();

        try {
            SolrQuery solrQuery = new SolrQuery();
            queryFormatUtils.formatSearchQuery(downloadParams);

            String requestedFieldsParam = getDownloadFields(downloadParams);

            if (includeSensitive) {
                //include raw latitude and longitudes
                if (requestedFieldsParam.contains("decimalLatitude_p")) {
                    requestedFieldsParam = requestedFieldsParam.replaceFirst("decimalLatitude_p", "sensitive_latitude,sensitive_longitude,decimalLatitude_p");
                } else if (requestedFieldsParam.contains("decimalLatitude")) {
                    requestedFieldsParam = requestedFieldsParam.replaceFirst("decimalLatitude", "sensitive_latitude,sensitive_longitude,decimalLatitude");
                }
                if (requestedFieldsParam.contains(",locality,")) {
                    requestedFieldsParam = requestedFieldsParam.replaceFirst(",locality,", ",locality,sensitive_locality,");
                }
                if (requestedFieldsParam.contains(",locality_p,")) {
                    requestedFieldsParam = requestedFieldsParam.replaceFirst(",locality_p,", ",locality_p,sensitive_locality,");
                }
            }

            StringBuilder dbFieldsBuilder = new StringBuilder(requestedFieldsParam);
            if (!downloadParams.getExtra().isEmpty()) {
                dbFieldsBuilder.append(",").append(downloadParams.getExtra());
            }

            String[] requestedFields = dbFieldsBuilder.toString().split(",");
            List<String>[] indexedFields;
            if (downloadFields == null) {
                //default to include everything
                java.util.List<String> mappedNames = new java.util.LinkedList<String>();
                for (int i = 0; i < requestedFields.length; i++) mappedNames.add(requestedFields[i]);

                indexedFields = new List[]{mappedNames, new java.util.LinkedList<String>(), mappedNames, mappedNames, new ArrayList(), new ArrayList()};
            } else {
                indexedFields = downloadFields.getIndexFields(requestedFields, downloadParams.getDwcHeaders(), downloadParams.getLayersServiceUrl());
            }
            //apply custom header
            String[] customHeader = dd.getRequestParams().getCustomHeader().split(",");
            for (int i = 0; i + 1 < customHeader.length; i += 2) {
                for (int j = 0; j < indexedFields[0].size(); j++) {
                    if (customHeader[i].equals(indexedFields[0].get(j))) {
                        indexedFields[2].set(j, customHeader[i + 1]);
                    }
                }
                for (int j = 0; j < indexedFields[4].size(); j++) {
                    if (customHeader[i].equals(indexedFields[5].get(j))) {
                        indexedFields[4].set(j, customHeader[i + 1]);
                    }
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Fields included in download: " + indexedFields[0]);
                logger.debug("Fields excluded from download: " + indexedFields[1]);
                logger.debug("The headers in downloads: " + indexedFields[2]);
                logger.debug("Analysis headers: " + indexedFields[4]);
                logger.debug("Analysis fields: " + indexedFields[5]);
            }

            //set the fields to the ones that are available in the index
            String[] fields = indexedFields[0].toArray(new String[]{});
            solrQuery.setFields(fields);
            StringBuilder qasb = new StringBuilder();
            if (!"none".equals(downloadParams.getQa())) {
                solrQuery.addField("assertions");
                if (!"all".equals(downloadParams.getQa()) && !"includeall".equals(downloadParams.getQa())) {
                    //add all the qa fields
                    qasb.append(downloadParams.getQa());
                }
            }
            solrQuery.addField("institution_uid")
                    .addField("collection_uid")
                    .addField("data_resource_uid")
                    .addField("data_provider_uid");

            solrQuery.setQuery(downloadParams.getFormattedQuery());
            solrQuery.setFacetMinCount(1);
            solrQuery.setFacetLimit(-1);

            //get the assertion facets to add them to the download fields
            boolean getAssertionsFromFacets = "all".equals(downloadParams.getQa()) || "includeall".equals(downloadParams.getQa());
            SolrQuery monthAssertionsQuery = getAssertionsFromFacets ? solrQuery.getCopy().addFacetField("month", "assertions") : solrQuery.getCopy().addFacetField("month");
            if (getAssertionsFromFacets) {
                //set the order for the facet to be based on the index - this will force the assertions to be returned in the same order each time
                //based on alphabetical sort.  The number of QA's may change between searches so we can't guarantee that the order won't change
                monthAssertionsQuery.add("f.assertions.facet.sort", "index");
            }
            QueryResponse facetQuery = runSolrQuery(monthAssertionsQuery, downloadParams.getFormattedFq(), 0, 0, "score", "asc");

            //set the totalrecords for the download details
            dd.setTotalRecords(facetQuery.getResults().getNumFound());

            //use a separately configured and smaller limit when output will be unzipped
            final long maxDownloadSize;
            if (MAX_DOWNLOAD_SIZE > unzippedLimit && out instanceof OptionalZipOutputStream &&
                    ((OptionalZipOutputStream) out).getType() == OptionalZipOutputStream.Type.unzipped) {
                maxDownloadSize = unzippedLimit;
            } else {
                maxDownloadSize = MAX_DOWNLOAD_SIZE;
            }

            if (checkLimit && dd.getTotalRecords() < maxDownloadSize) {
                checkLimit = false;
            }

            //get the month facets to add them to the download fields get the assertion facets.
            List<Count> splitByFacet = null;

            for (FacetField facet : facetQuery.getFacetFields()) {
                if (facet.getName().equals("assertions") && facet.getValueCount() > 0) {
                    qasb.append(getQAFromFacet(facet));
                }
                if (facet.getName().equals("month") && facet.getValueCount() > 0) {
                    splitByFacet = facet.getValues();
                }
            }

            if ("includeall".equals(downloadParams.getQa())) {
                qasb = getAllQAFields();
            }

            String qas = qasb.toString();

            //include sensitive fields in the header when the output will be partially sensitive
            final String[] sensitiveFields;
            final String[] notSensitiveFields;
            if (dd.getSensitiveFq() != null) {
                List<String>[] sensitiveHdr = downloadFields.getIndexFields(sensitiveSOLRHdr, downloadParams.getDwcHeaders(), downloadParams.getLayersServiceUrl());

                //header for the output file
                indexedFields[2].addAll(sensitiveHdr[2]);

                //lookup for fields from sensitive queries
                sensitiveFields = org.apache.commons.lang3.ArrayUtils.addAll(indexedFields[0].toArray(new String[]{}),
                        sensitiveHdr[0].toArray(new String[]{}));

                //use general fields when sensitive data is not permitted
                notSensitiveFields = org.apache.commons.lang3.ArrayUtils.addAll(indexedFields[0].toArray(new String[]{}), notSensitiveSOLRHdr);
            } else {
                sensitiveFields = new String[0];
                notSensitiveFields = fields;
            }

            //add analysis headers
            indexedFields[2].addAll(indexedFields[4]);
            final String[] analysisFields = indexedFields[5].toArray(new String[0]);

            final String[] qaFields = qas.equals("") ? new String[]{} : qas.split(",");
            String[] qaTitles = downloadFields.getHeader(qaFields, false, false);

            String[] header = org.apache.commons.lang3.ArrayUtils.addAll(indexedFields[2].toArray(new String[]{}), qaTitles);

            //retain output header fields and field names for inclusion of header info in the download
            StringBuilder infoFields = new StringBuilder("infoFields");
            for (String h : indexedFields[3]) infoFields.append(",").append(h);
            for (String h : qaFields) infoFields.append(",").append(h);

            StringBuilder infoHeader = new StringBuilder("infoHeaders");
            for (String h : header) infoHeader.append(",").append(h);

            String info = infoFields.toString();
            while (info.contains(",,")) info = info.replace(",,", ",");
            uidStats.put(info,  new AtomicInteger(-1));
            String hdr = infoHeader.toString();
            while (hdr.contains(",,")) hdr = hdr.replace(",,", ",");
            uidStats.put(hdr, new AtomicInteger(-2));

            //construct correct RecordWriter based on the supplied fileType
            final RecordWriterError rw = downloadParams.getFileType().equals("csv") ?
                    new CSVRecordWriter(out, header, downloadParams.getSep(), downloadParams.getEsc()) :
                    (downloadParams.getFileType().equals("tsv") ? new TSVRecordWriter(out, header) :
                            new ShapeFileRecordWriter(tmpShapefileDir, downloadParams.getFile(), out, (String[]) ArrayUtils.addAll(fields, qaFields)));
            
            // Requirement to be able to propagate interruptions to all other threads for this execution
            // Doing this via this variable
            final AtomicBoolean interruptFound = dd != null ? dd.getInterrupt() : new AtomicBoolean(false);

            // Create a fixed length blocking queue for buffering results before they are written
            // This also creates a push-back effect to throttle the results generating threads
            // when it fills and offers to it are delayed until the writer consumes elements from the queue
            final BlockingQueue<String[]> queue = new ArrayBlockingQueue<>(resultsQueueLength);
            // Create a sentinel that we can check for reference equality to signal the end of the queue
            final String[] sentinel = new String[0];
            // An implementation of RecordWriter that adds to an in-memory queue
            final RecordWriter concurrentWrapper = new RecordWriter() {
                private final AtomicBoolean finalised = new AtomicBoolean(false);
                private final AtomicBoolean finalisedComplete = new AtomicBoolean(false);

                @Override
                public void write(String[] nextLine) {
                    try {
                        if (Thread.currentThread().isInterrupted() || interruptFound.get() || finalised.get()) {
                            finalise();
                            return;
                        }
                        while (!queue.offer(nextLine, writerTimeoutWaitMillis, TimeUnit.MILLISECONDS)) {
                            if (Thread.currentThread().isInterrupted() || interruptFound.get() || finalised.get()) {
                                finalise();
                                break;
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        interruptFound.set(true);
                        if (logger.isDebugEnabled()) {
                            logger.debug("Queue failed to accept the next record due to a thread interrupt, calling finalise the cleanup: ", e);
                        }
                        // If we were interrupted then we should call finalise to cleanup
                        finalise();
                    }
                }

                @Override
                public void finalise() {
                    if (finalised.compareAndSet(false, true)) {
                        try {
                            // Offer the sentinel at least once, even when the thread is interrupted
                            while (!queue.offer(sentinel, writerTimeoutWaitMillis, TimeUnit.MILLISECONDS)) {
                                // If the thread is interrupted then the queue may not have any active consumers,
                                // so don't loop forever waiting for capacity in this case
                                // The hard shutdown phase will use queue.clear to ensure that the
                                // sentinel gets onto the queue at least once
                                if (Thread.currentThread().isInterrupted() || interruptFound.get()) {
                                    break;
                                }
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            interruptFound.set(true);
                            if (logger.isDebugEnabled()) {
                                logger.debug("Queue failed to accept the sentinel in finalise due to a thread interrupt: ", e);
                            }
                        } finally {
                            finalisedComplete.set(true);
                        }
                    }
                }

                @Override
                public void initialise() {
                    // No resources to create
                }

                @Override
                public boolean finalised() {
                    return finalisedComplete.get();
                }
            };

            // A single thread that consumes elements put onto the queue until it sees the sentinel, finalising after the sentinel or an interrupt
            Runnable writerRunnable = new Runnable() {
                @Override
                public void run() {
                    try {
                        long counter = 0;
                        while (true) {
                            counter = counter + 1;

                            if (Thread.currentThread().isInterrupted() || interruptFound.get()) {
                                break;
                            }

                            String[] take = queue.take();
                            // Sentinel object equality check to see if we are done
                            if (take == sentinel || Thread.currentThread().isInterrupted() || interruptFound.get()) {
                                break;
                            }
                            // Otherwise write to the wrapped record writer
                            rw.write(take);

                            //test for errors. This can contain a flush so only test occasionally
                            if (counter % resultsQueueLength == 0 && rw.hasError()) {
                                throw RecordWriterException.newRecordWriterException(dd, downloadParams, true, rw);
                            }

                        }
                    } catch (RecordWriterException e) {
                        //no trace information is available to print for these errors
                        logger.error(e.getMessage());
                        interruptFound.set(true);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        interruptFound.set(true);
                    } catch (Exception e) {
                        // Reuse interruptFound variable to signal that the writer had issues
                        interruptFound.set(true);
                        logger.error("Download writer failed.", e);
                    } finally {
                        rw.finalise();
                    }
                }
            };
            Thread writerThread = new Thread(writerRunnable);

            try {
                rw.initialise();
                writerThread.start();
                if (rw instanceof ShapeFileRecordWriter) {
                    dd.setHeaderMap(((ShapeFileRecordWriter) rw).getHeaderMappings());
                }

                //order the query by _docid_ for faster paging - this breaks SOLR Cloud
//                solrQuery.addSortField("_docid_", ORDER.asc);

                //for each month create a separate query that pages through 500 records per page
                List<SolrQuery> queries = new ArrayList<SolrQuery>();
                if (splitByFacet != null) {
                    for (Count facet : splitByFacet) {
                        if (facet.getCount() > 0) {
                            SolrQuery splitByFacetQuery;
                            //do not add remainderQuery here
                            if (facet.getName() != null) {
                                splitByFacetQuery = solrQuery.getCopy().addFilterQuery(facet.getFacetField().getName() + ":" + facet.getName());
                                splitByFacetQuery.setFacet(false);
                                queries.add(splitByFacetQuery);
                            }

                        }
                    }
                    if (splitByFacet.size() > 0) {
                        SolrQuery remainderQuery = solrQuery.getCopy().addFilterQuery("-" + splitByFacet.get(0).getFacetField().getName() + ":[* TO *]");
                        queries.add(0, remainderQuery);
                    }
                } else {
                    queries.add(0, solrQuery);
                }

                //split into sensitive and non-sensitive queries when
                // - not including all sensitive values
                // - there is a sensitive fq
                final List<SolrQuery> sensitiveQ = new ArrayList<SolrQuery>();
                if (!includeSensitive && dd.getSensitiveFq() != null) {
                    sensitiveQ.addAll(splitQueries(queries, dd.getSensitiveFq(), sensitiveSOLRHdr, notSensitiveSOLRHdr));
                }

                //Set<Future<Integer>> futures = new HashSet<Future<Integer>>();
                final AtomicInteger resultsCount = new AtomicInteger(0);
                final boolean threadCheckLimit = checkLimit;

                List<Callable<Integer>> solrCallables = new ArrayList<>(queries.size());
                // execute each query, writing the results to stream
                for (final SolrQuery splitByFacetQuery : queries) {
                    // define a thread
                    Callable<Integer> solrCallable = new Callable<Integer>() {
                        @Override
                        public Integer call() throws Exception {
                            int startIndex = 0;
                            // Randomise the wakeup time so they don't all wakeup on a periodic cycle
                            long localThrottle = throttle + Math.round(Math.random() * throttle);

                            String[] fq = downloadParams.getFormattedFq();
                            if (splitByFacetQuery.getFilterQueries() != null && splitByFacetQuery.getFilterQueries().length > 0) {
                                if (fq == null) {
                                    fq = new String[0];
                                }
                                fq = org.apache.commons.lang3.ArrayUtils.addAll(fq, splitByFacetQuery.getFilterQueries());
                            }

                            splitByFacetQuery.setFilterQueries(fq);

                            QueryResponse qr = runSolrQueryWithCursorMark(splitByFacetQuery, downloadBatchSize, null);
                            AtomicInteger recordsForThread = new AtomicInteger(0);
                            if (logger.isDebugEnabled()) {
                                logger.debug(splitByFacetQuery.getQuery() + " - results: " + qr.getResults().size());
                            }

                            while (qr != null && !qr.getResults().isEmpty() && !interruptFound.get()) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Start index: " + startIndex + ", " + splitByFacetQuery.getQuery());
                                }
                                int count = 0;
                                if (sensitiveQ.contains(splitByFacetQuery)) {
                                    count = processQueryResults(uidStats, sensitiveFields, qaFields, concurrentWrapper, qr, dd, threadCheckLimit, resultsCount, maxDownloadSize, analysisFields);
                                } else {
                                    // write non-sensitive values into sensitive fields when not authorised for their sensitive values
                                    count = processQueryResults(uidStats, notSensitiveFields, qaFields, concurrentWrapper, qr, dd, threadCheckLimit, resultsCount, maxDownloadSize, analysisFields);
                                }
                                recordsForThread.addAndGet(count);
                                // we have already set the Filter query the first time the query was constructed
                                // rerun with the same params but different startIndex
                                if (!threadCheckLimit || resultsCount.get() < maxDownloadSize) {
                                    if (!threadCheckLimit) {
                                        // throttle the download by sleeping
                                        Thread.sleep(localThrottle);
                                    }
                                    qr = runSolrQueryWithCursorMark(splitByFacetQuery, downloadBatchSize, qr.getNextCursorMark());
                                } else {
                                    qr = null;
                                }
                            }
                            return recordsForThread.get();
                        }
                    };
                    solrCallables.add(solrCallable);
                }

                List<Future<Integer>> futures = new ArrayList<>(solrCallables.size());
                for (Callable<Integer> nextCallable : solrCallables) {
                    futures.add(nextExecutor.submit(nextCallable));
                }

                // Busy wait because we need to be able to respond to an interrupt on any callable
                // and propagate it to all of the others for this particular query
                // Because the executor service is shared to prevent too many concurrent threads being run,
                // this requires a busy wait loop on the main thread to monitor state
                boolean waitAgain = false;
                do {
                    waitAgain = false;
                    for (Future<Integer> future : futures) {
                        if (!future.isDone()) {
                            // Wait again even if an interrupt flag is set, as it may have been set partway through the iteration
                            // The calls to future.cancel will occur next time if the interrupt is setup partway through an iteration
                            waitAgain = true;
                            // If one thread finds an interrupt it is propagated to others using the interruptFound AtomicBoolean
                            if (interruptFound.get()) {
                                future.cancel(true);
                            }
                        }
                    }
                    // Don't trigger the timeout interrupt if we don't have to wait again as we are already done at this point
                    if (waitAgain && (System.currentTimeMillis() - start) > downloadMaxTime) {
                        logger.error("Download max time was exceeded: downloadMaxTime=" + downloadMaxTime + " duration=" + (System.currentTimeMillis() - start));
                        interruptFound.set(true);
                        break;
                    }

                    if (waitAgain) {
                        Thread.sleep(downloadCheckBusyWaitSleep);
                    }
                } while (waitAgain);

                AtomicInteger totalDownload = new AtomicInteger(0);
                for (Future<Integer> future : futures) {
                    if (future.isDone()) {
                        totalDownload.addAndGet(future.get());
                    } else {
                        // All incomplete futures that survived the loop above are cancelled here
                        future.cancel(true);
                    }
                }

                long finish = System.currentTimeMillis();
                long timeTakenInSecs = (finish - start) / 1000;
                if (timeTakenInSecs <= 0) timeTakenInSecs = 1;
                if (logger.isInfoEnabled()) {
                    logger.info("Download of " + resultsCount + " records in " + timeTakenInSecs + " seconds. Record/sec: " + resultsCount.intValue() / timeTakenInSecs);
                }

            } finally {
                try {
                    // Once we get here, we need to finalise starting at the concurrent wrapper,
                    // as there are no more non-sentinel records to be added to the queue
                    // This eventually triggers finalisation of the underlying writer when the queue empties
                    // This is a soft shutdown, and hence we wait below for this stage to complete in normal circumstances
                    // Note, this blocks for writerTimeoutWaitMillis trying to legitimately add the sentinel to the end of the queue
                    // We force the sentinel to be added in the hard shutdown phase below
                    concurrentWrapper.finalise();
                } finally {
                    try {
                        // Track the current time right now so we can abort after downloadMaxCompletionTime milliseconds in this phase
                        final long completionStartTime = System.currentTimeMillis();
                        // Busy wait check for finalised to be called in the RecordWriter or something is interrupted
                        // By this stage, there are at maximum download.internal.queue.size items remaining (default 1000)
                        while (writerThread.isAlive()
                                && !writerThread.isInterrupted()
                                && !interruptFound.get()
                                && !Thread.currentThread().isInterrupted()
                                && !rw.finalised()
                                && !((System.currentTimeMillis() - completionStartTime) > downloadMaxCompletionTime)) {
                            Thread.sleep(downloadCheckBusyWaitSleep);
                        }
                    } finally {
                        try {
                            // Attempt all actions that could trigger the writer thread to finalise, as by this stage we are in hard shutdown mode
                            // First signal that we are in hard shutdown mode
                            interruptFound.set(true);
                        } finally {
                            try {
                                // Add the sentinel or clear the queue and try again until it gets onto the queue
                                // We are in hard shutdown mode, so only priority is that the queue either
                                // gets the sentinel or the thread is interrupted to clean up resources
                                while (!queue.offer(sentinel)) {
                                    queue.clear();
                                }
                            } finally {
                                try {
                                    // Interrupt the single writer thread
                                    writerThread.interrupt();
                                } finally {
                                    try {
                                        // Explicitly call finalise on the RecordWriter as a backup
                                        // In normal circumstances it is called via the sentinel or the interrupt
                                        // This will not block if finalise has been called previously in the current three implementations
                                        rw.finalise();
                                    } finally {    
                                        if (rw != null && rw.hasError()) {
                                            throw RecordWriterException.newRecordWriterException(dd, downloadParams, true, rw);
                                        } else {
                                            // Flush whatever output was still pending for more deterministic debugging
                                            out.flush();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (SolrServerException ex) {
            logger.error("Problem communicating with SOLR server while processing download. " + ex.getMessage(), ex);
        }
        return uidStats;
    }

    private List<String[]> intersectResults(String layersServiceUrl, String[] analysisLayers, SolrDocumentList results) {
        List<String[]> intersection = new ArrayList<String[]>();

        if (analysisLayers.length > 0 && StringUtils.isNotEmpty(layersServiceUrl)) {
            try {
                double[][] points = new double[results.size()][2];
                int invalid = 0;
                int i = 0;
                for (SolrDocument sd : results) {
                    if (sd.containsKey("sensitive_longitude") && sd.containsKey("sensitive_latitude")) {
                        points[i][0] = (double) sd.getFirstValue("sensitive_longitude");
                        points[i][1] = (double) sd.getFirstValue("sensitive_latitude");
                    } else if (sd.containsKey("longitude") && sd.containsKey("latitude")) {
                        points[i][0] = (double) sd.getFirstValue("longitude");
                        points[i][1] = (double) sd.getFirstValue("latitude");
                    } else {
                        points[i][0] = 0;
                        points[i][1] = 0;
                        invalid++;
                    }
                    i++;
                }

                if (invalid < results.size()) {
                    LayersStore ls = new LayersStore(layersServiceUrl);
                    Reader reader = ls.sample(analysisLayers, points, null);

                    CSVReader csv = new CSVReader(reader);
                    intersection = csv.readAll();
                    csv.close();
                }
            } catch (IOException e) {
                logger.error("Failed to intersect analysis layers", e);
            }
        }

        return intersection;
    }

    private int processQueryResults(ConcurrentMap<String, AtomicInteger> uidStats, String[] fields, String[] qaFields, RecordWriter rw, QueryResponse qr, DownloadDetailsDTO dd, boolean checkLimit, AtomicInteger resultsCount, long maxDownloadSize, String[] analysisLayers) {
        //handle analysis layer intersections
        List<String[]> intersection = intersectResults(dd.getRequestParams().getLayersServiceUrl(), analysisLayers, qr.getResults());

        int count = 0;
        int record = 0;
        for (SolrDocument sd : qr.getResults()) {
            if (sd.getFieldValue("data_resource_uid") != null && (!checkLimit || (checkLimit && resultsCount.intValue() < maxDownloadSize))) {

                //resultsCount++;
                count++;
                resultsCount.incrementAndGet();

                //add the record
                String[] values = new String[fields.length + analysisLayers.length + qaFields.length];

                //get all the "single" values from the index
                for (int j = 0; j < fields.length; j++) {
                    Collection<Object> allValues = sd.getFieldValues(fields[j]);
                    if (allValues == null) {
                        values[j] = "";
                    } else {
                        Iterator it = allValues.iterator();
                        while (it.hasNext()) {
                            Object value = it.next();
                            if (values[j] != null && values[j].length() > 0) values[j] += "|"; //multivalue separator
                            if (value instanceof Date) {
                                values[j] = value == null ? "" : org.apache.commons.lang.time.DateFormatUtils.format((Date) value, "yyyy-MM-dd");
                            } else {
                                values[j] = value == null ? "" : value.toString();
                            }
                            //allow requests to include multiple values when requested
                            if (dd == null || dd.getRequestParams() == null ||
                                    dd.getRequestParams().getIncludeMultivalues() == null
                                    || !dd.getRequestParams().getIncludeMultivalues()) {
                                break;
                            }
                        }
                    }
                }

                //add analysis layer intersections
                if (analysisLayers.length > 0 && intersection.size() > record) {
                    //+1 offset for header in intersection list
                    String[] sampling = intersection.get(record + 1);
                    //+2 offset for latitude,longitude columns in sampling array
                    if (sampling != null && sampling.length == analysisLayers.length + 2) {
                        System.arraycopy(sampling, 2, values, fields.length, sampling.length - 2);
                    }
                }

                //now handle the assertions
                java.util.Collection<Object> assertions = sd.getFieldValues("assertions");

                //Handle the case where there a no assertions against a record
                if (assertions == null) {
                    assertions = Collections.EMPTY_LIST;
                }

                for (int k = 0; k < qaFields.length; k++) {
                    values[fields.length + k] = Boolean.toString(assertions.contains(qaFields[k]));
                }

                rw.write(values);

                //increment the counters....
                incrementCount(uidStats, sd.getFieldValue("institution_uid"));
                incrementCount(uidStats, sd.getFieldValue("collection_uid"));
                incrementCount(uidStats, sd.getFieldValue("data_provider_uid"));
                incrementCount(uidStats, sd.getFieldValue("data_resource_uid"));
            }

            record++;
        }
        dd.updateCounts(count);
        return count;
    }

    /**
     * Note - this method extracts from CASSANDRA rather than the Index.
     */
    public ConcurrentMap<String, AtomicInteger> writeResultsToStream(
            DownloadRequestParams downloadParams, OutputStream out, int i,
            boolean includeSensitive, DownloadDetailsDTO dd, boolean limit) throws Exception {
        expandRequestedFields(downloadParams, false);

        int resultsCount = 0;
        ConcurrentMap<String, AtomicInteger> uidStats = new ConcurrentHashMap<>();
        //stores the remaining limit for data resources that have a download limit
        Map<String, Integer> downloadLimit = new HashMap<>();

        try {
            SolrQuery solrQuery = initSolrQuery(downloadParams, false, null);
            //ensure that the qa facet is being ordered alphabetically so that the order is consistent.
            boolean getAssertionsFromFacets = "all".equals(downloadParams.getQa()) || "includeall".equals(downloadParams.getQa());
            if (getAssertionsFromFacets) {
                //set the order for the facet to be based on the index - this will force the assertions to be returned in the same order each time
                //based on alphabetical sort.  The number of QA's may change between searches so we can't guarantee that the order won't change
                solrQuery.add("f.assertions.facet.sort", "index");
            }
            queryFormatUtils.formatSearchQuery(downloadParams);
            if (logger.isInfoEnabled()) {
                logger.info("search query: " + downloadParams.getFormattedQuery());
            }
            solrQuery.setQuery(downloadParams.getFormattedQuery());
            //Only the fields specified below will be included in the results from the SOLR Query
            solrQuery.setFields("id", "institution_uid", "collection_uid", "data_resource_uid", "data_provider_uid");

            String dFields = getDownloadFields(downloadParams);

            if (includeSensitive) {
                //include raw latitude and longitudes
                dFields = dFields.replaceFirst("decimalLatitude_p", "decimalLatitude,decimalLongitude,decimalLatitude_p").replaceFirst(",locality,", ",locality,sensitive_locality,");
            }

            StringBuilder sb = new StringBuilder(dFields);
            if (downloadParams.getExtra().length() > 0) {
                sb.append(",").append(downloadParams.getExtra());
            }
            StringBuilder qasb = new StringBuilder();

            solrQuery.setFacet(true);
            QueryResponse qr = runSolrQuery(solrQuery, downloadParams.getFormattedFq(), 0, 0, "", "");
            dd.setTotalRecords(qr.getResults().getNumFound());
            //get the assertion facets to add them to the download fields
            List<FacetField> facets = qr.getFacetFields();
            for (FacetField facet : facets) {
                if (facet.getName().equals("assertions") && facet.getValueCount() > 0) {
                    qasb.append(getQAFromFacet(facet));
                } else if (facet.getName().equals("data_resource_uid") && checkDownloadLimits) {
                    //populate the download limit
                    initDownloadLimits(downloadLimit, facet);
                }
            }

            if ("includeall".equals(downloadParams.getQa())) {
                qasb = getAllQAFields();
            }

            //Write the header line
            String qas = qasb.toString();

            List<String>[] indexedFields = downloadFields.getIndexFields(getDownloadFields(downloadParams).split(","), false, downloadParams.getLayersServiceUrl());

            String[] fields = sb.toString().split(",");

            //avoid analysis field duplicates
            for (String s : indexedFields[5]) fields = (String[]) ArrayUtils.removeElement(fields, s);

            String[] qaFields = qas.equals("") ? new String[]{} : qas.split(",");
            String[] qaTitles = downloadFields.getHeader(qaFields, false, false);
            String[] titles = downloadFields.getHeader(fields, true, downloadParams.getDwcHeaders());
            String[] analysisHeaders = indexedFields[4].toArray(new String[0]);
            String[] analysisFields = indexedFields[5].toArray(new String[0]);

            //apply custom header
            String[] customHeader = dd.getRequestParams().getCustomHeader().split(",");
            for (i = 0; i + 1 < customHeader.length; i += 2) {
                for (int j = 0; j < analysisFields.length; j++) {
                    if (customHeader[i].equals(analysisFields[j])) {
                        analysisFields[j] = customHeader[i + 1];
                    }
                }
                for (int j = 0; j < qaFields.length; j++) {
                    if (customHeader[i].equals(qaFields[j])) {
                        qaTitles[j] = customHeader[i + 1];
                    }
                }
                for (int j = 0; j < fields.length; j++) {
                    if (customHeader[i].equals(fields[j])) {
                        titles[j] = customHeader[i + 1];
                    }
                }
            }

            //append sensitive fields for the header only
            if (!includeSensitive && dd.getSensitiveFq() != null) {
                //sensitive headers do not have a DwC name, always set getIndexFields dwcHeader=false
                List<String>[] sensitiveHdr = downloadFields.getIndexFields(sensitiveSOLRHdr, false, downloadParams.getLayersServiceUrl());

                titles = org.apache.commons.lang3.ArrayUtils.addAll(titles, sensitiveHdr[2].toArray(new String[]{}));
            }
            String[] header = org.apache.commons.lang3.ArrayUtils.addAll(org.apache.commons.lang3.ArrayUtils.addAll(titles, qaTitles), analysisHeaders);
            //Create the Writer that will be used to format the records
            //construct correct RecordWriter based on the supplied fileType
            final RecordWriterError rw = downloadParams.getFileType().equals("csv") ?
                    new CSVRecordWriter(out, header, downloadParams.getSep(), downloadParams.getEsc()) :
                    (downloadParams.getFileType().equals("tsv") ? new TSVRecordWriter(out, header) :
                            new ShapeFileRecordWriter(tmpShapefileDir, downloadParams.getFile(), out, (String[]) ArrayUtils.addAll(fields, qaFields)));

            try {
                rw.initialise();
                if (rw instanceof ShapeFileRecordWriter) {
                    dd.setHeaderMap(((ShapeFileRecordWriter) rw).getHeaderMappings());
                }

                //retain output header fields and field names for inclusion of header info in the download
                StringBuilder infoFields = new StringBuilder("infoFields,");
                for (String h : fields) infoFields.append(",").append(h);
                for (String h : analysisFields) infoFields.append(",").append(h);
                for (String h : qaFields) infoFields.append(",").append(h);

                StringBuilder infoHeader = new StringBuilder("infoHeaders,");
                for (String h : header) infoHeader.append(",").append(h);

                String info = infoFields.toString();
                while (info.contains(",,")) info = info.replace(",,", ",");
                uidStats.put(info,  new AtomicInteger(-1));
                String hdr = infoHeader.toString();
                while (hdr.contains(",,")) hdr = hdr.replace(",,", ",");
                uidStats.put(hdr, new AtomicInteger(-2));

                //download the records that have limits first...
                if (downloadLimit.size() > 0) {
                    String[] originalFq = downloadParams.getFormattedFq();
                    StringBuilder fqBuilder = new StringBuilder("-(");
                    for (String dr : downloadLimit.keySet()) {
                        //add another fq to the search for data_resource_uid
                        downloadParams.setFq((String[]) ArrayUtils.add(originalFq, "data_resource_uid:" + dr));
                        resultsCount = downloadRecords(downloadParams, rw, downloadLimit, uidStats, fields, qaFields,
                                resultsCount, dr, includeSensitive, dd, limit, analysisFields);
                        if (fqBuilder.length() > 2) {
                            fqBuilder.append(" OR ");
                        }
                        fqBuilder.append("data_resource_uid:").append(dr);
                    }
                    fqBuilder.append(")");
                    //now include the rest of the data resources
                    //add extra fq for the remaining records
                    downloadParams.setFq((String[]) ArrayUtils.add(originalFq, fqBuilder.toString()));
                    resultsCount = downloadRecords(downloadParams, rw, downloadLimit, uidStats, fields, qaFields,
                            resultsCount, null, includeSensitive, dd, limit, analysisFields);
                } else {
                    //download all at once
                    downloadRecords(downloadParams, rw, downloadLimit, uidStats, fields, qaFields, resultsCount,
                            null, includeSensitive, dd, limit, analysisFields);
                }
            } finally {
                rw.finalise();
            }
        } catch (SolrServerException ex) {
            logger.error("Problem communicating with SOLR server. " + ex.getMessage(), ex);
        }

        return uidStats;
    }

    /**
     * Expand field abbreviations
     * <p>
     * cl.p Include all contextual spatial layers
     * el.p Include all environmental spatial layers
     * allDwc Include all fields with a dwcTerm
     *
     * @param downloadParams
     */
    private void expandRequestedFields(DownloadRequestParams downloadParams, boolean isSolr) {
        String fields = getDownloadFields(downloadParams);

        try {
            Matcher matcher = clpField.matcher(fields);
            if (matcher.find()) {
                StringBuilder sb = new StringBuilder();
                for (IndexFieldDTO field : getIndexedFields()) {
                    if (field.getName().matches("cl[0-9]*")) {
                        if (sb.length() > 0 || matcher.start() > 0) sb.append(",");
                        sb.append(field.getName());
                    }
                }
                if (sb.length() > 0 && matcher.end() < fields.length()) sb.append(",");
                fields = matcher.replaceFirst(sb.toString());
            }

            matcher = elpField.matcher(fields);
            if (matcher.find()) {
                StringBuilder sb = new StringBuilder();
                for (IndexFieldDTO field : getIndexedFields()) {
                    if (field.getName().matches("el[0-9]*")) {
                        if (sb.length() > 0 || matcher.start() > 0) sb.append(",");
                        sb.append(field.getName());
                    }
                }
                if (sb.length() > 0 && matcher.end() < fields.length()) sb.append(",");
                fields = matcher.replaceFirst(sb.toString());
            }

            matcher = allDwcField.matcher(fields);
            if (matcher.find()) {
                StringBuilder sb = new StringBuilder();
                for (IndexFieldDTO field : getIndexedFields()) {
                    if (StringUtils.isNotEmpty(field.getDwcTerm()) &&
                            (!isSolr || field.isStored())) {
                        if (sb.length() > 0 || matcher.start() > 0) sb.append(",");
                        if (isSolr) {
                            sb.append(field.getName());
                        } else {
                            sb.append(field.getDownloadName());
                        }
                    }
                }
                if (sb.length() > 0 && matcher.end() < fields.length()) sb.append(",");
                fields = matcher.replaceFirst(sb.toString());
            }
        } catch (Exception e) {
            logger.error("failed to substitute fields", e);
        }

        downloadParams.setFields(fields);
    }

    private String getQAFromFacet(FacetField facet) {
        StringBuilder qasb = new StringBuilder();
        for (FacetField.Count facetEntry : facet.getValues()) {
            if (facetEntry.getCount() > 0) {
                if (qasb.length() > 0) {
                    qasb.append(",");
                }
                if (facetEntry.getName() != null) {
                    qasb.append(facetEntry.getName());
                }
            }
        }
        return qasb.toString();
    }

    private String getDownloadFields(DownloadRequestParams downloadParams) {
        String dFields = downloadParams.getFields();
        if(StringUtils.isEmpty(dFields)){
            dFields = defaultDownloadFields;
        }
        return dFields;
    }

    /**
     * Downloads the records for the supplied query. Used to break up the download into components
     * 1) 1 call for each data resource that has a download limit (supply the data resource uid as the argument dataResource)
     * 2) 1 call for the remaining records
     *
     * @param downloadParams
     * @param downloadLimit
     * @param uidStats
     * @param fields
     * @param qaFields
     * @param resultsCount
     * @param dataResource   The dataResource being download.  This should be null if multiple data resource are being downloaded.
     * @return
     * @throws Exception
     */
    private int downloadRecords(DownloadRequestParams downloadParams, RecordWriterError writer,
                                Map<String, Integer> downloadLimit, ConcurrentMap<String, AtomicInteger> uidStats,
                                String[] fields, String[] qaFields, int resultsCount, String dataResource, boolean includeSensitive,
                                DownloadDetailsDTO dd, boolean limit, String[] analysisLayers) throws Exception {
        if (logger.isInfoEnabled()) {
            logger.info("download query: " + downloadParams.getQ());
        }
        SolrQuery solrQuery = initSolrQuery(downloadParams, false, null);
        solrQuery.setRows(limit ? MAX_DOWNLOAD_SIZE : -1);
        queryFormatUtils.formatSearchQuery(downloadParams);
        solrQuery.setQuery(downloadParams.getFormattedQuery());
        //Only the fields specified below will be included in the results from the SOLR Query
        solrQuery.setFields("id", "institution_uid", "collection_uid", "data_resource_uid", "data_provider_uid");

        if (dd != null) {
            dd.resetCounts();
        }

        //get coordinates for analysis layer intersection
        if (analysisLayers.length > 0) {

            if (!includeSensitive && dd.getSensitiveFq() != null) {
                for (String s : sensitiveSOLRHdr) solrQuery.addField(s);
            } else {
                for (String s : notSensitiveSOLRHdr) solrQuery.addField(s);
            }
        }

        int pageSize = downloadBatchSize;
        StringBuilder sb = new StringBuilder(getDownloadFields(downloadParams));
        if (downloadParams.getExtra().length() > 0) {
            sb.append(",").append(downloadParams.getExtra());
        }

        List<SolrQuery> queries = new ArrayList<SolrQuery>();
        queries.add(solrQuery);

        //split into sensitive and non-sensitive queries when
        // - not including all sensitive values
        // - there is a sensitive fq
        List<SolrQuery> sensitiveQ = new ArrayList<SolrQuery>();
        if (!includeSensitive && dd.getSensitiveFq() != null) {
            sensitiveQ = splitQueries(queries, dd.getSensitiveFq(), null, null);
        }

        final String[] sensitiveFields;
        final String[] notSensitiveFields;
        if (!includeSensitive && dd.getSensitiveFq() != null) {
            //lookup for fields from sensitive queries
            sensitiveFields = org.apache.commons.lang3.ArrayUtils.addAll(fields, sensitiveCassandraHdr);

            //use general fields when sensitive data is not permitted
            notSensitiveFields = org.apache.commons.lang3.ArrayUtils.addAll(fields, notSensitiveCassandraHdr);
        } else {
            sensitiveFields = new String[0];
            notSensitiveFields = fields;
        }

        for (SolrQuery q : queries) {
            int startIndex = 0;

            String[] fq = downloadParams.getFormattedFq();
            if (q.getFilterQueries() != null && q.getFilterQueries().length > 0) {
                if (fq == null) {
                    fq = new String[0];
                }
                fq = org.apache.commons.lang3.ArrayUtils.addAll(fq, q.getFilterQueries());
            }

            QueryResponse qr = runSolrQuery(q, fq, pageSize, startIndex, "", "");
            List<String> uuids = new ArrayList<String>();

            List<String[]> intersectionAll = intersectResults(dd.getRequestParams().getLayersServiceUrl(), analysisLayers, qr.getResults());

            while (qr.getResults().size() > 0 && (!limit || resultsCount < MAX_DOWNLOAD_SIZE) &&
                    shouldDownload(dataResource, downloadLimit, false)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Start index: " + startIndex);
                }

                Map<String, String[]> dataToInsert = new HashMap<String, String[]>();

                //cycle through the results adding them to the list that will be sent to cassandra
                int row = 0;
                for (SolrDocument sd : qr.getResults()) {
                    if (sd.getFieldValue("data_resource_uid") != null) {
                        String druid = sd.getFieldValue("data_resource_uid").toString();
                        if (shouldDownload(druid, downloadLimit, true) && (!limit || resultsCount < MAX_DOWNLOAD_SIZE)) {
                            resultsCount++;
                            uuids.add(sd.getFieldValue("id").toString());

                            //include analysis layer intersections
                            if (intersectionAll.size() > row + 1)
                                dataToInsert.put(sd.getFieldValue("id").toString(), (String[]) ArrayUtils.subarray(intersectionAll.get(row + 1), 2, intersectionAll.get(row + 1).length));

                            //increment the counters....
                            incrementCount(uidStats, sd.getFieldValue("institution_uid"));
                            incrementCount(uidStats, sd.getFieldValue("collection_uid"));
                            incrementCount(uidStats, sd.getFieldValue("data_provider_uid"));
                            incrementCount(uidStats, druid);
                        }
                    }
                    row++;
                }

                String[] newMiscFields;
                if (sensitiveQ.contains(q)) {
                    newMiscFields = au.org.ala.biocache.Store.writeToWriter(writer, uuids.toArray(new String[]{}), sensitiveFields, qaFields, true, (dd.getRequestParams() != null ? dd.getRequestParams().getIncludeMisc() : false), dd.getMiscFields(), dataToInsert);
                } else {
                    newMiscFields = au.org.ala.biocache.Store.writeToWriter(writer, uuids.toArray(new String[]{}), notSensitiveFields, qaFields, includeSensitive, (dd.getRequestParams() != null ? dd.getRequestParams().getIncludeMisc() : false), dd.getMiscFields(), dataToInsert);
                }

                //test for errors
                if (writer.hasError()) {
                    throw RecordWriterException.newRecordWriterException(dd, downloadParams, false, writer);
                }

                dd.setMiscFields(newMiscFields);
                startIndex += pageSize;
                uuids.clear();
                dd.updateCounts(qr.getResults().size());
                if (!limit || resultsCount < MAX_DOWNLOAD_SIZE) {
                    //we have already set the Filter query the first time the query was constructed rerun with he same params but different cursor
                    qr = runSolrQuery(q, null, pageSize, startIndex, "", "");
                }
            }
        }
        return resultsCount;
    }

    /**
     * Split a list of queries by a fq.
     */
    private List<SolrQuery> splitQueries(List<SolrQuery> queries, String fq, String[] fqFields, String[] notFqFields) {
        List<SolrQuery> notFQ = new ArrayList<SolrQuery>();
        List<SolrQuery> fQ = new ArrayList<SolrQuery>();

        for (SolrQuery query : queries) {
            SolrQuery nsq = query.getCopy().addFilterQuery("-(" + fq + ")");
            if (notFqFields != null) {
                for (String field : notFqFields) nsq.addField(field);
            }
            notFQ.add(nsq);

            SolrQuery sq = query.getCopy().addFilterQuery(fq);
            if (fqFields != null) {
                for (String field : fqFields) sq.addField(field);
            }
            fQ.add(sq);
        }

        queries.clear();
        queries.addAll(notFQ);
        queries.addAll(fQ);

        return fQ;
    }

    /**
     * Indicates whether or not a records from the supplied data resource should be included
     * in the download. (based on download limits)
     *
     * @param druid
     * @param limits
     * @param decrease whether or not to decrease the download limit available
     */
    private boolean shouldDownload(String druid, Map<String, Integer> limits, boolean decrease) {
        if (checkDownloadLimits) {
            if (!limits.isEmpty() && limits.containsKey(druid)) {
                Integer remainingLimit = limits.get(druid);
                if (remainingLimit == 0) {
                    return false;
                }
                if (decrease) {
                    limits.put(druid, remainingLimit - 1);
                }
            }
        }
        return true;
    }

    /**
     * Initialises the download limit tracking
     *
     * @param map
     * @param facet
     */
    private void initDownloadLimits(Map<String, Integer> map, FacetField facet) {
        //get the download limits from the cache
        Map<String, Integer> limits = collectionCache.getDownloadLimits();
        for (FacetField.Count facetEntry : facet.getValues()) {
            String name = facetEntry.getName() != null ? facetEntry.getName() : "";
            Integer limit = limits.get(name);
            if (limit != null && limit > 0) {
                //check to see if the number of records returned from the query execeeds the limit
                if (limit < facetEntry.getCount())
                    map.put(name, limit);
            }
        }
        if (logger.isDebugEnabled() && map.size() > 0) {
            logger.debug("Downloading with the following limits: " + map);
        }
    }

    private static void incrementCount(ConcurrentMap<String, AtomicInteger> values, Object uid) {
        if (uid != null) {
            String nextKey = uid.toString();
            // TODO: When bumping to Java-8 this can use computeIfAbsent to avoid all unnecessary object creation and putIfAbsent
            if (values.containsKey(nextKey)) {
                values.get(nextKey).incrementAndGet();
            } else {
                // This checks whether another thread inserted the count first
                AtomicInteger putIfAbsent = values.putIfAbsent(nextKey, new AtomicInteger(1));
                if (putIfAbsent != null) {
                    // Another thread inserted first, increment its counter instead
                    putIfAbsent.incrementAndGet();
                }
            }
        }
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#getFacetPoints(au.org.ala.biocache.dto.SpatialSearchRequestParams, au.org.ala.biocache.dto.PointType)
     */
    @Override
    public List<OccurrencePoint> getFacetPoints(SpatialSearchRequestParams searchParams, PointType pointType) throws Exception {
        return getPoints(searchParams, pointType, -1);
    }

    private List<OccurrencePoint> getPoints(SpatialSearchRequestParams searchParams, PointType pointType, int max) throws Exception {
        List<OccurrencePoint> points = new ArrayList<OccurrencePoint>(); // new OccurrencePoint(PointType.POINT);
        queryFormatUtils.formatSearchQuery(searchParams);
        if (logger.isInfoEnabled()) {
            logger.info("search query: " + searchParams.getFormattedQuery());
        }
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.setQuery(searchParams.getFormattedQuery());
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField(pointType.getLabel());
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(max);  // unlimited = -1

        QueryResponse qr = runSolrQuery(solrQuery, searchParams.getFormattedFq(), 1, 0, "score", "asc");
        List<FacetField> facets = qr.getFacetFields();

        if (facets != null) {
            for (FacetField facet : facets) {
                List<FacetField.Count> facetEntries = facet.getValues();
                if (facet.getName().contains(pointType.getLabel()) && (facetEntries != null) && (facetEntries.size() > 0)) {

                    for (FacetField.Count fcount : facetEntries) {
                        if (StringUtils.isNotEmpty(fcount.getName()) && fcount.getCount() > 0) {
                            OccurrencePoint point = new OccurrencePoint(pointType);
                            point.setCount(fcount.getCount());
                            String[] pointsDelimited = StringUtils.split(fcount.getName(), ',');
                            List<Float> coords = new ArrayList<Float>();

                            for (String coord : pointsDelimited) {
                                try {
                                    Float decimalCoord = Float.parseFloat(coord);
                                    coords.add(decimalCoord);
                                } catch (NumberFormatException numberFormatException) {
                                    logger.warn("Error parsing Float for Lat/Long: " + numberFormatException.getMessage(), numberFormatException);
                                }
                            }

                            if (!coords.isEmpty()) {
                                Collections.reverse(coords); // must be long, lat order
                                point.setCoordinates(coords);
                                points.add(point);
                            }
                        }
                    }
                }
            }
        }
        return points;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#getFacetPointsShort(au.org.ala.biocache.dto.SpatialSearchRequestParams, String)
     */
    @Override
    public FacetField getFacetPointsShort(SpatialSearchRequestParams searchParams, String pointType) throws Exception {
        queryFormatUtils.formatSearchQuery(searchParams);
        if (logger.isInfoEnabled()) {
            logger.info("search query: " + searchParams.getFormattedQuery());
        }
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.setQuery(searchParams.getFormattedQuery());
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField(pointType);
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(searchParams.getFlimit());//MAX_DOWNLOAD_SIZE);  // unlimited = -1

        QueryResponse qr = runSolrQuery(solrQuery, searchParams.getFormattedFq(), 0, 0, "", "");
        List<FacetField> facets = qr.getFacetFields();

        //return first facet, there should only be 1
        if (facets != null && facets.size() > 0) {
            return facets.get(0);
        }
        return null;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#getOccurrences(au.org.ala.biocache.dto.SpatialSearchRequestParams, au.org.ala.biocache.dto.PointType, String)
     */
    @Override
    public List<OccurrencePoint> getOccurrences(SpatialSearchRequestParams searchParams, PointType pointType, String colourBy) throws Exception {

        List<OccurrencePoint> points = new ArrayList<OccurrencePoint>();
        searchParams.setPageSize(100);

        String queryString = "";
        queryFormatUtils.formatSearchQuery(searchParams);
        queryString = searchParams.getFormattedQuery();

        if (logger.isInfoEnabled()) {
            logger.info("search query: " + queryString);
        }
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.setQuery(queryString);
//        solrQuery.setRows(0);
//        solrQuery.setFacet(true);
//        solrQuery.addFacetField(pointType.getLabel());
//        solrQuery.setFacetMinCount(1);
//        solrQuery.setFacetLimit(MAX_DOWNLOAD_SIZE);  // unlimited = -1

        QueryResponse qr = runSolrQuery(solrQuery, searchParams);
        SearchResultDTO searchResults = processSolrResponse(searchParams, qr, solrQuery, OccurrenceIndex.class);
        List<OccurrenceIndex> ocs = searchResults.getOccurrences();

        if (!ocs.isEmpty() && ocs.size() > 0) {

            for (OccurrenceIndex oc : ocs) {

                List<Float> coords = new ArrayList<Float>();
                coords.add(oc.getDecimalLongitude().floatValue());
                coords.add(oc.getDecimalLatitude().floatValue());

                OccurrencePoint point = new OccurrencePoint();
                point.setCoordinates(coords);

                point.setOccurrenceUid(oc.getUuid());

                points.add(point);
            }
        }

        return points;
    }

    /**
     * http://ala-biocache1.vm.csiro.au:8080/solr/select?q=*:*&rows=0&facet=true&facet.field=data_provider_id&facet.field=data_provider&facet.sort=data_provider_id
     *
     * @see au.org.ala.biocache.dao.SearchDAO#getDataProviderCounts()
     */
    //IS THIS BEING USED BY ANYTHING??
    @Override
    public List<DataProviderCountDTO> getDataProviderCounts() throws Exception {

        List<DataProviderCountDTO> dpDTOs = new ArrayList<DataProviderCountDTO>(); // new OccurrencePoint(PointType.POINT);
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.setQuery("*:*");
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField("data_provider_uid");
        solrQuery.addFacetField("data_provider");
        solrQuery.setFacetMinCount(1);
        QueryResponse qr = runSolrQuery(solrQuery, null, 1, 0, "data_provider", "asc");
        List<FacetField> facets = qr.getFacetFields();

        if (facets != null && facets.size() == 2) {

            FacetField dataProviderIdFacet = facets.get(0);
            FacetField dataProviderNameFacet = facets.get(1);

            List<FacetField.Count> dpIdEntries = dataProviderIdFacet.getValues();
            List<FacetField.Count> dpNameEntries = dataProviderNameFacet.getValues();

            if (dpIdEntries != null) {
                for (int i = 0; i < dpIdEntries.size(); i++) {

                    FacetField.Count dpIdEntry = dpIdEntries.get(i);
                    FacetField.Count dpNameEntry = dpNameEntries.get(i);

                    String dataProviderId = dpIdEntry.getName();
                    String dataProviderName = dpNameEntry.getName();
                    long count = dpIdEntry.getCount();

                    if (count > 0) {
                        DataProviderCountDTO dto = new DataProviderCountDTO(dataProviderId, dataProviderName, count);
                        dpDTOs.add(dto);
                    }
                }
            }
        }
        if (logger.isInfoEnabled()) {
            logger.info("Find data providers = " + dpDTOs.size());
        }
        return dpDTOs;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#findRecordsForLocation(au.org.ala.biocache.dto.SpatialSearchRequestParams, au.org.ala.biocache.dto.PointType)
     * This is used by explore your area
     */
    @Override
    public List<OccurrencePoint> findRecordsForLocation(SpatialSearchRequestParams requestParams, PointType pointType) throws Exception {
        return getPoints(requestParams, pointType, MAX_DOWNLOAD_SIZE);
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#findAllSpeciesByCircleAreaAndHigherTaxa(au.org.ala.biocache.dto.SpatialSearchRequestParams, String)
     */
    @Override
    public List<TaxaCountDTO> findAllSpeciesByCircleAreaAndHigherTaxa(SpatialSearchRequestParams requestParams, String speciesGroup) throws Exception {
        // format query so lsid searches are properly escaped, etc
        queryFormatUtils.formatSearchQuery(requestParams);
        String queryString = requestParams.getFormattedQuery();
        if (logger.isDebugEnabled()) {
            logger.debug("The species count query " + queryString);
        }
        List<String> fqList = new ArrayList<String>();
        //only add the FQ's if they are not the default values
        if (requestParams.getFormattedFq().length > 0) {
            org.apache.commons.collections.CollectionUtils.addAll(fqList, requestParams.getFormattedFq());
        }
        List<TaxaCountDTO> speciesWithCounts = getSpeciesCounts(queryString, fqList, CollectionUtils.arrayToList(requestParams.getFacets()), requestParams.getPageSize(), requestParams.getStart(), requestParams.getSort(), requestParams.getDir());

        return speciesWithCounts;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#findRecordByStateFor(java.lang.String)
     * IS THIS BEGIN USED OR NECESSARY
     */
    @Override
    public List<FieldResultDTO> findRecordByStateFor(String query)
            throws Exception {
        List<FieldResultDTO> fDTOs = new ArrayList<FieldResultDTO>(); // new OccurrencePoint(PointType.POINT);
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.setQuery(query);
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField("state");
        solrQuery.setFacetMinCount(1);
        QueryResponse qr = runSolrQuery(solrQuery, null, 1, 0, "data_provider", "asc");
        FacetField ff = qr.getFacetField("state");
        if (ff != null) {
            for (Count count : ff.getValues()) {
                //only start adding counts when we hit a decade with some results.
                if (count.getCount() > 0) {
                    FieldResultDTO f = new FieldResultDTO(count.getName(), "state" + "." + count.getName(), count.getCount());
                    fDTOs.add(f);
                }
            }
        }
        return fDTOs;
    }

    /**
     * Calculates the breakdown of the supplied query based on the supplied params
     */
    public TaxaRankCountDTO calculateBreakdown(BreakdownRequestParams queryParams) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Attempting to find the counts for " + queryParams);
        }
        TaxaRankCountDTO trDTO = null;
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        queryFormatUtils.formatSearchQuery(queryParams);
        solrQuery.setQuery(queryParams.getFormattedQuery());
        queryParams.setPageSize(0);
        solrQuery.setFacet(true);
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetSort("count");
        solrQuery.setFacetLimit(-1);

        //add the rank:name as a fq if necessary
        if (StringUtils.isNotEmpty(queryParams.getName()) && StringUtils.isNotEmpty(queryParams.getRank())) {
            queryParams.setFormattedFq((String[]) ArrayUtils.addAll(queryParams.getFormattedFq(), new String[]{queryParams.getRank() + ":" + queryParams.getName()}));
        }
        //add the ranks as facets
        if (queryParams.getLevel() == null) {
            List<String> ranks = queryParams.getRank() != null ? searchUtils.getNextRanks(queryParams.getRank(), queryParams.getName() == null) : searchUtils.getRanks();
            for (String r : ranks) {
                solrQuery.addFacetField(r);
            }
        } else {
            //the user has supplied the "exact" level at which to perform the breakdown
            solrQuery.addFacetField(queryParams.getLevel());
        }
        QueryResponse qr = runSolrQuery(solrQuery, queryParams);
        if (queryParams.getMax() != null && queryParams.getMax() > 0) {
            //need to get the return level that the number of facets are <=max ranks need to be processed in reverse order until max is satisfied
            if (qr.getResults().getNumFound() > 0) {
                List<FacetField> ffs = qr.getFacetFields();
                //reverse the facets so that they are returned in rank reverse order species, genus, family etc
                Collections.reverse(ffs);
                for (FacetField ff : ffs) {
                    //logger.debug("Handling " + ff.getName());
                    trDTO = new TaxaRankCountDTO(ff.getName());
                    if (ff.getValues() != null && ff.getValues().size() <= queryParams.getMax()) {
                        List<FieldResultDTO> fDTOs = new ArrayList<FieldResultDTO>();
                        for (Count count : ff.getValues()) {
                            if (count.getCount() > 0) {
                                FieldResultDTO f = new FieldResultDTO(count.getName(), count.getFacetField().getName() + "." + count.getName(), count.getCount());
                                fDTOs.add(f);
                            }
                        }
                        trDTO.setTaxa(fDTOs);
                        break;
                    }
                }

            }
        } else if (queryParams.getRank() != null || queryParams.getLevel() != null) {
            //just want to process normally the rank to facet on will start with the highest rank and then go down until one exists for
            if (qr.getResults().getNumFound() > 0) {
                List<FacetField> ffs = qr.getFacetFields();
                for (FacetField ff : ffs) {
                    trDTO = new TaxaRankCountDTO(ff.getName());
                    if (ff != null && ff.getValues() != null) {
                        List<Count> counts = ff.getValues();
                        if (counts.size() > 0) {
                            List<FieldResultDTO> fDTOs = new ArrayList<FieldResultDTO>();
                            for (Count count : counts) {
                                if (count.getCount() > 0) {
                                    FieldResultDTO f = new FieldResultDTO(count.getName(), count.getFacetField().getName() + "." + count.getName(), count.getCount());
                                    fDTOs.add(f);
                                }
                            }
                            trDTO.setTaxa(fDTOs);
                            break;
                        }
                    }
                }
            }

        }
        return trDTO;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#findTaxonCountForUid(au.org.ala.biocache.dto.BreakdownRequestParams, String)
     * @deprecated use {@link #calculateBreakdown(BreakdownRequestParams)} instead
     */
    @Deprecated
    public TaxaRankCountDTO findTaxonCountForUid(BreakdownRequestParams breakdownParams, String query) throws Exception {
        TaxaRankCountDTO trDTO = null;
        List<String> ranks = breakdownParams.getLevel() == null ? searchUtils.getNextRanks(breakdownParams.getRank(), breakdownParams.getName() == null) : new ArrayList<String>();
        if (breakdownParams.getLevel() != null)
            ranks.add(breakdownParams.getLevel());
        if (ranks != null && ranks.size() > 0) {
            SolrQuery solrQuery = new SolrQuery();
            solrQuery.setRequestHandler("standard");
            solrQuery.setQuery(query);
            solrQuery.setRows(0);
            solrQuery.setFacet(true);
            solrQuery.setFacetMinCount(1);
            solrQuery.setFacetSort("count");
            solrQuery.setFacetLimit(-1); //we want all facets
            for (String r : ranks) {
                solrQuery.addFacetField(r);
            }
            QueryResponse qr = runSolrQuery(solrQuery, queryFormatUtils.getQueryContextAsArray(breakdownParams.getQc()), 1, 0, breakdownParams.getRank(), "asc");
            if (qr.getResults().size() > 0) {
                for (String r : ranks) {
                    trDTO = new TaxaRankCountDTO(r);
                    FacetField ff = qr.getFacetField(r);
                    if (ff != null && ff.getValues() != null) {
                        List<Count> counts = ff.getValues();
                        if (counts.size() > 0) {
                            List<FieldResultDTO> fDTOs = new ArrayList<FieldResultDTO>();
                            for (Count count : counts) {
                                FieldResultDTO f = new FieldResultDTO(count.getName(), count.getFacetField().getName() + "." + count.getName(), count.getCount());
                                fDTOs.add(f);
                            }
                            trDTO.setTaxa(fDTOs);
                            break;
                        }
                    }
                }
            }
        }
        return trDTO;
    }

    /**
     * Convenience method for running solr query
     *
     * @param solrQuery
     * @param filterQuery
     * @param pageSize
     * @param startIndex
     * @param sortField
     * @param sortDirection
     * @return
     * @throws SolrServerException
     */
    private QueryResponse runSolrQuery(SolrQuery solrQuery, String filterQuery[], Integer pageSize,
                                       Integer startIndex, String sortField, String sortDirection) throws SolrServerException {
        SearchRequestParams requestParams = new SearchRequestParams();
        requestParams.setFq(filterQuery);
        requestParams.setFormattedFq(filterQuery);
        requestParams.setPageSize(pageSize);
        requestParams.setStart(startIndex);
        requestParams.setSort(sortField);
        requestParams.setDir(sortDirection);
        return runSolrQuery(solrQuery, requestParams);
    }

    /**
     * Perform SOLR query - takes a SolrQuery and search params
     *
     * @param solrQuery
     * @param requestParams
     * @return
     * @throws SolrServerException
     */
    private QueryResponse runSolrQuery(SolrQuery solrQuery, SearchRequestParams requestParams) throws SolrServerException {

        if (requestParams.getFormattedFq() != null) {
            for (String fq : requestParams.getFormattedFq()) {
                if (StringUtils.isNotEmpty(fq)) {
                    solrQuery.addFilterQuery(fq);
                }
            }
        }

        //include null facets
        solrQuery.setFacetMissing(true);
        solrQuery.setRows(requestParams.getPageSize());
        solrQuery.setStart(requestParams.getStart());
        if(StringUtils.isNotEmpty(requestParams.getDir())){
            solrQuery.setSort(requestParams.getSort(), SolrQuery.ORDER.valueOf(requestParams.getDir()));
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Solr query: " + solrQuery.toString());
        }
        QueryResponse qr = query(solrQuery, queryMethod); // can throw exception
        if (logger.isDebugEnabled()) {
            logger.debug("qtime:" + qr.getQTime());
            if (qr.getResults() == null) {
                logger.debug("no results");
            } else {
                logger.debug("Matched records: " + qr.getResults().getNumFound());
            }
        }
        return qr;
    }

    /**
     * Perform SOLR query - takes a SolrQuery and search params
     *
     * @param solrQuery
     * @return
     * @throws SolrServerException
     */
    private QueryResponse runSolrQueryWithCursorMark(SolrQuery solrQuery, int pageSize, String cursorMark) throws SolrServerException {

        //include null facets
        solrQuery.setFacetMissing(true);
        solrQuery.setRows(pageSize);

        //if set to true, use the cursor mark - for better deep paging performance
        if(cursorMark == null){
            cursorMark = CursorMarkParams.CURSOR_MARK_START;
        }
        solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);

        //if using cursor mark, avoid sorting
        solrQuery.setSort("id", SolrQuery.ORDER.desc);

        if (logger.isDebugEnabled()) {
            logger.debug("SOLR query (cursor mark): " + solrQuery.toString());
        }
        QueryResponse qr = query(solrQuery, queryMethod); // can throw exception
        if (logger.isDebugEnabled()) {
            logger.debug("SOLR query (cursor mark): " + solrQuery.toString() + " qtime:" + qr.getQTime());
            if (qr.getResults() == null) {
                logger.debug("no results");
            } else {
                logger.debug("matched records: " + qr.getResults().getNumFound());
            }
        }
        return qr;
    }

    /**
     * Process the {@see org.apache.solr.client.solrj.response.QueryResponse} from a SOLR search and return
     * a {@link au.org.ala.biocache.dto.SearchResultDTO}
     *
     * @param qr
     * @param solrQuery
     * @return
     */
    private SearchResultDTO processSolrResponse(SearchRequestParams params, QueryResponse qr, SolrQuery solrQuery, Class resultClass) {
        SearchResultDTO searchResult = new SearchResultDTO();
        SolrDocumentList sdl = qr.getResults();
        // Iterator it = qr.getResults().iterator() // Use for download
        List<FacetField> facets = qr.getFacetFields();
        List<FacetField> facetDates = qr.getFacetDates();
        Map<String, Integer> facetQueries = qr.getFacetQuery();
        if (facetDates != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Facet dates size: " + facetDates.size());
            }
            facets.addAll(facetDates);
        }

        List<OccurrenceIndex> results = qr.getBeans(resultClass);

        //facet results
        searchResult.setTotalRecords(sdl.getNumFound());
        searchResult.setStartIndex(sdl.getStart());
        searchResult.setPageSize(solrQuery.getRows()); //pageSize
        searchResult.setStatus("OK");
        String[] solrSort = StringUtils.split(solrQuery.getSortField(), " "); // e.g. "taxon_name asc"
        if (logger.isDebugEnabled()) {
            logger.debug("sortField post-split: " + StringUtils.join(solrSort, "|"));
        }
        if(solrSort != null && solrSort.length == 2) {
            searchResult.setSort(solrSort[0]); // sortField
            searchResult.setDir(solrSort[1]); // sortDirection
        }
        searchResult.setQuery(params.getUrlParams()); //this needs to be the original URL>>>>
        searchResult.setOccurrences(results);

        List<FacetResultDTO> facetResults = buildFacetResults(facets);

        //all belong to uncertainty range for now
        if (facetQueries != null && !facetQueries.isEmpty()) {
            Map<String, String> rangeMap = rangeBasedFacets.getRangeMap("uncertainty");
            List<FieldResultDTO> fqr = new ArrayList<FieldResultDTO>();
            for (String value : facetQueries.keySet()) {
                if (facetQueries.get(value) > 0)
                    fqr.add(new FieldResultDTO(rangeMap.get(value), rangeMap.get(value), facetQueries.get(value), value));
            }
            facetResults.add(new FacetResultDTO("uncertainty", fqr));
        }

        //handle all the range based facets
        if (qr.getFacetRanges() != null) {
            for (RangeFacet rfacet : qr.getFacetRanges()) {
                List<FieldResultDTO> fqr = new ArrayList<FieldResultDTO>();
                if (rfacet instanceof Numeric) {
                    Numeric nrfacet = (Numeric) rfacet;
                    List<RangeFacet.Count> counts = nrfacet.getCounts();
                    //handle the before
                    if (nrfacet.getBefore().intValue() > 0) {
                        String name = "[* TO " + getUpperRange(nrfacet.getStart().toString(), nrfacet.getGap(), false) + "]";

                        fqr.add(new FieldResultDTO(name,
                                name,
                                nrfacet.getBefore().intValue()));
                    }
                    for (RangeFacet.Count count : counts) {
                        String title = getRangeValue(count.getValue(), nrfacet.getGap());
                        fqr.add(new FieldResultDTO(title, title, count.getCount()));
                    }
                    //handle the after
                    if (nrfacet.getAfter().intValue() > 0) {
                        fqr.add(new FieldResultDTO("[" + nrfacet.getEnd().toString() + " TO *]", "[" + nrfacet.getEnd().toString() + " TO *]", nrfacet.getAfter().intValue()));
                    }
                    facetResults.add(new FacetResultDTO(nrfacet.getName(), fqr));
                } else {
                    // Looks like date facets are no longer coming from qr.getFacetDates() but as Range facets instead
                    List<RangeFacet.Count> facetEntries = rfacet.getCounts();
                    final String facetName = rfacet.getName();

                    addFacetResultsFromSolrFacets(facetResults, facetEntries, facetName);

                }
            }
        }

        //update image URLs
        for (OccurrenceIndex oi : results) {
            updateImageUrls(oi);
        }

        searchResult.setFacetResults(facetResults);
        // The query result is stored in its original format so that all the information
        // returned is available later on if needed
        searchResult.setQr(qr);
        return searchResult;
    }

    /**
     * Build the facet results.
     *
     * @param facets
     * @return
     */
    private List<FacetResultDTO> buildFacetResults(List<FacetField> facets) {
        List<FacetResultDTO> facetResults = new ArrayList<FacetResultDTO>();
        // populate SOLR facet results
        if (facets != null) {
            for (FacetField facet : facets) {
                List<?> facetEntries = facet.getValues();
                final String facetName = facet.getName();

                addFacetResultsFromSolrFacets(facetResults, facetEntries, facetName);
            }
        }
        return facetResults;
    }


    /**
     * Add {@link FacetResultDTO} instances to facetResults from facetEntries either coming from Solr facet  or facet ranges entries
     * @param facetResults A non null list where FacetResultDTO instances will be added.
     * @param facetEntries The solr facet entries
     * @param facetName The name of the facet
     * @throws IllegalArgumentException if facetEntries is not a List containing either {@link FacetField.Count} or @{@link RangeFacet.Count} instances
     *
     */
    private void addFacetResultsFromSolrFacets(List<FacetResultDTO> facetResults, List<?> facetEntries, String facetName) {
        if ((facetEntries != null) && (facetEntries.size() > 0)) {
            ArrayList<FieldResultDTO> r = new ArrayList<FieldResultDTO>();

            long entryCount;
            String countEntryName;

            for (Object  facetCountEntryObject : facetEntries) {
                if(facetCountEntryObject instanceof Count) {
                    Count facetCountEntry = (Count) facetCountEntryObject;
                    entryCount = facetCountEntry.getCount();
                    countEntryName = facetCountEntry.getName();
                } else if(facetCountEntryObject instanceof RangeFacet.Count)  {
                    RangeFacet.Count raengeFacetCountEntry = (RangeFacet.Count) facetCountEntryObject;
                    entryCount = raengeFacetCountEntry.getCount();
                    countEntryName = raengeFacetCountEntry.getValue();

                } else {
                    throw new IllegalArgumentException("facetCountEntry is not an instance of FacetField.Count nor RangeFacet.Count: "+ facetCountEntryObject.getClass());
                }

                //check to see if the facet field is an uid value that needs substitution
                if (entryCount == 0) continue;

                if (countEntryName == null) {

                    String label = "";
                    if(messageSource != null){
                        label = messageSource.getMessage(facetName + ".novalue", null, "Not supplied", null);
                    }
                    r.add(new FieldResultDTO(label, facetName + ".novalue", entryCount, "-" + facetName + ":*"));
                } else {
                    if (countEntryName.equals(DECADE_PRE_1850_LABEL)) {
                        r.add(0, new FieldResultDTO(
                                getFacetValueDisplayName(facetName, countEntryName),
                                facetName + "." + countEntryName,
                                entryCount,
                                getFormattedFqQuery(facetName, countEntryName)
                        ));
                    } else {
                        r.add(new FieldResultDTO(
                                getFacetValueDisplayName(facetName, countEntryName),
                                facetName + "." + countEntryName,
                                entryCount,
                                getFormattedFqQuery(facetName, countEntryName)
                        ));
                    }
                }
            }
            // only add facets if there are more than one facet result
            if (r.size() > 0) {
                FacetResultDTO fr = new FacetResultDTO(facetName, r);
                facetResults.add(fr);
            }
        }
    }

    private void updateImageUrls(OccurrenceIndex oi) {

        if (!StringUtils.isNotBlank(oi.getImage()))
            return;

        try {
            Map<String, String> formats = Config.mediaStore().getImageFormats(oi.getImage());
            oi.setImageUrl(formats.get("raw"));
            oi.setThumbnailUrl(formats.get("thumb"));
            oi.setSmallImageUrl(formats.get("small"));
            oi.setLargeImageUrl(formats.get("large"));
            String[] images = oi.getImages();
            if (images != null && images.length > 0) {
                String[] imageUrls = new String[images.length];
                for (int i = 0; i < images.length; i++) {
                    try {
                        Map<String, String> availableFormats = Config.mediaStore().getImageFormats(images[i]);
                        imageUrls[i] = availableFormats.get("large");
                    } catch (Exception ex) {
                        logger.warn("Unable to update image URL for " + images[i] + ": " + ex.getMessage());
                    }
                }
                oi.setImageUrls(imageUrls);
            }
        } catch (Exception ex) {
            logger.warn("Unable to update image URL for " + oi.getImage() + ": " + ex.getMessage());
        }
    }

    private String getRangeValue(String lower, Number gap) {
        StringBuilder value = new StringBuilder("[");
        value.append(lower).append(" TO ").append(getUpperRange(lower, gap, true));
        return value.append("]").toString();
    }

    private String getUpperRange(String lower, Number gap, boolean addGap) {
        if (gap instanceof Integer) {
            Integer upper = Integer.parseInt(lower) - 1;
            if (addGap)
                upper += (Integer) gap;
            return upper.toString();
        } else if (gap instanceof Double) {
            BigDecimal upper = new BigDecimal(lower).add(new BigDecimal(-0.001));
            if (addGap) {
                upper = upper.add(new BigDecimal(gap.doubleValue()));
            }
            return upper.setScale(3, RoundingMode.HALF_UP).toString();
        } else {
            return lower;
        }
    }

    protected void initDecadeBasedFacet(SolrQuery solrQuery, String field) {
        //Solr 6.x don't use facet.date but facef.range instead
        solrQuery.add("facet.range", field);
        solrQuery.add("facet.range.start", DECADE_FACET_START_DATE); // facet date range starts from 1850
        solrQuery.add("facet.range.end", "NOW/DAY"); // facet date range ends for current date (gap period)
        solrQuery.add("facet.range.gap", "+10YEAR"); // gap interval of 10 years
        solrQuery.add("facet.range.other", DECADE_PRE_1850_LABEL); // include counts before the facet start date ("before" label)
        solrQuery.add("facet.range.include", "lower"); // counts will be included for dates on the starting date but not ending date
    }

    /**
     * Helper method to create SolrQuery object and add facet settings
     *
     * @return solrQuery the SolrQuery
     */
    protected SolrQuery initSolrQuery(SearchRequestParams searchParams, boolean substituteDefaultFacetOrder, Map<String, String[]> extraSolrParams) {

        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        boolean rangeAdded = false;
        // Facets
        solrQuery.setFacet(searchParams.getFacet());
        if (searchParams.getFacet()) {
            for (String facet : searchParams.getFacets()) {
                if (facet.equals("date") || facet.equals("decade")) {
                    String fname = facet.equals("decade") ? OCCURRENCE_YEAR_INDEX_FIELD : "occurrence_" + facet;
                    initDecadeBasedFacet(solrQuery, fname);
                } else if (facet.equals("uncertainty")) {
                    Map<String, String> rangeMap = rangeBasedFacets.getRangeMap("uncertainty");
                    for (String range : rangeMap.keySet()) {
                        solrQuery.add("facet.query", range);
                    }
                } else if (facet.endsWith(RANGE_SUFFIX)) {
                    //this facte need to have it ranges included.
                    if (!rangeAdded) {
                        solrQuery.add("facet.range.other", "before");
                        solrQuery.add("facet.range.other", "after");
                    }
                    String field = facet.replaceAll(RANGE_SUFFIX, "");
                    StatsIndexFieldDTO details = getRangeFieldDetails(field);
                    if (details != null) {
                        solrQuery.addNumericRangeFacet(field, details.getStart(), details.getEnd(), details.getGap());
                    }
                } else {
                    solrQuery.addFacetField(facet);

                    if ("".equals(searchParams.getFsort()) && substituteDefaultFacetOrder && FacetThemes.getFacetsMap().containsKey(facet)) {
                        //now check if the sort order is different to supplied
                        String thisSort = FacetThemes.getFacetsMap().get(facet).getSort();
                        if (!searchParams.getFsort().equalsIgnoreCase(thisSort))
                            solrQuery.add("f." + facet + ".facet.sort", thisSort);
                    }

                }
            }

            solrQuery.setFacetMinCount(1);
            solrQuery.setFacetLimit(searchParams.getFlimit());
            //include this so that the default fsort is still obeyed.
            String fsort = "".equals(searchParams.getFsort()) ? "count" : searchParams.getFsort();
            solrQuery.setFacetSort(fsort);
            if (searchParams.getFoffset() > 0)
                solrQuery.add("facet.offset", Integer.toString(searchParams.getFoffset()));
            if (StringUtils.isNotEmpty(searchParams.getFprefix()))
                solrQuery.add("facet.prefix", searchParams.getFprefix());
        }

        solrQuery.setRows(10);
        solrQuery.setStart(0);

        if (searchParams.getFl().length() > 0) {
            solrQuery.setFields(searchParams.getFl());
        }

        //add the extra SOLR params
        if (extraSolrParams != null) {
            //automatically include the before and after params...
            if (!rangeAdded) {
                solrQuery.add("facet.range.other", "before");
                solrQuery.add("facet.range.other", "after");
            }
            for (String key : extraSolrParams.keySet()) {
                String[] values = extraSolrParams.get(key);
                solrQuery.add(key, values);
            }
        }
        return solrQuery;
    }

    /**
     * Obtains the Statistics for the supplied field so it can be used to determine the ranges.
     *
     * @param field
     * @return
     */
    private StatsIndexFieldDTO getRangeFieldDetails(String field) {
        StatsIndexFieldDTO details = rangeFieldCache.get(field);
        Map<String, IndexFieldDTO> nextIndexFieldMap = indexFieldMap;
        if (details == null && nextIndexFieldMap != null) {
            //get the details
            SpatialSearchRequestParams searchParams = new SpatialSearchRequestParams();
            searchParams.setQ("*:*");
            searchParams.setFacets(new String[]{field});
            try {
                Map<String, FieldStatsInfo> stats = getStatistics(searchParams);
                if (stats != null) {
                    IndexFieldDTO ifdto = nextIndexFieldMap.get(field);
                    if (ifdto != null) {
                        String type = ifdto.getDataType();
                        details = new StatsIndexFieldDTO(stats.get(field), type);
                        rangeFieldCache.put(field, details);
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Unable to locate field:  " + field);
                        }
                        return null;
                    }
                }
            } catch (Exception e) {
                logger.warn("Unable to obtain range from cache.", e);
                details = null;
            }
        }

        return details;
    }

    /**
     * Get a distinct list of species and their counts using a facet search
     *
     * @param queryString
     * @param pageSize
     * @param sortField
     * @param sortDirection
     * @return
     * @throws SolrServerException
     */
    protected List<TaxaCountDTO> getSpeciesCounts(String queryString, List<String> filterQueries, List<String> facetFields, Integer pageSize,
                                                  Integer startIndex, String sortField, String sortDirection) throws SolrServerException {

        List<TaxaCountDTO> speciesCounts = new ArrayList<TaxaCountDTO>();
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.setQuery(queryString);

        if (filterQueries != null && filterQueries.size() > 0) {
            //solrQuery.addFilterQuery("(" + StringUtils.join(filterQueries, " OR ") + ")");
            for (String fq : filterQueries) {
                solrQuery.addFilterQuery(fq);
            }
        }
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.setFacetSort(sortField);
        for (String facet : facetFields) {
            solrQuery.addFacetField(facet);
            if (logger.isDebugEnabled()) {
                logger.debug("adding facetField: " + facet);
            }
        }
        //set the facet starting point based on the paging information
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(pageSize); // unlimited = -1 | pageSize
        solrQuery.add("facet.offset", Integer.toString(startIndex));
        if (logger.isDebugEnabled()) {
            logger.debug("getSpeciesCount query :" + solrQuery.getQuery());
        }
        QueryResponse qr = runSolrQuery(solrQuery, null, 1, 0, "score", sortDirection);
        if (logger.isInfoEnabled()) {
            logger.info("SOLR query: " + solrQuery.getQuery() + "; total hits: " + qr.getResults().getNumFound());
        }
        List<FacetField> facets = qr.getFacetFields();
        java.util.regex.Pattern p = java.util.regex.Pattern.compile("\\|");

        if (facets != null && facets.size() > 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Facets: " + facets.size() + "; facet #1: " + facets.get(0).getName());
            }
            for (FacetField facet : facets) {
                List<FacetField.Count> facetEntries = facet.getValues();
                if ((facetEntries != null) && (facetEntries.size() > 0)) {

                    for (FacetField.Count fcount : facetEntries) {
                        TaxaCountDTO tcDTO = null;
                        String name = fcount.getName() != null ? fcount.getName() : "";
                        if (fcount.getFacetField().getName().equals(NAMES_AND_LSID)) {
                            String[] values = p.split(name, 5);

                            if (values.length >= 5) {
                                if (!"||||".equals(name)) {
                                    tcDTO = new TaxaCountDTO(values[0], fcount.getCount());
                                    tcDTO.setGuid(StringUtils.trimToNull(values[1]));
                                    tcDTO.setCommonName(values[2]);
                                    tcDTO.setKingdom(values[3]);
                                    tcDTO.setFamily(values[4]);
                                    if (StringUtils.isNotEmpty(tcDTO.getGuid()))
                                        tcDTO.setRank(searchUtils.getTaxonSearch(tcDTO.getGuid())[1].split(":")[0]);
                                }
                            } else {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("The values length: " + values.length + " :" + name);
                                }
                                tcDTO = new TaxaCountDTO(name, fcount.getCount());
                            }
                            //speciesCounts.add(i, tcDTO);
                            if (tcDTO != null && tcDTO.getCount() > 0)
                                speciesCounts.add(tcDTO);
                        } else if (fcount.getFacetField().getName().equals(COMMON_NAME_AND_LSID)) {
                            String[] values = p.split(name, 6);

                            if (values.length >= 5) {
                                if (!"|||||".equals(name)) {
                                    tcDTO = new TaxaCountDTO(values[1], fcount.getCount());
                                    tcDTO.setGuid(StringUtils.trimToNull(values[2]));
                                    tcDTO.setCommonName(values[0]);
                                    //cater for the bug of extra vernacular name in the result
                                    tcDTO.setKingdom(values[values.length - 2]);
                                    tcDTO.setFamily(values[values.length - 1]);
                                    if (StringUtils.isNotEmpty(tcDTO.getGuid()))
                                        tcDTO.setRank(searchUtils.getTaxonSearch(tcDTO.getGuid())[1].split(":")[0]);
                                }
                            } else {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("The values length: " + values.length + " :" + name);
                                }
                                tcDTO = new TaxaCountDTO(name, fcount.getCount());
                            }
                            //speciesCounts.add(i, tcDTO);
                            if (tcDTO != null && tcDTO.getCount() > 0) {
                                speciesCounts.add(tcDTO);
                            }
                        }
                    }
                }
            }
        }

        return speciesCounts;
    }

    /**
     * Obtains a list and facet count of the source uids for the supplied query.
     *
     * @param searchParams
     * @return
     * @throws Exception
     */
    public Map<String, Integer> getSourcesForQuery(SpatialSearchRequestParams searchParams) throws Exception {

        Map<String, Integer> uidStats = new HashMap<String, Integer>();
        SolrQuery solrQuery = new SolrQuery();
        queryFormatUtils.formatSearchQuery(searchParams);
        if (logger.isInfoEnabled()) {
            logger.info("The query : " + searchParams.getFormattedQuery());
        }
        solrQuery.setQuery(searchParams.getFormattedQuery());
        solrQuery.setRequestHandler("standard");
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.setFacetMinCount(1);
        solrQuery.addFacetField("data_provider_uid");
        solrQuery.addFacetField("data_resource_uid");
        solrQuery.addFacetField("collection_uid");
        solrQuery.addFacetField("institution_uid");
        QueryResponse qr = runSolrQuery(solrQuery, searchParams.getFormattedFq(), 1, 0, "score", "asc");
        //now cycle through and get all the facets
        List<FacetField> facets = qr.getFacetFields();
        for (FacetField facet : facets) {
            if (facet.getValues() != null) {
                for (FacetField.Count ffc : facet.getValues()) {
                    if (ffc.getCount() > 0) {
                        uidStats.put(ffc.getName() != null ? ffc.getName() : "", new Integer((int) ffc.getCount()));
                    }
                }
            }
        }
        return uidStats;
    }

    /**
     * Gets the details about the SOLR fields using the LukeRequestHandler:
     * See http://wiki.apache.org/solr/LukeRequestHandler  for more information
     */
    public Set<IndexFieldDTO> getIndexFieldDetails(String... fields) throws Exception {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("qt", "/admin/luke");

        params.set("tr", "luke.xsl");
        if (fields != null) {
            params.set("fl", fields);
            params.set("numTerms", "1");
        } else {
            // TODO: We should be caching the result locally without calling Solr in this case, as it is called very often
            params.set("numTerms", "0");
        }
        QueryResponse response = query(params, queryMethod);
        return parseLukeResponse(response.toString(), fields != null);
    }

    /**
     * Returns the count of distinct values for the facets.
     * This is an altered implementation that is SOLRCloud friendly (ngroups are not SOLR Cloud compatible)
     */
    public List<FacetResultDTO> getFacetCounts(SpatialSearchRequestParams searchParams) throws Exception {

        queryFormatUtils.formatSearchQuery(searchParams, true);
        String queryString = searchParams.getFormattedQuery();
        searchParams.setFacet(true);
        searchParams.setPageSize(0);
        SolrQuery facetQuery = initSolrQuery(searchParams, false, null);
        facetQuery.setQuery(queryString);
        facetQuery.setFields(null);
        facetQuery.setRows(0);
        facetQuery.setFacetLimit(-1);

        List<String> fqList = new ArrayList<String>();
        //only add the FQ's if they are not the default values
        if (searchParams != null && searchParams.getFormattedFq() != null && searchParams.getFormattedFq().length > 0) {
            org.apache.commons.collections.CollectionUtils.addAll(fqList, searchParams.getFormattedFq());
        }

        facetQuery.setFilterQueries(fqList.toArray(new String[0]));

        QueryResponse qr = query(facetQuery, queryMethod);
        SearchResultDTO searchResults = processSolrResponse(searchParams, qr, facetQuery, OccurrenceIndex.class);

        List<FacetResultDTO> facetResults = searchResults.getFacetResults();
        if (facetResults != null) {
            for (FacetResultDTO fr : facetResults) {
                FacetResultDTO frDTO = new FacetResultDTO();
                frDTO.setCount(fr.getCount());
                frDTO.setFieldName(fr.getFieldName());
                if (fr.getCount() == null) {
                    fr.setCount(fr.getFieldResult().size());
                }
                //reduce the number of facets returned...
                if(searchParams.getFlimit() != null && searchParams.getFlimit() < fr.getFieldResult().size()){
                    fr.getFieldResult().subList(0, searchParams.getFlimit());
                }
            }
        }
        return facetResults;
    }

    @Override
    public Set<IndexFieldDTO> getIndexedFields() throws Exception {
        return getIndexedFields(false);
    }

    /**
     * Returns details about the fields in the index.
     */
    @Cacheable(cacheName = "getIndexedFields")
    public Set<IndexFieldDTO> getIndexedFields(boolean update) throws Exception {
        Set<IndexFieldDTO> result = indexFields;
        if (result.size() == 0 || update) {
            synchronized (solrIndexVersionLock) {
                result = indexFields;
                if (result.size() == 0 || update) {
                    result = getIndexFieldDetails(null);
                    if (result != null && result.size() > 0) {
                        Map<String, IndexFieldDTO> resultMap = new HashMap<String, IndexFieldDTO>();
                        for (IndexFieldDTO field : result) {
                            resultMap.put(field.getName(), field);
                        }
                        indexFields = result;
                        indexFieldMap = resultMap;
                    }
                }
            }
        }
        return result;
    }

    @Override
    public Map<String, IndexFieldDTO> getIndexedFieldsMap() throws Exception {
        // Refresh/populate the map if necessary
        getIndexedFields();
        return indexFieldMap;
    }

    /**
     * parses the response string from the service that returns details about the indexed fields
     *
     * @param str
     * @return
     */
    private Set<IndexFieldDTO> parseLukeResponse(String str, boolean includeCounts) {

        //update index version
        Pattern indexVersion = Pattern.compile("(?:version=)([0-9]{1,})");
        try {
            Matcher indexVersionMatcher = indexVersion.matcher(str);
            if (indexVersionMatcher.find(0)) {
                solrIndexVersion = Long.parseLong(indexVersionMatcher.group(1));
                solrIndexVersionTime = System.currentTimeMillis();
            }
        } catch (Exception e) {}

        Set<IndexFieldDTO> fieldList = includeCounts ? new java.util.LinkedHashSet<IndexFieldDTO>() : new java.util.TreeSet<IndexFieldDTO>();

        Pattern typePattern = Pattern.compile("(?:type=)([a-z]{1,})");

        Pattern schemaPattern = Pattern.compile("(?:schema=)([a-zA-Z\\-]{1,})");

        Pattern distinctPattern = Pattern.compile("(?:distinct=)([0-9]{1,})");

        String[] fieldsStr = str.split("fields=\\{");

        Map<String, String> indexToJsonMap = new OccurrenceIndex().indexToJsonMap();


        for (String fieldStr : fieldsStr) {
            if (fieldStr != null && !"".equals(fieldStr) ) {
                String[] fields = includeCounts ? fieldStr.split("\\}\\},") : fieldStr.split("\\},");

                //sort fields for later use of indexOf
                Arrays.sort(fields);

                for (String field : fields) {
                    formatIndexField(field, null, fieldList, typePattern, schemaPattern, indexToJsonMap, distinctPattern);
                }
            }
        }

        //add CASSANDRA fields that are not indexed
        if (!downloadService.downloadSolrOnly) {
            for (String cassandraField : Store.getStorageFieldMap().keySet()) {
                boolean found = false;
                //ignore fields with multiple items
                if (cassandraField != null && !cassandraField.contains(",") ) {
                    for (IndexFieldDTO field : fieldList) {
                        if (field.isIndexed() || field.isStored()) {
                            if (field.getDownloadName() != null && field.getDownloadName().equals(cassandraField)) {

                                found = true;
                                break;
                            }
                        }
                    }
                    if (!found) {
                        formatIndexField(cassandraField, cassandraField, fieldList, typePattern, schemaPattern, indexToJsonMap, distinctPattern);
                    }
                }
            }
        }

        //filter fields, to hide deprecated terms
        List<String> toIgnore = new ArrayList<String>();
        Set<IndexFieldDTO> filteredFieldList = new HashSet<IndexFieldDTO>();
        if(indexFieldsToHide != null){
            toIgnore = Arrays.asList(indexFieldsToHide.split(","));
        }
        for(IndexFieldDTO indexedField: fieldList){
            if(!toIgnore.contains(indexedField.getName())){
                filteredFieldList.add(indexedField);
            }
        }

        return filteredFieldList;
    }

    private void formatIndexField(String indexField, String cassandraField, Set<IndexFieldDTO> fieldList, Pattern typePattern,
                                  Pattern schemaPattern, Map indexToJsonMap, Pattern distinctPattern) {

        if (indexField != null && !"".equals(indexField)) {
            IndexFieldDTO f = new IndexFieldDTO();

            String fieldName = indexField.split("=")[0];
            String type = null;
            String schema = null;
            Matcher typeMatcher = typePattern.matcher(indexField);
            if (typeMatcher.find(0)) {
                type = typeMatcher.group(1);
            }

            Matcher schemaMatcher = schemaPattern.matcher(indexField);
            if (schemaMatcher.find(0)) {
                schema = schemaMatcher.group(1);
            }

            //don't allow the sensitive coordinates to be exposed via ws and don't allow index fields without schema
            if (StringUtils.isNotEmpty(fieldName) && !fieldName.startsWith("sensitive_") && (cassandraField != null || schema != null)) {

                f.setName(fieldName);
                if (type != null) f.setDataType(type);
                else f.setDataType("string");

                //interpret the schema information
                if (schema != null) {
                    f.setIndexed(schema.contains("I"));
                    f.setStored(schema.contains("S"));
                    f.setMultivalue(schema.contains("M"));
                }

                //now add the i18n and associated strings to the field.
                //1. description: display name from fieldName= in i18n
                //2. info: details about this field from description.fieldName= in i18n
                //3. dwcTerm: DwC field name for this field from dwc.fieldName= in i18n
                //4. jsonName: json key as returned by occurrences/search
                //5. downloadField: biocache-store column name that is usable in DownloadRequestParams.fl
                //if the field has (5) downloadField, use it to find missing (1), (2) or (3)
                //6. downloadDescription: the column name when downloadField is used in
                //   DownloadRequestParams.fl and a translation occurs
                //7. i18nValues: true | false, indicates that the values returned by this field can be
                //   translated using facetName.value= in /facets/i18n
                //8. class value for this field
                if (layersPattern.matcher(fieldName).matches()) {
                    f.setDownloadName(fieldName);
                    String description = layersService.getLayerNameMap().get(fieldName);
                    f.setDescription(description);
                    f.setDownloadDescription(description);
                    f.setInfo(layersServiceUrl + "/layers/view/more/" + fieldName);
                    if(fieldName.startsWith("el")){
                        f.setClasss("Environmental");
                    } else {
                        f.setClasss("Contextual");
                    }
                } else {
                    //(5) check as a downloadField
                    String downloadField = fieldName;
                    if (cassandraField != null) {
                        downloadField = cassandraField;
                    } else if (!downloadService.downloadSolrOnly) {
                        downloadField = Store.getIndexFieldMap().get(fieldName);
                        //exclude compound fields
                        if (downloadField != null && downloadField.contains(",")) downloadField = null;
                    }
                    if (downloadField != null) {
                        f.setDownloadName(downloadField);
                    }

                    //(6) downloadField description
                    String downloadFieldDescription = messageSource.getMessage(downloadField, null, "", Locale.getDefault());
                    if (downloadFieldDescription.length() > 0) {
                        f.setDownloadDescription(downloadFieldDescription);
                        f.setDescription(downloadFieldDescription); //(1)
                    }

                    //(1) check as a field name
                    String description = messageSource.getMessage("facet." + fieldName, null, "", Locale.getDefault());
                    if (description.length() > 0 && (downloadField == null || downloadService.downloadSolrOnly)) {
                        f.setDescription(description);
                    } else if (downloadField != null) {
                        description = messageSource.getMessage(downloadField, null, "", Locale.getDefault());
                        if (description.length() > 0) {
                            f.setDescription(description);
                        }
                    }

                    //(2) check as a description
                    String info = messageSource.getMessage("description." + fieldName, null, "", Locale.getDefault());
                    if (info.length() > 0 && (downloadField == null || downloadService.downloadSolrOnly)) {
                        f.setInfo(info);
                    } else if (downloadField != null) {
                        info = messageSource.getMessage("description." + downloadField, null, "", Locale.getDefault());
                        if (info.length() > 0) {
                            f.setInfo(info);
                        }
                    }

                    //(3) check as a dwcTerm
                    String camelCase = LOWER_UNDERSCORE.to(LOWER_CAMEL, fieldName);

                    Term term = null;
                    try {
                        //find matching Darwin core term
                        term = DwcTerm.valueOf(camelCase);
                    } catch (IllegalArgumentException e) {
                        //enum not found
                    }
                    boolean dcterm = false;
                    try {
                        //find matching Dublin core terms that are not in miscProperties
                        // include case fix for rightsHolder
                        term = DcTerm.valueOf(camelCase.replaceAll("rightsholder", "rightsHolder"));
                        dcterm = true;
                    } catch (IllegalArgumentException e) {
                        //enum not found
                    }
                    if (term == null) {
                        //look in message properties. This is for irregular fieldName to DwcTerm matches
                        String dwcTerm = messageSource.getMessage("dwc." + fieldName, null, "", Locale.getDefault());
                        if (downloadField != null) {
                            dwcTerm = messageSource.getMessage("dwc." + downloadField, null, "", Locale.getDefault());
                        }

                        if (dwcTerm.length() > 0) {
                            f.setDwcTerm(dwcTerm);

                            try {
                                //find the term now
                                term = DwcTerm.valueOf(dwcTerm);
                                if(term != null){
                                    f.setClasss(((DwcTerm) term).getGroup()); //(8)
                                }
                                DwcTermDetails dwcTermDetails = DwCTerms.getInstance().getDwCTermDetails(term.simpleName());
                                if(dwcTermDetails !=null){
                                    f.setDescription(dwcTermDetails.comment);
                                }
                            } catch (IllegalArgumentException e) {
                                //enum not found
                            }
                        }
                    } else {

                        f.setDwcTerm(term.simpleName());
                        if (term instanceof DwcTerm) {
                            f.setClasss(((DwcTerm) term).getGroup()); //(8)
                        } else {
                            f.setClasss(DwcTerm.GROUP_RECORD); // Assign dcterms to the Record group.
                        }

                        DwcTermDetails dwcTermDetails = DwCTerms.getInstance().getDwCTermDetails(term.simpleName());
                        if(dwcTermDetails != null){
                            f.setDescription(dwcTermDetails.comment);
                        }
                    }

                    //append dwc url to info
                    if (!dcterm && f.getDwcTerm() != null && !f.getDwcTerm().isEmpty() && StringUtils.isNotEmpty(dwcUrl)) {
                        if (info.length() > 0) info += " ";
                        f.setInfo(info + dwcUrl + f.getDwcTerm());
                    }

                    //(4) check as json name
                    String json = (String) indexToJsonMap.get(fieldName);
                    if (json != null) {
                        f.setJsonName(json);
                    }

                    //(7) has lookupValues in i18n
                    String i18nValues = messageSource.getMessage("i18nvalues." + fieldName, null, "", Locale.getDefault());
                    if (i18nValues.length() > 0) {
                        f.setI18nValues("true".equalsIgnoreCase(i18nValues));
                    }

                    //(8) get class. This will override any DwcTerm.group
                    String classs = messageSource.getMessage("class." + fieldName, null, "", Locale.getDefault());
                    if (downloadField != null && !downloadService.downloadSolrOnly) {
                        classs = messageSource.getMessage("class." + downloadField, null, "", Locale.getDefault());
                    }
                    if (classs.length() > 0) {
                        f.setClasss(classs);
                    }
                }


                fieldList.add(f);
            }

            Matcher distinctMatcher = distinctPattern.matcher(indexField);
            if (distinctMatcher.find(0)) {
                Integer distinct = Integer.parseInt(distinctMatcher.group(1));
                f.setNumberDistinctValues(distinct);
            }
        }
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#findByFulltext(SpatialSearchRequestParams)
     */
    @Override
    public SolrDocumentList findByFulltext(SpatialSearchRequestParams searchParams) throws Exception {
        SolrDocumentList sdl = null;

        try {
            queryFormatUtils.formatSearchQuery(searchParams);
            String queryString = searchParams.getFormattedQuery();
            SolrQuery solrQuery = new SolrQuery();
            solrQuery.setQuery(queryString);
            solrQuery.setFields(searchParams.getFl());
            solrQuery.setFacet(false);
            solrQuery.setRows(searchParams.getPageSize());

            sdl = runSolrQuery(solrQuery, searchParams).getResults();
        } catch (SolrServerException ex) {
            logger.error("Problem communicating with SOLR server. " + ex.getMessage(), ex);
        }

        return sdl;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#getStatistics(SpatialSearchRequestParams)
     */
    public Map<String, FieldStatsInfo> getStatistics(SpatialSearchRequestParams searchParams) throws Exception {
        try {
            queryFormatUtils.formatSearchQuery(searchParams);
            String queryString = searchParams.getFormattedQuery();
            SolrQuery solrQuery = new SolrQuery();
            solrQuery.setQuery(queryString);
            for (String field : searchParams.getFacets()) {
                solrQuery.setGetFieldStatistics(field);
            }
            QueryResponse qr = runSolrQuery(solrQuery, searchParams);
            if (logger.isDebugEnabled()) {
                logger.debug(qr.getFieldStatsInfo());
            }
            return qr.getFieldStatsInfo();

        } catch (SolrServerException ex) {
            logger.error("Problem communicating with SOLR server. " + ex.getMessage(), ex);
        }
        return null;
    }

    @Cacheable(cacheName = "legendCache")
    public List<LegendItem> getLegend(SpatialSearchRequestParams searchParams, String facetField, String[] cutpoints) throws Exception {
        List<LegendItem> legend = new ArrayList<LegendItem>();

        queryFormatUtils.formatSearchQuery(searchParams);
        if (logger.isInfoEnabled()) {
            logger.info("search query: " + searchParams.getFormattedQuery());
        }
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.setQuery(searchParams.getFormattedQuery());
        solrQuery.setRows(0);
        solrQuery.setFacet(true);

        //is facet query?
        if (cutpoints == null) {
            //special case for the decade
            if (DECADE_FACET_NAME.equals(facetField))
                initDecadeBasedFacet(solrQuery, "occurrence_year");
            else
                solrQuery.addFacetField(facetField);
        } else {
            solrQuery.addFacetQuery("-" + facetField + ":[* TO *]");

            for (int i = 0; i < cutpoints.length; i += 2) {
                solrQuery.addFacetQuery(facetField + ":[" + cutpoints[i] + " TO " + cutpoints[i + 1] + "]");
            }
        }

        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(-1);//MAX_DOWNLOAD_SIZE);  // unlimited = -1

        solrQuery.setFacetMissing(true);

        QueryResponse qr = runSolrQuery(solrQuery, searchParams.getFormattedFq(), 1, 0, "score", "asc");
        List<FacetField> facets = qr.getFacetFields();
        if (facets != null) {
            for (FacetField facet : facets) {
                List<FacetField.Count> facetEntries = facet.getValues();
                if (facet.getName().contains(facetField) && (facetEntries != null) && (facetEntries.size() > 0)) {
                    int i = 0;
                    for (i = 0; i < facetEntries.size(); i++) {
                        FacetField.Count fcount = facetEntries.get(i);
                        if (fcount.getCount() > 0) {
                            String fq = facetField + ":\"" + fcount.getName() + "\"";
                            if (fcount.getName() == null) {
                                fq = "-" + facetField + ":[* TO *]";
                            }

                            String i18nCode = null;
                            if(StringUtils.isNotBlank(fcount.getName())){
                                i18nCode = facetField + "." + fcount.getName();
                            } else {
                                i18nCode = facetField + ".novalue";
                            }

                            legend.add(new LegendItem(getFacetValueDisplayName(facetField, fcount.getName()), i18nCode, fcount.getCount(), fq));
                        }
                    }
                    break;
                }
            }
        }
        //check if we have query based facets
        Map<String, Integer> facetq = qr.getFacetQuery();
        if (facetq != null && facetq.size() > 0) {
            for (Entry<String, Integer> es : facetq.entrySet()) {
                legend.add(new LegendItem( getFacetValueDisplayName(facetField, es.getKey()), facetField + "." + es.getKey() , es.getValue(), es.getKey()));
            }
        }

        //check to see if we have a date range facet
        List<FacetField> facetDates = qr.getFacetDates();
        if (facetDates != null && !facetDates.isEmpty()) {
            FacetField ff = facetDates.get(0);
            String firstDate = null;
            for (FacetField.Count facetEntry : ff.getValues()) {
                String startDate = facetEntry.getName();
                if (firstDate == null) {
                    firstDate = startDate;
                }
                String finishDate;
                if (DECADE_PRE_1850_LABEL.equals(startDate)) {
                    startDate = "*";
                    finishDate = firstDate;
                } else {
                    int startYear = Integer.parseInt(startDate.substring(0, 4));
                    finishDate = (startYear - 1) + "-12-31T23:59:59Z";
                }
                legend.add(
                        new LegendItem(
                                getFacetValueDisplayName(facetEntry.getFacetField().getName(), facetEntry.getName()),
                                facetEntry.getFacetField().getName() + "." + facetEntry.getName(),
                                facetEntry.getCount(),
                                "occurrence_year:[" + startDate + " TO " + finishDate + "]")
                );
            }
        }
        return legend;
    }

    public FacetField getFacet(SpatialSearchRequestParams searchParams, String facet) throws Exception {
        queryFormatUtils.formatSearchQuery(searchParams);
        if (logger.isInfoEnabled()) {
            logger.info("search query: " + searchParams.getFormattedQuery());
        }
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.setQuery(searchParams.getFormattedQuery());
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField(facet);
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(-1); //MAX_DOWNLOAD_SIZE);  // unlimited = -1

        QueryResponse qr = runSolrQuery(solrQuery, searchParams.getFormattedFq(), 1, 0, null, null);
        return qr.getFacetFields().get(0);
    }

    public List<DataProviderCountDTO> getDataProviderList(SpatialSearchRequestParams requestParams) throws Exception {
        List<DataProviderCountDTO> dataProviderList = new ArrayList<DataProviderCountDTO>();
        FacetField facet = getFacet(requestParams, "data_provider_uid");
        String[] oldFq = requestParams.getFacets();
        if (facet != null) {
            String[] dp = new String[1];
            List<FacetField.Count> facetEntries = facet.getValues();
            if (facetEntries != null && facetEntries.size() > 0) {
                for (int i = 0; i < facetEntries.size(); i++) {
                    FacetField.Count fcount = facetEntries.get(i);

                    String dataProviderName = collectionCache.getNameForCode(fcount.getName());
                    if (StringUtils.isNotEmpty(dataProviderName)) {
                        dataProviderList.add(new DataProviderCountDTO(fcount.getName(), dataProviderName, fcount.getCount()));
                    }
                }
            }
        }
        requestParams.setFacets(oldFq);
        return dataProviderList;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#findAllSpecies(SpatialSearchRequestParams)
     */
    @Override
    public List<TaxaCountDTO> findAllSpecies(SpatialSearchRequestParams requestParams) throws Exception {
        queryFormatUtils.formatSearchQuery(requestParams);
        //add the context information
        List<String> facetFields = new ArrayList<String>();
        facetFields.add(NAMES_AND_LSID);
        if (logger.isDebugEnabled()) {
            logger.debug("The species count query " + requestParams.getFormattedQuery());
        }
        List<String> fqList = new ArrayList<String>();
        //only add the FQ's if they are not the default values
        if (requestParams.getFormattedFq().length > 0) {
            org.apache.commons.collections.CollectionUtils.addAll(fqList, requestParams.getFormattedFq());
        }

        String query = requestParams.getFormattedQuery();
        List<TaxaCountDTO> speciesWithCounts = getSpeciesCounts(
                query,
                fqList,
                facetFields,
                requestParams.getPageSize(),
                requestParams.getStart(),
                requestParams.getSort(),
                requestParams.getDir()
        );

        return speciesWithCounts;
    }

    /**
     * Retrieves a set of counts for the supplied list of taxa.
     *
     * @param taxa
     * @param filterQueries
     * @return
     * @throws Exception
     */
    public Map<String, Integer> getOccurrenceCountsForTaxa(List<String> taxa, String[] filterQueries) throws Exception {
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.setFacetLimit(taxa.size());
        if(filterQueries != null && filterQueries.length > 0) {
            solrQuery.setFilterQueries(filterQueries);
        }
        StringBuilder sb = new StringBuilder();
        Map<String,Integer> counts = new HashMap<String,Integer>();
        Map<String, String> lftToGuid = new HashMap<String,String>();
        for(String lsid : taxa){
            //get the lft and rgt value for the taxon
            String[] values = searchUtils.getTaxonSearch(lsid);
            //first value is the search string
            if(sb.length() > 0) {
                sb.append(" OR ");
            }
            sb.append(values[0]);
            lftToGuid.put(values[0], lsid);
            //add the query part as a facet
            solrQuery.add("facet.query", values[0]);
        }
        solrQuery.setQuery(sb.toString());

        //solrQuery.add("facet.query", "confidence:" + os.getRange());
        QueryResponse qr = runSolrQuery(solrQuery, null, 1, 0, "score", "asc");
        Map<String, Integer> facetQueries = qr.getFacetQuery();
        for(String facet:facetQueries.keySet()){
            //add all the counts based on the query value that was substituted
            String lsid = lftToGuid.get(facet);
            Integer count = facetQueries.get(facet);
            if(lsid != null && count!= null)
                counts.put(lsid,  count);
        }
        logger.debug(facetQueries);
        return counts;
    }

    /**
     * @return the maxEndemicQueryThreads for endemic queries
     */
    public Integer getMaxEndemicQueryThreads() {
        return maxEndemicQueryThreads;
    }

    /**
     * @param maxEndemicQueryThreads the maxEndemicQueryThreads to set for endemic queries
     */
    public void setMaxEndemicQueryThreads(Integer maxEndemicQueryThreads) {
        this.maxEndemicQueryThreads = Objects.requireNonNull(maxEndemicQueryThreads, "Max endemic multipart threads cannot be null");
    }

    /**
     * @return the maxSolrDownloadThreads for solr download queries
     */
    public Integer getMaxSolrOnlineDownloadThreads() {
        return maxSolrDownloadThreads;
    }

    /**
     * @param maxSolrDownloadThreads the maxSolrDownloadThreads to set for solr download queries
     */
    public void setMaxSolrDownloadThreads(Integer maxSolrDownloadThreads) {
        this.maxSolrDownloadThreads = Objects.requireNonNull(maxSolrDownloadThreads, "Max solr download threads cannot be null");
    }

    /**
     * @return the throttle
     */
    public Integer getThrottle() {
        return throttle;
    }

    /**
     * @param throttle the throttle to set
     */
    public void setThrottle(Integer throttle) {
        this.throttle = Objects.requireNonNull(throttle, "Throttle cannot be null");
    }

    private QueryResponse query(SolrParams query, SolrRequest.METHOD queryMethod) throws SolrServerException {
        int retry = 0;

        QueryResponse qr = null;
        while (retry < maxRetries && qr == null) {
            retry++;
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("SOLR query:" + query.toString());
                }

                qr = solrClient.query(query, queryMethod == null ? this.queryMethod : queryMethod); // can throw exception
            } catch (SolrServerException e) {
                //want to retry IOException and Proxy Error
                if (retry < maxRetries && (e.getMessage().contains("IOException") || e.getMessage().contains("Proxy Error"))) {
                    if (retryWait > 0) {
                        try {
                            Thread.sleep(retryWait);
                        } catch (InterruptedException ex) {
                            // If the Thread sleep is interrupted, we shouldn't attempt to continue
                            Thread.currentThread().interrupt();
                            throw e;
                        }
                    }
                } else {
                    //throw all other errors
                    throw e;
                }
            } catch (HttpSolrClient.RemoteSolrException e) {
                //report failed query
                logger.error("query failed: " + query.toString() + " : " + e.getMessage());
                throw e;
            } catch (IOException ioe) {
                //report failed query
                logger.error("query failed: " + query.toString() + " : " + ioe.getMessage());
                throw new SolrServerException(ioe);
            } catch (Exception ioe) {
                //report failed query
                logger.error("query failed: " + query.toString() + " : " + ioe.getMessage());
                throw new SolrServerException(ioe);
            }
        }

        return qr;
    }

    /**
     * Get SOLR max boolean clauses.
     *
     * @return
     */
    public int getMaxBooleanClauses() {
        if (maxBooleanClauses == 0) {
            SolrQuery solrQuery = new SolrQuery();
            solrQuery.setFacet(false);
            solrQuery.setRows(0);

            int value = 1024;
            boolean ok = false;
            int step = -1;
            while (step != 0 || ok == false) {
                String q = 1 + StringUtils.repeat(" AND 1", value - 1);
                solrQuery.setQuery(q);
                try {
                    query(solrQuery, queryMethod);  //throws exception when too many boolean clauses
                    if (step == -1) value *= 2;  //push upper limit
                    else step /= 2;
                    ok = true;
                } catch (Exception e) {
                    if (step == -1) step = value / 2;  //set initial step value
                    else if (ok == false && step > 1) step /= 2;
                    ok = false;
                }
                if (step != -1) {
                    if (ok) value += step;
                    else value -= step;
                }
            }
            maxBooleanClauses = value;
        }

        queryFormatUtils.setMaxBooleanClauses(maxBooleanClauses);

        return maxBooleanClauses;
    }

    /**
     * Get the SOLR index version. Trigger a background refresh on a timeout.
     * <p>
     * Forcing an updated value will perform a new SOLR query for each request to be run in the foreground.
     *
     * @param force
     * @return
     */
    public Long getIndexVersion(Boolean force) {
        Thread t = null;
        synchronized (solrIndexVersionLock) {
            boolean immediately = solrIndexVersionTime == 0;

            if (force || solrIndexVersionTime < System.currentTimeMillis() - solrIndexVersionRefreshTime) {
                solrIndexVersionTime = System.currentTimeMillis();

                t = new Thread() {
                    @Override
                    public void run() {
                        try {
                            getIndexFieldDetails(null);
                        } catch (Exception e) {
                            logger.error("Failed to update solrIndexVersion", e);
                        }
                    }
                };

                if (immediately) {
                    //wait with lock
                    t.start();
                    try {
                        t.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.error("Failed to update solrIndexVersion", e);
                    }
                } else if (!force) {
                    //run in background
                    t.start();
                }
            }
        }

        if (force && t != null) {
            //wait without lock
            t.start();
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Failed to update solrIndexVersion", e);
            }
        }

        return solrIndexVersion;
    }

    /**
     * Perform grouped facet query.
     * <p>
     * facets is the list of grouped facets required
     * flimit restricts the number of groups returned
     * pageSize restricts the number of docs in each group returned
     * fl is the list of fields in the returned docs
     */
    public List<GroupFacetResultDTO> searchGroupedFacets(SpatialSearchRequestParams searchParams) throws Exception {
        queryFormatUtils.formatSearchQuery(searchParams);
        String queryString = searchParams.getFormattedQuery();
        searchParams.setFacet(false);

        //get facet group counts
        SolrQuery query = initSolrQuery(searchParams, false, null);
        query.setQuery(queryString);
        query.setFields(null);
        //now use the supplied facets to add groups to the query
        query.add("group", "true");
        query.add("group.ngroups", "true");
        query.add("group.limit", String.valueOf(searchParams.getPageSize()));
        query.setRows(searchParams.getFlimit());
        query.setFields(searchParams.getFl());
        for (String facet : searchParams.getFacets()) {
            query.add("group.field", facet);
        }
        QueryResponse response = runSolrQuery(query, searchParams);
        GroupResponse groupResponse = response.getGroupResponse();

        List<GroupFacetResultDTO> output = new ArrayList();
        for (GroupCommand gc : groupResponse.getValues()) {
            List<GroupFieldResultDTO> list = new ArrayList<GroupFieldResultDTO>();

            String facet = gc.getName();
            for (Group v : gc.getValues()) {
                List<OccurrenceIndex> docs = (new DocumentObjectBinder()).getBeans(OccurrenceIndex.class, v.getResult());

                //build facet displayName and fq
                String value = v.getGroupValue();
                Long count = v.getResult() != null ? v.getResult().getNumFound() : 0L;
                if (value == null) {
                    list.add(new GroupFieldResultDTO("", count, "-" + facet + ":*", docs));
                } else {
                    list.add(new GroupFieldResultDTO(getFacetValueDisplayName(facet, value), count, facet + ":\"" + value + "\"", docs));
                }
            }

            output.add(new GroupFacetResultDTO(gc.getName(), list, gc.getNGroups()));
        }

        return output;
    }

    /**
     * Generates a FQ value for use in the returning query response.
     *
     * @param facet
     * @param value
     * @return
     */
    String getFormattedFqQuery(String facet, String value) {
        if (facet.equals(OCCURRENCE_YEAR_INDEX_FIELD)) {

            if (value.equals(DECADE_PRE_1850_LABEL)) {
                return facet + ":" + "[* TO " + DECADE_FACET_START_DATE + "]";
            } else {
                SimpleDateFormat sdf = new SimpleDateFormat(SOLR_DATE_FORMAT);
                try {
                    Date date = sdf.parse(value);
                    Date endDate = DateUtils.addYears(date, 10);
                    endDate = DateUtils.addMilliseconds(endDate, -1);
                    return facet + ":" + "[" + value + " TO " + sdf.format(endDate) + "]";
                } catch (ParseException e) {
                    //do nothing
                }
            }
        }

        return facet + ":\"" + value + "\"";
    }

    /**
     * Formats the facet value using i18n where possible.
     * Special logic here for formating decades.
     *
     * @param facet
     * @param value
     * @return
     */
    String getFacetValueDisplayName(String facet, String value) {
        if (facet.endsWith("_uid")) {
            return searchUtils.getUidDisplayString(facet, value, false);
        } else if ("occurrence_year".equals(facet) && value != null) {
            try {
                if (DECADE_PRE_1850_LABEL.equals(value)) {
                    return messageSource.getMessage("decade.pre.start", null, "pre 1850", null); // "pre 1850";
                }
                SimpleDateFormat sdf = new SimpleDateFormat(SOLR_DATE_FORMAT);
                Date date = sdf.parse(value);
                SimpleDateFormat df = new SimpleDateFormat("yyyy");
                String year = df.format(date);
                return year + "-" + (Integer.parseInt(year) + 9);
            } catch (ParseException pe) {
                return facet;
            }
            //1850-01-01T00:00:00Z
        } else if (searchUtils.getAuthIndexFields().contains(facet)) {
            //if the facet field is collector or assertion_user_id we need to perform the substitution
            return authService.getDisplayNameFor(value);
        } else {
            if (messageSource != null) {

                if(StringUtils.isNotBlank(value)) {
                    return messageSource.getMessage(
                            facet + "." + value,
                            null,
                            value,
                            (Locale) null);
                } else {
                    return messageSource.getMessage(
                            facet + ".novalue",
                            null,
                            "Not supplied",
                            (Locale) null);
                }
            } else {
                return value;
            }
        }
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#searchPivot(au.org.ala.biocache.dto.SpatialSearchRequestParams)
     */
    public List<FacetPivotResultDTO> searchPivot(SpatialSearchRequestParams searchParams) throws Exception {
        String pivot = StringUtils.join(searchParams.getFacets(), ",");
        searchParams.setFacets(new String[]{});

        queryFormatUtils.formatSearchQuery(searchParams);
        String queryString = searchParams.getFormattedQuery();
        searchParams.setFacet(true);

        //get facet group counts
        SolrQuery query = initSolrQuery(searchParams, false, null);
        query.setQuery(queryString);
        query.setFields(null);
        //now use the supplied facets to add groups to the query
        query.add("facet.pivot", pivot);
        query.add("facet.pivot.mincount", "1");
        query.add("facet.missing", "true");
        query.setRows(0);
        searchParams.setPageSize(0);
        QueryResponse response = runSolrQuery(query, searchParams);
        NamedList<List<PivotField>> result = response.getFacetPivot();

        List<FacetPivotResultDTO> output = new ArrayList();
        for (Entry<String, List<PivotField>> pfl : result) {
            List<PivotField> list = pfl.getValue();
            if (list != null && list.size() > 0) {
                output.add(new FacetPivotResultDTO(
                        list.get(0).getField(),
                        getFacetPivotResults(list),
                        null,
                        (int) response.getResults().getNumFound())
                );
            }

            //should only be one result
            break;
        }

        return output;
    }

    /**
     * Read nested pivot results.
     *
     * @param pfl
     * @return
     */
    private List<FacetPivotResultDTO> getFacetPivotResults(List<PivotField> pfl) {
        if (pfl == null || pfl.size() == 0) {
            return null;
        }

        List<FacetPivotResultDTO> list = new ArrayList<FacetPivotResultDTO>();
        for (PivotField pf : pfl) {
            String value = pf.getValue() != null ? pf.getValue().toString() : null;
            if (pf.getPivot() == null || pf.getPivot().size() == 0) {
                list.add(new FacetPivotResultDTO(null, null, value, pf.getCount()));
            } else {
                list.add(new FacetPivotResultDTO(pf.getPivot().get(0).getField(), getFacetPivotResults(pf.getPivot()), value, pf.getCount()));
            }
        }

        return list;
    }

    public StringBuilder getAllQAFields() {
        //include all assertions
        StringBuilder qasb = new StringBuilder();
        ErrorCode[] errorCodes = Store.retrieveAssertionCodes();
        Arrays.sort(errorCodes, new Comparator<ErrorCode>() {
            @Override
            public int compare(ErrorCode o1, ErrorCode o2) {
                return o1.getName().compareToIgnoreCase(o2.getName());
            }
        });
        for (ErrorCode assertionCode : errorCodes) {
            if (qasb.length() > 0)
                qasb.append(",");
            qasb.append(assertionCode.getName());
        }

        return qasb;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#searchStat
     */
    public List<FieldStatsItem> searchStat(SpatialSearchRequestParams searchParams, String field, String facet) throws Exception {
        searchParams.setFacets(new String[]{});

        queryFormatUtils.formatSearchQuery(searchParams);
        String queryString = searchParams.getFormattedQuery();

        if (facet != null) searchParams.setFacet(true);

        //get facet group counts
        SolrQuery query = initSolrQuery(searchParams, false, null);
        query.setQuery(queryString);
        query.setFields(null);
        //query.setFacetLimit(-1);

        //stats parameters
        query.add("stats", "true");
        if (facet != null) query.add("stats.facet", facet);
        query.add("stats.field", field);

        query.setRows(0);
        searchParams.setPageSize(0);
        QueryResponse response = runSolrQuery(query, searchParams);

        List<FieldStatsItem> output = new ArrayList();
        if (facet != null && response.getFieldStatsInfo().size() > 0) {
            for (FieldStatsInfo f : response.getFieldStatsInfo().values().iterator().next().getFacets().values().iterator().next()) {
                FieldStatsItem item = new FieldStatsItem(f);
                if (f.getName() == null) {
                    item.setFq("-" + facet + ":*");
                } else {
                    item.setFq(facet + ":\"" + f.getName() + "\"");
                }
                item.setLabel(f.getName());
                output.add(item);
            }
        } else {
            if (response.getFieldStatsInfo().size() > 0) {
                output.add(new FieldStatsItem(response.getFieldStatsInfo().values().iterator().next()));
            }
        }

        return output;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#getColours
     */
    @Cacheable(cacheName = "getColours")
    public List<LegendItem> getColours(SpatialSearchRequestParams request, String colourMode) throws Exception {
        List<LegendItem> colours = new ArrayList<LegendItem>();
        if (colourMode.equals("grid")) {
            for (int i = 0; i <= 500; i += 100) {
                LegendItem li;
                if (i == 0) {
                    li = new LegendItem(">0", "",0, null);
                } else {
                    li = new LegendItem(String.valueOf(i),  "",0, null);
                }
                li.setColour((((500 - i) / 2) << 8) | 0x00FF0000);
                colours.add(li);
            }
        } else {
            SpatialSearchRequestParams requestParams = new SpatialSearchRequestParams();
            requestParams.setFormattedQuery(request.getFormattedQuery());
            requestParams.setWkt(request.getWkt());
            requestParams.setRadius(request.getRadius());
            requestParams.setLat(request.getLat());
            requestParams.setLon(request.getLon());
            requestParams.setQ(request.getQ());
            requestParams.setQc(request.getQc());
            requestParams.setFq(qidCacheDao.getFq(request));
            requestParams.setFoffset(-1);

            //test for cutpoints on the back of colourMode
            String[] s = colourMode.split(",");
            String[] cutpoints = null;
            if (s.length > 1) {
                cutpoints = new String[s.length - 1];
                System.arraycopy(s, 1, cutpoints, 0, cutpoints.length);
            }
            if (s[0].equals("-1") || s[0].equals("grid")) {
                return null;
            } else {
                List<LegendItem> legend = getLegend(requestParams, s[0], cutpoints);

                if (cutpoints == null) {     //do not sort if cutpoints are provided
                    java.util.Collections.sort(legend);
                }
                int i = 0;
                int offset = 0;
                for (i = 0; i < legend.size() && i < ColorUtil.colourList.length - 1; i++) {

                    LegendItem li = legend.get(i);

                    colours.add(new LegendItem(li.getName(), li.getName(), li.getCount(), li.getFq()));
                    int colour = DEFAULT_COLOUR;
                    if (cutpoints == null) {
                        colour = ColorUtil.colourList[i];
                    } else if (cutpoints != null && i - offset < cutpoints.length) {
                        if (StringUtils.isEmpty(legend.get(i).getName()) || legend.get(i).getName().equals("Unknown") || legend.get(i).getName().startsWith("-")) {
                            offset++;
                        } else {
                            colour = ColorUtil.getRangedColour(i - offset, cutpoints.length / 2);
                        }
                    }
                    colours.get(colours.size() - 1).setColour(colour);
                }
            }
        }

        return colours;
    }

    /**
     * Get bounding box for a query.
     *
     * @param requestParams
     * @return
     * @throws Exception
     */
    public double[] getBBox(SpatialSearchRequestParams requestParams) throws Exception {
        double[] bbox = new double[4];
        String[] sort = {"longitude", "latitude", "longitude", "latitude"};
        String[] dir = {"asc", "asc", "desc", "desc"};

        //Filter for -180 +180 longitude and -90 +90 latitude to match WMS request bounds.
        String [] bounds = new String[]{"longitude:[-180 TO 180]", "latitude:[-90 TO 90]"};

        queryFormatUtils.addFqs(bounds, requestParams);

        requestParams.setFq(requestParams.getFq());
        requestParams.setPageSize(10);

        for (int i = 0; i < sort.length; i++) {
            requestParams.setSort(sort[i]);
            requestParams.setDir(dir[i]);
            requestParams.setFl(sort[i]);

            SolrDocumentList sdl = findByFulltext(requestParams);
            if (sdl != null && sdl.size() > 0) {
                if (sdl.get(0) != null) {
                    bbox[i] = (Double) sdl.get(0).getFieldValue(sort[i]);
                } else {
                    logger.error("searchDAO.findByFulltext returning SolrDocumentList with null records");
                }
            }
        }
        return bbox;
    }

    @Override
    public List<String> listFacets(SpatialSearchRequestParams searchParams) throws Exception {
        queryFormatUtils.formatSearchQuery(searchParams);
        searchParams.setFacet(true);
        searchParams.setFacets(new String[]{});

        String queryString = searchParams.getFormattedQuery();
        SolrQuery solrQuery = initSolrQuery(searchParams, false, null); // general search settings
        solrQuery.setQuery(queryString);
        solrQuery.setFacetLimit(-1);
        solrQuery.setRows(0);

        if (searchParams.getFormattedFq() != null) {
            for (String fq : searchParams.getFormattedFq()) {
                if (StringUtils.isNotEmpty(fq)) {
                    solrQuery.addFilterQuery(fq);
                }
            }
        }

        ArrayList<String> found = new ArrayList<>();

        for (IndexFieldDTO s : indexFields) {
            // this only works for non-tri fields
            if (!s.getDataType().startsWith("t")) {
                solrQuery.set("facet.field", "{!facet.method=enum facet.exists=true}" + s.getName());

                QueryResponse qr = query(solrQuery, queryMethod); // can throw exception

                for (FacetField f : qr.getFacetFields()) {
                    if (!f.getValues().isEmpty()) {
                        found.add(f.getName());
                    }
                }
            }
        }

        return found;
    }
}