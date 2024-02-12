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
import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.service.*;
import au.org.ala.biocache.stream.*;
import au.org.ala.biocache.util.*;
import au.org.ala.biocache.util.solr.FieldMappingUtil;
import au.org.ala.biocache.writer.CSVRecordWriter;
import au.org.ala.biocache.writer.RecordWriterError;
import au.org.ala.biocache.writer.TSVRecordWriter;
import au.org.ala.ws.security.profile.AlaUserProfile;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.RangeFacet.Numeric;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.slf4j.MDC;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
//import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static au.org.ala.biocache.dto.OccurrenceIndex.*;

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
    public static final String SOLR_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    /**
     * SOLR client instance
     */
    @Inject
    protected IndexDAO indexDao;

    @Inject
    protected OccurrenceUtils occurrenceUtils;

    /**
     * Limit search results - for performance reasons
     */
    @Value("${download.max:500000}")
    public Integer MAX_DOWNLOAD_SIZE = 500000;

    /**
     * The threshold to use the export handler instead of search handler when streaming from SOLR.
     */
    @Value("${solr.export.handler.threshold:10000}")
    public Integer EXPORT_THRESHOLD = 10000;

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

    /**
     * List of fields that are stored, have no DwC class, and must be excluded from downloads when includeMisc=true
     */
    @Value("${download.excluded.misc.fields:_root_,geospatialIssues,assertions,geohash,label,lat_long,lft,names_and_lsid,nick,null,packedQuad,point-0.0001,point-0.001,point-0.01,point-0.02,point-0.1,point-1,quad,rgt,aust_conservation,state_conservation,species_group,species_subgroup}")
    protected String downloadExcludedMiscFields;

    protected Pattern clpField = Pattern.compile("(,|^)cl.p(,|$)");
    protected Pattern elpField = Pattern.compile("(,|^)el.p(,|$)");
    protected Pattern allDwcField = Pattern.compile("(,|^)allDwc(,|$)");

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
    public FieldMappingUtil fieldMappingUtil;

    @Inject
    public CollectionsCache collectionCache;

    @Inject
    public AbstractMessageSource messageSource;

    @Inject
    public SpeciesLookupService speciesLookupService;

    @Inject
    protected AuthService authService;

    @Inject
    protected LayersService layersService;

    @Inject
    protected RangeBasedFacets rangeBasedFacets;

    @Inject
    protected SpeciesCountsService speciesCountsService;

    @Inject
    protected SpeciesImageService speciesImageService;

    @Inject
    public ListsService listsService;

    @Inject
    protected DownloadService downloadService;

    @Value("${media.store.local:true}")
    protected Boolean usingLocalMediaRepo = true;

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
     * Occurrence count where < uses pivot and > uses facet for retrieving points. Can be fine tuned with
     * multiple queries and comparing DEBUG *
     */
    @Value("${wms.legendMaxItems:30}")
    private int wmslegendMaxItems;

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

    @Value("${media.url:https://biocache.ala.org.au/biocache-media/}")
    public String biocacheMediaUrl = "https://biocache.ala.org.au/biocache-media/";

    /**
     * A list of fields that are left in the index for legacy reasons, but are removed from the public API to avoid confusion.
     */
    @Value("${index.fields.tohide:collector_text,location_determined,row_key,matched_name,decimal_latitudelatitude,collectors,default_values_used,generalisation_to_apply_in_metres,geohash,ibra_subregion,identifier_by,occurrence_details,text,photo_page_url,photographer,places,portal_id,quad,rem_text,occurrence_status_s,identification_qualifier_s}")
    protected String indexFieldsToHide;

    @Value("${default.download.fields:id,dataResourceUid,dataResourceName,license,catalogNumber,taxonConceptID,raw_scientificName,raw_vernacularName,scientificName,taxonRank,vernacularName,kingdom,phylum,class,order,family,genus,species,subspecies,institutionCode,collectionCode,raw_locality,raw_decimalLatitude,raw_decimalLongitude,raw_geodeticDatum,decimalLatitude,decimalLongitude,coordinatePrecision,coordinateUncertaintyInMeters,country,stateProvince,cl959,minimumElevationInMeters,maximumElevationInMeters,minimumDepthInMeters,maximumDepthInMeters,individualCount,recordedBy,year,month,day,verbatimEventDate,basisOfRecord,raw_basisOfRecord,sex,preparations,informationWithheld,dataGeneralizations,outlierLayer}")
    protected String defaultDownloadFields;

    @Value("${wms.colour:0x00000000}")
    protected int DEFAULT_COLOUR;

    @Value("${dwc.url:http://rs.tdwg.org/dwc/terms/}")
    protected String dwcUrl = "http://rs.tdwg.org/dwc/terms/";

    /**
     * max.boolean.clauses is automatically determined when set to 0
     */
    @Value("${max.boolean.clauses:1024}")
    private int maxBooleanClauses;

    @Value("${layers.service.url:https://spatial.ala.org.au/ws}")
    protected String layersServiceUrl;

    @Value("${solr.collection:biocache1}")
    protected String solrCollection;

    public CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * Initialise the SOLR server instance
     */
    public SearchDAOImpl() {
    }

    @PostConstruct
    public void init() throws Exception {

        logger.debug("Initialising SearchDAOImpl");

        // TODO: There was a note about possible issues with the following two lines
        Set<IndexFieldDTO> indexedFields = indexDao.getIndexedFields(true);
        indexDao.getSchemaFields(true);

        if (downloadFields == null) {
            downloadFields = new DownloadFields(fieldMappingUtil, indexedFields, messageSource, layersService, listsService);
        } else {
            downloadFields.update(indexedFields);
        }

        getMaxBooleanClauses();

        initSensitiveFieldMapping();

        countDownLatch.countDown();
    }

    @Override
    public boolean isInitialized() throws InterruptedException {
        countDownLatch.await();
        return true;
    }

    public void refreshCaches() {

        try {
            init(); // In the past the call internally logged the exception but the caller was unaware of any issues
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        collectionCache.updateCache();
        try {
            //update indexed fields
            downloadFields.update(indexDao.getIndexedFields(true));
        } catch (Exception e) {
            logger.error("Unable to refresh cache.", e);
        }
        speciesImageService.resetCache();
        speciesCountsService.resetCache();

        listsService.refreshCache();
        layersService.refreshCache();
    }

    /**
     * (Endemic)
     * <p>
     * Returns a list of species that are only within a subQuery.
     * <p>
     * The subQuery is a subset of parentQuery
     * e.g. subQuery is the area of interest. parentQuery is all species.
     */
    public List<FieldResultDTO> getSubquerySpeciesOnly(SpatialSearchRequestDTO subQuery, SpatialSearchRequestDTO parentQuery) throws Exception {
        SolrQuery subset = initSolrQuery(subQuery, false, null);
        SolrQuery superset = initSolrQuery(parentQuery, false, null);

        List<FieldResultDTO> output = new ArrayList();
        indexDao.streamingQuery(subset, null, new EndemicFacet(output, subQuery.getFacets()[0]), superset);

        return output;
    }

    public void writeEndemicFacetToStream(SpatialSearchRequestDTO subQuery, SpatialSearchRequestDTO parentQuery, boolean includeCount, boolean lookupName, boolean includeSynonyms, boolean includeLists, OutputStream out) throws Exception {
        List<FieldResultDTO> list = getSubquerySpeciesOnly(subQuery, parentQuery);
        String facet = parentQuery.getFacets()[0];

        // shouldLookup is valid for 1.0 and 2.0 SOLR schema
        boolean shouldLookup = lookupName && (facet.contains("_guid") || facet.contains("_lsid") || facet.endsWith("ID"));

        String[] header = new String[]{facet};
        if (shouldLookup) {
            header = speciesLookupService.getHeaderDetails(fieldMappingUtil.translateFieldName(facet), includeCount, includeSynonyms);
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
    public List<FieldResultDTO> getValuesForFacet(SpatialSearchRequestDTO requestParams) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        writeFacetToStream(requestParams, true, false, false, false, outputStream, null);
        outputStream.flush();
        outputStream.close();
        CSVReader csvReader = new CSVReader(new StringReader(outputStream.toString(StandardCharsets.UTF_8)));
        List<FieldResultDTO> list = new ArrayList<FieldResultDTO>();
        boolean first = true;
        String[] line;
        while ((line = csvReader.readNext()) != null) {
            if (first) {
                first = false;
            } else if (line.length == 2) {
                String name = line[0];
                list.add(
                        new FieldResultDTO(
                                name,
                                name,
                                Long.parseLong(line[1])
                        )
                );
            }
        }
        return list;
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
    public SearchResultDTO findByFulltextSpatialQuery(SpatialSearchRequestDTO searchParams,
                                                      boolean includeSensitive, Map<String, String[]> extraParams) throws Exception {
        SearchResultDTO searchResults = new SearchResultDTO();
        SpatialSearchRequestDTO original = new SpatialSearchRequestDTO();
        BeanUtils.copyProperties(searchParams, original);
        Map[] fqMaps = queryFormatUtils.formatSearchQuery(searchParams, true);
        SolrQuery solrQuery = initSolrQuery(searchParams, true, extraParams); // general search settings

        QueryResponse qr = indexDao.runSolrQuery(solrQuery);

        //need to set the original q to the processed value so that we remove the wkt etc that is added from paramcache object
        Class resultClass = includeSensitive ? au.org.ala.biocache.dto.SensitiveOccurrenceIndex.class : OccurrenceIndex.class;

        searchResults = processSolrResponse(original, qr, solrQuery, resultClass);
        searchResults.setQueryTitle(searchParams.getDisplayString());
        searchResults.setUrlParameters(original.getUrlParams());

        //now update the fq display map...
        searchResults.setActiveFacetMap(fqMaps[0]);
        searchResults.setActiveFacetObj(fqMaps[1]);

        if (logger.isDebugEnabled()) {
            logger.debug("spatial search query: " + solrQuery.toQueryString());
        }

        return searchResults;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#writeSpeciesCountByCircleToStream(SpatialSearchRequestDTO, String, javax.servlet.ServletOutputStream)
     */
    public int writeSpeciesCountByCircleToStream(SpatialSearchRequestDTO searchParams, String speciesGroup, ServletOutputStream out) throws Exception {

        //get the species counts:
        if (logger.isDebugEnabled()) {
            logger.debug("Writing CSV file for species count by circle");
        }
        searchParams.setFlimit(-1);

        getSpeciesCountsTSVCircle(searchParams, out);

        return 0;
    }

    /**
     * Writes the values for the first supplied facet to output stream
     *
     * @param includeCount true when the count should be included in the download
     * @param lookupName   true when a name lsid should be looked up in the bie
     *                     <p>
     *                     TODO: use streaming service instead of paging the SOLR facet request
     */
    public void writeFacetToStream(SpatialSearchRequestDTO searchParams, boolean includeCount, boolean lookupName, boolean includeSynonyms, boolean includeLists, OutputStream out, DownloadDetailsDTO dd) throws Exception {
        //set to unlimited facets
        searchParams.setFlimit(-1);

        SolrQuery solrQuery = initSolrQuery(searchParams, false, null);

        //don't want any results returned
        solrQuery.setRows(0);
        solrQuery.setFacetLimit(searchParams.getFlimit());

        if (dd != null) {
            dd.resetCounts();
        }

        StreamFacet procFacet = new StreamFacet(this, dd, searchParams, lookupName, includeCount, includeSynonyms, includeLists, 0, out);
        indexDao.streamingQuery(solrQuery, null, procFacet, null);
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
    public void writeTaxonDetailsToStream(List<String> guids, List<Long> counts, boolean includeCounts, boolean includeSynonyms, boolean includeLists, CSVRecordWriter writer) throws Exception {
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
    @Deprecated
    public void writeCoordinatesToStream(SpatialSearchRequestDTO searchParams, OutputStream out) throws Exception {
        SolrQuery solrQuery = initSolrQuery(searchParams, false, null);

        //We want all the facets so we can dump all the coordinates
        solrQuery.setFacetLimit(-1);
        solrQuery.setFacetSort("count");
        solrQuery.setRows(0);
        solrQuery.setQuery(searchParams.getQ());

        QueryResponse qr = indexDao.runSolrQuery(solrQuery);
        if (qr.getResults().size() > 0) {
            FacetField ff = qr.getFacetField(searchParams.getFacets()[0]);
            if (ff != null && ff.getValueCount() > 0) {
                out.write("latitude,longitude\n".getBytes(StandardCharsets.UTF_8));
                //write the facets to file
                for (FacetField.Count value : ff.getValues()) {
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
     * @param dd               The details of the download
     * @param checkLimit
     * @param nextExecutor     The ExecutorService to use to process results on different threads
     * @throws Exception
     */
    @Override
    public DownloadHeaders writeResultsFromIndexToStream(final DownloadRequestDTO downloadParams,
                                                         final OutputStream out,
                                                         final DownloadStats downloadStats,
                                                         final DownloadDetailsDTO dd,
                                                         boolean checkLimit,
                                                         ExecutorService nextExecutor) throws Exception {
        if (downloadFields == null) {
            // PostConstruct not finished
            throw new Exception("PostConstruct not finished, downloadFields==null");
        }

        // reset counts when retrying a download
        dd.resetCounts();

        // prepare requested download fields (defaults, substitutions)
        boolean hasSensitiveRecordAccess = downloadService.getSensitiveFq(dd.getAlaUser() == null ? Collections.emptySet() : dd.getAlaUser().getRoles()) != null;
        prepareRequestedFields(downloadParams, hasSensitiveRecordAccess);

        // prepare headers
        DownloadHeaders downloadHeaders = prepareHeaders(downloadParams);

        // create writer
        RecordWriter recordWriter = createRecordWriter(downloadParams, downloadHeaders, out);

        // submit download to executor
        if (nextExecutor != null) {
            // TODO: remove when deprecated services are removed: /occurrences/download and /occurrences/download/batchFile
            Future future = nextExecutor.submit(prepareDownloadRunner(downloadParams, downloadHeaders, dd, downloadStats, recordWriter));

            // wait for download to finish
            // Busy wait because we need to be able to respond to an interrupt on any callable
            // and propagate it to all of the others for this particular query
            // Because the executor service is shared to prevent too many concurrent threads being run,
            // this requires a busy wait loop on the main thread to monitor state
            boolean waitAgain = false;
            do {
                waitAgain = false;
                if (!future.isDone()) {
                    // Wait again even if an interrupt flag is set, as it may have been set partway through the iteration
                    // The calls to future.cancel will occur next time if the interrupt is setup partway through an iteration
                    waitAgain = true;
                }

                if (waitAgain) {
                    Thread.sleep(downloadCheckBusyWaitSleep);
                }
            } while (waitAgain);
        } else {
            // This is already running in an executor
            prepareDownloadRunner(downloadParams, downloadHeaders, dd, downloadStats, recordWriter).call();
        }



        // close writer
        recordWriter.finalise();

        return downloadHeaders;
    }

    private RecordWriter createRecordWriter(DownloadRequestDTO downloadParams, DownloadHeaders downloadHeaders, OutputStream out) {
        RecordWriterError recordWriter = downloadParams.getFileType().equals("csv") ?
                new CSVRecordWriter(out, downloadHeaders.joinedHeader(), downloadParams.getSep(), downloadParams.getEsc()) :
                new TSVRecordWriter(out, downloadHeaders.joinedHeader());

        recordWriter.initialise();

        return recordWriter;
    }

    private Callable prepareDownloadRunner(DownloadRequestDTO downloadParams, DownloadHeaders downloadHeaders,
                                           DownloadDetailsDTO dd, DownloadStats downloadStats,
                                           RecordWriter recordWriter) throws QidMissingException {
        queryFormatUtils.formatSearchQuery(downloadParams);

        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery(downloadParams.getFormattedQuery());
        solrQuery.setFilterQueries(downloadParams.getFormattedFq());
        solrQuery.setRows(-1);
        solrQuery.setStart(0);
        String sensitiveFq = downloadService.getSensitiveFq(dd.getAlaUser() == null ? Collections.emptySet() : dd.getAlaUser().getRoles());
        // Split into sensitive and non-sensitive queries when
        // - not including all sensitive values
        // - there is a sensitive fq
        List<SolrQuery> queries = new ArrayList<>();
        if (sensitiveFq != null) {
            queries.addAll(splitQueries(solrQuery, sensitiveFq, downloadHeaders.included,
                    Arrays.stream(downloadHeaders.included).filter(field -> {
                        return !ArrayUtils.contains(sensitiveSOLRHdr, field);
                    }).collect(Collectors.toList()).toArray(new String[0])));
        } else {
            solrQuery.setFields(Arrays.stream(downloadHeaders.included).filter(field -> {
                return !ArrayUtils.contains(sensitiveSOLRHdr, field);
            }).collect(Collectors.toList()).toArray(new String[0]));
            queries.add(solrQuery);
        }

        ProcessDownload procDownload = new ProcessDownload(downloadStats, downloadHeaders, recordWriter, dd,
                checkDownloadLimits, downloadService.dowloadOfflineMaxSize,
                listsService, layersService);

        return new DownloadCallable(queries, indexDao, procDownload);
    }


    Map<String, String[]> sensitiveFieldMapping = new HashMap();

    private void initSensitiveFieldMapping() {

        sensitiveFieldMapping.put("longitude", new String[]{"sensitive_decimalLongitude"});
        sensitiveFieldMapping.put("decimalLongitude", new String[]{"sensitive_decimalLongitude"});
        sensitiveFieldMapping.put("latitude", new String[]{"sensitive_decimalLatitude"});
        sensitiveFieldMapping.put("decimalLatitude", new String[]{"sensitive_decimalLatitude"});

        sensitiveFieldMapping.put("locality", new String[]{"sensitive_locality"});

        sensitiveFieldMapping.put("footprint_wkt", new String[]{"sensitive_footprintWKT"});
        sensitiveFieldMapping.put("footprintWKT", new String[]{"sensitive_footprintWKT"});

        sensitiveFieldMapping.put("location_remarks", new String[]{"sensitive_locationRemarks"});
        sensitiveFieldMapping.put("locationRemarks", new String[]{"sensitive_locationRemarks"});

        sensitiveFieldMapping.put("verbatim_coordinates", new String[]{"sensitive_verbatimCoordinates"});
        sensitiveFieldMapping.put("verbatimCoordinates", new String[]{"sensitive_verbatimCoordinates"});

        sensitiveFieldMapping.put("verbatim_latitude", new String[]{"sensitive_verbatimLatitude"});
        sensitiveFieldMapping.put("verbatimLatitude", new String[]{"sensitive_verbatimLatitude"});

        sensitiveFieldMapping.put("verbatim_locality", new String[]{"sensitive_verbatimLocality"});
        sensitiveFieldMapping.put("verbatimLocality", new String[]{"sensitive_verbatimLocality"});

        sensitiveFieldMapping.put("verbatim_longitude", new String[]{"sensitive_verbatimLongitude"});
        sensitiveFieldMapping.put("verbatimLongitude", new String[]{"sensitive_verbatimLongitude"});

        sensitiveFieldMapping.put("day", new String[]{"sensitive_day"});

        sensitiveFieldMapping.put("occurrence_date", new String[]{"sensitive_eventDate"});
        sensitiveFieldMapping.put("eventDate", new String[]{"sensitive_eventDate"});

        sensitiveFieldMapping.put("event_id", new String[]{"sensitive_eventID"});
        sensitiveFieldMapping.put("eventID", new String[]{"sensitive_eventID"});

        sensitiveFieldMapping.put("event_time", new String[]{"sensitive_eventTime"});
        sensitiveFieldMapping.put("eventTime", new String[]{"sensitive_eventTime"});

        sensitiveFieldMapping.put("month", new String[]{"sensitive_month"});

        sensitiveFieldMapping.put("verbatim_event_date", new String[]{"sensitive_verbatimEventDate"});
        sensitiveFieldMapping.put("verbatimEventDate", new String[]{"sensitive_verbatimEventDate"});
    }

    /**
     * insert sensitive versions of requested fields
     *
     * @param downloadParams
     */
    private void insertSensitiveFields(DownloadRequestDTO downloadParams) {
        String[] originalFields = downloadParams.getFields().split(",");
        List<String> fieldsWithSensitive = new ArrayList<>();
        Set<String> fieldsWithSensitiveSet = new HashSet<>();
        for (String field : originalFields) {
            field = StringUtils.trim(field);
            // put back the original field
            fieldsWithSensitive.add(field);
            // if this filed has a sensitive mapping
            if (sensitiveFieldMapping.containsKey(field)) {
                // make sure sensitive fields only added once
                fieldsWithSensitive.addAll(Arrays.stream(sensitiveFieldMapping.get(field)).filter(it -> !fieldsWithSensitiveSet.contains(it)).collect(Collectors.toList()));
                fieldsWithSensitiveSet.addAll(Arrays.stream(sensitiveFieldMapping.get(field)).filter(it -> !fieldsWithSensitiveSet.contains(it)).collect(Collectors.toList()));
            }
        }

        downloadParams.setFields(String.join(",", fieldsWithSensitive));
    }

    /**
     * Process downloadRequestParams.fields and update for sensitive requests.
     *
     * @param downloadParams
     * @param includeSensitive
     */
    private void prepareRequestedFields(DownloadRequestDTO downloadParams, boolean includeSensitive) {
        // process field abbreviations, defaults
        expandRequestedFields(downloadParams, true);

        // include sensitive versions of requested fields
        if (includeSensitive) {
            insertSensitiveFields(downloadParams);
        }
    }

    private void requestFields(DownloadHeaders downloadHeaders, String[] fields) {
        for (String field : fields) {
            if (!ArrayUtils.contains(downloadHeaders.included, field)) {
                downloadHeaders.included = (String[]) ArrayUtils.add(downloadHeaders.included, field);
            }
        }
    }

    private DownloadHeaders prepareHeaders(DownloadRequestDTO downloadParams) {
        DownloadHeaders downloadHeaders = downloadFields.newDownloadHeader(downloadParams, Arrays.asList(downloadExcludedMiscFields.split(",")));

        // add fields that are required for post-processing
        addPostProcessingFields(downloadParams, downloadHeaders);

        return downloadHeaders;
    }

    /**
     * Add fields to downloadHeaders that are required by the download request. This appends to
     * downloadHeaders.included for additional fields required from SOLR.
     * <p>
     * Additions are for:
     * - assertions value for qa columns requested
     * - attribution values for logger.ala
     * - species lft and rgt values for species list lookups
     * - misc field for inclusion of miscellanious columns.
     *
     * @param downloadParams
     * @param downloadHeaders
     */
    private void addPostProcessingFields(DownloadRequestDTO downloadParams, DownloadHeaders downloadHeaders) {
        // include assertion fields
        if (StringUtils.isNotBlank(downloadParams.getQa()) && !"none".equals(downloadParams.getQa())) {
            requestFields(downloadHeaders, new String[]{"assertions"});

            // Map assertions to the output columns with the assertion name in the header.
            // These assertion columns will contain the passed/failed values.
            if ("includeall".equals(downloadParams.getQa())) {
                downloadHeaders.qaIds = getAllQAFields().toString().split(",");
            } else if ("all".equals(downloadParams.getQa())) {
                try {
                    downloadHeaders.qaIds = getFacet(downloadParams, "assertions").getValues().stream().filter(count -> {
                        return count.getCount() > 0;
                    }).map(count -> {
                        return count.getName();
                    }).filter(s -> s != null).collect(Collectors.toList()).toArray(new String[0]);
                } catch (Exception e) {
                    logger.error("error getting assertions facet for download: " + downloadParams, e);
                }
            } else {
                // translate qa names into new assertion names
                downloadHeaders.qaIds = Arrays.stream(downloadParams.getQa().split(","))
                        .map((qa) -> fieldMappingUtil.translateFieldValue("assertions", qa))
                        .toArray(String[]::new);
            }

            // if dwcHeadings == true return qa code as headings, else return description.
            String headingPrefix = downloadParams.getDwcHeaders() ? "headings.assertions." : "assertions.";
            downloadHeaders.qaLabels = Arrays.stream(downloadHeaders.qaIds).map(id -> {
                return messageSource.getMessage(headingPrefix + id, null, id, null);
            }).collect(Collectors.toList()).toArray(new String[0]);
        }

        // fields required for logger.ala and doi minting
        requestFields(downloadHeaders, new String[]{DATA_PROVIDER_UID, INSTITUTION_UID, COLLECTION_UID, DATA_RESOURCE_UID, LICENSE});

        // 'lft' and 'rgt' is mandatory when there are species list fields
        if (downloadHeaders.speciesListIds.length > 0) {
            requestFields(downloadHeaders, new String[]{LFT, RGT});
        }

        // include misc fields
        if (downloadParams.getIncludeMisc()) {
            requestFields(downloadHeaders, new String[]{SENSITIVE, OccurrenceIndex.MISC});
        }
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
    private void expandRequestedFields(DownloadRequestDTO downloadParams, boolean isSolr) {
        String fields = getDownloadFields(downloadParams);

        try {
            Matcher matcher = clpField.matcher(fields);
            if (matcher.find()) {
                StringBuilder sb = new StringBuilder();
                for (IndexFieldDTO field : indexDao.getIndexedFields()) {
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
                for (IndexFieldDTO field : indexDao.getIndexedFields()) {
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
                for (IndexFieldDTO field : indexDao.getIndexedFields()) {
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

    private String getDownloadFields(DownloadRequestDTO downloadParams) {
        String dFields = downloadParams.getFields();
        if (StringUtils.isEmpty(dFields)) {
            dFields = defaultDownloadFields;
        }
        // append any extra fields
        String extra = downloadParams.getExtra();
        if (!StringUtils.isEmpty(extra)) {
            dFields += "," + extra;
        }
        return dFields;
    }

    /**
     * Split a list of queries by a fq.
     */
    private List<SolrQuery> splitQueries(SolrQuery query, String fq, String[] fqFields, String[] notFqFields) {
        List<SolrQuery> queries = new ArrayList<SolrQuery>();


        SolrQuery nsq = query.getCopy().addFilterQuery("-(" + fq + ")");
        if (notFqFields != null) {
            Arrays.stream(notFqFields).forEach(nsq::addField);
        }
        queries.add(nsq);

        SolrQuery sq = query.getCopy().addFilterQuery(fq);
        if (fqFields != null) {
            Arrays.stream(fqFields).forEach(sq::addField);
        }
        queries.add(sq);

        return queries;
    }

    public static void incrementCount(ConcurrentMap<String, AtomicInteger> values, Object uid) {
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
     * @see au.org.ala.biocache.dao.SearchDAO#getFacetPoints(SpatialSearchRequestDTO, au.org.ala.biocache.dto.PointType)
     */
    @Deprecated
    @Override
    public List<OccurrencePoint> getFacetPoints(SpatialSearchRequestDTO searchParams, PointType pointType) throws Exception {
        return getPoints(searchParams, pointType, -1);
    }

    @Deprecated
    private List<OccurrencePoint> getPoints(SpatialSearchRequestDTO searchParams, PointType pointType, int max) throws Exception {
        List<OccurrencePoint> points = new ArrayList<>();

        SolrQuery solrQuery = initSolrQuery(searchParams, false, null);
        emptyFacetRequest(solrQuery, max, 0, false);
        solrQuery.addFacetField(pointType.getLabel());

        QueryResponse qr = indexDao.runSolrQuery(solrQuery);
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
     * @see au.org.ala.biocache.dao.SearchDAO#findRecordsForLocation(SpatialSearchRequestDTO, au.org.ala.biocache.dto.PointType)
     * This is no longer used by explore your area
     */
    @Deprecated
    @Override
    public List<OccurrencePoint> findRecordsForLocation(SpatialSearchRequestDTO requestParams, PointType pointType) throws Exception {
        return getPoints(requestParams, pointType, MAX_DOWNLOAD_SIZE);
    }

    /**
     * Calculates the breakdown of the supplied query based on the supplied params
     */
    public TaxaRankCountDTO calculateBreakdown(BreakdownRequestDTO queryParams) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Attempting to find the counts for " + queryParams);
        }
        TaxaRankCountDTO trDTO = null;

        queryParams.setPageSize(0);
        queryParams.setFacet(true);
        queryParams.setFsort("count");
        queryParams.setFlimit(-1);
        SolrQuery solrQuery = initSolrQuery(queryParams, false, null);
        solrQuery.setFacetMissing(false);   // do not return missing facet

        //add the rank:name as a fq if necessary
        if (StringUtils.isNotEmpty(queryParams.getName()) && StringUtils.isNotEmpty(queryParams.getRank())) {
            solrQuery.addFilterQuery(queryParams.getRank() + ":" + queryParams.getName());
        }
        //add the ranks as facets
        if (queryParams.getLevel() == null) {
            List<String> ranks = queryParams.getRank() != null ? searchUtils.getNextRanks(queryParams.getRank(), queryParams.getName() == null) : searchUtils.getRanks();

            if (queryParams.getMax() != null && queryParams.getMax() > 0) {
                //reverse the facets so that they are returned in rank reverse order species, genus, family etc
                Collections.reverse(ranks);
            }

            // need to get the return level that the number of facets are <=max ranks need to be processed in reverse order until max is satisfied
            // or the first rank with the number of facets > 0 when the rank is specified
            for (String r : ranks) {
                long count = estimateUniqueValues(queryParams, r);
                if ((queryParams.getMax() != null && queryParams.getMax() > 0 && count <= queryParams.getMax()) ||
                        (queryParams.getRank() != null && count > 0)) {
                    solrQuery.addFacetField(r);
                    break;
                }
            }
        } else {
            //the user has supplied the "exact" level at which to perform the breakdown
            solrQuery.addFacetField(queryParams.getLevel());
        }
        QueryResponse qr = indexDao.runSolrQuery(solrQuery);

        // process results for the 1 facetField requested
        if (qr.getResults().getNumFound() > 0 && qr.getFacetFields().size() > 0) {
            FacetField ff = qr.getFacetFields().get(0);
            trDTO = new TaxaRankCountDTO(ff.getName());

            List<FieldResultDTO> fDTOs = new ArrayList<FieldResultDTO>();
            for (Count count : ff.getValues()) {
                if (count.getCount() > 0) {
                    FieldResultDTO f = new FieldResultDTO(count.getName(), count.getFacetField().getName() + "." + count.getName(), count.getCount());
                    fDTOs.add(f);
                }
            }
            trDTO.setTaxa(fDTOs);
        }
        return trDTO;
    }

    /**
     * Process the {@see org.apache.solr.client.solrj.response.QueryResponse} from a SOLR search and return
     * a {@link au.org.ala.biocache.dto.SearchResultDTO}
     *
     * @param qr
     * @param solrQuery
     * @return
     */
    private SearchResultDTO processSolrResponse(SearchRequestDTO params, QueryResponse qr, SolrQuery solrQuery, Class resultClass) {
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
        searchResult.setTotalRecords(sdl.getNumFound());        // TODO: PIPELINES: SolrDocumentList::getNumFound entry point
        searchResult.setStartIndex(sdl.getStart());             // TODO: PIPELINES: SolrDocumentList::getStart entry point
        searchResult.setPageSize(solrQuery.getRows()); //pageSize
        searchResult.setStatus("OK");
        String[] solrSort = StringUtils.split(solrQuery.getSortField(), " "); // e.g. "taxon_name asc"
        if (logger.isDebugEnabled()) {
            logger.debug("sortField post-split: " + StringUtils.join(solrSort, "|"));
        }
        if (solrSort != null && solrSort.length == 2) {
            searchResult.setSort(solrSort[0]); // sortField
            searchResult.setDir(solrSort[1]); // sortDirection
        }
        searchResult.setQuery(params.getUrlParams()); //this needs to be the original URL>>>>
        searchResult.setOccurrences(results);

        List<FacetResultDTO> facetResults = buildFacetResults(facets);

        //all belong to uncertainty range for now
        if (facetQueries != null && !facetQueries.isEmpty()) {
            Map<String, String> rangeMap = rangeBasedFacets.getRangeMap(OccurrenceIndex.COORDINATE_UNCERTAINTY);
            List<FieldResultDTO> fqr = new ArrayList<FieldResultDTO>();
            for (String value : facetQueries.keySet()) {
                if (facetQueries.get(value) > 0)
                    fqr.add(new FieldResultDTO(rangeMap.get(value), rangeMap.get(value), facetQueries.get(value), value));
            }
            facetResults.add(new FacetResultDTO(OccurrenceIndex.COORDINATE_UNCERTAINTY, fqr));
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
     *
     * @param facetResults A non null list where FacetResultDTO instances will be added.
     * @param facetEntries The solr facet entries
     * @param facetName    The name of the facet
     * @throws IllegalArgumentException if facetEntries is not a List containing either {@link FacetField.Count} or @{@link RangeFacet.Count} instances
     */
    private void addFacetResultsFromSolrFacets(List<FacetResultDTO> facetResults, List<?> facetEntries, String facetName) {
        if ((facetEntries != null) && (facetEntries.size() > 0)) {
            ArrayList<FieldResultDTO> r = new ArrayList<FieldResultDTO>();

            long entryCount;
            String countEntryName;

            for (Object facetCountEntryObject : facetEntries) {
                if (facetCountEntryObject instanceof Count) {
                    Count facetCountEntry = (Count) facetCountEntryObject;
                    entryCount = facetCountEntry.getCount();
                    countEntryName = facetCountEntry.getName();
                } else if (facetCountEntryObject instanceof RangeFacet.Count) {
                    RangeFacet.Count raengeFacetCountEntry = (RangeFacet.Count) facetCountEntryObject;
                    entryCount = raengeFacetCountEntry.getCount();
                    countEntryName = raengeFacetCountEntry.getValue();

                } else {
                    throw new IllegalArgumentException("facetCountEntry is not an instance of FacetField.Count nor RangeFacet.Count: " + facetCountEntryObject.getClass());
                }

                //check to see if the facet field is an uid value that needs substitution
                if (entryCount == 0) continue;

                if (countEntryName == null) {

                    String label = "";
                    if (messageSource != null) {
                        label = messageSource.getMessage(fieldMappingUtil.translateFieldName(facetName) + ".novalue", null, "Not supplied", null);
                    }
                    r.add(new FieldResultDTO(label, facetName + ".novalue", entryCount, "-" + facetName + ":*"));
                } else {
                    if (countEntryName.equals(DECADE_PRE_1850_LABEL)) {
                        r.add(0, new FieldResultDTO(
                                getFacetValueDisplayName(fieldMappingUtil.translateFieldName(facetName), countEntryName),
                                facetName + "." + countEntryName,
                                entryCount,
                                getFormattedFqQuery(facetName, countEntryName)
                        ));
                    } else {
                        r.add(new FieldResultDTO(
                                getFacetValueDisplayName(fieldMappingUtil.translateFieldName(facetName), countEntryName),
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
            Map<String, String> formats = occurrenceUtils.getImageFormats(oi.getImage());
            oi.setImageUrl(formats.get("raw"));
            oi.setThumbnailUrl(formats.get("thumb"));
            oi.setSmallImageUrl(formats.get("small"));
            oi.setLargeImageUrl(formats.get("large"));
            String[] images = oi.getImages();
            if (images != null && images.length > 0) {
                String[] imageUrls = new String[images.length];
                for (int i = 0; i < images.length; i++) {
                    try {
                        Map<String, String> availableFormats = occurrenceUtils.getImageFormats(images[i]);
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
            int upper = Integer.parseInt(lower) - 1;
            if (addGap)
                upper += (Integer) gap;
            return Integer.toString(upper);
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

    /**
     * Helper method to create SolrQuery object and add facet settings
     *
     * @return solrQuery the SolrQuery
     */
    public SolrQuery initSolrQuery(SpatialSearchRequestDTO searchParams, boolean substituteDefaultFacetOrder, Map<String, String[]> extraSolrParams) throws QidMissingException {
        queryFormatUtils.formatSearchQuery(searchParams);

        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery(searchParams.getFormattedQuery());

        if (searchParams.getFormattedFq() != null) {
            for (String fq : searchParams.getFormattedFq()) {
                if (StringUtils.isNotEmpty(fq)) {
                    solrQuery.addFilterQuery(fq);
                }
            }
        }

        solrQuery.setRequestHandler("standard");
        boolean rangeAdded = false;
        // Facets
        solrQuery.setFacet(searchParams.getFacet());
        if (searchParams.getFacet()) {
            for (String facet : searchParams.getFacets()) {
                if (facet.equals("uncertainty")) {
                    Map<String, String> rangeMap = rangeBasedFacets.getRangeMap("uncertainty");
                    for (String range : rangeMap.keySet()) {
                        solrQuery.add("facet.query", range);
                    }
                } else {

                    solrQuery.addFacetField(facet);

                    if ("".equals(searchParams.getFsort()) && substituteDefaultFacetOrder && FacetThemes.getFacetsMap().containsKey(facet)) {
                        //now check if the sort order is different to supplied
                        String thisSort = FacetThemes.getFacetsMap().get(facet).getSort();  // thisSort = index or count
                        if (!searchParams.getFsort().equalsIgnoreCase(thisSort))
                            solrQuery.add("f." + fieldMappingUtil.translateFieldName(facet) + ".facet.sort", thisSort);
                    }

                }
            }

            solrQuery.setFacetMinCount(1);
            solrQuery.setFacetLimit(searchParams.getFlimit());
            //include this so that the default fsort is still obeyed.
            String fsort = StringUtils.isEmpty(searchParams.getFsort()) ? "count" : searchParams.getFsort();
            solrQuery.setFacetSort(fsort);
            if (searchParams.getFoffset() > 0)
                solrQuery.add("facet.offset", Integer.toString(searchParams.getFoffset()));
            if (StringUtils.isNotEmpty(searchParams.getFprefix()))
                solrQuery.add("facet.prefix", searchParams.getFprefix());
        }

        solrQuery.setRows(searchParams.getPageSize());
        solrQuery.setStart(searchParams.getStart());
        if (StringUtils.isNotEmpty(searchParams.getDir()) && StringUtils.isNotEmpty(searchParams.getSort())) {
            solrQuery.setSort(searchParams.getSort(), SolrQuery.ORDER.valueOf(searchParams.getDir()));
        }

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
     * Get a distinct list of species and their counts using a facet search.
     * <p>
     * This method expects requestParams.getFacets() == [OccurrenceIndex.COMMON_NAME_AND_LSID] or [OccurrenceIndex.NAMES_AND_LSID]
     * <p>
     * TODO: searchUtils.getTaxonSearch uses nameUsageMatchService.get(). These requests must be batched.
     *
     * @param requestParams
     * @return
     * @throws SolrServerException
     */
    protected void getSpeciesCountsJSON(SpatialSearchRequestDTO requestParams, OutputStream outputStream) throws Exception {
        SolrQuery solrQuery = initSolrQuery(requestParams, false, null);
        solrQuery.setFacetMissing(false);

        StreamTaxaCount procFacet = new StreamTaxaCount(this, searchUtils, requestParams, outputStream);
        indexDao.streamingQuery(solrQuery, null, procFacet, null);
    }

    protected void getSpeciesCountsCSV(SpatialSearchRequestDTO requestParams, OutputStream outputStream) throws Exception {
        SolrQuery solrQuery = initSolrQuery(requestParams, false, null);
        solrQuery.setFacetMissing(false);

        StreamTaxaAsCSV procFacet = new StreamTaxaAsCSV(this, searchUtils, requestParams, outputStream);
        indexDao.streamingQuery(solrQuery, null, procFacet, null);
    }

    protected void getSpeciesCountsTSVCircle(SpatialSearchRequestDTO requestParams, OutputStream outputStream) throws Exception {
        SolrQuery solrQuery = initSolrQuery(requestParams, false, null);
        solrQuery.setFacetMissing(false);

        StreamTaxaAsTSVCircle procFacet = new StreamTaxaAsTSVCircle(this, searchUtils, requestParams, outputStream);
        indexDao.streamingQuery(solrQuery, null, procFacet, null);
    }

    /**
     * Obtains a list and facet count of the source uids for the supplied query.
     *
     * @param searchParams
     * @return
     * @throws Exception
     */
    public Map<String, Integer> getSourcesForQuery(SpatialSearchRequestDTO searchParams) throws Exception {

        Map<String, Integer> uidStats = new HashMap<String, Integer>();
        SolrQuery solrQuery = initSolrQuery(searchParams, false, null);

        // convert to facet query
        emptyFacetRequest(solrQuery, -1, 0, false);
        solrQuery.addFacetField(OccurrenceIndex.INSTITUTION_UID,
                OccurrenceIndex.COLLECTION_UID,
                OccurrenceIndex.DATA_RESOURCE_UID,
                OccurrenceIndex.DATA_PROVIDER_UID);

        QueryResponse qr = indexDao.runSolrQuery(solrQuery);
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
     * Returns the count of distinct values for the facets.
     * This is an altered implementation that is SOLRCloud friendly (ngroups are not SOLR Cloud compatible)
     * <p>
     * The group count is only accurate when foffset == 0
     */
    public List<FacetResultDTO> getFacetCounts(SpatialSearchRequestDTO searchParams) throws Exception {
        searchParams.setFacet(true);
        searchParams.setPageSize(0);

        SolrQuery facetQuery = initSolrQuery(searchParams, false, null);
        facetQuery.setFields();

        List<String> fqList = new ArrayList<String>();
        //only add the FQ's if they are not the default values
        if (searchParams != null && searchParams.getFormattedFq() != null && searchParams.getFormattedFq().length > 0) {
            org.apache.commons.collections.CollectionUtils.addAll(fqList, searchParams.getFormattedFq());
        }

        facetQuery.setFilterQueries(fqList.stream().toArray(String[]::new));

        if (searchParams.getFlimit() == 0) {
            //add the estimates
            List<FacetResultDTO> facetResults = new ArrayList<>();
            for (String facetName : searchParams.getFacets()) {
                FacetResultDTO frDTO = new FacetResultDTO();
                frDTO.setFieldName(facetName);
                frDTO.setCount((int) estimateUniqueValues(searchParams, facetName));
                frDTO.setFieldResult(new ArrayList<>());
                facetResults.add(frDTO);
            }
            return facetResults;
        } else {
            QueryResponse qr = query(facetQuery);
            SearchResultDTO searchResults = processSolrResponse(searchParams, qr, facetQuery, OccurrenceIndex.class);

            List<FacetResultDTO> facetResults = searchResults.getFacetResults();
            if (facetResults != null) {
                for (FacetResultDTO fr : facetResults) {
                    FacetResultDTO frDTO = new FacetResultDTO();
                    frDTO.setCount(fr.getCount());
                    frDTO.setFieldName(fr.getFieldName());

                    // only calculate the correct count when foffset == 0 and output is limited
                    if (searchParams.getFoffset() == 0 && searchParams.getFlimit() >= 0) {
                        // An estimate is suitable here as this is used for paging through facet values.
                        // The alternative is to download all unique values and count them.
                        fr.setCount((int) estimateUniqueValues(searchParams, searchParams.getFacets()[0]));
                    } else if (fr.getCount() == null) {
                        fr.setCount(fr.getFieldResult().size());
                    }

                    //reduce the number of facets returned...
                    if (searchParams.getFlimit() != null && searchParams.getFlimit() < fr.getFieldResult().size() &&
                            searchParams.getFlimit() >= 0) {
                        fr.setFieldResult(fr.getFieldResult().subList(0, searchParams.getFlimit()));
                    }
                }
            }
            return facetResults;
        }
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#findByFulltext(SpatialSearchRequestDTO)
     */
    @Override
    public SolrDocumentList findByFulltext(SpatialSearchRequestDTO searchParams) throws Exception {
        SolrDocumentList sdl = null;

        try {
            searchParams.setFacet(false);

            SolrQuery solrQuery = initSolrQuery(searchParams, false, null);

            sdl = indexDao.runSolrQuery(solrQuery).getResults();
        } catch (SolrServerException ex) {
            logError("Problem communicating with SOLR server. ", ex.getMessage());
        }

        return sdl;
    }

    private void logError(String description, String message) {
        String requestID = MDC.get("X-Request-ID");
        if (requestID != null) {
            logger.error(description + ", RequestID:" + requestID + " Error : " + message);
        } else {
            logger.error(description + " : " + message);
        }
    }


    @Inject
    CacheManager cacheManager;

    @Cacheable("legendCache")
    public List<LegendItem> getLegend(SpatialSearchRequestDTO searchParams, String facetField, String[] cutpoints) throws Exception {
        return getLegend(searchParams, facetField, cutpoints, false);
    }

    /**
     * legend sorting and limits are fixed.
     * <p>
     * year legend: all years shown, sorted descending order by year (indexed as string)
     * decade legend: all decades shown, sorted descending order by decade (indexed as string)
     * month legend: all months shown, sorted ascending order by month (indexed as string without 0 padding)
     * all other legends: limited by wmsLegendMaxItems, sorted by descending count, appends the aggregated cut off items
     *
     * @param searchParams
     * @param facetField
     * @param cutpoints
     * @param skipI18n
     * @return
     * @throws Exception
     */
    @Cacheable("legendCache")
    public List<LegendItem> getLegend(SpatialSearchRequestDTO searchParams, String facetField, String[] cutpoints, boolean skipI18n) throws Exception {
        List<LegendItem> legend = new ArrayList<LegendItem>();

        queryFormatUtils.formatSearchQuery(searchParams);
        if (logger.isDebugEnabled()) {
            logger.info("getLegend -search query: " + searchParams.getFormattedQuery());
        }
        SolrQuery solrQuery = initSolrQuery(searchParams, false, null);

        // convert to facet query
        emptyFacetRequest(solrQuery, wmslegendMaxItems - 1, 0, true);

        //is facet query?
        if (cutpoints == null) {
            solrQuery.addFacetField(facetField);
        } else {
            solrQuery.addFacetQuery("-" + facetField + ":*");

            for (int i = 0; i < cutpoints.length; i += 2) {
                solrQuery.addFacetQuery(facetField + ":[" + cutpoints[i] + " TO " + cutpoints[i + 1] + "]");
            }
        }

        // Always use fsort=count unless facet is year or decade (integer values stored as strings).
        // Month sorting (asc) is done later (string value of month number stored without '0' padding).
        if (YEAR.equals(facetField) || DECADE_FACET_NAME.equals(facetField)) {
            solrQuery.setFacetSort("index");

            // legends containing year and decade are not limited by `wmslegendMaxItems`
            solrQuery.setFacetLimit(-1);
        } else {
            solrQuery.setFacetSort("count");
        }

        QueryResponse qr = indexDao.runSolrQuery(solrQuery);
        long remainderCount = qr.getResults().getNumFound();
        List<FacetField> facets = qr.getFacetFields();
        if (facets != null) {
            for (FacetField facet : facets) {
                List<FacetField.Count> facetEntries = facet.getValues();
                if (facet.getName().contains(facetField) && facetEntries != null && !facetEntries.isEmpty()) {

                    List<String> addedFqs = new ArrayList<>();

                    for (int i = 0; i < facetEntries.size(); i++) {
                        FacetField.Count fcount = facetEntries.get(i);
                        if (fcount.getCount() > 0) {
                            remainderCount -= fcount.getCount();
                            String fq = facetField + ":\"" + fcount.getName() + "\"";
                            if (fcount.getName() == null) {
                                fq = "-" + facetField + ":*";
                                addedFqs.add(facetField + ":*");
                            } else {
                                addedFqs.add("-" + fq);
                            }

                            if (skipI18n) {
                                legend.add(new LegendItem(fcount.getName(), null, fcount.getName(), fcount.getCount(), fq));
                            } else {
                                String i18nCode = null;
                                if (StringUtils.isNotBlank(fcount.getName())) {
                                    i18nCode = fieldMappingUtil.translateFieldName(facetField) + "." + fcount.getName();
                                } else {
                                    i18nCode = fieldMappingUtil.translateFieldName(facetField) + ".novalue";
                                }

                                legend.add(new LegendItem(
                                        getFacetValueDisplayName(facetField, fcount.getName()),
                                        i18nCode,
                                        fcount.getName(),
                                        fcount.getCount(),
                                        fq)
                                );
                            }
                        }
                    }

                    if (remainderCount > 0) {
                        String theFq = "-(" + StringUtils.join(addedFqs, " AND ") + ")";
                        // create a single catch remainder facet
                        legend.add(legend.size(), new LegendItem(
                                "Other " + messageSource.getMessage("facet." + facetField, null, messageSource.getMessage(facetField, null, facetField, null), null),
                                facetField + ".other",
                                "",
                                remainderCount,
                                theFq,
                                true
                        ));
                    }

                    break;
                }
            }
        }

        if (cutpoints == null) {
            // facet sort is descending count or ascending name
            if (facetField.equals(MONTH)) {
                // sort ascending month
                java.util.Collections.sort(legend, new Comparator<LegendItem>() {
                    @Override
                    public int compare(LegendItem o1, LegendItem o2) {
                        if (StringUtils.isEmpty(o1.getFacetValue()))
                            return -1;
                        if (StringUtils.isEmpty(o2.getFacetValue()))
                            return 1;

                        return Integer.parseInt(o1.getFacetValue()) > Integer.parseInt(o2.getFacetValue()) ? 1 : -1;
                    }
                });
            } else if (facetField.equals(YEAR) || facetField.equals(DECADE_FACET_NAME)) {
                // sort descending year or decade
                java.util.Collections.sort(legend, new Comparator<LegendItem>() {
                    @Override
                    public int compare(LegendItem o1, LegendItem o2) {
                        return o2.getName().compareTo(o1.getName());
                    }
                });
            }
        }

        String tFacetField = fieldMappingUtil.translateFieldName(facetField);

        //check if we have query based facets
        Map<String, Integer> facetq = qr.getFacetQuery();
        if (facetq != null && facetq.size() > 0) {
            for (Entry<String, Integer> es : facetq.entrySet()) {
                legend.add(new LegendItem(getFacetValueDisplayName(tFacetField, es.getKey()), tFacetField + "." + es.getKey(), es.getKey(), es.getValue(), es.getKey()));
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
                                facetEntry.getFacetField().getName(),
                                facetEntry.getCount(),
                                OCCURRENCE_YEAR_INDEX_FIELD + ":[" + startDate + " TO " + finishDate + "]")
                );
            }
        }

        addColours(legend, cutpoints);
        return legend;
    }

    private void addColours(List<LegendItem> legend, String[] cutpoints) {
        int offset = 0;

        for (int i = 0; i < legend.size(); i++) {
            LegendItem li = legend.get(i);

            int colour = DEFAULT_COLOUR;
            if (cutpoints == null) {
                colour = ColorUtil.colourList[Math.min(i, ColorUtil.colourList.length - 1)];
            } else if (cutpoints != null && i - offset < cutpoints.length) {
                if (StringUtils.isEmpty(legend.get(i).getName())
                        || legend.get(i).getName().equals("Unknown")
                        || legend.get(i).getName().startsWith("-")
                ) {
                    offset++;
                } else {
                    colour = ColorUtil.getRangedColour(i - offset, cutpoints.length / 2);
                }
            }
            li.setColour(colour);
        }
    }

    /**
     * Convert a solrQuery to a facet only request.
     * - rows=0
     * - facet=true
     * - facet.mincount=1
     * <p>
     * Also removes any facetQueries and facetFields
     *
     * @param solrQuery
     */
    private void emptyFacetRequest(SolrQuery solrQuery, int facetLimit, int facetOffset, boolean includeMissing) {
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(facetLimit);
        solrQuery.set("facet.offset", facetOffset);
        solrQuery.setFacetMissing(includeMissing);

        // remove facetFields and facetQueries
        if (solrQuery.getFacetFields() != null)
            Arrays.stream(solrQuery.getFacetFields()).forEach(solrQuery::removeFacetField);
        if (solrQuery.getFacetQuery() != null)
            Arrays.stream(solrQuery.getFacetQuery()).forEach(solrQuery::removeFacetQuery);
    }

    public FacetField getFacet(SpatialSearchRequestDTO searchParams, String facet) throws Exception {
        SolrQuery solrQuery = initSolrQuery(searchParams, false, null);

        // convert to a facet only request
        emptyFacetRequest(solrQuery, -1, 0, false);
        solrQuery.addFacetField(facet);

        QueryResponse qr = indexDao.runSolrQuery(solrQuery);
        return qr.getFacetFields().get(0);
    }

    public List<DataProviderCountDTO> getDataProviderList(SpatialSearchRequestDTO requestParams) throws Exception {
        List<DataProviderCountDTO> dataProviderList = new ArrayList<DataProviderCountDTO>();
        String dataProviderUid = OccurrenceIndex.DATA_PROVIDER_UID;
        FacetField facet = getFacet(requestParams, dataProviderUid);
        String[] oldFq = requestParams.getFacets();
        if (facet != null) {
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
     * @see au.org.ala.biocache.dao.SearchDAO#findAllSpeciesJSON(SpatialSearchRequestDTO, OutputStream)
     */
    @Override
    public void findAllSpeciesJSON(SpatialSearchRequestDTO requestParams, OutputStream outputStream) throws Exception {
        if (requestParams.getFacets() == null || requestParams.getFacets().length != 1) {
            requestParams.setFacets(new String[]{NAMES_AND_LSID});
        }

        getSpeciesCountsJSON(requestParams, outputStream);
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#findAllSpeciesJSON(SpatialSearchRequestDTO, OutputStream)
     */
    @Override
    public void findAllSpeciesCSV(SpatialSearchRequestDTO requestParams, OutputStream outputStream) throws Exception {
        if (requestParams.getFacets() == null || requestParams.getFacets().length != 1) {
            requestParams.setFacets(new String[]{NAMES_AND_LSID});
        }

        getSpeciesCountsCSV(requestParams, outputStream);
    }

    /**
     * Retrieves a set of counts for the supplied list of taxa.
     * <p>
     * TODO: searchUtils.getTaxonSearch uses nameUsageMatchService.get(). These requests must be batched.
     * TODO: deprecate for a standard facet query.
     *
     * @param taxa
     * @param filterQueries
     * @return
     * @throws Exception
     */
    public Map<String, Integer> getOccurrenceCountsForTaxa(List<String> taxa, String[] filterQueries) throws Exception {
        SolrQuery solrQuery = new SolrQuery();
        emptyFacetRequest(solrQuery, taxa.size(), 0, false);

        if (filterQueries != null && filterQueries.length > 0) {
            solrQuery.setFilterQueries(filterQueries);
        }
        StringBuilder sb = new StringBuilder();
        Map<String, Integer> counts = new HashMap<String, Integer>();
        Map<String, String> lftToGuid = new HashMap<String, String>();
        for (String lsid : taxa) {
            //get the lft and rgt value for the taxon
            String[] values = searchUtils.getTaxonSearch(lsid);
            //first value is the search string
            if (sb.length() > 0) {
                sb.append(" OR ");
            }
            sb.append(values[0]);
            lftToGuid.put(values[0], lsid);
            //add the query part as a facet
            solrQuery.add("facet.query", values[0]);
        }
        solrQuery.setQuery(sb.toString());

        QueryResponse qr = indexDao.runSolrQuery(solrQuery);
        Map<String, Integer> facetQueries = qr.getFacetQuery();
        for (String facet : facetQueries.keySet()) {
            //add all the counts based on the query value that was substituted
            String lsid = lftToGuid.get(facet);
            Integer count = facetQueries.get(facet);
            if (lsid != null && count != null)
                counts.put(lsid, count);
        }
        logger.debug(facetQueries);
        return counts;
    }

    /**
     * @return the maxSolrDownloadThreads for solr download queries
     */
    public Integer getMaxSolrOnlineDownloadThreads() {
        return maxSolrDownloadThreads;
    }

    private QueryResponse query(SolrParams query) throws Exception {
        return indexDao.query(query);
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
                    query(solrQuery);  //throws exception when too many boolean clauses
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
     * Perform grouped facet query.
     * <p>
     * facets is the list of grouped facets required
     * flimit restricts the number of groups returned
     * pageSize restricts the number of docs in each group returned
     * fl is the list of fields in the returned docs
     */
    public QueryResponse searchGroupedFacets(SpatialSearchRequestDTO searchParams) throws Exception {
        searchParams.setPageSize(0);
        searchParams.setFacet(false);

        //get facet group counts
        SolrQuery solrQuery = initSolrQuery(searchParams, false, null);

        StringBuilder sb = new StringBuilder("{");
        int facets = 0;
        for (String facet : searchParams.getFacets()) {
            if (StringUtils.isNotEmpty(searchParams.getFl())) {
                if (facets > 0) sb.append(",");
                facets++;

                sb.append(facet).append(":{type:terms,limit:-1,sort:index,field:").
                        append(fieldMappingUtil.translateFieldName(facet)).append(",facet:{");

                int fls = 0;
                for (String fl : searchParams.getFl().split(",")) {
                    if (fls > 0) sb.append(",");
                    fls++;

                    sb.append(fieldMappingUtil.translateFieldName(fl)).append(":{type:terms,limit:1,sort:index,field:").
                            append(fieldMappingUtil.translateFieldName(fl)).append("}");
                }
                sb.append("}}}");
            }
        }

        solrQuery.add("json.facet", sb.toString());

        return query(solrQuery);
    }

    /**
     * Generates a FQ value for use in the returning query response.
     *
     * @param facet
     * @param value
     * @return
     */
    String getFormattedFqQuery(String facet, String value) {
//        if (facet.equals(DECADE_FACET_NAME)) {
//
//            if (value.equals(DECADE_PRE_1850_LABEL)) {
//                return facet + ":" + "[* TO " + DECADE_FACET_START_DATE + "]";
//            } else {
//                SimpleDateFormat sdf = new SimpleDateFormat(SOLR_DATE_FORMAT);
//                try {
//                    Date date = sdf.parse(value);
//                    Date endDate = DateUtils.addYears(date, 10);
//                    endDate = DateUtils.addMilliseconds(endDate, -1);
//                    return facet + ":" + "[" + value + " TO " + sdf.format(endDate) + "]";
//                } catch (ParseException e) {
//                    //do nothing
//                }
//            }
//        }

        return facet + ":\"" + value.replace("\"", "\\\"") + "\"";
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

        String tFacet = fieldMappingUtil.translateFieldName(facet);
        String tValue = fieldMappingUtil.translateFieldValue(tFacet, value);

        if (facet.endsWith("_uid") || facet.endsWith("Uid")) {
            return searchUtils.getUidDisplayString(tFacet, tValue, false);
        } else if (searchUtils.getAuthIndexFields().contains(tFacet)) {
            //if the facet field is collector or assertion_user_id we need to perform the substitution
            Optional<AlaUserProfile> profile = authService.lookupAuthUser(value);
            return profile.isPresent() ? profile.get().getName() : value;
        } else {
            if (messageSource != null) {

                if (StringUtils.isNotBlank(value)) {


                    return messageSource.getMessage(
                            tFacet + "." + tValue,
                            null,
                            value,
                            (Locale) null);
                } else {
                    return messageSource.getMessage(
                            fieldMappingUtil.translateFieldName(facet) + ".novalue",
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
     * @see au.org.ala.biocache.dao.SearchDAO#searchPivot(SpatialSearchRequestDTO)
     */
    public List<FacetPivotResultDTO> searchPivot(SpatialSearchRequestDTO searchParams) throws Exception {
        String pivot = StringUtils.join(searchParams.getFacets(), ",");
        searchParams.setFacets(new String[]{});
        searchParams.setFacet(true);
        searchParams.setPageSize(0);

        //get facet group counts
        SolrQuery query = initSolrQuery(searchParams, false, null);
        query.setFields();
        //now use the supplied facets to add groups to the query
        query.add("facet.pivot", pivot);
        query.add("facet.pivot.mincount", "1");
        query.add("facet.missing", "true");

        QueryResponse response = indexDao.runSolrQuery(query);
        NamedList<List<PivotField>> result = response.getFacetPivot();

        List<FacetPivotResultDTO> output = new ArrayList();
        for (Entry<String, List<PivotField>> pfl : result) {
            List<PivotField> list = pfl.getValue();
            if (list != null && list.size() > 0) {
//                // QueryResponse.getFacetPivot() is not legacy name translated by indexDao
//                //TODO: pipeline
//                String fieldName = list.get(0).getField();
//                fieldName = indexDao.getNewToLegacy().getOrDefault(fieldName, fieldName);

                output.add(new FacetPivotResultDTO(
                        list.get(0).getField(),         // TODO: PIPELINES: List<PivotField>::getField entry point
                        getFacetPivotResults(list),
                        null,
                        (int) response.getResults().getNumFound())  // // TODO: PIPELINES: QueryResponse::getResults & SolrDocumentList::getNumFound entry point
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
                // QueryResponse.getFacetPivot() is not legacy name translated by indexDao
                //TODO:PIPELINE
//                String fieldName = pf.getPivot().get(0).getField();
//                fieldName = indexDao.getNewToLegacy().getOrDefault(fieldName, fieldName);
//
//                list.add(new FacetPivotResultDTO(fieldName, getFacetPivotResults(pf.getPivot()), value, pf.getCount()));
            }
        }

        return list;
    }

    public StringBuilder getAllQAFields() {
        //include all assertions
        StringBuilder qasb = new StringBuilder();
        ErrorCode[] errorCodes = AssertionCodes.getAll();
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
    public List<FieldStatsItem> searchStat(SpatialSearchRequestDTO searchParams, String field, String facet,
                                           Collection<String> statType) throws Exception {
        searchParams.setFacets(new String[]{});

        if (facet != null) searchParams.setFacet(true);
        searchParams.setFacets(new String[]{});

        //get facet group counts
        SolrQuery query = initSolrQuery(searchParams, false, null);
        query.setRows(0);
        query.setFields();

        //stats parameters
        query.add("stats", "true");
        if (facet != null) query.add("stats.facet", facet);
        query.add("stats.field", "{!" + StringUtils.join(statType, "=true ") + "=true}" + fieldMappingUtil.translateFieldName(field));

        QueryResponse response = indexDao.runSolrQuery(query);

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
    @Cacheable("getColours")
    public List<LegendItem> getColours(SpatialSearchRequestDTO request, String colourMode) throws Exception {
        List<LegendItem> colours = new ArrayList<LegendItem>();
        if (colourMode.equals("grid")) {
            for (int i = 0; i <= 500; i += 100) {
                LegendItem li;
                if (i == 0) {
                    li = new LegendItem(">0", "", "", 0, null);
                } else {
                    li = new LegendItem(String.valueOf(i), "", "", 0, null);
                }
                li.setColour((((500 - i) / 2) << 8) | 0x00FF0000);
                colours.add(li);
            }
        } else {
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
                colours = getLegend(request, s[0], cutpoints, true);
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
    public double[] getBBox(SpatialSearchRequestDTO requestParams) throws Exception {
        SolrQuery query = initSolrQuery(requestParams, false, null);
        query.setRows(0);
        query.setFacet(false);

        query.add("json.facet", "{x2:\"max(decimalLongitude)\",x1:\"min(decimalLongitude)\",y2:\"max(decimalLatitude)\",y1:\"min(decimalLatitude)\"}");
        QueryResponse qr = indexDao.query(query);

        SimpleOrderedMap facets = SearchUtils.getMap(qr.getResponse(), "facets");

        return new double[]{toDouble(facets.get("x1")), toDouble(facets.get("y1")), toDouble(facets.get("x2")), toDouble(facets.get("y2"))};
    }

    /**
     * Get estimated number of unique values for a facet.
     *
     * @param requestParams
     * @param facet
     * @return
     * @throws Exception
     */
    @Override
    public long estimateUniqueValues(SpatialSearchRequestDTO requestParams, String facet) throws Exception {
        SolrQuery query = initSolrQuery(requestParams, false, null);
        query.setRows(0);
        query.setFacet(false);

        // hll() == distributed cardinality estimate via hyper-log-log algorithm
        query.add("json.facet", "{unique:\"hll(" + fieldMappingUtil.translateFieldName(facet) + ")\"}");
        QueryResponse qr = indexDao.query(query);

        SimpleOrderedMap facets = SearchUtils.getMap(qr.getResponse(), "facets");
        Object value = facets.get("unique");
        if (value == null) {
            return 0;
        }
        return toLong(value);
    }

    private long toLong(Object o) {
        if (o instanceof Long) {
            return (Long) o;
        } else if (o instanceof Integer) {
            return (Integer) o;
        } else {
            return Long.parseLong(o.toString());
        }
    }

    private double toDouble(Object o) {
        if (o instanceof Double) {
            return (Double) o;
        } else if (o instanceof Float) {
            return (Float) o;
        } else {
            return Double.parseDouble(o.toString());
        }
    }

    @Deprecated
    @Override
    public List<String> listFacets(SpatialSearchRequestDTO searchParams) throws Exception {
        searchParams.setFacet(true);
        searchParams.setFacets(new String[]{});

        SolrQuery solrQuery = initSolrQuery(searchParams, false, null); // general search settings

        solrQuery.setFacetLimit(-1);
        solrQuery.setRows(0);

        ArrayList<String> found = new ArrayList<>();

        for (IndexFieldDTO s : indexDao.getIndexedFields()) {
            // this only works for non-tri fields
            if (!s.isDeprecated() && s.getDataType() != null && !s.getDataType().startsWith("t")) {
                solrQuery.set("facet.field", "{!facet.method=enum facet.exists=true}" + s.getName());

                QueryResponse qr = query(solrQuery); // can throw exception

                for (FacetField f : qr.getFacetFields()) {
                    if (!f.getValues().isEmpty()) {
                        found.add(f.getName());
                    }
                }
            }
        }

        return found;
    }

    @Override
    @Cacheable("heatmapCache")
    public HeatmapDTO getHeatMap(
            String query,
            String[] filterQueries,
            Double minx,
            Double miny,
            Double maxx,
            Double maxy,
            List<LegendItem> legend,
            int gridSizeInPixels)
            throws Exception {

        List<List<List<Integer>>> layers = new ArrayList<>();

        // limit miny maxy to -90 90
        if (miny < -90) miny = -90.0;
        if (maxy > 90) maxy = 90.0;

        // fix date line
        while (maxx > 180) {
            maxx -= 360;
        }
        while (minx < -180) {
            minx += 360;
        }

        // single layers
        if (gridSizeInPixels > 1 || legend == null || legend.isEmpty()) {
            // single layer
            QueryResponse qr = null;
            SolrQuery solrQuery =
                    createHeatmapQuery(
                            query,
                            filterQueries,
                            minx,
                            miny,
                            maxx,
                            maxy);
            qr = query(solrQuery); // can throw exception

            // FIXME UGLY - not needed with SOLR8, but current constraint is SOLR 6 API
            // See SpatialHeatmapFacets.HeatmapFacet in SOLR 8 API
            SimpleOrderedMap facetHeatMaps =
                    ((SimpleOrderedMap)
                            ((SimpleOrderedMap) ((qr.getResponse().get("facet_counts")))).get("facet_heatmaps"));

            Integer gridLevel = -1;
            if (facetHeatMaps != null) {
                SimpleOrderedMap heatmap = (SimpleOrderedMap) facetHeatMaps.get(spatialFieldWMS);
                gridLevel = (Integer) heatmap.get("gridLevel");
                Integer rows = (Integer) heatmap.get("rows");
                Integer columns = (Integer) heatmap.get("columns");
                List<List<Integer>> layer = (List<List<Integer>>) heatmap.get("counts_ints2D");
                Double hminx = (Double) heatmap.get("minX");
                Double hminy = (Double) heatmap.get("minY");
                Double hmaxx = (Double) heatmap.get("maxX");
                Double hmaxy = (Double) heatmap.get("maxY");
                layers.add(layer);
                return new HeatmapDTO(
                        gridLevel, layers, legend, gridSizeInPixels, rows, columns, hminx, hminy, hmaxx, hmaxy);
            }
        } else {
            // multiple layers
            Integer gridLevel = -1;
            Integer rows = 0;
            Integer columns = 0;
            Double hminx = minx;
            Double hminy = miny;
            Double hmaxx = maxx;
            Double hmaxy = maxy;

            int zoomOffset = 0;
            for (int legendIdx = 0; legendIdx < legend.size(); legendIdx++) {
                LegendItem legendItem = legend.get(legendIdx);

                // add the FQ for the legend item
                QueryResponse qr = null;

                SolrQuery solrQuery =
                        createHeatmapQuery(
                                query, filterQueries, minx, miny, maxx, maxy);
                String[] fqs =
                        Arrays.copyOf(
                                solrQuery.getFilterQueries(), solrQuery.getFilterQueries().length + 1);
                fqs[fqs.length - 1] = legendItem.getFq();
                solrQuery.setFilterQueries(fqs);

                // query
                qr = query(solrQuery); // can throw exception

                if (qr != null) {
                    SimpleOrderedMap facetHeatMaps =
                            ((SimpleOrderedMap)
                                    ((SimpleOrderedMap) ((qr.getResponse().get("facet_counts"))))
                                            .get("facet_heatmaps"));

                    if (facetHeatMaps != null) {
                        // iterate over legend
                        SimpleOrderedMap heatmap = (SimpleOrderedMap) facetHeatMaps.get(spatialFieldWMS);
                        gridLevel = (Integer) heatmap.get("gridLevel");
                        List<List<Integer>> layer = (List<List<Integer>>) heatmap.get("counts_ints2D");
                        rows = (Integer) heatmap.get("rows");
                        columns = (Integer) heatmap.get("columns");
                        hminx = (Double) heatmap.get("minX");
                        hminy = (Double) heatmap.get("minY");
                        hmaxx = (Double) heatmap.get("maxX");
                        hmaxy = (Double) heatmap.get("maxY");
                        layers.add(layer);
                    } else {
                        layers.add(null);
                    }
                } else {
                    layers.add(null);
                }
            }

            return new HeatmapDTO(
                    gridLevel, layers, legend, gridSizeInPixels, rows, columns, hminx, hminy, hmaxx, hmaxy);
        }

        return null;
    }

    private SolrQuery createHeatmapQuery(
            String query,
            String[] filterQueries,
            Double minx,
            Double miny,
            Double maxx,
            Double maxy) {
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.set("facet.heatmap", spatialFieldWMS);

        // heatmaps support international date line
        if (minx < -180) {
            minx = minx + 360;
        }

        if (maxx > 180) {
            maxx = maxx - 360;
        }

        String geom = "[\"" + minx + " " + miny + "\" TO \"" + maxx + " " + maxy + "\"]";
        solrQuery.set("facet.heatmap.geom", geom);

        // Calculate the tile width in degrees. minx and maxx may independently wrap the date line (180 degrees).
        double tileWidth = maxx > minx ? maxx - minx : maxx - (minx - 360);

        // This is the map for the tile width (or tile height) and the facet.heatmap.gridLevel.
        // gridLevel must be between 1 and 26 inclusive for the SOLR quad index.
        // At the gridLevel 1 it is a 1x1 cell for the whole world (360 degrees x 180 degrees)
        // Add 7 grid levels to get a heatmap of size 2^7 x 2^7 grid cells (128x128) - approximately
        double[] solrGridLevelMap = new double[]{360, 180, 90, 45, 22.5, 11.25, 5.625, 2.8125, 1.40625, 0.703125, 0.3515625, 0.17578125, 0.087890625, 0.0439453125, 0.02197265625, 0.010986328125, 0.0054931640625, 0.00274658203125, 0.001373291015625, 0.0006866455078125};
        int zoomLevelByWidth = 0;
        while (zoomLevelByWidth < solrGridLevelMap.length && tileWidth < solrGridLevelMap[zoomLevelByWidth]) {
            zoomLevelByWidth++;
        }

        int zoomLevelByHeight = 0;
        while (zoomLevelByHeight + 1 < solrGridLevelMap.length && maxy - miny < solrGridLevelMap[zoomLevelByHeight + 1]) {
            zoomLevelByHeight++;
        }

        // Add 7 to the min zoom level to get the most appropriate number of cells
        int gridLevel = Math.min(zoomLevelByWidth, zoomLevelByHeight) + 7;
        solrQuery.set(
                "facet.heatmap.gridLevel",
                String.valueOf(gridLevel)); // good for points, probably

        solrQuery.setFacetLimit(-1);
        solrQuery.setFacet(true);
        solrQuery.setFilterQueries(filterQueries);
        solrQuery.setRows(0);
        solrQuery.setQuery(query);
        return solrQuery;
    }

    @Override
    public List<RecordJackKnifeStats> getOutlierStatsFor(String uuid) throws Exception {

        // query by taxonConceptID, get outlerForLayers
        SolrQuery query = new SolrQuery();
        query.setQuery("id: \"" + uuid + "\"");
        query.setFields("taxonConceptID", "outlierLayer", "el*");
        query.setRows(1);

        QueryResponse qr = query(query);
        if (qr.getResults() == null || qr.getResults().isEmpty()) {
            return new ArrayList<>();
        }

        SolrDocument doc = qr.getResults().get(0);

        Collection outlierLayers = doc.getFieldValues("outlierLayer");
        String taxonConceptID = (String) doc.getFieldValue("taxonConceptID");

        if (outlierLayers == null || outlierLayers.isEmpty() || taxonConceptID == null) {
            return new ArrayList<>();
        }

        List<RecordJackKnifeStats> statsList = new ArrayList<>();

        for (Object layerId : outlierLayers) {

            RecordJackKnifeStats stats = new RecordJackKnifeStats();
            SolrQuery layerQuery = new SolrQuery();
            layerQuery.setQuery("taxonConceptID: \"" + taxonConceptID + "\" AND outlierLayer:\"" + layerId + "\"");
            layerQuery.setFacet(true);
            layerQuery.addFacetField((String) layerId);
            layerQuery.setRows(0);
            QueryResponse facetQr = query(layerQuery);

            FacetField ff = facetQr.getFacetFields().get(0);
            List<Float> outlierFieldValues = ff.getValues().stream().map(count -> Float.parseFloat(count.getName())).collect(Collectors.toList());

            stats.setOutlierValues(outlierFieldValues);
            stats.setLayerId((String) layerId);
            stats.setRecordLayerValue((Float) doc.getFieldValue((String) layerId));
            statsList.add(stats);
        }
        return statsList;
    }

    @Override
    public int streamingQuery(SpatialSearchRequestDTO request, ProcessInterface procSearch, ProcessInterface procFacet) throws Exception {
        return indexDao.streamingQuery(initSolrQuery(request, true, null), procSearch, procFacet, null);
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#getFacetPointsShort(au.org.ala.biocache.dto.SpatialSearchRequestDTO, String, Double, Double, Double, Double)
     */
    @Override
    public FacetField getFacetPointsShort(SpatialSearchRequestDTO searchParams, String pointType, Double minx, Double miny, Double maxx, Double maxy) throws Exception {

        // limit miny maxy to -90 90
        if (miny < -90) miny = -90.0;
        if (maxy > 90) maxy = 90.0;

        // fix date line
        while (maxx > 180) {
            maxx = 180.0;
        }
        while (minx < -180) {
            minx = -180.0;
        }

        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.setQuery(searchParams.getFormattedQuery());
        solrQuery.setFilterQueries(searchParams.getFormattedFq());
        solrQuery.addFilterQuery("decimalLongitude:[" + minx + " TO " + maxx + "]");
        solrQuery.addFilterQuery("decimalLatitude:[" + miny + " TO " + maxy + "]");
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField(pointType);
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(searchParams.getFlimit());

        QueryResponse qr = query(solrQuery); // can throw exception

        List<FacetField> facets = qr.getFacetFields();

        // return first facet, there should only be 1
        if (facets != null && !facets.isEmpty()) {
            return facets.get(0);
        }
        return null;
    }
}
