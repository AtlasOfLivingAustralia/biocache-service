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
import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.service.*;
import au.org.ala.biocache.stream.OptionalZipOutputStream;
import au.org.ala.biocache.util.*;
import au.org.ala.biocache.util.solr.FieldMappingUtil;
import au.org.ala.biocache.util.thread.EndemicCallable;
import au.org.ala.biocache.writer.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.googlecode.ehcache.annotations.Cacheable;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.RangeFacet.Numeric;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import java.io.*;
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
    public static final String SOLR_DATE_FORMAT = "yyyy-MM-dd'T'hh:mm:ss'Z'";

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
    public Integer EXPORT_THREASHOLD = 10000;

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
    protected static final Integer FACET_PAGE_SIZE = 1000;
//    protected static final String RANGE_SUFFIX = "_RNG";

    protected Pattern clpField = Pattern.compile("(,|^)cl.p(,|$)");
    protected Pattern elpField = Pattern.compile("(,|^)el.p(,|$)");
    protected Pattern allDwcField = Pattern.compile("(,|^)allDwc(,|$)");

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
    protected FieldMappingUtil fieldMappingUtil;

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
     * Occurrence count where < uses pivot and > uses facet for retrieving points. Can be fine tuned with
     * multiple queries and comparing DEBUG *
     */
    @Value("${wms.legendMaxItems:30}")
    private int wmslegendMaxItems;

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

    @Value("${media.url:https://biocache.ala.org.au/biocache-media/}")
    public String biocacheMediaUrl = "https://biocache.ala.org.au/biocache-media/";

    @Value("${media.dir:/data/biocache-media/}")
    public String biocacheMediaDir = "/data/biocache-media/";

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
                if (facet.equals(OccurrenceIndex.NAMES_AND_LSID)) {
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
    public List<FieldResultDTO> getValuesForFacet(SpatialSearchRequestParams requestParams) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        writeFacetToStream(requestParams, true, false, false, false, outputStream, null);
        outputStream.flush();
        outputStream.close();
        CSVReader csvReader = new CSVReader(new StringReader(outputStream.toString(StandardCharsets.UTF_8)));
        List<FieldResultDTO> list = new ArrayList<FieldResultDTO>();
        boolean first = true;
        String [] line;
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
            Map[] fqMaps = queryFormatUtils.formatSearchQuery(searchParams, true);

            String queryString = searchParams.getFormattedQuery();
            SolrQuery solrQuery = initSolrQuery(searchParams, true, extraParams); // general search settings
            solrQuery.setQuery(queryString);    // PIPELINES: SolrQuery::setQuery entry point

            QueryResponse qr = indexDao.runSolrQuery(solrQuery, searchParams);
            //need to set the original q to the processed value so that we remove the wkt etc that is added from paramcache object
            Class resultClass;
            resultClass = includeSensitive ? au.org.ala.biocache.dto.SensitiveOccurrenceIndex.class : OccurrenceIndex.class;

            searchResults = processSolrResponse(original, qr, solrQuery, resultClass);
            searchResults.setQueryTitle(searchParams.getDisplayString());
            searchResults.setUrlParameters(original.getUrlParams());

            //now update the fq display map...
            searchResults.setActiveFacetMap(fqMaps[0]);
            searchResults.setActiveFacetObj(fqMaps[1]);

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
        solrQuery.setQuery(queryString);    // PIPELINES: SolrQuery::setQuery entry point

        //don't want any results returned
        solrQuery.setRows(0);
        searchParams.setPageSize(0);
        solrQuery.setFacetLimit(FACET_PAGE_SIZE);
        int offset = 0;
        boolean isGuid = searchParams.getFacets()[0].contains("_guid") ||
                searchParams.getFacets()[0].endsWith("ID");
        boolean isLsid = searchParams.getFacets()[0].contains("_lsid") || searchParams.getFacets()[0].contains(OccurrenceIndex.TAXON_CONCEPT_ID);
        boolean shouldLookupTaxon = lookupName && (isLsid || isGuid);
        boolean isUid = searchParams.getFacets()[0].contains("_uid") || searchParams.getFacets()[0].endsWith("Uid");
        boolean shouldLookupAttribution = lookupName && isUid;

        if (dd != null) {
            dd.resetCounts();
        }

        QueryResponse qr = indexDao.runSolrQuery(solrQuery, searchParams);
        if (logger.isDebugEnabled()) {
            logger.debug("Retrieved facet results from server...");
        }
        if (qr.getResults().getNumFound() > 0) {
            FacetField ff = qr.getFacetField(searchParams.getFacets()[0]);

            //write the header line
            if (ff != null) {
                String[] header = new String[]{ff.getName()};
                if (shouldLookupTaxon) {
                    header = speciesLookupService.getHeaderDetails(fieldMappingUtil.translateFieldName(ff.getName()), includeCount, includeSynonyms);
                } else if (shouldLookupAttribution) {
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
                        qr = indexDao.runSolrQuery(solrQuery, searchParams);
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
        solrQuery.setQuery(searchParams.getQ());    // PIPELINES: SolrQuery::setQuery entry point

        QueryResponse qr = indexDao.runSolrQuery(solrQuery, srp);
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
                if (requestedFieldsParam.contains(",decimalLatitude")) {
                    requestedFieldsParam = requestedFieldsParam.replaceFirst(",decimalLatitude", ",sensitive_decimalLatitude,sensitive_decimalLongitude,decimalLatitude");
                } else if (requestedFieldsParam.contains("raw_decimalLatitude")) {
                    requestedFieldsParam = requestedFieldsParam.replaceFirst("raw_decimalLatitude", "sensitive_decimalLatitude,sensitive_decimalLongitude,raw_decimalLatitude");
                }
                if (requestedFieldsParam.contains(",raw_locality,")) {
                    requestedFieldsParam = requestedFieldsParam.replaceFirst(",raw_locality,", ",raw_locality,sensitive_locality,");
                }
                if (requestedFieldsParam.contains(",locality,")) {
                    requestedFieldsParam = requestedFieldsParam.replaceFirst(",locality,", ",locality,sensitive_locality,");
                }
            }

            StringBuilder dbFieldsBuilder = new StringBuilder(requestedFieldsParam);
            if (!downloadParams.getExtra().isEmpty()) {
                dbFieldsBuilder.append(",").append(downloadParams.getExtra());
            }

            List<String> requestedFields = Arrays.stream(dbFieldsBuilder.toString()
                    .split(","))
                    .map(field -> fieldMappingUtil.translateFieldName(field))
                    .collect(Collectors.toList());

            List<String>[] indexedFields;
            if (downloadFields == null) {
                //default to include everything
                java.util.List<String> mappedNames = new java.util.LinkedList<>();
                for (int i = 0; i < requestedFields.size(); i++) {
                    mappedNames.add(requestedFields.get(i));
                }
                indexedFields = new List[]{
                        mappedNames,
                        new java.util.LinkedList<String>(),
                        mappedNames,
                        mappedNames,
                        new ArrayList(),
                        new ArrayList()};
            } else {
                indexedFields = downloadFields.getIndexFields(
                        requestedFields.toArray(new String[0]),
                        downloadParams.getDwcHeaders(),
                        downloadParams.getLayersServiceUrl()
                );
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
                logger.debug("Species List headers: " + indexedFields[6]);
                logger.debug("Species List fields: " + indexedFields[7]);
            }

            //set the fields to the ones that are available in the index
            String[] fields = indexedFields[0].toArray(new String[]{});
            solrQuery.setFields(fields);
            StringBuilder qasb = new StringBuilder();
            if (!"none".equals(downloadParams.getQa())) {
                solrQuery.addField("assertions"); // PIPELINES: SolrQuery::addField entry point
                if (!"all".equals(downloadParams.getQa()) && !"includeall".equals(downloadParams.getQa())) {
                    //add all the qa fields
                    qasb.append(downloadParams.getQa());
                }
            }

            solrQuery.addField(OccurrenceIndex.INSTITUTION_UID)
                    .addField(OccurrenceIndex.COLLECTION_UID)
                    .addField(OccurrenceIndex.DATA_RESOURCE_UID)
                    .addField(OccurrenceIndex.DATA_PROVIDER_UID);

            // 'lft' and 'rgt' is mandatory when there are species list fields (indexedFields[7])
            if (indexedFields[7].size() > 0) {
                String lft = OccurrenceIndex.LFT;
                String rgt = OccurrenceIndex.RGT;
                if (!solrQuery.getFields().matches("($" + lft + ",|," + lft + ",|," + lft + "^)")) {
                    solrQuery.addField(lft);
                }
                if (!solrQuery.getFields().matches("($" + rgt + ",|," + rgt + ",|," + rgt + "^)")) {
                    solrQuery.addField(rgt);
                }
            }

            solrQuery.setQuery(downloadParams.getFormattedQuery()); // PIPELINES: SolrQuery::setQuery entry point
            solrQuery.setFacetMinCount(1);
            solrQuery.setFacetLimit(-1);

            //get the assertion facets to add them to the download fields
            boolean getAssertionsFromFacets = "all".equals(downloadParams.getQa()) || "includeall".equals(downloadParams.getQa());
            SolrQuery monthAssertionsQuery = getAssertionsFromFacets ? solrQuery.getCopy().addFacetField(OccurrenceIndex.MONTH, OccurrenceIndex.ASSERTIONS) : solrQuery.getCopy().addFacetField(OccurrenceIndex.MONTH);
            if (getAssertionsFromFacets) {
                //set the order for the facet to be based on the index - this will force the assertions to be returned in the same order each time
                //based on alphabetical sort.  The number of QA's may change between searches so we can't guarantee that the order won't change
                monthAssertionsQuery.add("f." + OccurrenceIndex.ASSERTIONS + ".facet.sort", "index");
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

            // include all misc fields if required
            if (dd.getRequestParams() != null ? dd.getRequestParams().getIncludeMisc() : false) {
                for (IndexFieldDTO f : indexDao.getIndexFieldDetails()) {
                    // identify misc fields that are in the index
                    if (f.isStored() && f.getName() != null && f.getName().startsWith("_"))
                        solrQuery.addField(f.getName());    // PIPELINES: SolrQuery::addField entry point
                }
                // include record sensitive flag
                if (!solrQuery.getFields().contains("," + OccurrenceIndex.SENSITIVE + ",")) {
                    solrQuery.addField(OccurrenceIndex.SENSITIVE);
                }
            }

            //get the month facets to add them to the download fields get the assertion facets.
            List<Count> splitByFacet = null;

            for (FacetField facet : facetQuery.getFacetFields()) {
                if (facet.getName().equals(OccurrenceIndex.ASSERTIONS) && facet.getValueCount() > 0) {
                    qasb.append(getQAFromFacet(facet));
                }
                if (facet.getName().equals(OccurrenceIndex.MONTH) && facet.getValueCount() > 0) {
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
                List<String>[] sensitiveHdr;
                sensitiveHdr = downloadFields.getIndexFields(sensitiveSOLRHdr, downloadParams.getDwcHeaders(), downloadParams.getLayersServiceUrl());

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

            //add species list headers
            indexedFields[2].addAll(indexedFields[6]);
            final String[] speciesListFields = indexedFields[7].toArray(new String[0]);

            final String[] qaFields = qas.equals("") ? new String[]{} : qas.split(",");
            String[] qaTitles = downloadFields.getHeader(qaFields, false, false);

            String[] header = org.apache.commons.lang3.ArrayUtils.addAll(indexedFields[2].toArray(new String[]{}), qaTitles);

            //retain output header fields and field names for inclusion of header info in the download
            StringBuilder infoFields = new StringBuilder("infoFields");
            for (String h : indexedFields[3]) infoFields.append(",").append(h);
            for (String h : analysisFields) infoFields.append(",").append(h);
            for (String h : speciesListFields) infoFields.append(",").append(h);
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

                String[] notSensitiveSOLRHdr = OccurrenceIndex.notSensitiveSOLRHdr;

                //split into sensitive and non-sensitive queries when
                // - not including all sensitive values
                // - there is a sensitive fq
                final List<SolrQuery> sensitiveQ = new ArrayList<SolrQuery>();
                if (!includeSensitive && dd.getSensitiveFq() != null) {
                    sensitiveQ.addAll(splitQueries(queries, dd.getSensitiveFq(), sensitiveSOLRHdr, notSensitiveSOLRHdr));
                }

                final AtomicInteger resultsCount = new AtomicInteger(0);
                final boolean threadCheckLimit = checkLimit;
                final ArrayList<String> miscFields = new ArrayList<String>(0);

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

                            splitByFacetQuery.setFilterQueries(fq);     // PIPELINES: SolarQuery::setFilterQueries entry point

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
                                    count = processQueryResults(uidStats, sensitiveFields, qaFields, concurrentWrapper, qr, dd, threadCheckLimit, resultsCount, maxDownloadSize, analysisFields, speciesListFields, miscFields, true);
                                } else {
                                    // write non-sensitive values into sensitive fields when not authorised for their sensitive values
                                    count = processQueryResults(uidStats, notSensitiveFields, qaFields, concurrentWrapper, qr, dd, threadCheckLimit, resultsCount, maxDownloadSize, analysisFields, speciesListFields, miscFields, false);
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

                // this will trigger DownloadService to add the discovered, non-empty miscFields to the output header and fields description file.
                if (dd != null && miscFields.size() > 0) {
                    String[] newMiscFields = new String[miscFields.size()];
                    miscFields.toArray(newMiscFields);
                    dd.setMiscFields(newMiscFields);
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
                    if (sd.containsKey(SensitiveOccurrenceIndex.SENSITIVE_LONGITUDE) && sd.containsKey(SensitiveOccurrenceIndex.SENSITIVE_LATITUDE)) {
                        points[i][0] = (double) sd.getFirstValue(SensitiveOccurrenceIndex.SENSITIVE_LONGITUDE);
                        points[i][1] = (double) sd.getFirstValue(SensitiveOccurrenceIndex.SENSITIVE_LATITUDE);
                    } else if (sd.containsKey(OccurrenceIndex.LATITUDE) && sd.containsKey(OccurrenceIndex.LONGITUDE)) {
                        points[i][0] = (double) sd.getFirstValue(OccurrenceIndex.LONGITUDE);
                        points[i][1] = (double) sd.getFirstValue(OccurrenceIndex.LATITUDE);
                    } else {
                        points[i][0] = 0;
                        points[i][1] = 0;
                        invalid++;
                    }
                    i++;
                }

                if (invalid < results.size()) {
                    Reader reader = layersService.sample(analysisLayers, points, null);

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

    private int processQueryResults(ConcurrentMap<String, AtomicInteger> uidStats, String[] fields, String[] qaFields,
                                    RecordWriter rw, QueryResponse qr, DownloadDetailsDTO dd, boolean checkLimit,
                                    AtomicInteger resultsCount, long maxDownloadSize, String[] analysisLayers,
                                    String[] speciesListFields,
                                    List<String> miscFields, Boolean sensitiveDataAllowed) {
        //handle analysis layer intersections
        List<String[]> intersection = intersectResults(dd.getRequestParams().getLayersServiceUrl(), analysisLayers, qr.getResults());

        String institutionUid = OccurrenceIndex.INSTITUTION_UID;
        String collectionUid = OccurrenceIndex.COLLECTION_UID;
        String dataProviderUid = OccurrenceIndex.DATA_PROVIDER_UID;
        String dataResourceUid = OccurrenceIndex.DATA_RESOURCE_UID;

        int count = 0;
        int record = 0;
        for (SolrDocument sd : qr.getResults()) {
            if (sd.getFieldValue(dataResourceUid) != null && (!checkLimit || (checkLimit && resultsCount.intValue() < maxDownloadSize))) {

                //resultsCount++;
                count++;
                resultsCount.incrementAndGet();

                //add the record
                String[] values = new String[fields.length + analysisLayers.length + speciesListFields.length + qaFields.length];

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
                            values[j] = formatValue(value);

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

                // add species list fields
                if (speciesListFields.length > 0) {
                    String lftString = String.valueOf(sd.getFieldValue(OccurrenceIndex.LFT));
                    String rgtString = String.valueOf(sd.getFieldValue(RGT));
                    if (StringUtils.isNumeric(lftString)) {
                        long lft = Long.parseLong(lftString);
                        long rgt = Long.parseLong(rgtString);
                        Kvp lftrgt = new Kvp(lft, rgt);

                        String drDot = ".";
                        String dr = "";
                        int fieldIdx = 0;
                        for (int i = 0; i < speciesListFields.length; i++) {
                            if (speciesListFields[i].startsWith(drDot)) {
                                fieldIdx++;
                            } else {
                                dr = speciesListFields[i].split("\\.", 2)[0];
                                drDot = dr + ".";
                                fieldIdx = 0;
                            }

                            values[analysisLayers.length + fields.length + i] =
                                    listsService.getKvpValue(
                                            fieldIdx,
                                            listsService.getKvp(dr),
                                            lftrgt);
                        }
                    }
                }

                //now handle the assertions
                java.util.Collection<Object> assertions = sd.getFieldValues(ASSERTIONS);

                //Handle the case where there a no assertions against a record
                if (assertions == null) {
                    assertions = Collections.EMPTY_LIST;
                }

                for (int k = 0; k < qaFields.length; k++) {
                    values[fields.length + analysisLayers.length + speciesListFields.length + k] = Boolean.toString(assertions.contains(qaFields[k]));
                }

                // append previous and new non-empty misc fields
                // do not include misc fields if this is a sensitive record and sensitive data is not permitted
                if (dd != null && dd.getRequestParams() != null && dd.getRequestParams().getIncludeMisc() &&
                        (sensitiveDataAllowed || "Not sensitive".equals(formatValue(sd.getFieldValue(SENSITIVE))) ||
                                "".equals(formatValue(sd.getFieldValue(SENSITIVE))))) {

                    // append miscValues for columns found
                    List<String> miscValues = new ArrayList<String>(miscFields.size());  // TODO: reuse

                    // maintain miscFields order using synchronized
                    synchronized (miscFields) {
                        for (String f : miscFields) {
                            miscValues.add(formatValue(sd.getFieldValue(f)));
                            //clear field to avoid repeating the value when looking for new miscValues
                            sd.setField(f, null);
                        }
                        // find and append new miscValues
                        for (String key : sd.getFieldNames()) {
                            if (key != null && key.startsWith("_")) {
                                String value = formatValue(sd.getFieldValue(key));
                                if (StringUtils.isNotEmpty(value)) {
                                    miscValues.add(value);
                                    miscFields.add(key);
                                }
                            }
                        }
                    }

                    // append miscValues to values
                    if (miscValues.size() > 0) {
                        String[] newValues = new String[miscValues.size() + values.length];
                        System.arraycopy(values, 0, newValues, 0, values.length);
                        for (int i = 0; i < miscValues.size(); i++) {
                            newValues[values.length + i] = miscValues.get(i);
                        }
                        values = newValues;
                    }
                }

                rw.write(values);

                //increment the counters....
                incrementCount(uidStats, sd.getFieldValue(institutionUid));
                incrementCount(uidStats, sd.getFieldValue(collectionUid));
                incrementCount(uidStats, sd.getFieldValue(dataProviderUid));
                incrementCount(uidStats, sd.getFieldValue(dataResourceUid));
            }

            record++;
        }
        dd.updateCounts(count);
        return count;
    }

    private String formatValue(Object value) {
        if (value instanceof Date) {
            return value == null ? "" : org.apache.commons.lang.time.DateFormatUtils.format((Date) value, "yyyy-MM-dd");
        } else {
            return value == null ? "" : value.toString();
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
    private void expandRequestedFields(DownloadRequestParams downloadParams, boolean isSolr) {
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
        if (StringUtils.isEmpty(dFields)) {
            dFields = defaultDownloadFields;
        }
        return dFields;
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
                Arrays.stream(notFqFields).forEach(nsq::addField);  // PIPELINES: SolrQuery::addField entry point
            }
            notFQ.add(nsq);

            SolrQuery sq = query.getCopy().addFilterQuery(fq);
            if (fqFields != null) {
                Arrays.stream(fqFields).forEach(sq::addField);      // PIPELINES: SolrQuery::addField entry point
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
        solrQuery.setQuery(searchParams.getFormattedQuery());   // PIPELINES: SolrQuery::setQuery entry point
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField(pointType.getLabel());  // PIPELINES: SolrQuery::addFacetField entry point
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
        solrQuery.setQuery(searchParams.getFormattedQuery());   // PIPELINES: SolrQuery::setQuery entry point
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField(pointType); // PIPELINES: SolrQuery::addFacetField entry point
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

        QueryResponse qr = indexDao.runSolrQuery(solrQuery, searchParams);
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
    //IS THIS BEING USED BY ANYTHING?? maybe dashboard?
    @Override
    @Deprecated
    public List<DataProviderCountDTO> getDataProviderCounts() throws Exception {

        List<DataProviderCountDTO> dpDTOs = new ArrayList<DataProviderCountDTO>(); // new OccurrencePoint(PointType.POINT);
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.setQuery("*:*");  // PIPELINES: SolrQuery::setQuery entry point
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField(OccurrenceIndex.DATA_PROVIDER_UID);
        solrQuery.addFacetField(OccurrenceIndex.DATA_PROVIDER_NAME);
        solrQuery.setFacetMinCount(1);
        QueryResponse qr = runSolrQuery(solrQuery, null, 1, 0, OccurrenceIndex.DATA_PROVIDER_NAME, "asc");
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
    @Deprecated
    public List<FieldResultDTO> findRecordByStateFor(String query)
            throws Exception {
        String dataProviderName = OccurrenceIndex.DATA_PROVIDER_NAME;
        List<FieldResultDTO> fDTOs = new ArrayList<FieldResultDTO>(); // new OccurrencePoint(PointType.POINT);
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.setQuery(query);  // PIPELINES: SolrQuery::setQuery entry point
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField(STATE);
        solrQuery.setFacetMinCount(1);
        QueryResponse qr = runSolrQuery(solrQuery, null, 1, 0, dataProviderName, "asc");
        FacetField ff = qr.getFacetField(STATE);
        if (ff != null) {
            for (Count count : ff.getValues()) {
                //only start adding counts when we hit a decade with some results.
                if (count.getCount() > 0) {
                    FieldResultDTO f = new FieldResultDTO(count.getName(), STATE + "." + count.getName(), count.getCount());
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
        solrQuery.setQuery(queryParams.getFormattedQuery());    // PIPELINES: SolrQuery::setQuery entry point
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
                solrQuery.addFacetField(r); // PIPELINES: SolrQuery::addFacetField entry point
            }
        } else {
            //the user has supplied the "exact" level at which to perform the breakdown
            solrQuery.addFacetField(queryParams.getLevel());    // PIPELINES: SolrQuery::addFacetField entry point
        }
        QueryResponse qr = indexDao.runSolrQuery(solrQuery, queryParams);
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
            solrQuery.setQuery(query);  // PIPELINES: SolrQuery::setQuery entry point
            solrQuery.setRows(0);
            solrQuery.setFacet(true);
            solrQuery.setFacetMinCount(1);
            solrQuery.setFacetSort("count");
            solrQuery.setFacetLimit(-1); //we want all facets
            for (String r : ranks) {
                solrQuery.addFacetField(r); // PIPELINES: SolrQuery::addFacetField entry point
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
                                       Integer startIndex, String sortField, String sortDirection) throws Exception {
        SearchRequestParams requestParams = new SearchRequestParams();
        requestParams.setFq(filterQuery);
        requestParams.setFormattedFq(filterQuery);
        requestParams.setPageSize(pageSize);
        requestParams.setStart(startIndex);
        requestParams.setSort(sortField);
        requestParams.setDir(sortDirection);
        return indexDao.runSolrQuery(solrQuery, requestParams);
    }

    /**
     * Perform SOLR query - takes a SolrQuery and search params
     *
     * @param solrQuery
     * @return
     * @throws SolrServerException
     */
    private QueryResponse runSolrQueryWithCursorMark(SolrQuery solrQuery, int pageSize, String cursorMark) throws Exception {

        //include null facets
        solrQuery.setFacetMissing(true);
        solrQuery.setRows(pageSize);

        //if set to true, use the cursor mark - for better deep paging performance
        if (cursorMark == null) {
            cursorMark = CursorMarkParams.CURSOR_MARK_START;
        }
        solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);

        //if using cursor mark, avoid sorting
        solrQuery.setSort(ID, SolrQuery.ORDER.desc);

        if (logger.isDebugEnabled()) {
            logger.debug("SOLR query (cursor mark): " + solrQuery.toString());
        }
        QueryResponse qr = query(solrQuery); // can throw exception
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
        //Solr 6.x don't use facet.date but facet.range instead
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

        String occurrenceDate = OccurrenceIndex.OCCURRENCE_DATE;
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        boolean rangeAdded = false;
        // Facets
        solrQuery.setFacet(searchParams.getFacet());
        if (searchParams.getFacet()) {
            for (String facet : searchParams.getFacets()) {
                if (facet.equals(DECADE_FACET_NAME) || facet.equals("date")) {
                    String fname = facet.equals(DECADE_FACET_NAME) ? OCCURRENCE_YEAR_INDEX_FIELD : occurrenceDate;
                    initDecadeBasedFacet(solrQuery, fname);
                    rangeAdded = true; // the initDecadeBasedFacet adds a range
                } else if (facet.equals("uncertainty")) {
                    Map<String, String> rangeMap = rangeBasedFacets.getRangeMap("uncertainty");
                    for (String range : rangeMap.keySet()) {
                        solrQuery.add("facet.query", range);
                    }
//                } else if (facet.endsWith(RANGE_SUFFIX)) {
//                    //this facte need to have it ranges included.
//                    if (!rangeAdded) {
//                        solrQuery.add("facet.range.other", "before");
//                        solrQuery.add("facet.range.other", "after");
//                    }
//                    String field = facet.replaceAll(RANGE_SUFFIX, "");
//                    StatsIndexFieldDTO details = indexDao.getRangeFieldDetails(field);
//                    if (details != null) {
//                        solrQuery.addNumericRangeFacet(field, details.getStart(), details.getEnd(), details.getGap());
//                    }
                } else {

                    solrQuery.addFacetField(facet); // PIPELINES: SolrQuery::addFacetField entry point

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

            solrQuery.setFields(searchParams.getFl()); // PIPELINES: SolrQuery::setFields entry point
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
                                                  Integer startIndex, String sortField, String sortDirection) throws Exception {

        List<TaxaCountDTO> speciesCounts = new ArrayList<TaxaCountDTO>();
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.setQuery(queryString);    // PIPELINES: SolrQuery::setQuery entry point

        if (filterQueries != null && filterQueries.size() > 0) {
            //solrQuery.addFilterQuery("(" + StringUtils.join(filterQueries, " OR ") + ")");
            for (String fq : filterQueries) {
                solrQuery.addFilterQuery(fq);   // PIPELINES: SolarQuery::addFilterQuery entry point
            }
        }
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.setFacetSort(sortField);
        for (String facet : facetFields) {
            solrQuery.addFacetField(facet); // PIPELINES: SolrQuery::addFacetField entry point
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
                                    tcDTO.setCommonName("null".equals(values[2]) ? "" : values[2]);
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
                                    tcDTO.setCommonName("null".equals(values[0]) ? "" : values[0]);
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
        solrQuery.setQuery(searchParams.getFormattedQuery());   // PIPELINES: SolrQuery::setQuery entry point
        solrQuery.setRequestHandler("standard");
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.setFacetMinCount(1);

        solrQuery.addField(OccurrenceIndex.INSTITUTION_UID)
                .addField(OccurrenceIndex.COLLECTION_UID)
                .addField(OccurrenceIndex.DATA_RESOURCE_UID)
                .addField(OccurrenceIndex.DATA_PROVIDER_UID);

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
     * Returns the count of distinct values for the facets.
     * This is an altered implementation that is SOLRCloud friendly (ngroups are not SOLR Cloud compatible)
     *
     * The group count is only accurate when foffset == 0
     */
    public List<FacetResultDTO> getFacetCounts(SpatialSearchRequestParams searchParams) throws Exception {

        queryFormatUtils.formatSearchQuery(searchParams, true);
        String queryString = searchParams.getFormattedQuery();
        searchParams.setFacet(true);
        searchParams.setPageSize(0);
        SolrQuery facetQuery = initSolrQuery(searchParams, false, null);
        facetQuery.setQuery(queryString);   // PIPELINES: SolrQuery::setQuery entry point
        facetQuery.setFields(null);
        facetQuery.setRows(0);
        facetQuery.setFacetLimit(-1);

        List<String> fqList = new ArrayList<String>();
        //only add the FQ's if they are not the default values
        if (searchParams != null && searchParams.getFormattedFq() != null && searchParams.getFormattedFq().length > 0) {
            org.apache.commons.collections.CollectionUtils.addAll(fqList, searchParams.getFormattedFq());
        }

        facetQuery.setFilterQueries(fqList.stream().toArray(String[]::new));    // PIPELINES: SolarQuery::setFilterQueries entry point

        QueryResponse qr = query(facetQuery);
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
                if (searchParams.getFlimit() != null && searchParams.getFlimit() < fr.getFieldResult().size() &&
                        searchParams.getFlimit() >= 0) {
                    fr.setFieldResult(fr.getFieldResult().subList(0, searchParams.getFlimit()));
                }
            }
        }
        return facetResults;
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
            solrQuery.setQuery(queryString);    // PIPELINES: SolrQuery::setQuery entry point
            solrQuery.setFields(searchParams.getFl().split(",")); // PIPELINES: field names entry point
            solrQuery.setFacet(false);
            solrQuery.setRows(searchParams.getPageSize());

            sdl = indexDao.runSolrQuery(solrQuery, searchParams).getResults();
        } catch (SolrServerException ex) {
            logger.error("Problem communicating with SOLR server. " + ex.getMessage(), ex);
        }

        return sdl;
    }

    public List<LegendItem> getLegend(SpatialSearchRequestParams searchParams, String facetField, String[] cutpoints) throws Exception {
        return getLegend(searchParams, facetField, cutpoints, false);
    }

    @Cacheable(cacheName = "legendCache")
    public List<LegendItem> getLegend(SpatialSearchRequestParams searchParams, String facetField, String[] cutpoints, boolean skipI18n) throws Exception {
        List<LegendItem> legend = new ArrayList<LegendItem>();

        queryFormatUtils.formatSearchQuery(searchParams);
        if (logger.isInfoEnabled()) {
            logger.info("search query: " + searchParams.getFormattedQuery());
        }
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.setQuery(searchParams.getFormattedQuery());   // PIPELINES: SolrQuery::setQuery entry point
        solrQuery.setRows(0);
        solrQuery.setFacet(true);

        //is facet query?
        if (cutpoints == null) {
            solrQuery.addFacetField(facetField);    // PIPELINES: SolrQuery::addFacetField entry point
        } else {
            solrQuery.addFacetQuery("-" + facetField + ":*");

            for (int i = 0; i < cutpoints.length; i += 2) {
                solrQuery.addFacetQuery(facetField + ":[" + cutpoints[i] + " TO " + cutpoints[i + 1] + "]");
            }
        }

        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(-1);//MAX_DOWNLOAD_SIZE);  // unlimited = -1

        FacetDTO facetDTO = FacetThemes.getFacetsMap().get(fieldMappingUtil.translateFieldName(facetField));
        if (facetDTO != null) {
            String thisSort = facetDTO.getSort();
            if (thisSort != null) {
                solrQuery.setFacetSort(thisSort);
            }
        }
        solrQuery.setFacetMissing(true);

        QueryResponse qr = runSolrQuery(solrQuery, searchParams.getFormattedFq(), 1, 0, "score", "asc");
        List<FacetField> facets = qr.getFacetFields();  // TODO: PIPELINES: QueryResponse::getFacetFields entry point
        if (facets != null) {
            for (FacetField facet : facets) {           // TODO: PIPELINES: List<FacetField>::iterator entry point
                List<FacetField.Count> facetEntries = facet.getValues();    // TODO: PIPELINES: FacetField::getValues entry point
                if (facet.getName().contains(facetField) && facetEntries != null && !facetEntries.isEmpty()) {  // TODO: PIPELINES: FacetField::getName & List<FacetField.Count>::size entry point

                    List<String> addedFqs = new ArrayList<>();

                    for (int i = 0; i < facetEntries.size() && i < wmslegendMaxItems; i++) {         // TODO: PIPELINES: List<FacetField.Count>::size entry point
                        FacetField.Count fcount = facetEntries.get(i);  // TODO: PIPELINES: List<FacetField.Count>::get entry point
                        if (fcount.getCount() > 0) {                    // TODO: PIPELINES: FacetField.Count::getCount entry point
                            String fq = facetField + ":\"" + fcount.getName() + "\"";   // TODO: PIPELINES: FacetField.Count::getName entry point
                            if (fcount.getName() == null) {             // TODO: PIPELINES: FacetField.Count::getName entry point
                                fq = "-" + facetField + ":*";
                                addedFqs.add(facetField + ":*");
                            } else {
                                addedFqs.add("-" + fq);
                            }

                            if (skipI18n) {
                                legend.add(new LegendItem(fcount.getName(), null, fcount.getName(), fcount.getCount(), fq));  // TODO: PIPELINES: FacetField.Count::getName & FacetField.Count::getCount entry point
                            } else {
                                String i18nCode = null;
                                if (StringUtils.isNotBlank(fcount.getName())) {
                                    i18nCode = fieldMappingUtil.translateFieldName(facetField) + "." + fcount.getName();
                                } else {
                                    i18nCode = fieldMappingUtil.translateFieldName(facetField) + ".novalue";
                                }

                                legend.add(new LegendItem(
                                        getFacetValueDisplayName(fieldMappingUtil.translateFieldName(facetField), fcount.getName()),
                                        i18nCode,
                                        fcount.getName(),
                                        fcount.getCount(),
                                        fq)
                                );
                            }
                        }
                    }

                    if (facetEntries.size() > wmslegendMaxItems){

                        long remainderCount = 0;
                        for (int i = wmslegendMaxItems; i < facetEntries.size(); i++) {
                            FacetField.Count fcount = facetEntries.get(i);
                            remainderCount += fcount.getCount();
                        }

                        String theFq = "-(" + StringUtils.join(addedFqs, " AND ") +")";
                            // create a single catch remainder facet
                        legend.add(legend.size(), new LegendItem(
                            "Other " + facetField,
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

        String tFacetField = fieldMappingUtil.translateFieldName(facetField);

        //check if we have query based facets
        Map<String, Integer> facetq = qr.getFacetQuery();
        if (facetq != null && facetq.size() > 0) {
            for (Entry<String, Integer> es : facetq.entrySet()) {
                legend.add(new LegendItem( getFacetValueDisplayName(tFacetField, es.getKey()), tFacetField + "." + es.getKey(), es.getKey(), es.getValue(), es.getKey()));
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
        return legend;
    }

    public FacetField getFacet(SpatialSearchRequestParams searchParams, String facet) throws Exception {
        queryFormatUtils.formatSearchQuery(searchParams);
        if (logger.isInfoEnabled()) {
            logger.info("search query: " + searchParams.getFormattedQuery());
        }
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.setQuery(searchParams.getFormattedQuery());   // PIPELINES: SolrQuery::setQuery entry point
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField(facet); // PIPELINES: SolrQuery::addFacetField entry point
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(-1); //MAX_DOWNLOAD_SIZE);  // unlimited = -1

        QueryResponse qr = runSolrQuery(solrQuery, searchParams.getFormattedFq(), 1, 0, null, null);
        return qr.getFacetFields().get(0);
    }

    public List<DataProviderCountDTO> getDataProviderList(SpatialSearchRequestParams requestParams) throws Exception {
        List<DataProviderCountDTO> dataProviderList = new ArrayList<DataProviderCountDTO>();
        String dataProviderUid = OccurrenceIndex.DATA_PROVIDER_UID;
        FacetField facet = getFacet(requestParams, dataProviderUid);
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
        if (requestParams.getFormattedFq() != null && requestParams.getFormattedFq().length > 0) {
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
        if (filterQueries != null && filterQueries.length > 0) {
            solrQuery.setFilterQueries(filterQueries);  // PIPELINES: SolarQuery::setFilterQueries entry point
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
        solrQuery.setQuery(sb.toString());  // PIPELINES: SolrQuery::setQuery entry point

        //solrQuery.add("facet.query", "confidence:" + os.getRange());
        QueryResponse qr = runSolrQuery(solrQuery, null, 1, 0, "score", "asc");
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
    public QueryResponse searchGroupedFacets(SpatialSearchRequestParams searchParams) throws Exception {
        queryFormatUtils.formatSearchQuery(searchParams);
        String queryString = searchParams.getFormattedQuery();

        //get facet group counts
        SolrQuery solrQuery = initSolrQuery(searchParams, false, null);
        solrQuery.setQuery(queryString);    // PIPELINES: SolrQuery::setQuery entry point
        solrQuery.setRows(0);
        solrQuery.setFacet(false);

        StringBuilder sb = new StringBuilder("{");
        int facets = 0;
        for (String facet : searchParams.getFacets()) {
            if (StringUtils.isNotEmpty(searchParams.getFl())) {
                if (facets > 0) sb.append(",");
                facets++;

                sb.append(facet).append(":{type:terms,limit:-1,sort:index,field:").append(facet).append(",facet:{");

                int fls = 0;
                for (String fl : searchParams.getFl().split(",")) {
                    if (fls > 0) sb.append(",");
                    fls++;

                    sb.append(fl).append(":{type:terms,limit:1,sort:index,field:").append(fl).append("}");
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
        if (facet.endsWith("_uid") || facet.endsWith("Uid")) {
            return searchUtils.getUidDisplayString(facet, value, false);
        } else if (OccurrenceIndex.OCCURRENCE_YEAR_INDEX_FIELD.equals(facet) && value != null) {
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
                            fieldMappingUtil.translateFieldName(facet) + "." + value,
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
        query.setQuery(queryString);    // PIPELINES: SolrQuery::setQuery entry point
        query.setFields(null);
        //now use the supplied facets to add groups to the query
        query.add("facet.pivot", pivot);
        query.add("facet.pivot.mincount", "1");
        query.add("facet.missing", "true");
        query.setRows(0);
        searchParams.setPageSize(0);
        QueryResponse response = indexDao.runSolrQuery(query, searchParams);
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
    public List<FieldStatsItem> searchStat(SpatialSearchRequestParams searchParams, String field, String facet,
                                           Collection<String> statType) throws Exception {
        searchParams.setFacets(new String[]{});

        queryFormatUtils.formatSearchQuery(searchParams);
        String queryString = searchParams.getFormattedQuery();

        if (facet != null) searchParams.setFacet(true);

        //get facet group counts
        SolrQuery query = initSolrQuery(searchParams, false, null);
        query.setQuery(queryString);    // PIPELINES: SolrQuery::setQuery entry point
        query.setFields(null);
        //query.setFacetLimit(-1);

        //stats parameters
        query.add("stats", "true");
        if (facet != null) query.add("stats.facet", facet);
        query.add("stats.field", "{!" + StringUtils.join(statType, "=true ") + "=true}" + field);

        query.setRows(0);
        searchParams.setPageSize(0);
        QueryResponse response = indexDao.runSolrQuery(query, searchParams);

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
                    li = new LegendItem(">0", "", "", 0, null);
                } else {
                    li = new LegendItem(String.valueOf(i),  "", "", 0, null);
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
                List<LegendItem> legend = getLegend(requestParams, s[0], cutpoints, true);

                if (cutpoints == null) {     //do not sort if cutpoints are provided
                    java.util.Collections.sort(legend);
                }
                int offset = 0;

                for (int i = 0; i < legend.size(); i++) {

                    LegendItem li = legend.get(i);

                    colours.add(new LegendItem(li.getName(), li.getName(), li.getFacetValue(), li.getCount(), li.getFq(), li.isRemainder()));
                    int colour = DEFAULT_COLOUR;
                    if (cutpoints == null) {
                        colour = ColorUtil.colourList[i];
                    } else if (cutpoints != null && i - offset < cutpoints.length) {
                        if (li.isRemainder()){
                            colour = ColorUtil.getRangedColour(i - offset, cutpoints.length / 2);
                        } else if ( StringUtils.isEmpty(legend.get(i).getName())
                                || legend.get(i).getName().equals("Unknown")
                                || legend.get(i).getName().startsWith("-")
                            ) {
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
        String latitude = OccurrenceIndex.LATITUDE;
        String longitude = OccurrenceIndex.LONGITUDE;

        double[] bbox = new double[4];
        String[] sort = {longitude, latitude, longitude, latitude};
        String[] dir = {"asc", "asc", "desc", "desc"};

        //Filter for -180 +180 longitude and -90 +90 latitude to match WMS request bounds.
        String[] bounds = new String[]{longitude + ":[-180 TO 180]", latitude + ":[-90 TO 90]"};

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
        solrQuery.setQuery(queryString);    // PIPELINES: SolrQuery::setQuery entry point
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

        for (IndexFieldDTO s : indexDao.getIndexedFields()) {
            // this only works for non-tri fields
            if (!s.getDataType().startsWith("t")) {
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
    @Cacheable(cacheName = "heatmapCache")
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
        if (miny < -90) miny = 90.0;
        if (maxy > 90) maxy = 90.0;

        // fix date line
        while (maxx > 180) {
            maxx -= 360;
            minx -= 360;
        }
        while (minx < -180) {
            maxx += 360;
            minx += 360;
        }

        // single layers
        if (gridSizeInPixels > 1 || legend == null || legend.isEmpty()) {
            // single layer
            QueryResponse qr = null;
            int zoomOffset = 0;
            while (qr == null && zoomOffset < 10) {
                try {
                    SolrQuery solrQuery =
                            createHeatmapQuery(
                                    query, filterQueries, minx, miny, maxx, maxy, gridSizeInPixels, zoomOffset);
                    qr = query(solrQuery); // can throw exception
                } catch (Exception e) {
                    zoomOffset++;
                }
            }
            // FIXME UGLY - not needed with SOLR8, but current constraint is SOLR 6 API
            // See SpatialHeatmapFacets.HeatmapFacet in SOLR 8 API
            SimpleOrderedMap facetHeatMaps =
                    ((SimpleOrderedMap)
                            ((SimpleOrderedMap) ((qr.getResponse().get("facet_counts")))).get("facet_heatmaps"));

            Integer gridLevel = -1;
            if (facetHeatMaps != null) {
                SimpleOrderedMap heatmap = (SimpleOrderedMap) facetHeatMaps.get(spatialField);
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

                while (qr == null && zoomOffset < 10) {
                    try {
                        SolrQuery solrQuery =
                                createHeatmapQuery(
                                        query, filterQueries, minx, miny, maxx, maxy, gridSizeInPixels, zoomOffset);
                        String[] fqs =
                                Arrays.copyOf(
                                        solrQuery.getFilterQueries(), solrQuery.getFilterQueries().length + 1);
                        fqs[fqs.length - 1] = legendItem.getFq();
                        solrQuery.setFilterQueries(fqs);

                        // query
                        qr = query(solrQuery); // can throw exception
                    } catch (Exception ex) {
                        zoomOffset++;
                    }
                }
                if (qr != null) {
                    SimpleOrderedMap facetHeatMaps =
                            ((SimpleOrderedMap)
                                    ((SimpleOrderedMap) ((qr.getResponse().get("facet_counts"))))
                                            .get("facet_heatmaps"));

                    if (facetHeatMaps != null) {
                        // iterate over legend
                        SimpleOrderedMap heatmap = (SimpleOrderedMap) facetHeatMaps.get(spatialField);
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
            Double maxy,
            int gridSize,
            int zoomOffset) {
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler("standard");
        solrQuery.set("facet.heatmap", spatialField);

        // heatmaps support international date line
        if (minx < -180) {
            minx = minx + 360;
        }

        if (maxx > 180) {
            maxx = maxx - 360;
        }

        String geom = "[\"" + minx + " " + miny + "\" TO \"" + maxx + " " + maxy + "\"]";
        solrQuery.set("facet.heatmap.geom", geom);

        // limit output grid to < distErr
        double tileWidthInDecimalDegrees = maxx - minx;
        double tileHeightInDecimalDegrees = maxy - miny;
        int zoomLevel = 0;
        double tileWidthInDDAtZoomLevel = 360;
        while (Math.max(tileWidthInDecimalDegrees, tileHeightInDecimalDegrees)
                < tileWidthInDDAtZoomLevel / (double) gridSize) {
            zoomLevel++;
            tileWidthInDDAtZoomLevel /= 2.0;
        }
        // max grid level is 11
        if (zoomLevel > 11) {
            zoomLevel = 11;
        }

        // TODO: Zoom level is calculated poorly. zoomOffset is a temporary fix.
        solrQuery.set(
                "facet.heatmap.gridLevel",
                String.valueOf(zoomLevel - zoomOffset)); // good for points, probably

        solrQuery.setFacetLimit(-1);
        solrQuery.setFacet(true);
        solrQuery.setFilterQueries(filterQueries);
        solrQuery.setRows(0);
        solrQuery.setQuery(query);
        return solrQuery;
    }
}