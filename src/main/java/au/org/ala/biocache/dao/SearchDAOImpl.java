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

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;
import javax.servlet.ServletOutputStream;

import au.org.ala.biocache.dto.BreakdownRequestParams;
import au.org.ala.biocache.dto.DataProviderCountDTO;
import au.org.ala.biocache.dto.DownloadDetailsDTO;
import au.org.ala.biocache.dto.DownloadRequestParams;
import au.org.ala.biocache.dto.FacetResultDTO;
import au.org.ala.biocache.dto.FacetThemes;
import au.org.ala.biocache.dto.FieldResultDTO;
import au.org.ala.biocache.dto.IndexFieldDTO;
import au.org.ala.biocache.dto.OccurrenceIndex;
import au.org.ala.biocache.dto.OccurrencePoint;
import au.org.ala.biocache.dto.PointType;
import au.org.ala.biocache.dto.SearchRequestParams;
import au.org.ala.biocache.dto.SearchResultDTO;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.dto.StatsIndexFieldDTO;
import au.org.ala.biocache.dto.TaxaCountDTO;
import au.org.ala.biocache.dto.TaxaRankCountDTO;
import au.org.ala.biocache.service.AuthService;
import au.org.ala.biocache.service.LayersService;
import au.org.ala.biocache.service.SpeciesLookupService;
import au.org.ala.biocache.util.CollectionsCache;
import au.org.ala.biocache.util.DownloadFields;
import au.org.ala.biocache.util.LegendItem;
import au.org.ala.biocache.util.ParamsCache;
import au.org.ala.biocache.util.ParamsCacheMissingException;
import au.org.ala.biocache.util.ParamsCacheObject;
import au.org.ala.biocache.util.RangeBasedFacets;
import au.org.ala.biocache.util.SearchUtils;
import au.org.ala.biocache.util.SpatialUtils;
import au.org.ala.biocache.util.thread.EndemicCallable;
import au.org.ala.biocache.writer.CSVRecordWriter;
import au.org.ala.biocache.writer.ShapeFileRecordWriter;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.GroupCommand;
import org.apache.solr.client.solrj.response.GroupResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RangeFacet;
import org.apache.solr.client.solrj.response.RangeFacet.Numeric;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.springframework.beans.BeanUtils;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import au.com.bytecode.opencsv.CSVWriter;
import au.org.ala.biocache.index.IndexDAO;
import au.org.ala.biocache.RecordWriter;
import au.org.ala.biocache.index.SolrIndexDAO;

import com.googlecode.ehcache.annotations.Cacheable;

/**
 * SOLR implementation of SearchDao. Uses embedded SOLR server (can be a memory hog).
 *
 * @see au.org.ala.biocache.dao.SearchDAO
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
@Component("searchDao")
public class SearchDAOImpl implements SearchDAO {

    /** log4 j logger */
    private static final Logger logger = Logger.getLogger(SearchDAOImpl.class);
    /** SOLR server instance */
    protected SolrServer server;
    protected SolrRequest.METHOD queryMethod;
    /** Limit search results - for performance reasons */
    protected Integer MAX_DOWNLOAD_SIZE = 500000;
    private Integer throttle = 100;
    /** Batch size for a download */
    protected Integer downloadBatchSize = 500;
    public static final String NAMES_AND_LSID = "names_and_lsid";
    public static final String COMMON_NAME_AND_LSID = "common_name_and_lsid";
    protected static final String DECADE_FACET_NAME = "decade";
    protected static final Integer FACET_PAGE_SIZE = 1000;
    protected static final String QUOTE = "\"";
    protected static final char[] CHARS = {' ',':'};
    protected static final String RANGE_SUFFIX = "_RNG";  

    private String spatialField = "geohash";
    
    //Patterns that are used to prepare a SOLR query for execution
    protected Pattern lsidPattern = Pattern.compile("(^|\\s|\"|\\(|\\[|')lsid:\"?([a-zA-Z0-9\\.:\\-_]*)\"?");
    protected Pattern urnPattern = Pattern.compile("urn:[a-zA-Z0-9\\.:-]*");
    protected Pattern spacesPattern = Pattern.compile("[^\\s\"\\(\\)\\[\\]{}']+|\"[^\"]*\"|'[^']*'");
    protected Pattern uidPattern = Pattern.compile("(?:[\"]*)?([a-z_]*_uid:)([a-z0-9]*)(?:[\"]*)?");
    protected Pattern spatialPattern = Pattern.compile(spatialField+":\"Intersects\\([a-zA-Z=\\-\\s0-9\\.\\,():]*\\)\\\"");
    protected Pattern qidPattern = ParamsCache.qidPattern;//Pattern.compile("qid:[0-9]*");
    protected Pattern termPattern = Pattern.compile("([a-zA-z_]+?):((\".*?\")|(\\\\ |[^: \\)\\(])+)"); // matches foo:bar, foo:"bar bash" & foo:bar\ bash
    protected Pattern indexFieldPatternMatcher = java.util.regex.Pattern.compile("[a-z_0-9]{1,}:");
    protected Pattern layersPattern = Pattern.compile("(el|cl)[0-9abc]+");

    /** Download properties */
    protected DownloadFields downloadFields;

    @Inject
    protected SearchUtils searchUtils;
    
    @Inject
    private CollectionsCache collectionCache;
    
    @Inject
    private AbstractMessageSource messageSource;

    @Inject
    private SpeciesLookupService speciesLookupService;
    
    @Inject
    protected AuthService authService;
    
    @Inject
    protected LayersService layersService;
    
    /** Max number of threads to use in endemic queries */
    protected Integer maxMultiPartThreads = 5;

    /** thread pool for multipart queries that take awhile */
    private ExecutorService executor = null;
    
    /** should we check download limits */
    private boolean checkDownloadLimits = false;

    /** should we check download limits */
    private int termQueryLimit = 1000;
    
    /** Comma separated list of solr fields that need to have the authService substitute values if they are used in a facet. - CAN be overridden */
    private String authServiceFields = "";
    
    private Set<IndexFieldDTO> indexFields = null;
    private Map<String, IndexFieldDTO> indexFieldMap = null;
    private Map<String, StatsIndexFieldDTO> rangeFieldCache = null;
    private Set<String> authIndexFields = null;

    /**
     * Initialise the SOLR server instance
     */
    public SearchDAOImpl() {}
    
    private SolrServer getServer(){
        if(server == null){
            initServer();
        }
        return server;
    }

    private void initServer() {
        if (this.server == null) {
            try {
                // use the solr server that has been in the biocache-store...
                SolrIndexDAO dao = (SolrIndexDAO) au.org.ala.biocache.Config
                    .getInstance(IndexDAO.class);
                dao.init();
                server = dao.solrServer();
                queryMethod = server instanceof EmbeddedSolrServer? SolrRequest.METHOD.GET:SolrRequest.METHOD.POST;
                logger.debug("The server " + server.getClass());
                //CAUSING THE HANG....
                downloadFields = new DownloadFields(getIndexedFields());
            } catch (Exception ex) {
                logger.error("Error initialising embedded SOLR server: " + ex.getMessage(), ex);
            }
        }
    }
    
    public Set<String> getAuthIndexFields(){
        if(authIndexFields == null){
            //set up the hash set of the fields that need to have the authentication service substitute
            authIndexFields = new java.util.HashSet<String>();
            CollectionUtils.mergeArrayIntoCollection(authServiceFields.split(","), authIndexFields);
        }
        return authIndexFields;
    }

    public void refreshCaches(){
        collectionCache.updateCache();
        //refreshes the list of index fields that are reported to the user
        indexFields = null;
        //empties the range cache to allow the settings to be recalculated.
        rangeFieldCache = null;
        try {
            indexFields = getIndexedFields();
            downloadFields = new DownloadFields(getIndexedFields());
        } catch(Exception e) {
            logger.error("Unable to refresh cache.", e);
        }
    }
    
    /**
     * Returns a list of species that are endemic to the supplied region. Values are cached 
     * due to the "expensive" operation.
     */
    @Cacheable(cacheName = "endemicCache")
    public List<FieldResultDTO> getEndemicSpecies(SpatialSearchRequestParams requestParams) throws Exception{
        if(executor == null){
            executor = Executors.newFixedThreadPool(maxMultiPartThreads);
        }
        // 1)get a list of species that are in the WKT
        logger.debug("Starting to get Endemic Species...");
        List<FieldResultDTO> list1 = getValuesForFacet(requestParams);//new ArrayList(Arrays.asList(getValuesForFacets(requestParams)));
        logger.debug("Retrieved species within area...("+list1.size()+")");                     
        // 2)get a list of species that occur in the inverse WKT
        
        String reverseQuery = SpatialUtils.getWKTQuery(spatialField, requestParams.getWkt(), true);//"-geohash:\"Intersects(" +wkt + ")\"";
        
        logger.debug("The reverse query:" + reverseQuery);
        
        requestParams.setWkt(null);
        
        int i = 0, localterms = 0;
        
        String facet = requestParams.getFacets()[0];
        String[] originalFqs = requestParams.getFq();
        //add the negated WKT query to the fq
        originalFqs= (String[])ArrayUtils.add(originalFqs ,reverseQuery);
        List<Future<List<FieldResultDTO>>> threads = new ArrayList<Future<List<FieldResultDTO>>>();
        //batch up the rest of the world query so that we have fqs based on species we want to test for. This should improve the performance of the endemic services.       
        while(i < list1.size()){
            StringBuffer sb = new StringBuffer();
            while((localterms == 0 || localterms % termQueryLimit != 0) && i < list1.size()){
                if(localterms != 0)
                    sb.append(" OR ");
                sb.append(facet).append(":").append(ClientUtils.escapeQueryChars(list1.get(i).getFieldValue()));
                i++;
                localterms++;
            }
            String newfq = sb.toString();
            if(localterms ==1)
                newfq = newfq+ " OR " + newfq; //cater for the situation where there is only one term.  We don't want the term to be escaped again
            localterms=0;
            //System.out.println("FQ = " + newfq);
            SpatialSearchRequestParams srp = new SpatialSearchRequestParams();
            BeanUtils.copyProperties(requestParams, srp);
            srp.setFq((String[])ArrayUtils.add(originalFqs, newfq));
            int batch = i / termQueryLimit;
            EndemicCallable callable = new EndemicCallable(srp, batch,this);
            threads.add(executor.submit(callable));           
        }
        for(Future<List<FieldResultDTO>> future: threads){
            List<FieldResultDTO> list = future.get();
            if(list != null)
                list1.removeAll(list);
        }
        logger.debug("Determined final endemic list ("+list1.size()+")...");        
        return list1;
    }
    
    /**
     * Returns the values and counts for a single facet field.    
     */
    public List<FieldResultDTO> getValuesForFacet(SpatialSearchRequestParams requestParams) throws Exception{
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        writeFacetToStream(requestParams, true, false, false, outputStream,null);
        outputStream.flush();
        outputStream.close();
        String includedValues = outputStream.toString();
        includedValues = includedValues == null ? "" : includedValues;
        String[] values = includedValues.split("\n");
        List<FieldResultDTO> list = new ArrayList<FieldResultDTO>();
        boolean first = true;
        for(String value: values){
            if(first){
                first = false;
            } else {
                int idx = value.lastIndexOf(",");                         
                list.add(new FieldResultDTO(value.substring(0,idx), Long.parseLong(value.substring(idx+1))));
            }
        }
        return list;
  }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#findByFulltextSpatialQuery
     */
    @Override
    public SearchResultDTO findByFulltextSpatialQuery(SpatialSearchRequestParams searchParams, Map<String,String[]> extraParams) throws Exception {
        return findByFulltextSpatialQuery(searchParams,false,extraParams);
    }
    
    @Override
    public SearchResultDTO findByFulltextSpatialQuery(SpatialSearchRequestParams searchParams, boolean includeSensitive, Map<String,String[]> extraParams) throws Exception {
        SearchResultDTO searchResults = new SearchResultDTO();
        SpatialSearchRequestParams original = new SpatialSearchRequestParams(); 
        BeanUtils.copyProperties(searchParams, original);
        try {
            //String queryString = formatSearchQuery(query);            
            formatSearchQuery(searchParams);            
            //add context information
            updateQueryContext(searchParams);
            String queryString = buildSpatialQueryString(searchParams);
            //logger.debug("The spatial query " + queryString);
            SolrQuery solrQuery = initSolrQuery(searchParams,true,extraParams); // general search settings
            solrQuery.setQuery(queryString);

            QueryResponse qr = runSolrQuery(solrQuery, searchParams);
            //need to set the original q to the processed value so that we remove the wkt etc that is added from paramcache object
            Class resultClass = includeSensitive? au.org.ala.biocache.dto.SensitiveOccurrenceIndex.class:OccurrenceIndex.class;
            searchResults = processSolrResponse(original, qr, solrQuery,resultClass);
            searchResults.setQueryTitle(searchParams.getDisplayString());
            searchResults.setUrlParameters(original.getUrlParams());
            //now update the fq display map...
            searchResults.setActiveFacetMap(searchUtils.addFacetMap(searchParams.getFq(), getAuthIndexFields()));
            
            logger.info("spatial search query: " + queryString);
        } catch (SolrServerException ex) {
            //logger.error("Problem communicating with SOLR server. " + ex.getMessage(), ex);
            logger.error("Error executing query  with requestParams: " + searchParams.toString()+ " EXCEPTION: " + ex.getMessage());
            searchResults.setStatus("ERROR"); // TODO also set a message field on this bean with the error message(?)
            searchResults.setErrorMessage(ex.getMessage());
        }

        return searchResults;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#writeSpeciesCountByCircleToStream(au.org.ala.biocache.dto.SpatialSearchRequestParams, String, javax.servlet.ServletOutputStream)
     * 
     */
    public int writeSpeciesCountByCircleToStream(SpatialSearchRequestParams searchParams, String speciesGroup, ServletOutputStream out) throws Exception {

        //get the species counts:
        logger.debug("Writing CSV file for species count by circle");
        searchParams.setPageSize(-1);
        List<TaxaCountDTO> species = findAllSpeciesByCircleAreaAndHigherTaxa(searchParams, speciesGroup);
        logger.debug("There are " + species.size() + "records being downloaded");
        CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(out), '\t', '"');
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
            csvWriter.flush();
            count++;
        }
        return count;
    }
    /**
     * Writes the values for the first supplied facet to output stream
     * 
     * @param includeCount true when the count should be included in the download
     * @param lookupName true when a name lsid should be looked up in the bie
     * 
     */
    public void writeFacetToStream(SpatialSearchRequestParams searchParams, boolean includeCount, boolean lookupName, boolean includeSynonyms, OutputStream out, DownloadDetailsDTO dd) throws Exception{
        //set to unlimited facets
        searchParams.setFlimit(-1);
        formatSearchQuery(searchParams);
        //add the context information
        updateQueryContext(searchParams);
        String queryString = buildSpatialQueryString(searchParams);
        SolrQuery solrQuery = initSolrQuery(searchParams,false,null);
        solrQuery.setQuery(queryString);

        //don't want any results returned
        solrQuery.setRows(0);
        solrQuery.setFacetLimit(FACET_PAGE_SIZE);        
        int offset = 0;
        boolean shouldLookup = lookupName && searchParams.getFacets()[0].contains("_guid");
        
        QueryResponse qr = runSolrQuery(solrQuery, searchParams);
        logger.debug("Retrieved facet results from server...");
        if (!qr.getResults().isEmpty()) {
            FacetField ff = qr.getFacetField(searchParams.getFacets()[0]);
            
            //write the header line
            if(ff != null){
                String[] header = new String[]{ff.getName()};
               // out.write(ff.getName().getBytes());
                if(shouldLookup){
                    header = new String[]{ff.getName(),"taxon name","kingdom","phylum","class","order","family","genus","vernacular name"};
                    //out.write((",species name,kingdom,phylum,class,order,family,genus").getBytes());
                    if(includeSynonyms){
                        header = new String[]{ff.getName(),"taxon name","kingdom","phylum","class","order","family","genus","vernacular name","synonyms"};
                        //out.write(",synonyms".getBytes());
                    }
                }
                if(includeCount){
                    //out.write(",Count".getBytes());
                    header = (String[])ArrayUtils.add(header, "count");
                }
                CSVRecordWriter writer = new CSVRecordWriter(out, header);
                //out.write("\n".getBytes());
                //PAGE through the facets until we reach the end.
                while(ff.getValueCount() >0){
                    if(ff.getValueCount() >0){
                      //process the "species_guid_ facet by looking up the list of guids                
                        if(shouldLookup){
                            
                            List<String> guids = new ArrayList<String>();
                            List<Long> counts = new ArrayList<Long> ();
                            logger.debug("Downloading " +  ff.getValueCount() + " species guids");
                            for(FacetField.Count value : ff.getValues()){
                                guids.add(value.getName());
                                if(includeCount)
                                    counts.add(value.getCount());
                                //Only want to send a sub set of the list so that the URI is not too long for BIE
                                if(guids.size()==30){
                                  //now get the list of species from the web service TODO may need to move this code                                   
                                    //handle null values being returned from the service...
                                    writeTaxonDetailsToStream(guids, counts, includeCount, includeSynonyms, writer);
                                    guids.clear();
                                }
                            }
                            //now write any guids that remain at the end of the looping
                            writeTaxonDetailsToStream(guids, counts, includeCount, includeSynonyms, writer);
                        } else {
                            //default processing of facets
                            for(FacetField.Count value : ff.getValues()){
                                String[] row = includeCount?new String[]{value.getName(), Long.toString(value.getCount())}:new String[]{value.getName()};
                                writer.write(row);
                            }
                        }
                        offset += FACET_PAGE_SIZE;
                        if(dd != null)
                            dd.updateCounts(FACET_PAGE_SIZE);
                        //get the next values
                        solrQuery.remove("facet.offset");
                        solrQuery.add("facet.offset", Integer.toString(offset));
                        qr = runSolrQuery(solrQuery, searchParams);
                        ff = qr.getFacetField(searchParams.getFacets()[0]);
                    }
                    writer.finalise();
                }
            }
        }
    }
    /**
     * Writes additional taxon information to the stream. It performs bulk lookups to the 
     * BIE in order to obtain extra classification information
     * @param guids The guids to lookup 
     * @param counts The occurrence counts for each guid if "includeCounts = true"
     * @param includeCounts Whether or not to include the occurrence counts in the download
     * @param includeSynonyms whether or not to include the synonyms in the download - when 
     * true this will perform additional lookups in the BIE
     * @param writer The CSV writer to write to.
     * @throws Exception
     */
    private void writeTaxonDetailsToStream(List<String> guids,List<Long> counts, boolean includeCounts, boolean includeSynonyms, CSVRecordWriter writer) throws Exception{
        List<Map<String,String>> values = speciesLookupService.getNameDetailsForGuids(guids);
        Map<String,List<Map<String, String>>> synonyms = includeSynonyms? speciesLookupService.getSynonymDetailsForGuids(guids):new HashMap<String,List<Map<String,String>>>();
        int size = includeSynonyms&&includeCounts?11:((includeCounts && !includeSynonyms)||(includeSynonyms && !includeCounts))?10:9;
        
        for(int i =0 ;i<guids.size();i++){
            int countIdx=9;
            String[] row = new String[size];
            //guid
            String guid = guids.get(i);
            row[0]=guid;
            if(values!= null && synonyms != null){
                Map<String,String> map = values.get(i);                               
                if(map!=null){
                    //scientific name
                    row[1]=map.get("nameComplete");
                    row[2]=map.get("kingdom");
                    row[3]=map.get("phylum");
                    row[4]=map.get("classs");
                    row[5]=map.get("order");
                    row[6]=map.get("family");
                    row[7]=map.get("genus"); 
                    row[8]=map.get("commonNameSingle");
                }
                
                if(includeSynonyms){
                    //retrieve a list of the synonyms
                    List<Map<String,String>> names =synonyms.get(guid);
                    StringBuilder sb =new StringBuilder();
                    for(Map<String,String> n :names){
                        if(!guid.equals(n.get("guid"))){
                            if(sb.length()>0){
                                sb.append(",");
                            }
                            sb.append(n.get("name"));
                        }
                    }
                    row[9] =sb.toString();
                    countIdx=10;
                }
            }
            if(includeCounts){
                row[countIdx] = counts.get(i).toString();
            }
            writer.write(row);
        }
    }
    
    /**
     * Writes all the distinct latitude and longitude in the index to the supplied
     * output stream.
     * 
     * @param out
     * @throws Exception
     */
    public void writeCoordinatesToStream(SearchRequestParams searchParams,OutputStream out) throws Exception{
        //generate the query to obtain the lat,long as a facet
        SearchRequestParams srp = new SearchRequestParams();
        searchUtils.setDefaultParams(srp);
        srp.setFacets(searchParams.getFacets());
        
        SolrQuery solrQuery = initSolrQuery(srp,false,null);
        //We want all the facets so we can dump all the coordinates
        solrQuery.setFacetLimit(-1);
        solrQuery.setFacetSort("count");
        solrQuery.setRows(0);
        solrQuery.setQuery(searchParams.getQ());

        QueryResponse qr = runSolrQuery(solrQuery, srp);
         if (qr.getResults().size() > 0) {
                FacetField ff = qr.getFacetField(searchParams.getFacets()[0]);
                if(ff != null && ff.getValueCount() > 0){
                    out.write("latitude,longitude\n".getBytes());
                    //write the facets to file
                    for(FacetField.Count value : ff.getValues()){
                        //String[] slatlon = value.getName().split(",");
                        out.write(value.getName().getBytes());
                        out.write("\n".getBytes());
                        
                    }
                }
        }
    }

    /**
     * Writes the index fields to the supplied output stream in CSV format.
     *
     * DM: refactored to split the query by month to improve performance.
     * Further enhancements possible:
     * 1) Multi threaded
     * 2) More filtering, by year or decade..
     *
     * @param downloadParams
     * @param out
     * @param includeSensitive
     * @throws Exception
     */
    public Map<String, Integer> writeResultsFromIndexToStream(final DownloadRequestParams downloadParams,
                                                                         OutputStream out,
                                                                         boolean includeSensitive, final DownloadDetailsDTO dd, boolean checkLimit) throws Exception {
        long start = System.currentTimeMillis();
        final Map<String, Integer> uidStats = new HashMap<String, Integer>();
        if(server == null){
            initServer();
        }
        try{
            SolrQuery solrQuery = new SolrQuery();
            formatSearchQuery(downloadParams);

            String dFields = downloadParams.getFields();

            if(includeSensitive){
                //include raw latitude and longitudes
                dFields = dFields.replaceFirst("decimalLatitude.p","sensitive_latitude,sensitive_longitude,decimalLatitude.p").replaceFirst(",locality,", ",locality,sensitive_locality,");
            }

            StringBuilder sb = new StringBuilder(dFields);
            if(!downloadParams.getExtra().isEmpty()){
                sb.append(",").append(downloadParams.getExtra());
            }

            String[] requestedFields = sb.toString().split(",");
            List<String>[] indexedFields = downloadFields.getIndexFields(requestedFields);
            logger.debug("Fields included in download: " +indexedFields[0]);
            logger.debug("Fields excluded from download: "+indexedFields[1]);
            logger.debug("The headers in downloads: "+indexedFields[2]);

            //set the fields to the ones that are available in the index
            final String[] fields = indexedFields[0].toArray(new String[]{});
            solrQuery.setFields(fields);
            StringBuilder qasb = new StringBuilder();
            if(!"none".equals(downloadParams.getQa())){
                solrQuery.addField("assertions"); 
                if(!"all".equals(downloadParams.getQa())){
                    //add all the qa fields
                    qasb.append(downloadParams.getQa());
                }
            } 
                solrQuery.addField("institution_uid")
                .addField("collection_uid")
                .addField("data_resource_uid")
                .addField("data_provider_uid");

            //add context information
            updateQueryContext(downloadParams);
            solrQuery.setQuery(buildSpatialQueryString(downloadParams));

            //get the assertion facets to add them to the download fields
            boolean getAssertionsFromFacets = "all".equals(downloadParams.getQa());
            SolrQuery monthAssertionsQuery = getAssertionsFromFacets?solrQuery.getCopy().addFacetField("month", "assertions"):solrQuery.getCopy().addFacetField("month");
            if(getAssertionsFromFacets){
                //set the order for the facet to be based on the index - this will force the assertions to be returned in the same order each time
                //based on alphabetical sort.  The number of QA's may change between searches so we can't guarantee that the order won't change
                monthAssertionsQuery.add("f.assertions.facet.sort","index");
            }
            QueryResponse facetQuery = runSolrQuery(monthAssertionsQuery, downloadParams.getFq(), 0, 0, "score", "asc");
            
            //set the totalrecords for the download details
            dd.setTotalRecords(facetQuery.getResults().getNumFound());
            if(checkLimit && dd.getTotalRecords() < MAX_DOWNLOAD_SIZE){
                checkLimit = false;
            }
            //get the month facets to add them to the download fields get the assertion facets.
            List<Count> splitByFacet = null;
           
            for(FacetField facet : facetQuery.getFacetFields()){
                if(facet.getName().equals("assertions") && facet.getValueCount()>0){
                   for(FacetField.Count facetEntry : facet.getValues()){
                       if(qasb.length()>0)
                           qasb.append(",");
                       qasb.append(facetEntry.getName());
                   }
                }
                if(facet.getName().equals("month") && facet.getValueCount()>0){
                   splitByFacet = facet.getValues();
                }
            }

            String qas = qasb.toString();
            final String[] qaFields = qas.equals("") ? new String[]{} : qas.split(",");
            String[] qaTitles = downloadFields.getHeader(qaFields, false);

            String[] header = org.apache.commons.lang3.ArrayUtils.addAll(indexedFields[2].toArray(new String[]{}),qaTitles);
            
            //construct correct RecordWriter based on the supplied fileType
            final au.org.ala.biocache.RecordWriter rw = downloadParams.getFileType().equals("csv")? new CSVRecordWriter(out, header) : new ShapeFileRecordWriter(downloadParams.getFile(), out, (String[])ArrayUtils.addAll(fields, qaFields));
            
            if(rw instanceof ShapeFileRecordWriter){
                dd.setHeaderMap(((ShapeFileRecordWriter)rw).getHeaderMappings());
            }
            
            //order the query by _docid_ for faster paging
            solrQuery.addSortField("_docid_", ORDER.asc);

            //for each month create a separate query that pages through 500 records per page
            List<SolrQuery> queries = new ArrayList<SolrQuery>();
            for(Count facet: splitByFacet){
                if(facet.getCount()>0){
                    SolrQuery splitByFacetQuery = solrQuery.getCopy().addFilterQuery(facet.getFacetField().getName() + ":" + facet.getName());
                    splitByFacetQuery.setFacet(false);
                    queries.add(splitByFacetQuery);
                }
            }
            SolrQuery remainderQuery = solrQuery.getCopy().addFilterQuery("-"+splitByFacet.get(0).getFacetField().getName() + ":[* TO *]");
            queries.add(0, remainderQuery);

            //multi-thread the requests...
            ExecutorService pool = Executors.newFixedThreadPool(6);
            Set<Future<Integer>> futures = new HashSet<Future<Integer>>();
            final AtomicInteger resultsCount = new AtomicInteger(0);
            final boolean threadCheckLimit = checkLimit;

            //execute each query, writing the results to stream
            for(final SolrQuery splitByFacetQuery: queries){
                //define a thread
                Callable<Integer> solrCallable = new Callable<Integer>(){

                    int startIndex = 0;

                    @Override
                    public Integer call() throws Exception {
                        QueryResponse qr = runSolrQuery(splitByFacetQuery, downloadParams.getFq(), downloadBatchSize, startIndex, "_docid_", "asc");
                        int recordsForThread = 0;
                        logger.debug(splitByFacetQuery.getQuery() + " - results: " + qr.getResults().size());

                        while (qr != null &&!qr.getResults().isEmpty()) {
                            logger.debug("Start index: " + startIndex + ", " + splitByFacetQuery.getQuery());
                            int count=0;
                            synchronized (rw) {
                                count = processQueryResults(uidStats, fields, qaFields, rw, qr, dd, threadCheckLimit, resultsCount);
                                recordsForThread += count;
                            }
                            startIndex += downloadBatchSize;
                            //we have already set the Filter query the first time the query was constructed rerun with he same params but different startIndex
                            if(!threadCheckLimit || resultsCount.intValue()<MAX_DOWNLOAD_SIZE){
                                if(!threadCheckLimit){
                                    //throttle the download by sleeping
                                    try{
                                        Thread.currentThread().sleep(throttle);
                                    } catch(InterruptedException e){
                                        //don't care if the sleep was interrupted
                                    }
                                }
                                qr = runSolrQuery(splitByFacetQuery, null, downloadBatchSize, startIndex, "_docid_", "asc");
                            } else {
                                qr = null;
                            }
                        }
                        return recordsForThread;
                    }
                };
                futures.add(pool.submit(solrCallable));
            }

            //check the futures until all have finished
            int totalDownload = 0;
            Set<Future<Integer>> completeFutures = new HashSet<Future<Integer>>();
            boolean allComplete = false;
            while(!allComplete){
                for(Future future: futures){
                    if(!completeFutures.contains(future)){
                        if(future.isDone()){                            
                            totalDownload += (Integer) future.get();
                            completeFutures.add(future);
                        }
                    }
                }
                allComplete = completeFutures.size() == futures.size();
                if(!allComplete){
                    Thread.sleep(1000);
                }
            }
            pool.shutdown();
            rw.finalise();
            out.flush();

            long finish = System.currentTimeMillis();
            long timeTakenInSecs = (finish-start)/1000;
            if(timeTakenInSecs ==0) timeTakenInSecs =1;
            logger.info("Download of " + resultsCount + " records in " + timeTakenInSecs + " seconds. Record/sec: " + resultsCount.intValue()/timeTakenInSecs);

        } catch (SolrServerException ex) {
            logger.error("Problem communicating with SOLR server while processing download. " + ex.getMessage(), ex);
        }
        return uidStats;
    }

    private int processQueryResults( Map<String, Integer> uidStats, String[] fields, String[] qaFields, RecordWriter rw, QueryResponse qr, DownloadDetailsDTO dd, boolean checkLimit,AtomicInteger resultsCount) {
        int count = 0;
        for (SolrDocument sd : qr.getResults()) {
            if(sd.getFieldValue("data_resource_uid") != null &&(!checkLimit || (checkLimit && resultsCount.intValue()<MAX_DOWNLOAD_SIZE))){

                //resultsCount++;
                count++;
                synchronized(resultsCount){
                    resultsCount.incrementAndGet();
                }

                //add the record
                String[] values = new String[fields.length + qaFields.length];

                //get all the "single" values from the index
                for(int j = 0; j < fields.length; j++){
                    Object value = sd.getFirstValue(fields[j]);
                    if(value instanceof Date)
                        values[j] = value == null ? "" : org.apache.commons.lang.time.DateFormatUtils.format((Date)value, "yyyy-MM-dd");
                    else
                        values[j] = value == null ? "" : value.toString();
                }

                //now handle the assertions
                java.util.Collection<Object> assertions = sd.getFieldValues("assertions");

                //Handle the case where there a no assertions against a record
                if(assertions == null){
                    assertions = Collections.EMPTY_LIST;
                }

                for(int k = 0; k < qaFields.length; k++){
                    values[fields.length + k] = Boolean.toString(assertions.contains(qaFields[k]));
                }

                rw.write(values);

                //increment the counters....
                incrementCount(uidStats, sd.getFieldValue("institution_uid"));
                incrementCount(uidStats, sd.getFieldValue("collection_uid"));
                incrementCount(uidStats, sd.getFieldValue("data_provider_uid"));
                incrementCount(uidStats,  sd.getFieldValue("data_resource_uid"));
            }
        }
        dd.updateCounts(count);
        return count;
    }

    /**
     * Note - this method extracts from CASSANDRA rather than the Index.
     */
    public Map<String, Integer> writeResultsToStream(DownloadRequestParams downloadParams, OutputStream out, int i, boolean includeSensitive, DownloadDetailsDTO dd) throws Exception {
        
        int resultsCount = 0;
        Map<String, Integer> uidStats = new HashMap<String, Integer>();
        //stores the remaining limit for data resources that have a download limit
        Map<String, Integer> downloadLimit = new HashMap<String,Integer>();
        
        try {
            SolrQuery solrQuery = initSolrQuery(downloadParams,false,null);
            //ensure that the qa facet is being ordered alphabetically so that the order is consistent.
            boolean getAssertionsFromFacets = "all".equals(downloadParams.getQa());            
            if(getAssertionsFromFacets){
                //set the order for the facet to be based on the index - this will force the assertions to be returned in the same order each time
                //based on alphabetical sort.  The number of QA's may change between searches so we can't guarantee that the order won't change
                solrQuery.add("f.assertions.facet.sort","index");
            }
            formatSearchQuery(downloadParams);            
            //add context information
            updateQueryContext(downloadParams);
            logger.info("search query: " + downloadParams.getFormattedQuery());
            solrQuery.setQuery(buildSpatialQueryString(downloadParams));
            //Only the fields specified below will be included in the results from the SOLR Query
            solrQuery.setFields("row_key", "institution_uid", "collection_uid", "data_resource_uid", "data_provider_uid");

            String dFields = downloadParams.getFields();
            
            if(includeSensitive){
                //include raw latitude and longitudes
                dFields = dFields.replaceFirst("decimalLatitude.p", "decimalLatitude,decimalLongitude,decimalLatitude.p").replaceFirst(",locality,", ",locality,sensitive_locality,");
            }
            
            StringBuilder  sb = new StringBuilder(dFields);
            if(downloadParams.getExtra().length()>0)
                sb.append(",").append(downloadParams.getExtra());
            StringBuilder qasb = new StringBuilder();
                        
            QueryResponse qr = runSolrQuery(solrQuery, downloadParams.getFq(), 0, 0, "_docid_", "asc");
            dd.setTotalRecords(qr.getResults().getNumFound());
            //get the assertion facets to add them to the download fields
            List<FacetField> facets = qr.getFacetFields();
            for(FacetField facet : facets){
                if(facet.getName().equals("assertions") && facet.getValueCount()>0){
                
                 for(FacetField.Count facetEntry : facet.getValues()){
                     //System.out.println("facet: " + facetEntry.getName());
                     if(qasb.length()>0)
                         qasb.append(",");
                     qasb.append(facetEntry.getName());
                 }
                }else if(facet.getName().equals("data_resource_uid") && checkDownloadLimits){
                    //populate the download limit
                    initDownloadLimits(downloadLimit, facet);
              }
            }
            
            //Write the header line
            String qas = qasb.toString();   
            
            String[] fields = sb.toString().split(",");            
            String[]qaFields = qas.equals("")?new String[]{}:qas.split(",");
            String[] qaTitles = downloadFields.getHeader(qaFields,false);
            String[] titles = downloadFields.getHeader(fields,true);
            String[] header = org.apache.commons.lang3.ArrayUtils.addAll(titles,qaTitles);
            //Create the Writer that will be used to format the records
            //construct correct RecordWriter based on the supplied fileType
            final au.org.ala.biocache.RecordWriter rw = downloadParams.getFileType().equals("csv")? new CSVRecordWriter(out, header) : new ShapeFileRecordWriter(downloadParams.getFile(), out, (String[])ArrayUtils.addAll(fields, qaFields));
            
            if(rw instanceof ShapeFileRecordWriter){
                dd.setHeaderMap(((ShapeFileRecordWriter)rw).getHeaderMappings());
            }
            

            //download the records that have limits first...
            if(downloadLimit.size() > 0){
                String[] originalFq = downloadParams.getFq();
                StringBuilder fqBuilder = new StringBuilder("-(");
                for(String dr : downloadLimit.keySet()){
                    //add another fq to the search for data_resource_uid                    
                     downloadParams.setFq((String[])ArrayUtils.add(originalFq, "data_resource_uid:" + dr));
                     resultsCount = downloadRecords(downloadParams, rw, downloadLimit, uidStats, fields, qaFields, resultsCount, dr, includeSensitive,dd);
                     if(fqBuilder.length()>2)
                         fqBuilder.append(" OR ");
                     fqBuilder.append("data_resource_uid:").append(dr);
                }
                fqBuilder.append(")");
                //now include the rest of the data resources
                //add extra fq for the remaining records
                downloadParams.setFq((String[])ArrayUtils.add(originalFq, fqBuilder.toString()));
                resultsCount =downloadRecords(downloadParams, rw, downloadLimit, uidStats, fields, qaFields, resultsCount, null, includeSensitive,dd);
            } else {
                //download all at once
                downloadRecords(downloadParams, rw, downloadLimit, uidStats, fields, qaFields, resultsCount, null, includeSensitive,dd);
            }
            rw.finalise();

        } catch (SolrServerException ex) {
            logger.error("Problem communicating with SOLR server. " + ex.getMessage(), ex);
        }
        
        return uidStats;
    }
    /**
     * Downloads the records for the supplied query. Used to break up the download into components
     * 1) 1 call for each data resource that has a download limit (supply the data resource uid as the argument dataResource)
     * 2) 1 call for the remaining records
     * @param downloadParams
     * @param downloadLimit
     * @param uidStats
     * @param fields
     * @param qaFields
     * @param resultsCount
     * @param dataResource The dataResource being download.  This should be null if multiple data resource are being downloaded.
     * @return
     * @throws Exception
     */
    private int downloadRecords(DownloadRequestParams downloadParams, au.org.ala.biocache.RecordWriter writer,
                Map<String, Integer> downloadLimit,  Map<String, Integer> uidStats,
                String[] fields, String[] qaFields,int resultsCount, String dataResource, boolean includeSensitive, DownloadDetailsDTO dd) throws Exception {
        logger.info("download query: " + downloadParams.getQ());
        SolrQuery solrQuery = initSolrQuery(downloadParams,false,null);
        solrQuery.setRows(MAX_DOWNLOAD_SIZE);
        formatSearchQuery(downloadParams);
        solrQuery.setQuery(buildSpatialQueryString(downloadParams));
        //Only the fields specified below will be included in the results from the SOLR Query
        solrQuery.setFields("row_key", "institution_uid", "collection_uid", "data_resource_uid", "data_provider_uid");
        
        int startIndex = 0;
        int pageSize = downloadBatchSize;
        StringBuilder  sb = new StringBuilder(downloadParams.getFields());
        if(downloadParams.getExtra().length()>0)
            sb.append(",").append(downloadParams.getExtra());
        StringBuilder qasb = new StringBuilder();
        QueryResponse qr = runSolrQuery(solrQuery, downloadParams.getFq(), pageSize, startIndex, "_docid_", "asc");
        List<String> uuids = new ArrayList<String>();
        
        while (qr.getResults().size() > 0 && resultsCount < MAX_DOWNLOAD_SIZE && shouldDownload(dataResource, downloadLimit, false)) {
            logger.debug("Start index: " + startIndex);
            //cycle through the results adding them to the list that will be sent to cassandra
            for (SolrDocument sd : qr.getResults()) {
                if(sd.getFieldValue("data_resource_uid") != null){
                String druid = sd.getFieldValue("data_resource_uid").toString();
                if(shouldDownload(druid,downloadLimit, true) && resultsCount < MAX_DOWNLOAD_SIZE){
                    resultsCount++;
                    uuids.add(sd.getFieldValue("row_key").toString());

                    //increment the counters....
                    incrementCount(uidStats, sd.getFieldValue("institution_uid"));
                    incrementCount(uidStats, sd.getFieldValue("collection_uid"));
                    incrementCount(uidStats, sd.getFieldValue("data_provider_uid"));
                    incrementCount(uidStats, druid);
                }}
            }
            //logger.debug("Downloading " + uuids.size() + " records");
            au.org.ala.biocache.Store.writeToWriter(writer, uuids.toArray(new String[]{}), fields, qaFields, includeSensitive);
            startIndex += pageSize;
            uuids.clear();
            dd.updateCounts(qr.getResults().size());
            if (resultsCount < MAX_DOWNLOAD_SIZE) {
                //we have already set the Filter query the first time the query was constructed rerun with he same params but different startIndex
                qr = runSolrQuery(solrQuery, null, pageSize, startIndex, "_docid_", "asc");
            }
        }
        return resultsCount;
    }
    
    /**
     * Indicates whether or not a records from the supplied data resource should be included 
     * in the download. (based on download limits)
     * @param druid
     * @param limits
     * @param decrease whether or not to decrease the download limit available
     */
    private boolean shouldDownload(String druid, Map<String, Integer>limits, boolean decrease){
        if(checkDownloadLimits){
            if(!limits.isEmpty() && limits.containsKey(druid)){
                Integer remainingLimit = limits.get(druid);
                if(remainingLimit == 0){
                    return false;
                }
                if(decrease){
                    limits.put(druid, remainingLimit-1);
                }
            }
        }
        return true;
    }
    /**
     * Initialises the download limit tracking
     * @param map
     * @param facet
     */
    private void initDownloadLimits(Map<String,Integer> map,FacetField facet){
        //get the download limits from the cache
        Map<String, Integer>limits = collectionCache.getDownloadLimits();
        for(FacetField.Count facetEntry :facet.getValues()){
            Integer limit = limits.get(facetEntry.getName());
            if(limit != null && limit >0){
                //check to see if the number of records returned from the query execeeds the limit
                if(limit < facetEntry.getCount())
                    map.put(facetEntry.getName(), limit);
            }
        }
        if(map.size()>0)
            logger.debug("Downloading with the following limits: " + map);
    }

    private void incrementCount(Map<String, Integer> values, Object uid) {
        if (uid != null) {
            Integer count = values.containsKey(uid) ? values.get(uid) : 0;
            count++;
            values.put(uid.toString(), count);
        }
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#getFacetPoints(au.org.ala.biocache.dto.SpatialSearchRequestParams, au.org.ala.biocache.dto.PointType)
     */
    @Override
    public List<OccurrencePoint> getFacetPoints(SpatialSearchRequestParams searchParams, PointType pointType) throws Exception {
        List<OccurrencePoint> points = new ArrayList<OccurrencePoint>(); // new OccurrencePoint(PointType.POINT);
        formatSearchQuery(searchParams);
        logger.info("search query: " + searchParams.getFormattedQuery());
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQueryType("standard");        
        solrQuery.setQuery(buildSpatialQueryString(searchParams));
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField(pointType.getLabel());
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(-1);//MAX_DOWNLOAD_SIZE);  // unlimited = -1

        //add the context information
        updateQueryContext(searchParams);

        QueryResponse qr = runSolrQuery(solrQuery, searchParams.getFq(), 1, 0, "score", "asc");
        List<FacetField> facets = qr.getFacetFields();

        if (facets != null) {
            for (FacetField facet : facets) {
                List<FacetField.Count> facetEntries = facet.getValues();
                if (facet.getName().contains(pointType.getLabel()) && (facetEntries != null) && (facetEntries.size() > 0)) {

                    for (FacetField.Count fcount : facetEntries) {
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

        return points;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#getOccurrences(au.org.ala.biocache.dto.SpatialSearchRequestParams, au.org.ala.biocache.dto.PointType, String, int)
     */
    @Override
    public List<OccurrencePoint> getOccurrences(SpatialSearchRequestParams searchParams, PointType pointType, String colourBy, int searchType) throws Exception {

        List<OccurrencePoint> points = new ArrayList<OccurrencePoint>();
        searchParams.setPageSize(100);

        String queryString = "";
        formatSearchQuery(searchParams);
        if (searchType == 0) {
            queryString = searchParams.getFormattedQuery();
        } else if (searchType == 1) {
            queryString = buildSpatialQueryString(searchParams.getFormattedQuery(), searchParams.getLat(), searchParams.getLon(), searchParams.getRadius());
        }

        logger.info("search query: " + queryString);
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQueryType("standard");
        solrQuery.setQuery(queryString);
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField(pointType.getLabel());
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(MAX_DOWNLOAD_SIZE);  // unlimited = -1

        //add the context information
        updateQueryContext(searchParams);

        QueryResponse qr = runSolrQuery(solrQuery, searchParams);
        SearchResultDTO searchResults = processSolrResponse(searchParams, qr, solrQuery,OccurrenceIndex.class);
        List<OccurrenceIndex> ocs = searchResults.getOccurrences();

        if (!ocs.isEmpty() && ocs.size() > 0) {

            for (OccurrenceIndex oc : ocs) {

                List<Float> coords = new ArrayList<Float>();
                coords.add(oc.getDecimalLongitude().floatValue());
                coords.add(oc.getDecimalLatitude().floatValue());

                OccurrencePoint point = new OccurrencePoint();
                point.setCoordinates(coords);

                if (searchType == 0) {
                    // for now, let's set the colour in this one.

                    String value = "Not available";
                    if (StringUtils.isNotBlank(colourBy)) {

                        try {
                            if(oc != null){
                                java.util.Map map = oc.toMap();
                                if (map != null) {
                                    //check to see if it is empty otherwise a NPE is thrown when option.get is called
                                    if (map.containsKey(colourBy)) {
                                        value = (String) map.get(colourBy);
                                    }
                                    point.setOccurrenceUid(value);
                                }
                            }
                        } catch (Exception e) {
                            logger.debug(e.getMessage(),e);
                        }
                    }

                } else if (searchType == 1) {
                    point.setOccurrenceUid(oc.getUuid());
                }

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
        solrQuery.setQueryType("standard");
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

            if(dpIdEntries != null){
                for (int i = 0; i < dpIdEntries.size(); i++) {

                    FacetField.Count dpIdEntry = dpIdEntries.get(i);
                    FacetField.Count dpNameEntry = dpNameEntries.get(i);

                    String dataProviderId = dpIdEntry.getName();
                    String dataProviderName = dpNameEntry.getName();
                    long count = dpIdEntry.getCount();

                    DataProviderCountDTO dto = new DataProviderCountDTO(dataProviderId, dataProviderName, count);
                    dpDTOs.add(dto);
                }
            }
        }
        logger.info("Find data providers = " + dpDTOs.size());
        return dpDTOs;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#findRecordsForLocation(au.org.ala.biocache.dto.SpatialSearchRequestParams, au.org.ala.biocache.dto.PointType)
     * This is used by explore your area
     */
    @Override
    public List<OccurrencePoint> findRecordsForLocation(SpatialSearchRequestParams requestParams, PointType pointType) throws Exception {
        List<OccurrencePoint> points = new ArrayList<OccurrencePoint>(); // new OccurrencePoint(PointType.POINT);
        updateQueryContext(requestParams);
        String queryString = buildSpatialQueryString(requestParams);
        //String queryString = formatSearchQuery(query);
        logger.info("location search query: " + queryString + "; pointType: " + pointType.getLabel());
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQueryType("standard");
        solrQuery.setQuery(queryString);

       
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField(pointType.getLabel());
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(MAX_DOWNLOAD_SIZE);  // unlimited = -1

        QueryResponse qr = runSolrQuery(solrQuery, requestParams.getFq(), 1, 0, "score", "asc");
        logger.info("qr number found: " + qr.getResults().getNumFound());
        List<FacetField> facets = qr.getFacetFields();

        if (facets != null) {
            for (FacetField facet : facets) {
                List<FacetField.Count> facetEntries = facet.getValues();
                if (facet.getName().contains(pointType.getLabel()) && (facetEntries != null) && (facetEntries.size() > 0)) {

                    for (FacetField.Count fcount : facetEntries) {
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
        logger.info("findRecordsForLocation: number of points = " + points.size());

        return points;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#findAllSpeciesByCircleAreaAndHigherTaxa(au.org.ala.biocache.dto.SpatialSearchRequestParams, String)
     */
    @Override
    public List<TaxaCountDTO> findAllSpeciesByCircleAreaAndHigherTaxa(SpatialSearchRequestParams requestParams, String speciesGroup) throws Exception {
        //add the context information
        updateQueryContext(requestParams);
        // format query so lsid searches are properly escaped, etc
        formatSearchQuery(requestParams);
        String queryString = buildSpatialQueryString(requestParams);
        logger.debug("The species count query " + queryString);
        List<String> fqList = new ArrayList<String>();
        //only add the FQ's if they are not the default values
        if(requestParams.getFq().length>0 && (requestParams.getFq()[0]).length()>0){
            org.apache.commons.collections.CollectionUtils.addAll(fqList, requestParams.getFq());
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
        solrQuery.setQueryType("standard");
        solrQuery.setQuery(query);
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField("state");
        solrQuery.setFacetMinCount(1);
        QueryResponse qr = runSolrQuery(solrQuery, null, 1, 0, "data_provider", "asc");
        List<FacetField> facets = qr.getFacetFields();
        FacetField ff = qr.getFacetField("state");
        if (ff != null) {
            for (Count count : ff.getValues()) {
                //only start adding counts when we hit a decade with some results.
                FieldResultDTO f = new FieldResultDTO(count.getName(), count.getCount());
                fDTOs.add(f);
            }
        }
        return fDTOs;
    }

    /**
     * Calculates the breakdown of the supplied query based on the supplied params
     */
    public TaxaRankCountDTO calculateBreakdown(BreakdownRequestParams queryParams) throws Exception {
        logger.debug("Attempting to find the counts for " + queryParams);
        TaxaRankCountDTO trDTO = null;
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQueryType("standard");
        formatSearchQuery(queryParams);
        solrQuery.setQuery(buildSpatialQueryString(queryParams));
        queryParams.setPageSize(0);
        solrQuery.setFacet(true);
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetSort("count");
        solrQuery.setFacetLimit(-1);
        //add the context information
        updateQueryContext(queryParams);
        //add the rank:name as a fq if necessary
        if(StringUtils.isNotEmpty(queryParams.getName()) && StringUtils.isNotEmpty(queryParams.getRank())){
            queryParams.setFq((String[])ArrayUtils.addAll(queryParams.getFq(), new String[]{queryParams.getRank() +":" + queryParams.getName()}));
        }
        //add the ranks as facets
        if(queryParams.getLevel() == null){
            List<String> ranks = queryParams.getRank()!= null?searchUtils.getNextRanks(queryParams.getRank(), queryParams.getName()==null) : searchUtils.getRanks();
            for (String r : ranks) {
                solrQuery.addFacetField(r);
            }
        } else {
            //the user has supplied the "exact" level at which to perform the breakdown
            solrQuery.addFacetField(queryParams.getLevel());
        }
        QueryResponse qr = runSolrQuery(solrQuery, queryParams);        
        if(queryParams.getMax() != null && queryParams.getMax() >0){
            //need to get the return level that the number of facets are <=max ranks need to be processed in reverse order until max is satisfied
            if (qr.getResults().getNumFound() > 0) {
                List<FacetField> ffs =qr.getFacetFields();
                //reverse the facets so that they are returned in rank reverse order species, genus, family etc
                Collections.reverse(ffs);
                for(FacetField ff : ffs){
                    //logger.debug("Handling " + ff.getName());
                    trDTO = new TaxaRankCountDTO(ff.getName());
                    if (ff.getValues() != null && ff.getValues().size() <= queryParams.getMax()){
                        List<FieldResultDTO> fDTOs = new ArrayList<FieldResultDTO>();
                        for (Count count : ff.getValues()) {
                            FieldResultDTO f = new FieldResultDTO(count.getName(), count.getCount());
                            fDTOs.add(f);
                        }
                        trDTO.setTaxa(fDTOs);
                        break;
                    }
                }
                
            }
        } else if(queryParams.getRank() != null || queryParams.getLevel() != null){
            //just want to process normally the rank to facet on will start with the highest rank and then go down until one exists for 
            if (qr.getResults().getNumFound() > 0) {
                List<FacetField> ffs =qr.getFacetFields();
                for (FacetField ff : ffs) {
                    trDTO = new TaxaRankCountDTO(ff.getName());                    
                    if (ff != null && ff.getValues() != null) {
                        List<Count> counts = ff.getValues();
                        if (counts.size() > 0) {
                            List<FieldResultDTO> fDTOs = new ArrayList<FieldResultDTO>();
                            for (Count count : counts) {
                                FieldResultDTO f = new FieldResultDTO(count.getName(), count.getCount());
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
     * @see au.org.ala.biocache.dao.SearchDAO#findTaxonCountForUid(au.org.ala.biocache.dto.BreakdownRequestParams, String)
     * @deprecated use {@link #calculateBreakdown(BreakdownRequestParams)} instead
     */
    @Deprecated
    public TaxaRankCountDTO findTaxonCountForUid(BreakdownRequestParams breakdownParams,String query) throws Exception {
        TaxaRankCountDTO trDTO = null;
        List<String> ranks = breakdownParams.getLevel()== null?searchUtils.getNextRanks(breakdownParams.getRank(), breakdownParams.getName()==null) : new ArrayList<String>();
        if(breakdownParams.getLevel() != null)
            ranks.add(breakdownParams.getLevel());
        if (ranks != null && ranks.size() > 0) {
            SolrQuery solrQuery = new SolrQuery();
            solrQuery.setQueryType("standard");
            solrQuery.setQuery(query);
            solrQuery.setRows(0);
            solrQuery.setFacet(true);
            solrQuery.setFacetMinCount(1);
            solrQuery.setFacetSort("count");
            solrQuery.setFacetLimit(-1); //we want all facets
            for (String r : ranks) {
                solrQuery.addFacetField(r);
            }
            QueryResponse qr = runSolrQuery(solrQuery, getQueryContextAsArray(breakdownParams.getQc()), 1, 0, breakdownParams.getRank(), "asc");
            if (qr.getResults().size() > 0) {
                for (String r : ranks) {
                    trDTO = new TaxaRankCountDTO(r);
                    FacetField ff = qr.getFacetField(r);
                    if (ff != null && ff.getValues() != null) {
                        List<Count> counts = ff.getValues();
                        if (counts.size() > 0) {
                            List<FieldResultDTO> fDTOs = new ArrayList<FieldResultDTO>();
                            for (Count count : counts) {
                                FieldResultDTO f = new FieldResultDTO(count.getName(), count.getCount());
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

        if (requestParams.getFq() != null) {
            for (String fq : requestParams.getFq()) {
                // pull apart fq. E.g. Rank:species and then sanitize the string parts
                // so that special characters are escaped appropriately
                if (fq ==null || fq.isEmpty()) {
                    continue;
                }
                // use of AND/OR requires correctly formed fq.
                // Can overlap with values containing the same,
                // case sensitivity may help.
                if(fq.contains(" OR ") || fq.contains(" AND ") || fq.contains("Intersects(")) { 
                    solrQuery.addFilterQuery(fq);
                    logger.info("adding filter query: " + fq);
                    continue;
                }
                String[] parts = fq.split(":", 2); // separate query field from query text
                if(parts.length>1){
                    logger.debug("fq split into: " + parts.length + " parts: " + parts[0] + " & " + parts[1]);
                    String prefix = null;
                    String suffix = null;
                    // don't escape range or '(multiple terms)' queries
                    if ((parts[1].contains("[") && parts[1].contains(" TO ") && parts[1].contains("]"))
                            || parts[0].startsWith("-(") || parts[0].startsWith("(")) {
                        prefix = parts[0];
                        suffix = parts[1];
                    } else {
                        if(parts[0].startsWith("-")) {
                            prefix = "-" + ClientUtils.escapeQueryChars(parts[0].substring(1));
                        } else {
                            prefix = ClientUtils.escapeQueryChars(parts[0]);
                        }
                        if(parts[1].equals("*")) {
                            suffix = parts[1];
                        } else {
                            boolean quoted = false;
                            StringBuffer sb = new StringBuffer();
                            if(parts[1].startsWith("\"") && parts[1].endsWith("\"")){
                                quoted = true;
                                parts[1] = parts[1].substring(1, parts[1].length()-1);
                                sb.append("\"");
                            }
                            sb.append(ClientUtils.escapeQueryChars(parts[1]));
                            if(quoted) sb.append("\"");
                            suffix = sb.toString();
                        }
                    }

                    //FIXME check for blank value and replace with constant
                    if(StringUtils.isEmpty(suffix)){
                        suffix = "Unknown";
                    }
                    solrQuery.addFilterQuery(prefix + ":" + suffix); // solrQuery.addFacetQuery(facetQuery)
                    logger.info("adding filter query: " + prefix + ":" + suffix);
                }
            }
        }

        solrQuery.setRows(requestParams.getPageSize());
        solrQuery.setStart(requestParams.getStart());
        solrQuery.setSortField(requestParams.getSort(), ORDER.valueOf(requestParams.getDir()));
        logger.debug("runSolrQuery: " + solrQuery.toString());
        QueryResponse qr =  getServer().query(solrQuery, queryMethod); // can throw exception
        if(logger.isDebugEnabled()){
            logger.debug("matched records: " + qr.getResults().getNumFound());
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
            logger.debug("Facet dates size: " + facetDates.size());
            facets.addAll(facetDates);
        }
        //Map<String, Map<String, List<String>>> highlights = qr.getHighlighting();
        List<OccurrenceIndex> results = qr.getBeans(resultClass);
        List<FacetResultDTO> facetResults = new ArrayList<FacetResultDTO>();
        searchResult.setTotalRecords(sdl.getNumFound());
        searchResult.setStartIndex(sdl.getStart());
        searchResult.setPageSize(solrQuery.getRows()); //pageSize
        searchResult.setStatus("OK");
        String[] solrSort = StringUtils.split(solrQuery.getSortField(), " "); // e.g. "taxon_name asc"
        logger.debug("sortField post-split: " + StringUtils.join(solrSort, "|"));
        searchResult.setSort(solrSort[0]); // sortField
        searchResult.setDir(solrSort[1]); // sortDirection
        searchResult.setQuery(params.getUrlParams()); //this needs to be the original URL>>>>
        searchResult.setOccurrences(results);
        // populate SOLR facet results
        if (facets != null) {
            for (FacetField facet : facets) {
                List<FacetField.Count> facetEntries = facet.getValues();                
                if ((facetEntries != null) && (facetEntries.size() > 0)) {
                    ArrayList<FieldResultDTO> r = new ArrayList<FieldResultDTO>();
                    Matcher m = uidPattern.matcher(facet.getName()+":value");
                    boolean isUid = m.matches();
                    for (FacetField.Count fcount : facetEntries) {
//                        String msg = fcount.getName() + ": " + fcount.getCount();                       
                        //logger.trace(fcount.getName() + ": " + fcount.getCount());
                        //check to see if the facet field is an uid value that needs substitution                        
                        if(isUid){
                            String displayName = searchUtils.getUidDisplayString(facet.getName(), fcount.getName(), false);
                            r.add(new FieldResultDTO(displayName, fcount.getCount(),facet.getName()+":\"" + fcount.getName()+"\""));
                        } else if(getAuthIndexFields().contains(facet.getName())){
                          //if the facet field is collector or assertion_user_id we need to perform the substitution
                            String displayName = authService.getDisplayNameFor(fcount.getName());                            
                            //now add the facet with the correct fq being supplied
                            r.add(new FieldResultDTO(displayName, fcount.getCount(),facet.getName()+":\"" + fcount.getName()+"\""));
                        } else {
                            r.add(new FieldResultDTO(fcount.getName(), fcount.getCount()));
                        }
                    }
                    // only add facets if there are more than one facet result
                    if (r.size() > 0) {
                        FacetResultDTO fr = new FacetResultDTO(facet.getName(), r);
                        facetResults.add(fr);
                    }
                }
            }
        }
        //all belong to uncertainty range for now
        if(facetQueries != null && !facetQueries.isEmpty()) {
            Map<String, String> rangeMap = RangeBasedFacets.getRangeMap("uncertainty");
            List<FieldResultDTO> fqr = new ArrayList<FieldResultDTO>();
            for(String value: facetQueries.keySet()){
                if(facetQueries.get(value)>0)
                    fqr.add(new FieldResultDTO(rangeMap.get(value), facetQueries.get(value),value));
            }
            facetResults.add(new FacetResultDTO("uncertainty", fqr));
        }
        
        //handle all the range based facets
        if(qr.getFacetRanges() != null){
            for(RangeFacet rfacet : qr.getFacetRanges()){
                List<FieldResultDTO> fqr = new ArrayList<FieldResultDTO>();
                if(rfacet instanceof Numeric){
                    Numeric nrfacet = (Numeric)rfacet;
                    List<RangeFacet.Count> counts= nrfacet.getCounts();
                    //handle the before
                    if(nrfacet.getBefore().intValue()>0){
                      fqr.add(new FieldResultDTO("[* TO "+getUpperRange(nrfacet.getStart().toString(), nrfacet.getGap(),false)+"]",nrfacet.getBefore().intValue()));
                    }
                    for(RangeFacet.Count count:counts){                        
                        String title = getRangeValue(count.getValue(), nrfacet.getGap());
                        fqr.add(new FieldResultDTO(title,count.getCount())); 
                    }
                    //handle the after 
                    if(nrfacet.getAfter().intValue()>0){
                      fqr.add(new FieldResultDTO("["+nrfacet.getEnd().toString()+" TO *]",nrfacet.getAfter().intValue()));
                    }
                    facetResults.add(new FacetResultDTO(nrfacet.getName(), fqr));
                }
              //org.apache.solr.client.solrj.response.RangeFacet$Numeric
                //int gap = rfacet.getGap() -1;
                //System.out.println(rfacet.getClass());
            }
        }

        searchResult.setFacetResults(facetResults);
        // The query result is stored in its original format so that all the information
        // returned is available later on if needed
        searchResult.setQr(qr);
        return searchResult;
    }
    
    private String getRangeValue(String lower, Number gap){
        StringBuilder value=new StringBuilder("[");
        value.append(lower). append(" TO ").append(getUpperRange(lower,gap,true));
        return value.append("]").toString();
    }

    private String getUpperRange(String lower, Number gap, boolean addGap){
        if (gap instanceof Integer) {
          Integer upper = Integer.parseInt(lower) - 1;
          if(addGap)
              upper+= (Integer) gap;
          return upper.toString();
        } else if (gap instanceof Double) {
          BigDecimal upper = new BigDecimal(lower).add(new BigDecimal(-0.001));
          if(addGap)
              upper = upper.add(new BigDecimal(gap.doubleValue()));
          return upper.setScale(3, RoundingMode.HALF_UP).toString();
        } else {
          return lower;
        }    
    }

    /**
     * Build the query string for a spatial query (using Spatial-Solr plugin syntax)
     *
     * TODO change param type to SearchRequestParams
     *
     * New plugin syntax
     * {!spatial circles=52.347,4.453,10}
     *
     * TODO different types of spatial queries...
     *
     * @param fullTextQuery
     * @param latitude
     * @param longitude
     * @param radius
     * @return
     */
    protected String buildSpatialQueryString(String fullTextQuery, Float latitude, Float longitude, Float radius) {
        StringBuilder sb= new StringBuilder();
        sb.append(spatialField).append(":\"Intersects(Circle(").append(longitude.toString());
        sb.append(" ").append(latitude.toString()).append(" d=").append(SpatialUtils.convertToDegrees(radius).toString());
        sb.append("))\"");
        if(StringUtils.isNotEmpty(fullTextQuery)){
          sb.append(" AND (").append(fullTextQuery).append(")");
        }
        return sb.toString();
    }
    
    protected String buildSpatialQueryString(SpatialSearchRequestParams searchParams){
        if(searchParams != null){
            StringBuilder sb = new StringBuilder();
            if(searchParams.getLat() != null){
                sb.append(spatialField).append(":\"Intersects(Circle(");
                sb.append(searchParams.getLon().toString()).append(" ").append(searchParams.getLat().toString());
                sb.append(" d=").append(SpatialUtils.convertToDegrees(searchParams.getRadius()).toString());
                sb.append("))\"");
            } else if(!StringUtils.isEmpty(searchParams.getWkt())){
                //format the wkt               
                sb.append(SpatialUtils.getWKTQuery(spatialField, searchParams.getWkt(), false));
            }
            String query = StringUtils.isEmpty(searchParams.getFormattedQuery())? searchParams.getQ() : searchParams.getFormattedQuery();
            if(StringUtils.isNotEmpty(query)){
                if(sb.length()>0){
                    sb.append(" AND (");
                    sb.append(query).append(")");
                } else {
                    sb.append(query);
                }
            }
            return sb.toString();
        }
        return null;
    }

    protected void formatSearchQuery(SpatialSearchRequestParams searchParams) {
        formatSearchQuery(searchParams,false);
    }

    /**
     * Format the search input query for a full-text search.
     *
     * This includes constructing a user friendly version of the query to
     * be used for display purposes.
     * 
     * TODO Fix this to use a state.  REVISE!!
     *
     * @param searchParams
     */
    protected void formatSearchQuery(SpatialSearchRequestParams searchParams, boolean forceQueryFormat) {
        //Only format the query if it doesn't already supply a formattedQuery.
        if(forceQueryFormat || StringUtils.isEmpty(searchParams.getFormattedQuery())){
            // set the query
            String query = searchParams.getQ();
            
            //cached query parameters are already formatted
            if(query.contains("qid:")) {            
                Matcher matcher = qidPattern.matcher(query);
                long qid = 0;
                while(matcher.find()) {
                    String value = matcher.group();
                    try {
                        String qidValue = SearchUtils.stripEscapedQuotes(value.substring(4));
                        qid = Long.parseLong(qidValue);
                        ParamsCacheObject pco = ParamsCache.get(qid);
                        if(pco != null) {
                            searchParams.setQId(qid);
                            searchParams.setQ(pco.getQ());
                            //add the fqs from the params cache
                            if(pco.getFqs() != null){
                                String [] currentFqs = searchParams.getFq();
                                if(currentFqs == null || (currentFqs.length==1&&currentFqs[0].length()==0)){
                                    searchParams.setFq(pco.getFqs());
                                } else{
                                    //we need to add the current Fqs together
                                    searchParams.setFq((String[])ArrayUtils.addAll(currentFqs, pco.getFqs()));
                                }
                            }
                            String displayString = pco.getDisplayString();

                            if(StringUtils.isNotEmpty(pco.getWkt())){
                                displayString = displayString + " within user defined polygon" ;
                            }
                            searchParams.setDisplayString(displayString);

                            if(searchParams instanceof SpatialSearchRequestParams) {
                                ((SpatialSearchRequestParams) searchParams).setWkt(pco.getWkt());
                            } else if(StringUtils.isNotEmpty(pco.getWkt())) {
                                String originalQ = searchParams.getQ();
                                searchParams.setQ(spatialField +":\"Intersects(" + pco.getWkt() +")");
                                if(StringUtils.isNotEmpty(originalQ))
                                  searchParams.setQ(searchParams.getQ() + " AND " + originalQ);
                            }
                            searchParams.setFormattedQuery(searchParams.getQ());
                            return;
                        }
                    } catch (NumberFormatException e) {
                    } catch (ParamsCacheMissingException e) {
                    }
                }
            }
            StringBuffer queryString = new StringBuffer();
            StringBuffer displaySb = new StringBuffer();
            String displayString = query;

            // look for field:term sub queries and catch fields: matched_name & matched_name_children
            if (query.contains(":")) {
                // will match foo:bar, foo:"bar bash" & foo:bar\ bash
                Matcher matcher = termPattern.matcher(query);
                queryString.setLength(0);

                while (matcher.find()) {
                    String value = matcher.group();
                    logger.debug("term query: " + value );
                    logger.debug("groups: " + matcher.group(1) + "|" + matcher.group(2) );

                    if ("matched_name".equals(matcher.group(1))) {
                        // name -> accepted taxon name (taxon_name:)
                        String field = matcher.group(1);
                        String queryText = matcher.group(2);

                        if (queryText != null && !queryText.isEmpty()) {
                            String guid = speciesLookupService.getGuidForName(queryText.replaceAll("\"", "")); // strip any quotes
                            logger.info("GUID for " + queryText + " = " + guid);

                            if (guid != null && !guid.isEmpty()) {
                                String acceptedName = speciesLookupService.getAcceptedNameForGuid(guid); // strip any quotes
                                logger.info("acceptedName for " + queryText + " = " + acceptedName);

                                if (acceptedName != null && !acceptedName.isEmpty()) {
                                    field = "taxon_name";
                                    queryText = acceptedName;
                                }
                            } else {
                                field = "taxon_name";
                            }

                            // also change the display query
                            displayString = displayString.replaceAll("matched_name", "taxon_name");
                        }

                        if (StringUtils.containsAny(queryText, CHARS) && !queryText.startsWith("[")) {
                            // quote any text that has spaces or colons but not range queries
                            queryText = QUOTE + queryText + QUOTE;
                        }

                        logger.debug("queryText: " + queryText);

                        matcher.appendReplacement(queryString, matcher.quoteReplacement(field + ":" + queryText));

                    } else if ("matched_name_children".equals(matcher.group(1))) {
                        String field = matcher.group(1);
                        String queryText = matcher.group(2);

                        if (queryText != null && !queryText.isEmpty()) {
                            String guid = speciesLookupService.getGuidForName(queryText.replaceAll("\"", "")); // strip any quotes
                            logger.info("GUID for " + queryText + " = " + guid);

                            if (guid != null && !guid.isEmpty()) {
                                field = "lsid";
                                queryText = guid;
                            }else {
                                field = "taxon_name";
                            }
                        }

                        if (StringUtils.containsAny(queryText, CHARS) && !queryText.startsWith("[")) {
                            // quote any text that has spaces or colons but not range queries
                            queryText = QUOTE + queryText + QUOTE;
                        }

                        matcher.appendReplacement(queryString, matcher.quoteReplacement(field + ":" + queryText));
                    } else {
                        matcher.appendReplacement(queryString, matcher.quoteReplacement(value));
                    }
                }
                matcher.appendTail(queryString);
                query = queryString.toString();
            }
            
            //if the query string contains lsid: we will need to replace it with the corresponding lft range
            int last = 0;
            if (query.contains("lsid:")) {
                Matcher matcher = lsidPattern.matcher(query);
                queryString.setLength(0);
                while (matcher.find()) {
                    //only want to process the "lsid" if it does not represent taxon_concept_lsid etc...
                    if((matcher.start() >0 && query.charAt(matcher.start()-1) != '_') || matcher.start() == 0){
                    String value = matcher.group();
                    logger.debug("preprocessing " + value);
                    String lsid = matcher.group(2);                    
                    if (lsid.contains("\"")) {
                        //remove surrounding quotes, if present
                        lsid = lsid.replaceAll("\"","");
                    }
                    if (lsid.contains("\\")) {
                        //remove internal \ chars, if present
                        //noinspection MalformedRegex
                        lsid = lsid.replaceAll("\\","");
                    }
                    logger.debug("lsid = " + lsid);
                    String[] values = searchUtils.getTaxonSearch(lsid);
                    String lsidHeader = matcher.group(1).length()>0? matcher.group(1):""; 
                    matcher.appendReplacement(queryString, lsidHeader +values[0]);
                    displaySb.append(query.substring(last, matcher.start()));
                    if(!values[1].startsWith("taxon_concept_lsid:"))
                        displaySb.append(lsidHeader).append("<span class='lsid' id='").append(lsid).append("'>").append(values[1]).append("</span>");
                    else
                        displaySb.append(lsidHeader).append(values[1]);
                    last = matcher.end();
                    //matcher.appendReplacement(displayString, values[1]);
                    }
                }
                matcher.appendTail(queryString);
                displaySb.append(query.substring(last, query.length()));
    
               
                query = queryString.toString();
                displayString = displaySb.toString();
            }

            if (query.contains("urn")) {
                //escape the URN strings before escaping the rest this avoids the issue with attempting to search on a urn field
                Matcher matcher = urnPattern.matcher(query);
                queryString.setLength(0);
                while (matcher.find()) {
                    String value = matcher.group();
                    
                    logger.debug("escaping lsid urns  " + value );
                    matcher.appendReplacement(queryString,prepareSolrStringForReplacement(value));
                }
                matcher.appendTail(queryString);
                query = queryString.toString();
            }

            if(query.contains("Intersects")){
                Matcher matcher = spatialPattern.matcher(query);
                if(matcher.find()){
                    String spatial = matcher.group();
                    SpatialSearchRequestParams subQuery = new SpatialSearchRequestParams();
                    logger.debug("region Start : " + matcher.regionStart() + " start :  "+ matcher.start() + " spatial length " + spatial.length() + " query length " + query.length());
                    //format the search query of the remaining text only
                    subQuery.setQ(query.substring(matcher.start() + spatial.length(), query.length()));
                    //format the remaining query
                    formatSearchQuery(subQuery);
                    
                    //now append Q's together
                    queryString.setLength(0);
                    //need to include the prefix
                    queryString.append(query.substring(0, matcher.start()));
                    queryString.append(spatial);
                    queryString.append(subQuery.getFormattedQuery());
                    searchParams.setFormattedQuery(queryString.toString());
                    //add the spatial information to the display string
                    if(spatial.contains("circles")){
                        String[] values = spatial.substring(spatial.indexOf("=") +1 , spatial.indexOf("}")).split(",");
                        if(values.length == 3){
                            displaySb.setLength(0);
                            displaySb.append(subQuery.getDisplayString());
                            displaySb.append(" - within ").append(values[2]).append(" km of point(")
                            .append(values[0]).append(",").append(values[1]).append(")");
                            searchParams.setDisplayString(displaySb.toString());
                        }
                        
                    } else {
                        searchParams.setDisplayString(subQuery.getDisplayString() + " - within supplied region");
                    }
                }
            } else {
                //escape reserved characters unless the colon represnts a field name colon
                queryString.setLength(0);
    
                Matcher matcher = spacesPattern.matcher(query);
                while(matcher.find()){
                    String value = matcher.group();
    
                    //special cases to ignore from character escaping
                    //if the value is a single - or * it means that we don't want to escape it as it is likely to have occurred in the following situation -(occurrence_date:[* TO *]) or *:*
                    if(!value.equals("-") && /*!value.equals("*")  && !value.equals("*:*") && */ !value.endsWith("*")){
    
                        //split on the colon
                        String[] bits = StringUtils.split(value, ":", 2);
                        if(bits.length == 2){
                            if(!bits[0].contains("urn") && !bits[1].contains("urn\\"))
                                matcher.appendReplacement(queryString, bits[0] +":"+ prepareSolrStringForReplacement(bits[1]));
    
                        } else if(!value.endsWith(":")){
                            //need to ignore field names where the : is at the end because the pattern matching will return field_name: as a match when it has a double quoted value
                            //default behaviour is to escape all 
                            matcher.appendReplacement(queryString, prepareSolrStringForReplacement(value));
                        }
                    }
                }
                matcher.appendTail(queryString);
    
                //substitute better display strings for collection/inst etc searches
                if(displayString.contains("_uid")){
                    displaySb.setLength(0);
                    String normalised = displayString.replaceAll("\"", "");
                    matcher = uidPattern.matcher(normalised);
                    while(matcher.find()){
                        String newVal = "<span>"+searchUtils.getUidDisplayString(matcher.group(1),matcher.group(2)) +"</span>";
                        if(newVal != null)
                            matcher.appendReplacement(displaySb, newVal);
                    }
                    matcher.appendTail(displaySb);
                    displayString = displaySb.toString();
                }
                if(searchParams.getQ().equals("*:*")){
                    displayString ="[all records]";
                }
                if(searchParams.getLat() != null && searchParams.getLon() != null && searchParams.getRadius() != null ){
                    displaySb.setLength(0);
                    displaySb.append(displayString);
                    displaySb.append(" - within ").append(searchParams.getRadius()).append(" km of point(")
                    .append(searchParams.getLat()).append(",").append(searchParams.getLon()).append(")");
                    displayString = displaySb.toString();
                    
                }

                // substitute i18n version of field name, if found in messages.properties
                displayString = formatDisplayStringWithI18n(displayString);

                searchParams.setFormattedQuery(queryString.toString());
                logger.debug("formattedQuery = " + queryString);
                logger.debug("displayString = " + displayString);
                searchParams.setDisplayString(displayString);
            }

            //format the fq's for facets that need ranges substituted
            for(int i=0; i<searchParams.getFq().length; i++){
                String fq = searchParams.getFq()[i];
                String[] parts = fq.split(":", 2);
                //check to see if the first part is a range based query and update if necessary
                Map<String, String> titleMap = RangeBasedFacets.getTitleMap(parts[0]);
                if(titleMap != null){
                    searchParams.getFq()[i]= titleMap.get(parts[1]);
                }
            }
        }
        searchParams.setDisplayString(formatDisplayStringWithI18n(searchParams.getDisplayString()));
    }

    /**
     * Substitute with i18n properties
     *
     * @param displayText
     * @return
     */
    public String formatDisplayStringWithI18n(String displayText){

        if(StringUtils.trimToNull(displayText) == null) return displayText;
        try {
            String formatted = displayText;

            Matcher m = indexFieldPatternMatcher.matcher(displayText);
            int currentPos = 0;
            while(m.find(currentPos)){
                String matchedIndexTerm = m.group(0).replaceAll(":","");
                MatchResult mr = m.toMatchResult();
                //if the matched term represents a layer lookup the title in the layers service
                Matcher lm = layersPattern.matcher(matchedIndexTerm);
                String i18n="";
                if(lm.matches()){
                    i18n = layersService.getName(matchedIndexTerm);
                    if(i18n == null){
                        i18n = matchedIndexTerm;
                    }
                } else {
                    i18n = messageSource.getMessage("facet."+matchedIndexTerm, null, matchedIndexTerm, null);
                }
                //System.out.println("i18n for " + matchedIndexTerm + " = " + i18n);
                if (!matchedIndexTerm.equals(i18n)) {

                  int nextWhitespace = displayText.substring(mr.end()).indexOf(" ");
                  String extractedValue = null;
                  if(nextWhitespace > 0){
                    extractedValue = displayText.substring(mr.end(), mr.end() + nextWhitespace);
                  } else {
                      //reached the end of the query
                    extractedValue = displayText.substring(mr.end());
                  }

                  String formattedExtractedValue = SearchUtils.stripEscapedQuotes(extractedValue);

                  String i18nForValue = messageSource.getMessage(matchedIndexTerm + "." + formattedExtractedValue, null, "", null);
                  if(i18nForValue.length() == 0) i18nForValue = messageSource.getMessage(formattedExtractedValue, null, "", null);

                  if(i18nForValue.length()>0){
                      formatted = formatted.replaceAll(matchedIndexTerm + ":"+ extractedValue, i18n + ":" + i18nForValue);
                  } else {
                      //just replace the matched index term
                      formatted = formatted.replaceAll(matchedIndexTerm,i18n);
                  }
                }
                currentPos = mr.end();
            }
            return formatted;

        } catch (Exception e){
            logger.debug(e.getMessage(),e);
            return displayText;
        }
    }

    /**
     * Creates a SOLR escaped string the can be used in a StringBuffer.appendReplacement
     * The appendReplacement needs an extra delimiting on the backslashes
     * @param value
     * @return
     */
    private String prepareSolrStringForReplacement(String value){
        //if starts and ends with quotes just escape the inside       
        boolean quoted = false;
        StringBuffer sb = new StringBuffer();
        if(value.startsWith("\"") && value.endsWith("\"")){
            quoted = true;
            value = value.substring(1, value.length()-1);
            sb.append("\"");
        }
        sb.append(ClientUtils.escapeQueryChars(value).replaceAll("\\\\", "\\\\\\\\"));
        if(quoted) sb.append("\"");        
        return sb.toString();
    }

    /**
     * Updates the supplied search params to cater for the query context
     * @param searchParams
     */
    protected void updateQueryContext(SearchRequestParams searchParams){
        //TODO better method of getting the mappings between qc on solr fields names
        String qc = searchParams.getQc();
        if(StringUtils.isNotEmpty(qc)){
            //add the query context to the filter query
            searchParams.setFq((String[])ArrayUtils.addAll(searchParams.getFq(), getQueryContextAsArray(qc)));
        }
    }

    protected String[] getQueryContextAsArray(String queryContext){
        if(StringUtils.isNotEmpty(queryContext)){
            String[] values = queryContext.split(",");
            for(int i =0; i<values.length;i++){
                String field = values[i];
                values[i]= field.replace("hub:", "data_hub_uid:");
            }
            //add the query context to the filter query
            return values;
        }
        return new String[]{};
    }
    
   protected void initDecadeBasedFacet(SolrQuery solrQuery,String field){
       solrQuery.add("facet.date", field);
       solrQuery.add("facet.date.start", "1850-01-01T00:00:00Z"); // facet date range starts from 1850
       solrQuery.add("facet.date.end", "NOW/DAY"); // facet date range ends for current date (gap period)
       solrQuery.add("facet.date.gap", "+10YEAR"); // gap interval of 10 years
       solrQuery.add("facet.date.other", "before"); // include counts before the facet start date ("before" label)
       solrQuery.add("facet.date.include", "lower"); // counts will be included for dates on the starting date but not ending date
   }

    /**
     * Helper method to create SolrQuery object and add facet settings
     *
     * @return solrQuery the SolrQuery
     */
    protected SolrQuery initSolrQuery(SearchRequestParams searchParams, boolean substituteDefaultFacetOrder,Map<String,String[]> extraSolrParams) {

        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQueryType("standard");
        boolean rangeAdded = false;
        // Facets
        solrQuery.setFacet(searchParams.getFacet());
        if(searchParams.getFacet()) {
            for (String facet : searchParams.getFacets()) {
                if (facet.equals("date") || facet.equals("decade")) {
                    String fname = facet.equals("decade") ? "occurrence_year" : "occurrence_"+facet;
                    initDecadeBasedFacet(solrQuery, fname);
                } else if(facet.equals("uncertainty")){
                    Map<String, String> rangeMap = RangeBasedFacets.getRangeMap("uncertainty");
                    for(String range: rangeMap.keySet()){
                        solrQuery.add("facet.query", range);
                    }
                } else if(facet.endsWith(RANGE_SUFFIX)){
                    //this facte need to have it ranges included.
                    if(!rangeAdded){
                        solrQuery.add("facet.range.other","before");
                        solrQuery.add("facet.range.other", "after");
                    }
                    String field = facet.replaceAll(RANGE_SUFFIX, "");
                    StatsIndexFieldDTO details = getRangeFieldDetails(field);
                    if(details != null){
                        solrQuery.addNumericRangeFacet(field, details.getStart(), details.getEnd(), details.getGap());
                    }
                } else {
                    solrQuery.addFacetField(facet);
                    
                    if("".equals(searchParams.getFsort()) && substituteDefaultFacetOrder && FacetThemes.facetsMap.containsKey(facet)){
                      //now check if the sort order is different to supplied
                      String thisSort = FacetThemes.facetsMap.get(facet).getSort();
                      if(!searchParams.getFsort().equalsIgnoreCase(thisSort))
                          solrQuery.add("f."+facet+".facet.sort",thisSort);
                    }

                }
            }

            solrQuery.setFacetMinCount(1);
            solrQuery.setFacetLimit(searchParams.getFlimit());
            //include this so that the default fsort is still obeyed.
            String fsort = "".equals(searchParams.getFsort()) ? "count":searchParams.getFsort();
            solrQuery.setFacetSort(fsort);
            if(searchParams.getFoffset()>0)
              solrQuery.add("facet.offset", Integer.toString(searchParams.getFoffset()));
            if(StringUtils.isNotEmpty(searchParams.getFprefix()))
              solrQuery.add("facet.prefix", searchParams.getFprefix());
        }

        solrQuery.setRows(10);
        solrQuery.setStart(0);

        if (searchParams.getFl().length() > 0) {
            solrQuery.setFields(searchParams.getFl());
        } else {
            solrQuery.setFields(OccurrenceIndex.defaultFields);
        }
        
        //add the extra SOLR params
        if(extraSolrParams != null){
            //automatically include the before and after params...
            if(!rangeAdded){
                solrQuery.add("facet.range.other","before");
                solrQuery.add("facet.range.other", "after");
            }
            for(String key : extraSolrParams.keySet()){
                String[] values = extraSolrParams.get(key);
                solrQuery.add(key, values);
            }
        }
        return solrQuery;
    }

    /**
     * Obtains the Statistics for the supplied field so it can be used to determine the ranges.
     * @param field
     * @return
     */
    private StatsIndexFieldDTO getRangeFieldDetails(String field){
        if(rangeFieldCache == null)
            rangeFieldCache = new HashMap<String, StatsIndexFieldDTO>();
        StatsIndexFieldDTO details=rangeFieldCache.get(field);
        if(details == null && indexFieldMap!=null){
            //get the details
            SpatialSearchRequestParams searchParams = new SpatialSearchRequestParams();
            searchParams.setQ("*:*");
            searchParams.setFacets(new String[]{field});
            try {
                Map<String, FieldStatsInfo> stats = getStatistics(searchParams);
                if(stats != null){
                    IndexFieldDTO ifdto =indexFieldMap.get(field);
                    if(ifdto !=null){
                        String type = ifdto.getDataType();                    
                        details = new StatsIndexFieldDTO(stats.get(field), type);
                        rangeFieldCache.put(field, details);
                    } else {
                        logger.debug("Unable to locate field:  " + field);
                        return null;
                    }
                }
            } catch(Exception e){
                logger.warn("Unable to obtain range from cache." ,e);
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
        solrQuery.setQueryType("standard");
        solrQuery.setQuery(queryString);
        
        if (filterQueries != null && filterQueries.size()>0) {
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
            logger.debug("adding facetField: " + facet);
        }
        //set the facet starting point based on the paging information
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(pageSize); // unlimited = -1 | pageSize
        solrQuery.add("facet.offset", Integer.toString(startIndex));
        logger.debug("getSpeciesCount query :" + solrQuery.getQuery());
        QueryResponse qr = runSolrQuery(solrQuery, null, 1, 0, "score", sortDirection);
        logger.info("SOLR query: " + solrQuery.getQuery() + "; total hits: " + qr.getResults().getNumFound());
        List<FacetField> facets = qr.getFacetFields();
        java.util.regex.Pattern p = java.util.regex.Pattern.compile("\\|");

        if (facets != null && facets.size() > 0) {
            logger.debug("Facets: " + facets.size() + "; facet #1: " + facets.get(0).getName());
            for (FacetField facet : facets) {
                List<FacetField.Count> facetEntries = facet.getValues();
                if ((facetEntries != null) && (facetEntries.size() > 0)) {
                    //NO need to page through all facets to locate the current page...
                    //for (int i = 0; i < facetEntries.size(); i++) {
                    //int highestEntry = (pageSize < 0) ? facetEntries.size() : startIndex + pageSize;
                    //int lastEntry = (highestEntry > facetEntries.size()) ? facetEntries.size() : highestEntry;
                    //logger.debug("highestEntry = " + highestEntry + ", facetEntries.size = " + facetEntries.size() + ", lastEntry = " + lastEntry);
                    //for (int i = startIndex; i < lastEntry; i++) {
                    for(FacetField.Count fcount : facetEntries){
                        //FacetField.Count fcount = facetEntries.get(i);
                        //speciesCounts.add(i, new TaxaCountDTO(fcount.getName(), fcount.getCount()));
                        TaxaCountDTO tcDTO = null;
                        if (fcount.getFacetField().getName().equals(NAMES_AND_LSID)) {
                            String[] values = p.split(fcount.getName(),5);
                            
                            if (values.length >= 5) {
                                if(!"||||".equals(fcount.getName())){
                                    tcDTO = new TaxaCountDTO(values[0], fcount.getCount());
                                    tcDTO.setGuid(StringUtils.trimToNull(values[1]));
                                    tcDTO.setCommonName(values[2]);
                                    tcDTO.setKingdom(values[3]);
                                    tcDTO.setFamily(values[4]);
                                    if(StringUtils.isNotEmpty(tcDTO.getGuid()))
                                        tcDTO.setRank(searchUtils.getTaxonSearch(tcDTO.getGuid())[1].split(":")[0]);
                                }
                            }
                            else{
                                logger.debug("The values length: " + values.length + " :" + fcount.getName());
                                tcDTO = new TaxaCountDTO(fcount.getName(), fcount.getCount());
                            }
                            //speciesCounts.add(i, tcDTO);
                            if(tcDTO != null)
                                speciesCounts.add(tcDTO);
                        }
                        else if(fcount.getFacetField().getName().equals(COMMON_NAME_AND_LSID)){
                            String[] values = p.split(fcount.getName(),6);
                            
                            if(values.length >= 5){
                                if(!"|||||".equals(fcount.getName())){
                                    tcDTO = new TaxaCountDTO(values[1], fcount.getCount());
                                    tcDTO.setGuid(StringUtils.trimToNull(values[2]));
                                    tcDTO.setCommonName(values[0]);
                                    //cater for the bug of extra vernacular name in the result
                                    tcDTO.setKingdom(values[values.length-2]);
                                    tcDTO.setFamily(values[values.length-1]);
                                    if(StringUtils.isNotEmpty(tcDTO.getGuid()))
                                        tcDTO.setRank(searchUtils.getTaxonSearch(tcDTO.getGuid())[1].split(":")[0]);
                                }
                            } else {
                                logger.debug("The values length: " + values.length + " :" + fcount.getName());
                                tcDTO = new TaxaCountDTO(fcount.getName(), fcount.getCount());
                            }
                            //speciesCounts.add(i, tcDTO);
                            if(tcDTO != null){
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
        formatSearchQuery(searchParams);
        logger.info("The query : " + searchParams.getFormattedQuery());
        solrQuery.setQuery(buildSpatialQueryString(searchParams));
        solrQuery.setQueryType("standard");
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.setFacetMinCount(1);
        solrQuery.addFacetField("data_provider_uid");
        solrQuery.addFacetField("data_resource_uid");
        solrQuery.addFacetField("collection_uid");
        solrQuery.addFacetField("institution_uid");
        QueryResponse qr = runSolrQuery(solrQuery, searchParams.getFq(), 1, 0, "score", "asc");
        //now cycle through and get all the facets
        List<FacetField> facets = qr.getFacetFields();
        for (FacetField facet : facets) {
            if (facet.getValues() != null) {
                for (FacetField.Count ffc : facet.getValues()) {
                    uidStats.put(ffc.getName(), new Integer((int) ffc.getCount()));
                }
            }
        }
        return uidStats;
    }

    /**
     * Gets the details about the SOLR fields using the LukeRequestHandler:
     * See http://wiki.apache.org/solr/LukeRequestHandler  for more information
     */
    public Set<IndexFieldDTO> getIndexFieldDetails(String... fields) throws Exception{
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("qt", "/admin/luke");
        
        params.set("tr", "luke.xsl");
        if(fields != null){
            params.set("fl" ,fields);
            params.set("numTerms", "1");
        }
        else
            params.set("numTerms", "0");        
        QueryResponse response = getServer().query(params, queryMethod);
        return parseLukeResponse(response.toString(), fields != null);
    }
    /**
     * Returns the count of distinct values for the facets.  Uses groups.  Needs some more 
     * work to determine best use...
     * 
     * TODO work out whether or not we should allow facet ranges to be downloaded....
     * 
     */
    public List<FacetResultDTO> getFacetCounts(SpatialSearchRequestParams searchParams) throws Exception{
        formatSearchQuery(searchParams);            
        //add context information
        updateQueryContext(searchParams);
        String queryString = buildSpatialQueryString(searchParams);        
        searchParams.setFacet(false);        
        searchParams.setPageSize(0);
        SolrQuery query = initSolrQuery(searchParams, false,null);
        query.setQuery(queryString);
        //now use the supplied facets to add groups to the query
        query.add("group", "true");
        query.add("group.ngroups","true");
        query.add("group.limit","0");        
        for(String facet: searchParams.getFacets()){
            //query.add("sort", facet + " asc");
            query.add("group.field",facet);
        }
        QueryResponse response = getServer().query(query, queryMethod);
        GroupResponse groupResponse = response.getGroupResponse();
        //System.out.println(groupResponse);
        List<FacetResultDTO> facetResults = new ArrayList<FacetResultDTO>();
        for(GroupCommand gc :groupResponse.getValues()){
            List<FieldResultDTO> r = new ArrayList<FieldResultDTO>();
            for(org.apache.solr.client.solrj.response.Group g: gc.getValues()){
                r.add(new FieldResultDTO(g.getGroupValue(), g.getResult().getNumFound()));
            }             
            facetResults.add(new FacetResultDTO(gc.getName(), r, gc.getNGroups()));
        }
        
        return facetResults;
    }
    
    /**
     * Returns details about the fields in the index.
     */
    public Set<IndexFieldDTO> getIndexedFields() throws Exception{        
        if(indexFields == null){
            indexFields = getIndexFieldDetails(null);
            indexFieldMap = new HashMap<String, IndexFieldDTO>();
            for(IndexFieldDTO field:indexFields){
                indexFieldMap.put(field.getName(), field);
            }
        }
        return indexFields;        
    }
    

    /**
     * parses the response string from the service that returns details about the indexed fields
     * @param str
     * @return
     */
    private  Set<IndexFieldDTO> parseLukeResponse(String str, boolean includeCounts) {

        Set<IndexFieldDTO> fieldList = includeCounts?new java.util.LinkedHashSet<IndexFieldDTO>():new java.util.TreeSet<IndexFieldDTO>();
        
        Pattern typePattern = Pattern.compile("(?:type=)([a-z]{1,})");

        Pattern schemaPattern = Pattern.compile("(?:schema=)([a-zA-Z\\-]{1,})");
        
        Pattern distinctPattern = Pattern.compile("(?:distinct=)([0-9]{1,})");

        String[] fieldsStr = str.split("fields=\\{");

        for (String fieldStr : fieldsStr) {
            if (fieldStr != null && !"".equals(fieldStr)) {
                String[] fields = includeCounts?fieldStr.split("\\}\\},"):fieldStr.split("\\},");

                for (String field : fields) {
                    if (field != null && !"".equals(field)) {
                        IndexFieldDTO f = new IndexFieldDTO();
                        
                        String fieldName = field.split("=")[0];
                        String type = null;
                        String schema = null;
                        Matcher typeMatcher = typePattern.matcher(field);
                        if (typeMatcher.find(0)) {
                            type = typeMatcher.group(1);
                        }
                        
                        Matcher schemaMatcher = schemaPattern.matcher(field);
                        if (schemaMatcher.find(0)) {
                            schema = schemaMatcher.group(1);
                        }
                        if(schema != null){
                            //logger.debug("fieldName:" + fieldName +", type:" + type);
//                            logger.debug("schema:" + schema);
                            //don't allow the sensitive coordinates to be exposed via ws
                            if(fieldName != null && !fieldName.startsWith("sensitive")){
                                
                                f.setName(fieldName);
                                f.setDataType(type);
                                //interpret the schema information
                                f.setIndexed(schema.contains("I"));
                                f.setStored(schema.contains("S"));
                                
                                //now add the i18n string associated with the field
                                if(layersPattern.matcher(fieldName).matches()){
                                    //System.out.println(layersService.getLayerNameMap());
                                    String description = layersService.getLayerNameMap().get(fieldName);                                    
                                    f.setDescription(description);
                                } else {
                                    //check as a field name
                                    String description =messageSource.getMessage("facet."+fieldName, null, "", Locale.getDefault());
                                    if(!description.startsWith("facet.")){
                                        f.setDescription(description);
                                    }
                                }
                                fieldList.add(f);
                            }
                        }
                        Matcher distinctMatcher = distinctPattern.matcher(field);
                        if(distinctMatcher.find(0)){
                            Integer distinct = Integer.parseInt(distinctMatcher.group(1));
                            f.setNumberDistinctValues(distinct);
                        }
                    }
                }
            }
        }
        return fieldList;
    }

    /**
     * @see au.org.ala.biocache.dao.SearchDAO#findByFulltext(SpatialSearchRequestParams)
     */
    @Override
    public SolrDocumentList findByFulltext(SpatialSearchRequestParams searchParams) throws Exception {
        SolrDocumentList sdl = null;

        try {
            //String queryString = formatSearchQuery(query);
            formatSearchQuery(searchParams);
            //add context information
            updateQueryContext(searchParams);
            String queryString = buildSpatialQueryString(searchParams);
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
    public Map<String, FieldStatsInfo> getStatistics(SpatialSearchRequestParams searchParams) throws Exception{
        String[] values = new String[2];
        try{
            formatSearchQuery(searchParams);
            //add context information
            updateQueryContext(searchParams);
            String queryString = buildSpatialQueryString((SpatialSearchRequestParams)searchParams);
            SolrQuery solrQuery = new SolrQuery();
            solrQuery.setQuery(queryString);            
            for(String field: searchParams.getFacets()){
                solrQuery.setGetFieldStatistics(field);
            }
            QueryResponse qr = runSolrQuery(solrQuery, searchParams);
            logger.debug(qr.getFieldStatsInfo());
            return  qr.getFieldStatsInfo();
            
        } catch (SolrServerException ex) {
            logger.error("Problem communicating with SOLR server. " + ex.getMessage(), ex);
        }
        return null;
    }

    public List<LegendItem> getLegend(SpatialSearchRequestParams searchParams, String facetField, String [] cutpoints) throws Exception {
        List<LegendItem> legend = new ArrayList<LegendItem>();

        formatSearchQuery(searchParams);
        logger.info("search query: " + searchParams.getFormattedQuery());
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQueryType("standard");
        solrQuery.setQuery(buildSpatialQueryString(searchParams));
        solrQuery.setRows(0);
        solrQuery.setFacet(true);

        //is facet query?
        if(cutpoints == null) {
            //special case for the decade 
            if(DECADE_FACET_NAME.equals(facetField))
                initDecadeBasedFacet(solrQuery, "occurrence_year");
            else
                solrQuery.addFacetField(facetField);
        } else {
            solrQuery.addFacetQuery("-" + facetField + ":[* TO *]");

            for(int i=0;i<cutpoints.length;i+=2) {
                solrQuery.addFacetQuery(facetField + ":[" + cutpoints[i] + " TO " + cutpoints[i+1] + "]");
            }
        }
        
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(-1);//MAX_DOWNLOAD_SIZE);  // unlimited = -1

        //add the context information
        updateQueryContext(searchParams);

        solrQuery.setFacetMissing(true);

        QueryResponse qr = runSolrQuery(solrQuery, searchParams.getFq(), 1, 0, "score", "asc");
        List<FacetField> facets = qr.getFacetFields();
        if (facets != null) {
            for (FacetField facet : facets) {
                List<FacetField.Count> facetEntries = facet.getValues();
                if (facet.getName().contains(facetField) && (facetEntries != null) && (facetEntries.size() > 0)) {
                    int i = 0;
                    for (i=0;i<facetEntries.size();i++) {
                        FacetField.Count fcount = facetEntries.get(i);
                        String fq = facetField + ":\"" + fcount.getName() + "\"";
                        if(fcount.getName() == null) {
                            fq = "-" + facetField + ":[* TO *]";
                        }
                        legend.add(new LegendItem(fcount.getName(), fcount.getCount(), fq));                                
                    }
                    break;
                }
            }
        }
        //check if we have query based facets
        Map<String, Integer> facetq = qr.getFacetQuery();
        if(facetq != null && facetq.size() > 0) {
            for(Entry<String, Integer> es : facetq.entrySet()) {
                legend.add(new LegendItem(es.getKey(), es.getValue(), es.getKey()));
            }
        }
        
        //check to see if we have a date range facet
        List<FacetField> facetDates = qr.getFacetDates();        
        if (facetDates != null && facetDates.size()>0) {
            FacetField ff = facetDates.get(0);
            String firstDate =null;
            for(FacetField.Count facetEntry: ff.getValues()){
                String startDate = facetEntry.getName();
                if(firstDate == null)
                    firstDate=startDate;
                String finishDate="*";
                if("before".equals(startDate)){
                    startDate = "*";
                    finishDate = firstDate;
                } else {
                    int startYear = Integer.parseInt(startDate.substring(0,4));
                    finishDate = (startYear-1) +"-12-31T23:59:59Z";
                }
                legend.add(new LegendItem(facetEntry.getName(), facetEntry.getCount(),"occurrence_year:["+ startDate+" TO "+finishDate+"]"));                
            }
        }
        return legend;
    }

    public FacetField getFacet(SpatialSearchRequestParams searchParams, String facet) throws Exception {
        formatSearchQuery(searchParams);
        logger.info("search query: " + searchParams.getFormattedQuery());
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQueryType("standard");
        solrQuery.setQuery(buildSpatialQueryString(searchParams));
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.addFacetField(facet);
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(-1); //MAX_DOWNLOAD_SIZE);  // unlimited = -1

        //add the context information
        updateQueryContext(searchParams);

        QueryResponse qr = runSolrQuery(solrQuery, searchParams.getFq(), 1, 0, "score", "asc");
        return qr.getFacetFields().get(0);
    }

    public List<DataProviderCountDTO> getDataProviderList(SpatialSearchRequestParams requestParams) throws Exception {
        List<DataProviderCountDTO> dataProviderList = new ArrayList<DataProviderCountDTO>();
        FacetField facet = getFacet(requestParams, "data_provider_uid");
        String [] oldFq = requestParams.getFacets();
        if(facet != null) {
            String [] dp = new String [1];
            List<FacetField.Count> facetEntries = facet.getValues();
            if (facetEntries != null && facetEntries.size() > 0) {
                for (int i=0;i<facetEntries.size();i++) {
                    FacetField.Count fcount = facetEntries.get(i);

                    //get data_provider value
                    dp[0] = fcount.getAsFilterQuery();
                    requestParams.setFq(dp);
                    String dataProviderName = getFacet(requestParams, "data_provider").getValues().get(0).getName();

                    dataProviderList.add(new DataProviderCountDTO(fcount.getName(), dataProviderName, fcount.getCount()));
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
        formatSearchQuery(requestParams);
        //add the context information
        List<String> facetFields = new ArrayList<String>();
        facetFields.add(NAMES_AND_LSID);
        logger.debug("The species count query " + requestParams.getFormattedQuery());
        List<String> fqList = new ArrayList<String>();
        //only add the FQ's if they are not the default values
        if(requestParams.getFq().length>0 && (requestParams.getFq()[0]).length()>0)
            org.apache.commons.collections.CollectionUtils.addAll(fqList, requestParams.getFq());
        String query = buildSpatialQueryString(requestParams);
        List<TaxaCountDTO> speciesWithCounts = getSpeciesCounts(query, fqList, facetFields, requestParams.getPageSize(), requestParams.getStart(), requestParams.getSort(), requestParams.getDir());

        return speciesWithCounts;
    }
    
    public Map<String, Integer> getOccurrenceCountsForTaxa(List<String> taxa) throws Exception{
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQueryType("standard");
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.setFacetLimit(taxa.size());
        StringBuilder sb = new StringBuilder();
        Map<String,Integer> counts = new HashMap<String,Integer>();
        Map<String, String> lftToGuid = new HashMap<String,String>();
        for(String lsid : taxa){
            //get the lft and rgt value for the taxon
            String[] values = searchUtils.getTaxonSearch(lsid);
            //first value is the search string
            if(sb.length()>0)
                sb.append(" OR ");
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
     * @return the maxMultiPartThreads
     */
    public Integer getMaxMultiPartThreads() {
      return maxMultiPartThreads;
    }

    /**
     * @param maxMultiPartThreads the maxMultiPartThreads to set
     */
    public void setMaxMultiPartThreads(Integer maxMultiPartThreads) {
      this.maxMultiPartThreads = maxMultiPartThreads;
    }

    /**
     * @return the checkDownloadLimits
     */
    public boolean getCheckDownloadLimits() {
        return checkDownloadLimits;
    }

    /**
     * @param checkDownloadLimits the checkDownloadLimits to set
     */
    public void setCheckDownloadLimits(boolean checkDownloadLimits) {
        this.checkDownloadLimits = checkDownloadLimits;
    }

    /**
     * @return the authServiceFields
     */
    public String getAuthServiceFields() {
        return authServiceFields;
    }

    /**
     * @param authServiceFields the authServiceFields to set
     */
    public void setAuthServiceFields(String authServiceFields) {
        this.authServiceFields = authServiceFields;
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
        this.throttle = throttle;        
    }
    
}