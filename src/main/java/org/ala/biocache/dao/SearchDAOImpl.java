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
package org.ala.biocache.dao;


import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletOutputStream;

import org.ala.biocache.dto.BreakdownRequestParams;
import org.ala.biocache.dto.DataProviderCountDTO;
import org.ala.biocache.dto.DownloadRequestParams;
import org.ala.biocache.dto.FacetResultDTO;
import org.ala.biocache.dto.FieldResultDTO;
import org.ala.biocache.dto.IndexFieldDTO;
import org.ala.biocache.dto.OccurrencePoint;
import org.ala.biocache.dto.PointType;
import org.ala.biocache.dto.SearchResultDTO;
import org.ala.biocache.dto.TaxaCountDTO;
import org.ala.biocache.dto.TaxaRankCountDTO;
import org.ala.biocache.util.CollectionsCache;
import org.ala.biocache.util.ParamsCacheMissingException;
import org.ala.biocache.util.RangeBasedFacets;
import org.ala.biocache.util.SearchUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;


import au.org.ala.biocache.OccurrenceIndex;

import au.com.bytecode.opencsv.CSVWriter;

import com.ibm.icu.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.ala.biocache.dto.SearchRequestParams;
import org.ala.biocache.dto.SpatialSearchRequestParams;
import org.ala.biocache.util.DownloadFields;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.ModifiableSolrParams;

import javax.inject.Inject;
import org.apache.commons.lang.ArrayUtils;
import au.org.ala.biocache.IndexDAO;
import au.org.ala.biocache.SolrIndexDAO;
import java.util.Map.Entry;
import org.ala.biocache.util.LegendItem;
import org.ala.biocache.util.ParamsCache;
import org.ala.biocache.util.ParamsCacheObject;
import org.apache.solr.client.solrj.SolrServer;

/**
 * SOLR implementation of SearchDao. Uses embedded SOLR server (can be a memory hog).
 *
 * @see org.ala.biocache.dao.SearchDAO
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
@Component("searchDao")
public class SearchDAOImpl implements SearchDAO {

    /** log4 j logger */
    private static final Logger logger = Logger.getLogger(SearchDAOImpl.class);
    /** SOLR home directory - injected by Spring from properties file */
    protected String solrHome = "/data/solr/bio-proto";
    /** SOLR server instance */
    protected SolrServer server;
    /** Limit search results - for performance reasons */
    protected Integer MAX_DOWNLOAD_SIZE = 500000;
    protected static final String POINT = "point-0.1";
    protected static final String KINGDOM = "kingdom";
    protected static final String KINGDOM_LSID = "kingdom_lsid";
    protected static final String SPECIES = "species";
    protected static final String SPECIES_LSID = "species_lsid";
    protected static final String NAMES_AND_LSID = "names_and_lsid";
    protected static final String TAXON_CONCEPT_LSID = "taxon_concept_lsid";
    protected static final Integer FACET_PAGE_SIZE =500;
    protected static final String QUOTE = "\"";
    protected static final char[] CHARS = {' ',':'};

    //Patterns that are used to prepares a SOLR query for execution
    protected Pattern lsidPattern= Pattern.compile("lsid:\"?[a-zA-Z0-9\\.:-]*\"?");
    protected Pattern urnPattern = Pattern.compile("urn:[a-zA-Z0-9\\.:-]*");
    protected Pattern spacesPattern =Pattern.compile("[^\\s\"()\\[\\]']+|\"[^\"]*\"|'[^']*'");
    protected Pattern uidPattern = Pattern.compile("([a-z_]*_uid:)([a-z0-9]*)");
    protected Pattern spatialPattern = Pattern.compile("\\{!spatial[a-zA-Z=\\-\\s0-9\\.\\,():]*\\}");
    protected Pattern qidPattern= Pattern.compile("qid:[0-9]*");
    protected Pattern termPattern = Pattern.compile("([a-zA-z_]+?):((\".*?\")|(\\\\ |[^: \\)\\(])+)"); // matches foo:bar, foo:"bar bash" & foo:bar\ bash
    
    protected String bieUri ="http://bie.ala.org.au";

    /** Download properties */
    protected DownloadFields downloadFields;

    @Inject
    protected SearchUtils searchUtils;
    
    @Inject
    private CollectionsCache collectionCache;
    
    @Inject
    private RestOperations restTemplate;

    @Inject
    private AbstractMessageSource messageSource;

    @Inject
    private BieService bieService;

    
    private List<IndexFieldDTO> indexFields = null;

    /**
     * Initialise the SOLR server instance
     */
    public SearchDAOImpl() {
        if (this.server == null & solrHome != null) {
            try {
//                System.setProperty("solr.solr.home", solrHome);
//                logger.info("Initialising SOLR HOME: "+ solrHome);
//                CoreContainer.Initializer initializer = new CoreContainer.Initializer();
//                CoreContainer coreContainer = initializer.initialize();
//                server = new EmbeddedSolrServer(coreContainer, "");
                
                //use the solr server that has been in the biocache-store...
                SolrIndexDAO dao = (SolrIndexDAO)au.org.ala.biocache.Config.getInstance(IndexDAO.class);
                dao.init();
                server = dao.solrServer();
                
            } catch (Exception ex) {
                logger.error("Error initialising embedded SOLR server: " + ex.getMessage(), ex);
            }
        }
        downloadFields = new DownloadFields();
    }

    public void refreshCaches(){
        collectionCache.updateCache();
    }

    @Override
    @Deprecated
    public SearchResultDTO findByFulltextQuery(SearchRequestParams requestParams) throws Exception {
        SearchResultDTO searchResults = new SearchResultDTO();

        try {            
            //formatSearchQuery(requestParams);            
            //add the context information
            updateQueryContext(requestParams);
            SolrQuery solrQuery = initSolrQuery(requestParams); // general search settings
            solrQuery.setQuery(requestParams.getFormattedQuery());
            QueryResponse qr = runSolrQuery(solrQuery, requestParams);
            searchResults = processSolrResponse(qr, solrQuery);
            //set the title for the results
            searchResults.setQueryTitle(requestParams.getDisplayString());
            searchResults.setUrlParameters(requestParams.getUrlParams());

            logger.info("search query: " + requestParams.getFormattedQuery());
        } catch (SolrServerException ex) {
            logger.error("Problem communicating with SOLR server. " + ex.getMessage(), ex);
            searchResults.setStatus("ERROR"); // TODO also set a message field on this bean with the error message(?)
        }

        return searchResults;
    }


    /**
     * @see org.ala.biocache.dao.SearchDAO#findByFulltextSpatialQuery(org.ala.biocache.dto.SpatialSearchRequestParams)
     */
    @Override
    public SearchResultDTO findByFulltextSpatialQuery(SpatialSearchRequestParams searchParams) throws Exception {
        SearchResultDTO searchResults = new SearchResultDTO();

        try {
            //String queryString = formatSearchQuery(query);            
            formatSearchQuery(searchParams);            
            //add context information
            updateQueryContext(searchParams);
            String queryString = buildSpatialQueryString(searchParams);
            //logger.debug("The spatial query " + queryString);
            SolrQuery solrQuery = initSolrQuery(searchParams); // general search settings
            solrQuery.setQuery(queryString);

            QueryResponse qr = runSolrQuery(solrQuery, searchParams);
            searchResults = processSolrResponse(qr, solrQuery);
            searchResults.setQueryTitle(searchParams.getDisplayString());
            searchResults.setUrlParameters(searchParams.getUrlParams());
            
            logger.info("spatial search query: " + queryString);
        } catch (SolrServerException ex) {
            logger.error("Problem communicating with SOLR server. " + ex.getMessage(), ex);
            searchResults.setStatus("ERROR"); // TODO also set a message field on this bean with the error message(?)
        }

        return searchResults;
    }

    /**
     * @see org.ala.biocache.dao.SearchDAO#writeSpeciesCountByCircleToStream(org.ala.biocache.dto.SpatialSearchRequestParams, String, javax.servlet.ServletOutputStream)
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
                item.getCount().toString(),};


            csvWriter.writeNext(record);
            csvWriter.flush();
            count++;
        }
        return count;
    }
    /**
     * Writes the values for the first supplied facet to output stream
     * 
     * @paramm includeCount true when the count should be included in the download
     * @param lookupName true when a name lsid should be looked up in the bie
     * 
     */
    public void writeFacetToStream(SpatialSearchRequestParams searchParams, boolean includeCount, boolean lookupName, OutputStream out) throws Exception{
        //set to unlimited facets
        searchParams.setFlimit(-1);
        formatSearchQuery(searchParams);
        //add the context information
        updateQueryContext(searchParams);
        String queryString = buildSpatialQueryString(searchParams);
        SolrQuery solrQuery = initSolrQuery(searchParams);
        
        solrQuery.setQuery(queryString);
        //don't want any results returned
        solrQuery.setRows(0);
        solrQuery.setFacetLimit(FACET_PAGE_SIZE);        
        int offset =0;
        boolean shouldLookup = lookupName && searchParams.getFacets()[0].contains("_guid");
        
        QueryResponse qr = runSolrQuery(solrQuery, searchParams);
        if (qr.getResults().size() > 0) {
            FacetField ff = qr.getFacetField(searchParams.getFacets()[0]);
            //write the header line
            if(ff != null){
                out.write(ff.getName().getBytes());
                if(shouldLookup){
                    out.write((",species name").getBytes());
                    
                }
                if(includeCount)
                    out.write(",Count".getBytes());
                out.write("\n".getBytes());
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
                                    String jsonUri = bieUri + "/species/namesFromGuids.json?guid=" + StringUtils.join(guids, "&guid=");
                                    List<String> entities = restTemplate.getForObject(jsonUri, List.class);
                                    for(int j = 0 ; j<guids.size();j++){
                                        out.write((guids.get(j) + ",").getBytes());
                                        out.write((entities.get(j) ).getBytes());
                                        if(includeCount)
                                            out.write((","+Long.toString(counts.get(j))).getBytes());
                                        out.write("\n".getBytes());
                                    }
                                    guids.clear();
                                }
                            }
                            //now get the list of species from the web service TODO may need to move this code
                            String jsonUri = bieUri + "/species/namesFromGuids.json?guid=" + StringUtils.join(guids, "&guid=");
                            List<String> entities = restTemplate.getForObject(jsonUri, List.class);
                            for(int i = 0 ; i<guids.size();i++){
                                out.write((guids.get(i) + ",").getBytes());
                                out.write((entities.get(i) ).getBytes());
                                if(includeCount)
                                    out.write((","+Long.toString(counts.get(i))).getBytes());
                                out.write("\n".getBytes());
                            }
                        }
                        else{
                            //default processing of facets
                            
                            for(FacetField.Count value : ff.getValues()){
                                out.write(value.getName().getBytes());
                                if(includeCount)
                                    out.write((","+Long.toString(value.getCount())).getBytes());
                                out.write("\n".getBytes());
                            }
                        }
                        offset += FACET_PAGE_SIZE;
                        //get the next values
                        solrQuery.remove("facet.offset");
                        solrQuery.add("facet.offset", Integer.toString(offset));
                        qr = runSolrQuery(solrQuery, searchParams);
                        ff = qr.getFacetField(searchParams.getFacets()[0]);
                    }
                }
            }
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
        
        SolrQuery solrQuery = initSolrQuery(srp);
        //We want all the facets so we candump all the coordinates
        solrQuery.setFacetLimit(-1);
        solrQuery.setFacetSort("count");
        solrQuery.setRows(0);
        solrQuery.setQuery(searchParams.getQ());

        QueryResponse qr = runSolrQuery(solrQuery, srp);
         if (qr.getResults().size() > 0) {
                FacetField ff = qr.getFacetField(searchParams.getFacets()[0]);
                if(ff != null && ff.getValueCount() >0){
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
     * @see org.ala.biocache.dao.SearchDAO#writeResultsToStream(org.ala.biocache.dto.DownloadRequestParams, java.io.OutputStream, int) 
     * 
     */
    public Map<String, Integer> writeResultsToStream(DownloadRequestParams downloadParams, OutputStream out, int i) throws Exception {

        int resultsCount = 0;
        Map<String, Integer> uidStats = new HashMap<String, Integer>();
        //stores the remaining limit for data resources that have a download limit
        Map<String, Integer> downloadLimit = new HashMap<String,Integer>();
        
        try {
            
            SolrQuery solrQuery = initSolrQuery(downloadParams);
            formatSearchQuery(downloadParams);
            //add context information
            updateQueryContext(downloadParams);
            logger.info("search query: " + downloadParams.getFormattedQuery());
            solrQuery.setQuery(buildSpatialQueryString(downloadParams));
            //Only the fields specified below will be included in the results from the SOLR Query
            solrQuery.setFields("row_key", "institution_uid", "collection_uid", "data_resource_uid", "data_provider_uid");
            

            int startIndex = 0;
            int pageSize = downloadParams.getPageSize();
            StringBuilder  sb = new StringBuilder(downloadParams.getFields());
            if(downloadParams.getExtra().length()>0)
                sb.append(",").append(downloadParams.getExtra());
            StringBuilder qasb = new StringBuilder();
            QueryResponse qr = runSolrQuery(solrQuery, downloadParams.getFq(), pageSize, startIndex, "score", "asc");
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
                }else if(facet.getName().equals("data_resource_uid")){
                    //populate the download limit
                    initDownloadLimits(downloadLimit, facet);
            	}
            }
            
            //Write the header line
            String qas = qasb.toString();   
            
            String[] fields = sb.toString().split(",");            
            String[]qaFields = qas.equals("")?new String[]{}:qas.split(",");
            String[] qaTitles = downloadFields.getHeader(qaFields);
            String[] titles = downloadFields.getHeader(fields);
            out.write(StringUtils.join(titles, ",").getBytes());
            
            if(qaFields.length >0){
                out.write(",".getBytes());
                out.write(StringUtils.join(qaTitles,",").getBytes());
            }
            out.write("\n".getBytes());
            //List<String> uuids = new ArrayList<String>();
           
            //download the records that have limits first...
            if(downloadLimit.size() > 0){
                String[] originalFq = downloadParams.getFq();
                StringBuilder fqBuilder = new StringBuilder("-(");
                for(String dr : downloadLimit.keySet()){
                    //add another fq to the search for data_resource_uid                    
                     downloadParams.setFq((String[])ArrayUtils.add(originalFq, "data_resource_uid:" + dr));
                     resultsCount =downloadRecords(downloadParams, out, downloadLimit, uidStats, fields, qaFields, resultsCount, dr);
                     if(fqBuilder.length()>2)
                         fqBuilder.append(" OR ");
                     fqBuilder.append("data_resource_uid:").append(dr);
                }
                fqBuilder.append(")");
                //now include the rest of the data resources
                //add extra fq for the remaining records
                downloadParams.setFq((String[])ArrayUtils.add(originalFq, fqBuilder.toString()));
                resultsCount =downloadRecords(downloadParams, out, downloadLimit, uidStats, fields, qaFields, resultsCount, null);
            }
            else{
                //download all at once
                downloadRecords(downloadParams, out, downloadLimit, uidStats, fields, qaFields, resultsCount, null);
            }

        } catch (SolrServerException ex) {
            logger.error("Problem communicating with SOLR server. " + ex.getMessage(), ex);
            //searchResults.setStatus("ERROR"); // TODO also set a message field on this bean with the error message(?)
        }
        return uidStats;
    }
    /**
     * downloads the records for the supplied query. Used to break up the download into components
     * 1) 1 call for each data resource that has a download limit (supply the data resource uid as the argument dataResource)
     * 2) 1 call for the remaining records
     * @param downloadParams
     * @param out
     * @param downloadLimit
     * @param uidStats
     * @param fields
     * @param qaFields
     * @param resultsCount
     * @param dataResource The dataResource being download.  This should be null if multiple data resource are being downloaded.
     * @return
     * @throws Exception
     */
    private int downloadRecords(DownloadRequestParams downloadParams, OutputStream out, 
                Map<String, Integer> downloadLimit,  Map<String, Integer> uidStats,
                String[] fields, String[] qaFields,int resultsCount, String dataResource) throws Exception {
        logger.info("download query: " + downloadParams.getQ());
        SolrQuery solrQuery = initSolrQuery(downloadParams);
        solrQuery.setRows(MAX_DOWNLOAD_SIZE);
        formatSearchQuery(downloadParams);
        solrQuery.setQuery(buildSpatialQueryString(downloadParams));
        //Only the fields specified below will be included in the results from the SOLR Query
        solrQuery.setFields("row_key", "institution_uid", "collection_uid", "data_resource_uid", "data_provider_uid");
        
        int startIndex = 0;
        int pageSize = 1000;
        StringBuilder  sb = new StringBuilder(downloadParams.getFields());
        if(downloadParams.getExtra().length()>0)
            sb.append(",").append(downloadParams.getExtra());
        StringBuilder qasb = new StringBuilder();
        QueryResponse qr = runSolrQuery(solrQuery, downloadParams.getFq(), pageSize, startIndex, "score", "asc");
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
            au.org.ala.biocache.Store.writeToStream(out, ",", "\n", uuids.toArray(new String[]{}),
                    fields, qaFields);
            startIndex += pageSize;
            uuids.clear();
            if (resultsCount < MAX_DOWNLOAD_SIZE) {
                //we have already set the Filter query the first time the query was constructed rerun with he same params but different startIndex
                qr = runSolrQuery(solrQuery, null, pageSize, startIndex, "score", "asc");
               
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
     * @return
     */
    private boolean shouldDownload(String druid, Map<String, Integer>limits, boolean decrease){
        if(limits.size()>0 && limits.containsKey(druid)){
            Integer remainingLimit = limits.get(druid);
            if(remainingLimit==0){
                return false;
            }
            if(decrease)
                limits.put(druid, remainingLimit-1);
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
     * @see org.ala.biocache.dao.SearchDAO#getFacetPoints(org.ala.biocache.dto.SpatialSearchRequestParams, org.ala.biocache.dto.PointType) 
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
     * @see org.ala.biocache.dao.SearchDAO#getOccurrences(org.ala.biocache.dto.SpatialSearchRequestParams, org.ala.biocache.dto.PointType, String, int) 
     */
    @Override
    public List<OccurrencePoint> getOccurrences(SpatialSearchRequestParams searchParams, PointType pointType, String colourBy, int searchType) throws Exception {


        List<OccurrencePoint> points = new ArrayList<OccurrencePoint>();
        List<String> colours = new ArrayList<String>();

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
        SearchResultDTO searchResults = processSolrResponse(qr, solrQuery);
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
                            if (oc.getMap() != null) {
                                java.util.Map map = oc.getMap();
                                if (map != null) {
                                    
                                    //check to see if it is empty otherwise a NPE is thrown when option.get is called
                                    if (map.containsKey(colourBy)) {
                                        value = (String) map.get(colourBy);
                                    }
                                    point.setOccurrenceUid(value);
                                }

                            } // check if oc is null
                        } catch (Exception e) {
                            //System.out.println("Error with getOccurrences:");
                            //e.printStackTrace(System.out);
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


    /*
    public List<Term> query(String q, int limit) {
    List<Term> items = null;
    initSolrServer();

    // escape special characters
    SolrQuery query = new SolrQuery();
    query.addTermsField("spell");
    query.setTerms(true);
    query.setTermsLimit(limit);
    query.setTermsLower(q);
    query.setTermsPrefix(q);
    query.setQueryType("/terms");

    try {
    QueryResponse qr = server.query(query);
    TermsResponse resp = qr.getTermsResponse();
    items = resp.getTerms("spell");
    } catch (SolrServerException e) {
    items = null;
    }

    return items;
    }
     */
    /**
     * http://ala-biocache1.vm.csiro.au:8080/solr/select?q=*:*&rows=0&facet=true&facet.field=data_provider_id&facet.field=data_provider&facet.sort=data_provider_id
     * 
     * @see org.ala.biocache.dao.SearchDAO#getDataProviderCounts()
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
        logger.info("Find data providers = " + dpDTOs.size());
        return dpDTOs;
    }

    /**
     * @see org.ala.biocache.dao.SearchDAO#findRecordsForLocation(org.ala.biocache.dto.SpatialSearchRequestParams, org.ala.biocache.dto.PointType) 
     * This is used by explore your area
     */
    @Override
    public List<OccurrencePoint> findRecordsForLocation(SpatialSearchRequestParams requestParams, PointType pointType) throws Exception {
        List<OccurrencePoint> points = new ArrayList<OccurrencePoint>(); // new OccurrencePoint(PointType.POINT);
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

    @Override
    //TODO: Not storing/indexing a user id
    //IS this being used
    public List<TaxaCountDTO> findTaxaByUserId(String userId) throws Exception {

        String queryString = "user_id:" + ClientUtils.escapeQueryChars(userId);
        List<String> facetFields = new ArrayList<String>();
        facetFields.add(TAXON_CONCEPT_LSID);
        List<TaxaCountDTO> speciesWithCounts = getSpeciesCounts(queryString, null, facetFields, 1000, 0, "taxon_name", "asc");

        return speciesWithCounts;
    }

    @Override
    //TODO Not storing/indexing a user
    //IS this being used
    public List<OccurrenceIndex> findPointsForUserId(String userId) throws Exception {
        String query = "user_id:" + ClientUtils.escapeQueryChars(userId);
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQueryType("standard");
        solrQuery.setQuery(query);
        QueryResponse qr = runSolrQuery(solrQuery, null, 1000000, 0, "score", "asc");
        SearchResultDTO searchResults = processSolrResponse(qr, solrQuery);
        logger.debug("solr result (size): " + searchResults.getOccurrences().size());

        return searchResults.getOccurrences();
    }



    /**
     * @see org.ala.biocache.dao.SearchDAO#findAllSpeciesByCircleAreaAndHigherTaxa(org.ala.biocache.dto.SpatialSearchRequestParams, String) 
     */
    @Override
    public List<TaxaCountDTO> findAllSpeciesByCircleAreaAndHigherTaxa(SpatialSearchRequestParams requestParams, String speciesGroup) throws Exception {
//        
        //add the context information
        updateQueryContext(requestParams);
        
        String queryString = buildSpatialQueryString(requestParams);
        List<String> facetFields = new ArrayList<String>();
        facetFields.add(NAMES_AND_LSID);
        logger.debug("The species count query " + queryString);
        List<String> fqList = new ArrayList<String>();
        //only add the FQ's if they are not the default values
        if(requestParams.getFq().length>0 && (requestParams.getFq()[0]).length()>0)
            org.apache.commons.collections.CollectionUtils.addAll(fqList, requestParams.getFq());
        List<TaxaCountDTO> speciesWithCounts = getSpeciesCounts(queryString, fqList, facetFields, requestParams.getPageSize(), requestParams.getStart(), requestParams.getSort(), requestParams.getDir());

        return speciesWithCounts;
    }

    /**
     * @see org.ala.biocache.dao.SearchDAO#findRecordByDecadeFor(java.lang.String)
     * IS THIS BEIGN USED OR NECESSARY?
     */
    @Override
    public List<FieldResultDTO> findRecordByDecadeFor(String query) throws Exception {
        List<FieldResultDTO> fDTOs = new ArrayList<FieldResultDTO>(); // new OccurrencePoint(PointType.POINT);
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQueryType("standard");
        solrQuery.add("facet.date", "occurrence_date");
        solrQuery.add("facet.date.start", "1850-01-01T00:00:00Z"); // facet date range starts from 1850
        solrQuery.add("facet.date.end", "NOW/DAY"); // facet date range ends for current date (gap period)
        solrQuery.add("facet.date.gap", "+10YEAR"); // gap interval of 10 years
        solrQuery.setQuery(query);
//        solrQuery.setQuery(query);
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        QueryResponse qr = runSolrQuery(solrQuery, null, 1, 0, "score", "asc");

        //get date fields
        List<FacetField> facetDateFields = qr.getFacetDates();
        FacetField ff = facetDateFields.get(0);

        System.out.println(ff.getName());
        boolean addCounts = false;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy");

        for (Count count : ff.getValues()) {
            //only start adding counts when we hit a decade with some results.
            if (addCounts || count.getCount() > 0) {
                addCounts = true;
                Date date = DateUtils.parseDate(count.getName(), new String[]{"yyyy-MM-dd'T'HH:mm:ss'Z'"});
                String year = sdf.format(date);
                FieldResultDTO f = new FieldResultDTO(year, count.getCount());
                fDTOs.add(f);
                System.out.println(f.getLabel() + " " + f.getCount());
            }
        }
        return fDTOs;
    }

    /**
     * @see org.ala.biocache.dao.SearchDAO#findRecordByStateFor(java.lang.String)
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
        }
        else{
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
        }
        else if(queryParams.getRank() != null || queryParams.getLevel() != null){
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
     * Finds the counts for the taxa starting at the family level.
     *
     * If the number of distinct taxa is higher than maximumFacets then we
     * move up to the next level of the taxonomic hierarchy.
     *
     *  @deprecated use {@link #calculateBreakdown(BreakdownRequestParams)} instead
     * @param query
     * @param maximumFacets
     * @return
     * @throws Exception
     */
    @Override
    @Deprecated
    public TaxaRankCountDTO findTaxonCountForUid(String query, String queryContext, int maximumFacets) throws Exception {
        logger.debug("Attempting to find the counts for " + query);
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQueryType("standard");
        solrQuery.setQuery(query);
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetSort("count");
        solrQuery.setFacetLimit(-1);
        TaxaRankCountDTO trDTO = null;
        for (int i = 5; i > 0 && trDTO == null; i--) {
            String ffname = searchUtils.getRankFacetName(i);
            solrQuery.addFacetField(ffname);
            solrQuery.setFacetMinCount(1);
            QueryResponse qr = runSolrQuery(solrQuery, getQueryContextAsArray(queryContext), 1, 0, ffname, "asc");
            if (qr.getResults().size() > 0) {
                FacetField ff = qr.getFacetField(ffname);
                if (ff.getValues() != null && ff.getValues().size() <= maximumFacets) {
                    trDTO = new TaxaRankCountDTO(ffname);
                    List<FieldResultDTO> fDTOs = new ArrayList<FieldResultDTO>();
                    for (Count count : ff.getValues()) {
                        FieldResultDTO f = new FieldResultDTO(count.getName(), count.getCount());
                        fDTOs.add(f);
                    }
                    trDTO.setTaxa(fDTOs);
                }
            } else {
                return null;
            }
        }
        return trDTO;
    }

    /**
     * @see org.ala.biocache.dao.SearchDAO#findTaxonCountForUid(org.ala.biocache.dto.BreakdownRequestParams, String) 
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

    //TODO delete me just temporary until all methods are modified to use SearchRequestParams requestParams
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
                // so that special characters are escaped apporpriately
                if (fq.isEmpty()) {
                    continue;
                }
                // use of AND/OR requires correctly formed fq.
                // Can overlap with values containing the same,
                // case sensitivity may help.
                if(fq.contains(" OR ") || fq.contains(" AND ")) { 
                    solrQuery.addFilterQuery(fq);
                    logger.info("adding filter query: " + fq);
                    continue;
                }
                String[] parts = fq.split(":", 2); // separate query field from query text
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

        solrQuery.setRows(requestParams.getPageSize());
        solrQuery.setStart(requestParams.getStart());
        solrQuery.setSortField(requestParams.getSort(), ORDER.valueOf(requestParams.getDir()));
        logger.info("runSolrQuery: " + solrQuery.toString());
        return server.query(solrQuery); // can throw exception
    }

    /**
     * Process the {@see org.​apache.​solr.​client.​solrj.​response.QueryResponse} from a SOLR search and return
     * a {@link org.ala.biocache.dto.SearchResultDTO}
     *
     * @param qr
     * @param solrQuery
     * @return
     */
    private SearchResultDTO processSolrResponse(QueryResponse qr, SolrQuery solrQuery) {
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
        List<OccurrenceIndex> results = qr.getBeans(OccurrenceIndex.class);
        List<FacetResultDTO> facetResults = new ArrayList<FacetResultDTO>();
        searchResult.setTotalRecords(sdl.getNumFound());
        searchResult.setStartIndex(sdl.getStart());
        searchResult.setPageSize(solrQuery.getRows()); //pageSize
        searchResult.setStatus("OK");
        String[] solrSort = StringUtils.split(solrQuery.getSortField(), " "); // e.g. "taxon_name asc"
        logger.debug("sortField post-split: " + StringUtils.join(solrSort, "|"));
        searchResult.setSort(solrSort[0]); // sortField
        searchResult.setDir(solrSort[1]); // sortDirection
        searchResult.setQuery(solrQuery.getQuery());
        searchResult.setOccurrences(results);
        // populate SOLR facet results
        if (facets != null) {
            for (FacetField facet : facets) {
                List<FacetField.Count> facetEntries = facet.getValues();                
                if ((facetEntries != null) && (facetEntries.size() > 0)) {
                    ArrayList<FieldResultDTO> r = new ArrayList<FieldResultDTO>();
                    for (FacetField.Count fcount : facetEntries) {
//                        String msg = fcount.getName() + ": " + fcount.getCount();                       
                        //logger.trace(fcount.getName() + ": " + fcount.getCount());        	
                    		r.add(new FieldResultDTO(fcount.getName(), fcount.getCount()));
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
        if(facetQueries != null) {
            Map<String, String> rangeMap = RangeBasedFacets.getRangeMap("uncertainty");
            List<FieldResultDTO> fqr = new ArrayList<FieldResultDTO>();
            for(String value: facetQueries.keySet()){
                if(facetQueries.get(value)>0)
                    fqr.add(new FieldResultDTO(rangeMap.get(value), facetQueries.get(value)));
            }
            facetResults.add(new FacetResultDTO("uncertainty", fqr));
        }

        //handle the confidence facets in the facetQueries
        //TODO Work out whether or not we need the confidence facets ie the confidence ratign indexed???
//       List<FieldResultDTO> fqr = new ArrayList<FieldResultDTO>();
//        if(facetQueries != null){
//            for(String range: facetQueries.keySet()){
//                //get the OccurrenceSource
//                OccurrenceSource os = OccurrenceSource.getForRange(range.substring(range.indexOf(":")+1));
//                if(os != null && facetQueries.get(range)>0){
//                    fqr.add(new FieldResultDTO(os.getDisplayName(), facetQueries.get(range)));
//                }
//
//            }
//        }
//        //Only add the confidence facetResult if there is more than 1 facet
//        if(fqr.size()>1){
//                FacetResultDTO fr = new FacetResultDTO(OccurrenceSource.FACET_NAME, fqr);
//                facetResults.add(fr);
//            }

        searchResult.setFacetResults(facetResults);
        // The query result is stored in its original format so that all the information
        // returned is available later on if needed
        searchResult.setQr(qr);

        //add the URL information necessary for the maps and downloads
//        String params = "?formattedQuery=" + solrQuery.getQuery();
//        if (solrQuery.getFilterQueries() != null && solrQuery.getFacetFields().length > 0) {
//            params += "&fq=" + StringUtils.join(solrQuery.getFilterQueries(), "&fq=");
//        }
//        logger.info("The download params: " + params);
//        searchResult.setUrlParameters(params);
        return searchResult;
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
        String queryString = "{!spatial circles=" + latitude.toString() + "," + longitude.toString()
                + "," + radius.toString() + "}" +  fullTextQuery;
        return queryString;
    }
    
    protected String buildSpatialQueryString(SpatialSearchRequestParams searchParams){
        if(searchParams != null){
            StringBuilder sb = new StringBuilder();
            if(searchParams.getLat() != null){
                sb.append("{!spatial ");
                sb.append("circles=").append(searchParams.getLat().toString()).append(",");
                sb.append(searchParams.getLon().toString()).append(",");
                sb.append(searchParams.getRadius().toString()).append("}");
            }
            else if(!StringUtils.isEmpty(searchParams.getWkt())){
                //format the wkt
                sb.append("{!spatial ");
                sb.append("wkt=").append(searchParams.getWkt()).append("}");
            }
            String query = StringUtils.isEmpty(searchParams.getFormattedQuery())? searchParams.getQ() : searchParams.getFormattedQuery();
            sb.append(query);
            return sb.toString();
        }
        return null;
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
     * @return
     */
    protected void formatSearchQuery(SpatialSearchRequestParams searchParams) {
        //Only format the query if it doesn't already supply a formattedQuery.
        if(StringUtils.isEmpty(searchParams.getFormattedQuery())){
            // set the query
            String query =searchParams.getQ();
            
            //cached query parameters are already formatted
            if(query.contains("qid:")) {            
                Matcher matcher = qidPattern.matcher(query);
                long qid = 0;
                while(matcher.find()) {
                    String value = matcher.group();
                    try {
                        qid = Long.parseLong(value.substring(4));
                        ParamsCacheObject pco = ParamsCache.get(qid);
                        if(pco != null) {
                            searchParams.setQ(pco.getQ());
                            searchParams.setDisplayString(pco.getDisplayString());
                            if(searchParams instanceof SpatialSearchRequestParams) {
                                ((SpatialSearchRequestParams) searchParams).setWkt(pco.getWkt());
                            } else if(!StringUtils.isEmpty(pco.getWkt())) {                            
                                searchParams.setQ(pco.getWkt() + "{!spatial wkt=" + pco.getWkt() + "}" + pco.getQ() );
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
                            String guid = bieService.getGuidForName(queryText.replaceAll("\"", "")); // strip any quotes
                            logger.info("GUID for " + queryText + " = " + guid);

                            if (guid != null && !guid.isEmpty()) {
                                String acceptedName = bieService.getAcceptedNameForGuide(guid); // strip any quotes
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
                            String guid = bieService.getGuidForName(queryText.replaceAll("\"", "")); // strip any quotes
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
            int last =0;
            if (query.contains("lsid:")) {
                Matcher matcher = lsidPattern.matcher(query);
                queryString.setLength(0);
                while (matcher.find()) {
                    String value = matcher.group();
                    logger.debug("preprocessing " + value);
                    String lsid = value.substring(5, value.length());
                    if (lsid.contains("\"")) {
                        //remove surrounding quotes, if present
                        lsid = lsid.replaceAll("\"","");
                    }
                    if (lsid.contains("\\")) {
                        //remove internal \ chars, if present
                        lsid = lsid.replaceAll("\\","");
                    }
                    logger.debug("lsid = " + lsid);
                    String[] values = searchUtils.getTaxonSearch(lsid);
                    matcher.appendReplacement(queryString, values[0]);
                    displaySb.append(query.substring(last, matcher.start()));
                    if(!values[1].startsWith("lsid:"))
                        displaySb.append("<span class='lsid' id='").append(lsid).append("'>").append(values[1]).append("</span>");
                    else
                        displaySb.append(values[1]);
                    last = matcher.end();
                    //matcher.appendReplacement(displayString, values[1]);
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

            if(query.contains("{!spatial")){
                Matcher matcher = spatialPattern.matcher(query);
                if(matcher.find()){
                    String spatial = matcher.group();
                    SpatialSearchRequestParams subQuery = new SpatialSearchRequestParams();
                    //format the search query of the remaining text only
                    subQuery.setQ(query.substring(matcher.regionStart() + spatial.length(), query.length()));
                    //format the remaining query
                    formatSearchQuery(subQuery);
                    
                    //now append Q's together
                queryString.setLength(0);
                    queryString.append(spatial);
                    queryString.append(subQuery.getFormattedQuery());
                    searchParams.setFormattedQuery(queryString.toString());
                    //add the spatial information to the display string
                    if(spatial.contains("circles")){
                        String[] values = spatial.substring(spatial.indexOf("=") +1 , spatial.indexOf("}")).split(",");
                        if(values.length ==3){
                            displaySb.setLength(0);
                            displaySb.append(subQuery.getDisplayString());
                            displaySb.append(" - within ").append(values[2]).append(" km of point(")
                            .append(values[0]).append(",").append(values[1]).append(")");
                            searchParams.setDisplayString(displaySb.toString());
                        }
                        
                    } else{
                        searchParams.setDisplayString(searchParams.getDisplayString() + " - within supplied region");
                    }
                }
    
    
            }
            else{
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
    
                        }
                        //need to ignore field names where the : is at the end because the pattern matching will return field_name: as a match when it has a double quoted value
                        else if(!value.endsWith(":")){
                            //default behaviour is to escape all 
                            matcher.appendReplacement(queryString, prepareSolrStringForReplacement(value));
                        }
                    }
    
                }
                matcher.appendTail(queryString);
    
                //substitute better display strings for collection/inst etc searches
                if(displayString.contains("_uid")){
                    displaySb.setLength(0);
                    matcher = uidPattern.matcher(displayString);
                    while(matcher.find()){
                        String newVal = "<span>"+searchUtils.getUidDisplayString(matcher.group(2)) +"</span>";
                        if(newVal != null)
                            matcher.appendReplacement(displaySb, newVal);
                    }
                    matcher.appendTail(displaySb);
                    displayString = displaySb.toString();
                }
                if(searchParams.getQ().equals("*:*")){
                    displayString ="[all records]";
                }
                if(searchParams.getLat() !=null && searchParams.getLon() != null && searchParams.getRadius() != null ){
                    displaySb.setLength(0);
                    displaySb.append(displayString);
                    displaySb.append(" - within ").append(searchParams.getRadius()).append(" km of point(")
                    .append(searchParams.getLat()).append(",").append(searchParams.getLon()).append(")");
                    displayString = displaySb.toString();
                    
                }

                // substitute i18n version of field name, if found in messages.properties
                int colonIndex = displayString.indexOf(":");

                if (colonIndex > 0 && colonIndex == displayString.lastIndexOf(":")) {
                    // only substitute if there is one search term
                    String fieldName = displayString.substring(0, colonIndex);
                    // i18n gets set to fieldName if not found
                    String i18n = messageSource.getMessage("facet."+fieldName, null, fieldName, null);
                    logger.debug("i18n = " + i18n);
                    if (!fieldName.equals(i18n)) {
                        displayString = i18n + displayString.substring(colonIndex);
                    }
                }

                searchParams.setFormattedQuery(queryString.toString());
                logger.debug("formattedQuery = " + queryString);
                logger.debug("displayString = " + displayString);
                searchParams.setDisplayString(displayString);
                //return queryString.toString();
            }
            
            //format the fq's for facets that need ranges substituted
            for(int i=0; i<searchParams.getFq().length;i++){
                String fq = searchParams.getFq()[i];
                String[] parts = fq.split(":", 2);
                //check to see if the first part is a range based query and update if necessary
                Map<String, String> titleMap = RangeBasedFacets.getTitleMap(parts[0]);
                if(titleMap != null){
                    searchParams.getFq()[i]= titleMap.get(parts[1]);
                }
            }
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
//            String[] values = qc.split(",");
//            for(int i =0; i<values.length;i++){
//                String field = values[i];
//                values[i]= field.replace("hub:", "data_hub_uid:");
//
//            }
            
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
   

    /**
     * Helper method to create SolrQuery object and add facet settings
     *
     * @return solrQuery the SolrQuery
     */
    protected SolrQuery initSolrQuery(SearchRequestParams searchParams) {

        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQueryType("standard");
        // Facets
        solrQuery.setFacet(searchParams.getFacet());
        if(searchParams.getFacet()) {
            for (String facet : searchParams.getFacets()) {
                if (facet.equals("month")) {
                    solrQuery.addFacetField("month");
                    solrQuery.add("f.month.facet.sort", "index"); // sort by Jan-Dec
                } else if (facet.equals("date") || facet.equals("year")) {
                    solrQuery.add("facet.date", "occurrence_" +facet);
                    solrQuery.add("facet.date.start", "1850-01-01T00:00:00Z"); // facet date range starts from 1850
                    solrQuery.add("facet.date.end", "NOW/DAY"); // facet date range ends for current date (gap period)
                    solrQuery.add("facet.date.gap", "+10YEAR"); // gap interval of 10 years
                    solrQuery.add("facet.date.other", "before"); // include counts before the facet start date ("before" label)
                    solrQuery.add("facet.date.include", "lower"); // counts will be included for dates on the starting date but not ending date
                } else if(facet.equals("uncertainty")){
                    Map<String, String> rangeMap = RangeBasedFacets.getRangeMap("uncertainty");
                    for(String range: rangeMap.keySet()){
                        solrQuery.add("facet.query", range);
                    }
                }
                else {
                    solrQuery.addFacetField(facet);
                }
            }
//        solrQuery.addFacetField("basis_of_record");
//        solrQuery.addFacetField("type_status");
//        solrQuery.addFacetField("institution_code_name");
//        //solrQuery.addFacetField("data_resource");
//        solrQuery.addFacetField("state");
//        solrQuery.addFacetField("biogeographic_region");
//        solrQuery.addFacetField("rank");
//        solrQuery.addFacetField("kingdom");
//        solrQuery.addFacetField("family");
//        solrQuery.addFacetField("assertions");
//        //solrQuery.addFacetField("data_provider");
//        solrQuery.addFacetField("month");
//        solrQuery.add("f.month.facet.sort","index"); // sort by Jan-Dec
//        // Date Facet Params
//        // facet.date=occurrence_date&facet.date.start=1900-01-01T12:00:00Z&facet.date.end=2010-01-01T12:00:00Z&facet.date.gap=%2B1YEAR
//        solrQuery.add("facet.date","occurrence_date");
//        solrQuery.add("facet.date.start", "1850-01-01T12:00:00Z"); // facet date range starts from 1850
//        solrQuery.add("facet.date.end", "NOW/DAY"); // facet date range ends for current date (gap period)
//        solrQuery.add("facet.date.gap", "+10YEAR"); // gap interval of 10 years
//        solrQuery.add("facet.date.other", "before"); // include counts before the facet start date ("before" label)
//        solrQuery.add("facet.date.include", "lower"); // counts will be included for dates on the starting date but not ending date
        //Manually add the integer ranges for the facet query required on the confidence field
        //TODO DO we need confidence/ Indivdual Sightings etc
//        for(OccurrenceSource os : OccurrenceSource.values()){
//            solrQuery.add("facet.query", "confidence:" + os.getRange());
//        }

        //solrQuery.add("facet.date.other", "after");

            solrQuery.setFacetMinCount(1);
            solrQuery.setFacetLimit(searchParams.getFlimit());
            if(searchParams.getFlimit() == -1)
                solrQuery.setFacetSort("count");
        }
        
        solrQuery.setRows(10);
        solrQuery.setStart(0);

        if (searchParams.getFl().length() > 0) {
            solrQuery.setFields(searchParams.getFl());
        }

//        add highlights
//        solrQuery.setHighlight(true);
//        solrQuery.setHighlightFragsize(40);
//        solrQuery.setHighlightSnippets(1);
//        solrQuery.setHighlightSimplePre("<b>");
//        solrQuery.setHighlightSimplePost("</b>");
//        solrQuery.addHighlightField("commonName");

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
            Integer startIndex, String sortField, String sortDirection) throws SolrServerException {

        List<TaxaCountDTO> speciesCounts = new ArrayList<TaxaCountDTO>();
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQueryType("standard");
        solrQuery.setQuery(queryString);
        
        if (filterQueries != null && filterQueries.size()>0) {
            solrQuery.addFilterQuery("(" + StringUtils.join(filterQueries, " OR ") + ")");
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
                                tcDTO = new TaxaCountDTO(values[0], fcount.getCount());
                                tcDTO.setGuid(StringUtils.trimToNull(values[1]));
                                tcDTO.setCommonName(values[2]);
                                tcDTO.setKingdom(values[3]);
                                tcDTO.setFamily(values[4]);
                                tcDTO.setRank(searchUtils.getTaxonSearch(tcDTO.getGuid())[1].split(":")[0]);
                            }
                            else{
                                logger.debug("The values length: " + values.length + " :" + fcount.getName());
                                tcDTO = new TaxaCountDTO(fcount.getName(), fcount.getCount());
                            }
                            //speciesCounts.add(i, tcDTO);
                            speciesCounts.add(tcDTO);
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

    public String getSolrHome() {
        return solrHome;
    }

    public void setSolrHome(String solrHome) {
        this.solrHome = solrHome;
    }
    /**
     * Returns details about the fields in the index.
     */
    public List<IndexFieldDTO> getIndexedFields() throws Exception{
        if(indexFields == null){
            ModifiableSolrParams params = new ModifiableSolrParams();
            params.set("qt", "/admin/luke");
            params.set("tr", "luke.xsl");
            QueryResponse response = server.query(params);            
            indexFields = parseLukeResponse(response.toString());
        }
        return indexFields;        
    }
    

    /**
     * parses the response string from the service that returns details about the indexed fields
     * @param str
     * @return
     */
    private  List<IndexFieldDTO> parseLukeResponse(String str) {
        List<IndexFieldDTO> fieldList = new ArrayList<IndexFieldDTO>();
        
        Pattern typePattern = Pattern.compile(
        "(?:type=)([a-z]{1,})");

        Pattern schemaPattern = Pattern.compile(
        "(?:schema=)([a-zA-Z\\-]{1,})");

        String[] fieldsStr = str.split("fields=\\{");

        for (String fieldStr : fieldsStr) {
            if (fieldStr != null && !"".equals(fieldStr)) {
                String[] fields = fieldStr.split("\\}\\},");

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
                            logger.debug("fieldName:" + fieldName);
                            logger.debug("type:" + type);
                            logger.debug("schema:" + schema);
                            
                            f.setName(fieldName);
                            f.setDataType(type);
                            //interpret the schema information
                            f.setIndexed(schema.contains("I"));
                            f.setStored(schema.contains("S"));
                            
                            fieldList.add(f);
                        }
                    }
                }
            }
        }
        
        return fieldList;
    }

    /**
     * @see org.ala.biocache.dao.SearchDAO#findByFulltext(SpatialSearchRequestParams)
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
     * @see org.ala.biocache.dao.SearchDAO#getStatistics(SpatialSearchRequestParams)
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
            
        }
        catch (SolrServerException ex) {
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

        Map<String, Integer> facetq = qr.getFacetQuery();
        if(facetq != null && facetq.size() > 0) {
            for(Entry<String, Integer> es : facetq.entrySet()) {
                legend.add(new LegendItem(es.getKey(), es.getValue(), es.getKey()));
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
        solrQuery.setFacetLimit(-1);//MAX_DOWNLOAD_SIZE);  // unlimited = -1

        //add the context information
        updateQueryContext(searchParams);

        QueryResponse qr = runSolrQuery(solrQuery, searchParams.getFq(), 1, 0, "score", "asc");
        return qr.getFacetFields().get(0);
    }

    public List<DataProviderCountDTO> getDataProviderList(SpatialSearchRequestParams requestParams) throws Exception {
        ArrayList<DataProviderCountDTO> dataProviderList = new ArrayList<DataProviderCountDTO>();
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
     * @see org.ala.biocache.dao.SearchDAO#findAllSpecies(SpatialSearchRequestParams)
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
}
