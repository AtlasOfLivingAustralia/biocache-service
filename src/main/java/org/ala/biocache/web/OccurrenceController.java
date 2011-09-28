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
import org.ala.biocache.*;
import org.ala.biocache.dao.SearchDAO;
import org.ala.biocache.dto.*;
import org.ala.biocache.dto.store.OccurrenceDTO;
import org.ala.biocache.util.MimeType;
import org.ala.biocache.util.SearchUtils;
import org.ala.client.appender.RestLevel;
import org.ala.client.model.LogEventType;
import org.ala.client.model.LogEventVO;
import org.ala.client.util.RestfulClient;
import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.bind.annotation.ResponseBody;

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
 * 
 */
@Controller
public class OccurrenceController {

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
	
	/** Name of view for site home page */
	private String HOME = "homePage";
	/** Name of view for list of taxa */
	private final String LIST = "occurrences/list";
	/** Name of view for a single taxon */
	private final String SHOW = "occurrences/show";

	protected String hostUrl = "http://localhost:8080/biocache-service";
	protected String bieBaseUrl = "http://bie.ala.org.au/";
	protected String collectoryBaseUrl = "http://collections.ala.org.au";
	protected String biocacheMediaBaseUrl = "http://biocache.ala.org.au/biocache-media";
	protected String citationServiceUrl = collectoryBaseUrl + "/ws/citations";
	protected String summaryServiceUrl  = collectoryBaseUrl + "/ws/summary";
	
	/** The response to be returned for the isAustralian test */
	private final String NOT_AUSTRALIAN = "Not Australian";
	private final String AUSTRALIAN_WITH_OCC = "Australian with occurrences";
	private final String AUSTRALIAN_LSID = "Australian based on LSID";
	protected Pattern austLsidPattern = Pattern.compile("urn:lsid:biodiversity.org.au[a-zA-Z0-9\\.:-]*");
	
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
	 * Returns the content of the messages.properties file.
	 * @param response
	 * @throws Exception
	 */
	@RequestMapping("/facets/i18n")
	public void writei18nPropertiesFile(HttpServletResponse response) throws Exception{	    
        InputStream is = getClass().getResourceAsStream("/messages.properties");
        OutputStream os = response.getOutputStream();
        byte[] buffer = new byte[1024]; 
        int bytesRead;
        while ((bytesRead = is.read(buffer)) != -1)
        {
            os.write(buffer, 0, bytesRead);
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
	public @ResponseBody List<IndexFieldDTO> getIndexedFields() throws Exception{
	    return searchDAO.getIndexedFields();
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
	    SearchRequestParams srp = new SearchRequestParams();
	    srp.setQ("lsid:" + guid);
	    srp.setPageSize(0);
	    srp.setFacets(new String[]{"image_url"});
	    SearchResultDTO results = searchDAO.findByFulltextQuery(srp);
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
	    SearchRequestParams requestParams = new SearchRequestParams();
	    requestParams.setPageSize(0);
	    requestParams.setFacets(new String[]{});
	    String query = "lsid:" +guid + " AND " + "(country:Australia OR state:[* TO *])";
	    requestParams.setQ(query);
	    AustralianDTO adto= new AustralianDTO();
	    adto.setTaxonGuid(guid);
	    SearchResultDTO results = searchDAO.findByFulltextQuery(requestParams);
	    adto.setHasOccurrenceRecords(results.getTotalRecords() > 0);
	    adto.setIsNSL(austLsidPattern.matcher(guid).matches());
	    if(adto.isHasOccurrences()){
	        //check to see if the records have only been provided by citizen science
	        //TODO change this to a confidence setting after it has been included in the index	        
	        requestParams.setQ("lsid:" + guid + " AND -(data_resource_uid:dr364)");
	        results = searchDAO.findByFulltextQuery(requestParams);
	        adto.setHasCSOnly(results.getTotalRecords()==0);
	    }
	    return adto;
	}

	/**
	 * Returns the complete list of Occurrences
	 */
	@RequestMapping(value = {"/occurrences", "/occurrences/collections", "/occurrences/institutions", "/occurrences/dataResources", "/occurrences/dataProviders", "/occurrences/taxa", "/occurrences/dataHubs"}, method = RequestMethod.GET)
	public @ResponseBody SearchResultDTO listOccurrences(Model model) throws Exception {
            SearchRequestParams srp = new SearchRequestParams();
            srp.setQ("*:*");
            return occurrenceSearch(srp, model);
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
            SearchRequestParams requestParams,
            @PathVariable("guid") String guid,
            Model model) throws Exception {
        requestParams.setQ("lsid:" + guid);
        SearchUtils.setDefaultParams(requestParams);
        SearchResultDTO searchResult = searchDAO.findByFulltextQuery(requestParams);
        model.addAttribute("searchResult", searchResult);
        return searchResult;
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
            SearchRequestParams requestParams,
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
            SearchRequestParams requestParams,
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
        searchResult = searchDAO.findByFulltextQuery(requestParams);
        model.addAttribute("searchResult", searchResult);
        return searchResult;
    }

    /**
     * Spatial search for either a taxon name or full text text search
    
     * @param model
     * @return
     * @throws Exception
     */
    @RequestMapping(value =  "/occurrences/searchByArea*", method = RequestMethod.GET)
	public @ResponseBody SearchResultDTO occurrenceSearchByArea(
			SpatialSearchRequestParams requestParams,
			Model model)
            throws Exception {
        SearchResultDTO searchResult = new SearchResultDTO();

        if (StringUtils.isEmpty(requestParams.getQ())) {
			return searchResult;
		}

        //searchUtils.updateSpatial(requestParams);
        searchResult = searchDAO.findByFulltextSpatialQuery(requestParams);
		model.addAttribute("searchResult", searchResult);

		if(logger.isDebugEnabled()){
			logger.debug("Returning results set with: "+searchResult.getTotalRecords());
		}

		return searchResult;
	}

	/**
	 * Occurrence search page uses SOLR JSON to display results
	 *
	 * @param model
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value = {"/occurrences/search.json*","/occurrences/search*"}, method = RequestMethod.GET)
	public @ResponseBody SearchResultDTO occurrenceSearch(SearchRequestParams requestParams,
            Model model) throws Exception {
        // handle empty param values, e.g. &sort=&dir=
        SearchUtils.setDefaultParams(requestParams);        
        SearchResultDTO searchResult = searchDAO.findByFulltextQuery(requestParams);
        model.addAttribute("searchResult", searchResult);
        logger.debug("query = " + requestParams.getQ());

		return searchResult;
	}

	/**
	 * Occurrence search page uses SOLR JSON to display results
	 *
	 * @param model
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
	 * @param request
	 * @throws Exception
	 */
	@RequestMapping(value = "/occurrences/facets/download*", method = RequestMethod.GET)
	public void downloadFacet(
            DownloadRequestParams requestParams,
            @RequestParam(value = "count", required = false, defaultValue="false") boolean includeCount,
            @RequestParam(value="lookup" ,required=false, defaultValue="false") boolean lookupName,
            HttpServletResponse response,
            HttpServletRequest request) throws Exception {
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
			HttpServletResponse response,
            HttpServletRequest request) throws Exception {
       
        String ip = request.getLocalAddr();
        //search params must have a query or formatted query for the downlaod to work
        if (requestParams.getQ().isEmpty() && requestParams.getFormattedQuery().isEmpty()) {
            return null;
        }

        String filename = requestParams.getFile();
        
        response.setHeader("Cache-Control", "must-revalidate");
        response.setHeader("Pragma", "must-revalidate");
        response.setHeader("Content-Disposition", "attachment;filename=" + filename +".zip");
        response.setContentType("application/zip");

        ServletOutputStream out = response.getOutputStream();
        
        //Use a zip output stream to include the data and citation together in the download
        ZipOutputStream zop = new ZipOutputStream(out);
        zop.putNextEntry(new java.util.zip.ZipEntry(filename + ".csv"));
        Map<String, Integer> uidStats = null;
        //put the factes
        requestParams.setFacets(new String[]{"assertions", "data_resource_uid"});
        uidStats = searchDAO.writeResultsToStream(requestParams, zop, 100);
        zop.closeEntry();

        if (!uidStats.isEmpty()) {
            //add the citations for the supplied uids
            zop.putNextEntry(new java.util.zip.ZipEntry("citation.csv"));
            try {
                getCitations(uidStats.keySet(), zop);
            } catch (Exception e) {
                logger.error(e);
            }
            zop.closeEntry();
        }
        zop.flush();
        zop.close();

        //logger.debug("UID stats : " + uidStats);
        //log the stats to ala logger

        LogEventVO vo = new LogEventVO(LogEventType.OCCURRENCE_RECORDS_DOWNLOADED, requestParams.getEmail(), requestParams.getReason(), ip, uidStats);
        logger.log(RestLevel.REMOTE, vo);
        return null;
	}

	/**
	 * get citation info from citation web service and write it into citation.txt file.
	 * 
	 * @param keys
	 * @param out
	 * @throws HttpException
	 * @throws IOException
	 */
	private void getCitations(Set<String> keys, OutputStream out) throws HttpException, IOException{
		if(keys == null || out == null){
			throw new NullPointerException("keys and/or out is null!!");
		}
		
        Object[] citations = restfulClient.restPost(citationServiceUrl, "text/plain", keys);
        if((Integer)citations[0] == HttpStatus.SC_OK){
        	out.write(((String)citations[1]).getBytes());
    	}
	}

	/**
	 * Utility method for retrieving a list of occurrences. Mainly added to help debug
     * webservices for that a developer can retrieve example UUIDs.
     *
	 * @throws Exception
	 */
	@RequestMapping(value = {"/occurrences/page"}, method = RequestMethod.GET)
	public @ResponseBody List<FullRecord> pageOccurrences(
            @RequestParam(value="pageSize", required=false, defaultValue="10") int pageSize) throws Exception {
        final List<FullRecord> records = new ArrayList<FullRecord>();
        Store.pageOverAll(Versions.RAW(), new OccurrenceConsumer(){
            public boolean consume(FullRecord record) {
                records.add(record);
                return false;
            }
        },null, pageSize);
        return records;
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
     * @param model
     * @throws Exception
     */
    @RequestMapping(value = {"/occurrence/compare/{uuid}.json"}, method = RequestMethod.GET)
    public @ResponseBody Object showOccurrence(@PathVariable("uuid") String uuid){
        Map values =Store.getComparisonByUuid(uuid);
        if(values.isEmpty())
            values = Store.getComparisonByRowKey(uuid);
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
	 * Occurrence record page
	 *
	 * When user supplies a uuid that is not found search for a unique record
	 * with the supplied occurrenc_id
	 *
	 * Returns a SearchResultDTO when there is more than 1 record with the supplied UUID
	 *
	 * @param uuid
	 * @param model
	 * @throws Exception
	 */
	@RequestMapping(value = {"/occurrence/{uuid:.+}","/occurrences/{uuid:.+}", "/occurrence/{uuid:.+}.json", "/occurrences/{uuid:.+}.json"}, method = RequestMethod.GET)
	public @ResponseBody Object showOccurrence(@PathVariable("uuid") String uuid,
        HttpServletRequest request, Model model) throws Exception {

		logger.debug("Retrieving occurrence record with guid: '"+uuid+"'");

        FullRecord[] fullRecord = Store.getAllVersionsByUuid(uuid);
        if(fullRecord == null){
            //get the rowKey for the supplied uuid in the index
            //This is a workaround.  There seems to be an issue on Cassandra with retrieving uuids that start with e or f
            SearchRequestParams srp = new SearchRequestParams();
            srp.setQ("id:"+uuid);
            srp.setPageSize(1);
            srp.setFacets(new String[]{});
            SearchResultDTO results = occurrenceSearch(srp, model);
            if(results.getTotalRecords()>0)
                fullRecord = Store.getAllVersionsByRowKey(results.getOccurrences().get(0).getRowKey());
            
        }
        if(fullRecord == null){
            //check to see if we have an occurrence id
            SearchRequestParams srp = new SearchRequestParams();
            srp.setQ("occurrence_id:" + uuid);
            SearchResultDTO result = occurrenceSearch(srp, model);
            if(result.getTotalRecords()>1)
                return result;
            else if(result.getTotalRecords()==0)
                return new OccurrenceDTO();
            else
                fullRecord = Store.getAllVersionsByUuid(result.getOccurrences().get(0).getUuid());
        }

        OccurrenceDTO occ = new OccurrenceDTO(fullRecord);
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
                m.setFilePath(MediaStore.convertPathToUrl(sound,biocacheMediaBaseUrl));

                String[] files = Store.getAlternativeFormats(sound);
                for(String fileName: files){
                    String contentType = MimeType.getForFileExtension(fileName).getMimeType();
                    String filePath = MediaStore.convertPathToUrl(fileName,biocacheMediaBaseUrl);
                    //System.out.println("#########Adding media path: " + m.getFilePath());
                    m.getAlternativeFormats().put(contentType,filePath);
                }
                soundDtos.add(m);
            }
            occ.setSounds(soundDtos);
        }

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
        MediaStore.convertPathsToUrls(occ.getRaw(), biocacheMediaBaseUrl);
        MediaStore.convertPathsToUrls(occ.getProcessed(), biocacheMediaBaseUrl);
            
        LogEventVO vo = new LogEventVO(LogEventType.OCCURRENCE_RECORDS_VIEWED, email, reason, ip, uidStats);
        logger.log(RestLevel.REMOTE, vo);

        return occ;
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

    /**
     * Simple check for valid latitude, longitude & radius values
     *
     * @param latitude
     * @param longitude
     * @param radius
     * @return
     */
    private boolean checkValidSpatialParams(Float latitude, Float longitude, Integer radius) {
        return (latitude != null && !latitude.isNaN() && longitude != null && !longitude.isNaN() && radius != null && radius > 0);
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

	public void setBiocacheMediaBaseUrl(String biocacheMediaBaseUrl) {
		this.biocacheMediaBaseUrl = biocacheMediaBaseUrl;
	}
}
