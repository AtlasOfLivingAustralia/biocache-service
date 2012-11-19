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
package org.ala.biocache.web;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.ala.biocache.dao.SearchDAO;
import org.ala.biocache.dao.SearchDAOImpl;
import org.ala.biocache.dto.OccurrencePoint;
import org.ala.biocache.dto.PointType;
import org.ala.biocache.dto.SpatialSearchRequestParams;
import org.ala.biocache.dto.TaxaCountDTO;
import org.ala.biocache.util.ParamsCache;
import org.ala.biocache.util.ParamsCacheObject;
import org.ala.biocache.util.SpatialUtils;
import org.ala.biocache.util.TaxaGroup;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.googlecode.ehcache.annotations.Cacheable;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import org.ala.biocache.dto.*;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Controller for the "explore your area" page
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 * @author "Natasha Carter <natasha.carter@csiro.au>"
 */
@Controller("exploreController")
public class ExploreController {
    /** Logger initialisation */
	private final static Logger logger = Logger.getLogger(ExploreController.class);

    /** Fulltext search DAO */
    @Inject
    protected SearchDAO searchDao;
    /** Name of view for site home page */
	private String YOUR_AREA = "explore/yourArea";
    private String speciesPageUrl = "http://bie.ala.org.au/species/";
    private static final String GEOIP_DATABASE = "/data/geoip/GeoLiteCity.dat"; // get from http://www.maxmind.com/app/geolitecity
//    private HashMap<String, List<Float>> addressCache = new HashMap<String, List<Float>>();
    private final String DEFAULT_LOCATION = "Clunies Ross St, Black Mountain, ACT";
    private static LookupService lookupService = null;
    private final String POINTS_GEOJSON = "json/pointsGeoJson";



    /** Mapping of radius in km to OpenLayers zoom level */
    public final static HashMap<Float, Integer> radiusToZoomLevelMap = new HashMap<Float, Integer>();
	static {
		radiusToZoomLevelMap.put(1f, 14);
        radiusToZoomLevelMap.put(5f, 12);
		radiusToZoomLevelMap.put(10f, 11);
		radiusToZoomLevelMap.put(50f, 9);

        try {
            if(new File(GEOIP_DATABASE).exists()){
                lookupService = new LookupService(GEOIP_DATABASE, LookupService.GEOIP_INDEX_CACHE);
            } else {
                logger.warn("Did not load GeoIP database. File unavailable: " + GEOIP_DATABASE);
            }
        } catch (IOException ex) {
            logger.error("Failed to load GeoIP database: " + ex.getMessage(), ex);
        }
	}

    /**
     *
     * Returns a list of species groups and counts that will need to be displayed.
     *
     * TODO: MOVE all the IP lat long lookup to the the client webapp.  The purposes
     * of biocache-service is to provide a service layer over the biocache.
     *
     */
    @RequestMapping(value = "/explore/groups*", method = RequestMethod.GET)
	public @ResponseBody List<SpeciesGroupDTO> yourAreaView(
            SpatialSearchRequestParams requestParams,
            @RequestParam(value="address", required=false, defaultValue=DEFAULT_LOCATION) String address,
            @RequestParam(value="location", required=false, defaultValue="") String location,
            HttpServletRequest request,
            Model model) throws Exception {
        

        //now we want to grab all the facets to get the counts associated with the species groups
        List<au.org.ala.biocache.SpeciesGroup> sgs = au.org.ala.biocache.Store.retrieveSpeciesGroups();
        List<SpeciesGroupDTO> speciesGroups = new java.util.ArrayList<SpeciesGroupDTO>();
        SpeciesGroupDTO all = new SpeciesGroupDTO();
        String originalQ = requestParams.getQ();
        all.setName("ALL_SPECIES");
        all.setLevel(0);
        Integer[] counts = getYourAreaCount(requestParams, "ALL_SPECIES");
        all.setCount(counts[0]);
        all.setSpeciesCount(counts[1]);
        speciesGroups.add(all);

        String oldName = null;
        String kingdom =null;
        //set the counts an indent levels for all the species groups
        for(au.org.ala.biocache.SpeciesGroup sg : sgs){
            logger.debug("name: " + sg.name() + " parent: " +sg.parent());
            int level =3;
            SpeciesGroupDTO sdto = new SpeciesGroupDTO();
            sdto.setName(sg.name());

            if(oldName!= null && sg.parent()!= null && sg.parent().equals(kingdom))
                level = 2;
            
            oldName = sg.name();
            if(sg.parent() == null){
                level = 1;
                kingdom = sg.name();
            }
            sdto.setLevel(level);
            //set the original query back to default to clean up after ourselves
            requestParams.setQ(originalQ);
            counts = getYourAreaCount(requestParams, sg.name());
            sdto.setCount(counts[0]);
            sdto.setSpeciesCount(counts[1]);
            speciesGroups.add(sdto);
        }
        return speciesGroups;

	}
    /**
     * Returns the number of records and distinct species in a particular species group
     * 
     * @param requestParams
     * @param group
     * @return
     * @throws Exception
     */
    @RequestMapping(value="/explore/counts/group/{group}*", method = RequestMethod.GET)
    public @ResponseBody Integer[] getYourAreaCount(SpatialSearchRequestParams requestParams,
            @PathVariable(value="group") String group) throws Exception{
        updateQuery(requestParams, group);        
        requestParams.setPageSize(0);
        requestParams.setFacets(new String[]{"taxon_name"});
        requestParams.setFlimit(-1);
        SearchResultDTO results = searchDao.findByFulltextSpatialQuery(requestParams,null);
        Integer speciesCount =0;        
        if(results.getFacetResults().size() >0){
            speciesCount = results.getFacetResults().iterator().next().getFieldResult().size();
        }
        
        return new Integer[]{(int)results.getTotalRecords() ,speciesCount};
    }

    /**
     * Updates the requestParams to take into account the provided species group
     * @param requestParams
     * @param group
     */
    private void updateQuery(SpatialSearchRequestParams requestParams, String group){
        StringBuilder sb = new StringBuilder();
        if(requestParams.getQ() != null && !requestParams.getQ().isEmpty())
            sb.append(requestParams.getQ());
        else{
            sb.append("*:*");
        }
        if(!group.equals("ALL_SPECIES"))
            sb.append(" species_group:").append(group);
        //now ignore the records that have been identified to a rank above species
        sb.append( " -rank:kingdom -rank:phylum -rank:class -rank:order -rank:family -rank:genus");
        //String query = sb.togroup.equals("ALL_SPECIES")? "*:*" : "species_group:" + group;
        requestParams.setQ(sb.toString());
      //don't care about the formatted query
        requestParams.setFormattedQuery(null);
    }
    
    /**
     * GeoJSON view of records as clusters of points within a specified radius of a given location
     *
     * This service will be used by explore your area.
     *
     * 
     *
     */
    @RequestMapping(value = "/geojson/radius-points", method = RequestMethod.GET)
        public String radiusPointsGeoJson(SpatialSearchRequestParams requestParams,
            @RequestParam(value="zoom", required=false, defaultValue="0") Integer zoomLevel,
            @RequestParam(value="bbox", required=false) String bbox,
            @RequestParam(value="group", required=false, defaultValue="ALL_SPECIES") String speciesGroup,
            Model model)
            throws Exception {
        updateQuery(requestParams, speciesGroup);
        PointType pointType = PointType.POINT_00001; // default value for when zoom is null
        pointType = getPointTypeForZoomLevel(zoomLevel);
        logger.info("PointType for zoomLevel ("+zoomLevel+") = "+pointType.getLabel());
        List<OccurrencePoint> points = searchDao.findRecordsForLocation(requestParams, pointType);
        logger.info("Points search for "+pointType.getLabel()+" - found: "+points.size());
        model.addAttribute("points", points);
        return POINTS_GEOJSON;
    }
    
    /**
     * Map a zoom level to a coordinate accuracy level
     *
     * @param zoomLevel
     * @return
     */
    protected PointType getPointTypeForZoomLevel(Integer zoomLevel) {
        PointType pointType = null;
        // Map zoom levels to lat/long accuracy levels
        if (zoomLevel != null) {
            if (zoomLevel >= 0 && zoomLevel <= 6) {
                // 0-6 levels
                pointType = PointType.POINT_1;
            } else if (zoomLevel > 6 && zoomLevel <= 8) {
                // 6-7 levels
                pointType = PointType.POINT_01;
            } else if (zoomLevel > 8 && zoomLevel <= 10) {
                // 8-9 levels
                pointType = PointType.POINT_001;
            } else if (zoomLevel > 10 && zoomLevel <= 13) {
                // 10-12 levels
                pointType = PointType.POINT_0001;
            } else if (zoomLevel > 13 && zoomLevel <= 15) {
                // 12-n levels
                pointType = PointType.POINT_00001;
            } else {
                // raw levels
                pointType = PointType.POINT_RAW;
            }
        }
        return pointType;
    }
    
    /**
     * Returns the number of species in all the groups.
     * @param requestParams
     * @return
     * @throws Exception
     */
//    @RequestMapping(value="/explore/counts*", method = RequestMethod.GET)
//    public @ResponseBody Map<String,String> getYourAreaCounts(SpatialSearchRequestParams requestParams) throws Exception{
//        Map<String,String> values = new HashMap<String, String>();
//        List<au.org.ala.biocache.SpeciesGroup> sgs =au.org.ala.biocache.Store.retrieveSpeciesGroups();
//        values.put("ALL_SPECIES", getYourAreaCount(requestParams, "ALL_SPECIES"));
//        for(au.org.ala.biocache.SpeciesGroup sg : sgs){
//            values.put(sg.name(), getYourAreaCount(requestParams,sg.name()));
//        }
//        return values;
//    }
    
 
    private void  applyFacetForCounts(SpatialSearchRequestParams requestParams, boolean useCommonName){
    	if(useCommonName)
    		requestParams.setFacets(new String[]{SearchDAOImpl.COMMON_NAME_AND_LSID});
    	else
    		requestParams.setFacets(new String[]{SearchDAOImpl.NAMES_AND_LSID});
    }

        /**
	 * Occurrence search page uses SOLR JSON to display results
	 *
     * @param query
     * @param model
     * @return
     * @throws Exception
     */
	@RequestMapping(value = "/explore/group/{group}/download*", method = RequestMethod.GET)
	public void yourAreaDownload(
            DownloadRequestParams requestParams,
            @PathVariable(value="group") String group,
            @RequestParam(value="common", required=false, defaultValue="false") boolean common,
            HttpServletResponse response)
            throws Exception {
	    String filename = requestParams.getFile() != null ? requestParams.getFile():"data"; 
        logger.debug("Downloading the species in your area... ");
        response.setHeader("Cache-Control", "must-revalidate");
        response.setHeader("Pragma", "must-revalidate");
        response.setHeader("Content-Disposition", "attachment;filename="+filename);
        response.setContentType("application/vnd.ms-excel");
       
        updateQuery(requestParams, group);
        applyFacetForCounts(requestParams, common);
        
        ServletOutputStream out = response.getOutputStream();
        int count = searchDao.writeSpeciesCountByCircleToStream(requestParams,group, out);
        logger.debug("Exported " + count + " species records in the requested area");
        
	}

    /**
     * JSON web service that returns a list of species and record counts for a given location search
     * and a higher taxa with rank. 
     *
     * @param model
     * @throws Exception
     */
    @RequestMapping(value = "/explore/group/{group}*", method = RequestMethod.GET)
	public @ResponseBody List<TaxaCountDTO> listSpeciesForHigherTaxa(
            SpatialSearchRequestParams requestParams,
            @PathVariable(value="group") String group,
            @RequestParam(value="common", required=false, defaultValue="false") boolean common,           
            Model model) throws Exception {

       
        updateQuery(requestParams,group);
        applyFacetForCounts(requestParams, common);
        
        return searchDao.findAllSpeciesByCircleAreaAndHigherTaxa(requestParams, group);
    }

	/**
	 * @param searchDao the searchDao to set
	 */
	public void setSearchDao(SearchDAO searchDao) {
		this.searchDao = searchDao;
	}
	/**
	 * @param speciesPageUrl the speciesPageUrl to set
	 */
	public void setSpeciesPageUrl(String speciesPageUrl) {
		this.speciesPageUrl = speciesPageUrl;
	}

	// The Endemism Web Services - Move these if they get too large...
	/**
	 * Returns the number of distinct species that are in the supplied region.
	 * @param requestParams
	 * @param response
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value = "/explore/counts/endemic*", method = RequestMethod.GET)
	public @ResponseBody int getSpeciesCountOnlyInWKT(SpatialSearchRequestParams requestParams, 
	          HttpServletResponse response) 
	          throws Exception{  
	    List list = getSpeciesOnlyInWKT(requestParams,response);
	    if(list !=null)
	        return list.size();
	    return 0;
	}
		
	/**
	 * Returns the species that only have occurrences in the supplied WKT.
	 * @return
	 */
	@RequestMapping(value = "/explore/endemic/species*", method = RequestMethod.GET)
	public @ResponseBody List<FieldResultDTO> getSpeciesOnlyInWKT(SpatialSearchRequestParams requestParams,
	          HttpServletResponse response) 
	              throws Exception{
	    ParamsCacheObject pco = ParamsCache.getParamCacheObjectFromQuery(requestParams.getQ());
	    String wkt =StringUtils.isNotBlank(requestParams.getWkt())?requestParams.getWkt():pco.getWkt();
	    if(pco != null){
	        requestParams.setQ(pco.getQ());
	        requestParams.setWkt(pco.getWkt());
	    }
	    
	    if(StringUtils.isNotBlank(wkt) ){
	        if(requestParams.getFacets() != null && requestParams.getFacets().length ==1){
	              return searchDao.getEndemicSpecies(requestParams);
//	            // 1)get a list of species that are in the WKT
//	            
//	            ArrayList list1 = getValuesForFacets(requestParams);//new ArrayList(Arrays.asList(getValuesForFacets(requestParams)));	              
//	            if(logger.isDebugEnabled())
//	                logger.debug("INCLUDED: "+list1.size() + " " +list1);	            
//	            // 2)get a list of species that occur in the inverse WKT
//	            String newWKT = SpatialUtils.getInverseWKT(wkt.replaceAll(":", " "));
//	            if(pco != null)
//	                requestParams.setQ(pco.getQ());	            
//	            requestParams.setWkt(newWKT);
//	            ArrayList list2 = getValuesForFacets(requestParams);//new ArrayList(Arrays.asList(getValuesForFacets(requestParams)));
//	            if(logger.isDebugEnabled())
//	                logger.debug("EXCLUDED: " +list2.size() + " " +list2);
//	            //return the values in 1) that don't exist in 2)
//	            list1.removeAll(list2);
//	            if(logger.isDebugEnabled())
//	                logger.debug("FINAL Species WKT " + list1.size() + " " + list1);
//	            return list1;
	        }
	        else{
	            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Please supply only one facet.");
	        }
	    }
	    else{
	        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Please supply a WKT area.");
	    }
	    return null;
	}
	

	
	/**
   * Returns the species that only have occurrences in the supplied WKT.
   * @return
   */
  @RequestMapping(value = "/explore/endemic/species.csv", method = RequestMethod.GET)
  public void getEndemicSpeciesCSV(SpatialSearchRequestParams requestParams,HttpServletResponse response) throws Exception{
      requestParams.setFacets(new String[]{SearchDAOImpl.NAMES_AND_LSID});
      requestParams.setFq((String[])ArrayUtils.add(requestParams.getFq(), "species_guid:[* TO *]"));
      List<FieldResultDTO> list = getSpeciesOnlyInWKT(requestParams, response);
      response.setCharacterEncoding("UTF-8");
      response.setContentType("text/plain");
      java.io.PrintWriter writer = response.getWriter();
      writer.write("Family,Scientific name,Common name,Taxon rank,LSID,# Occurrences");
      for(FieldResultDTO item: list){
          String[] values = item.getLabel().split("\\|",6);
          if(values.length>=5){
              writer.write("\n"+values[4]+",\""+values[0]+"\",\""+values[2]+"\",,"+values[1] + ","+item.getCount());
          }
      }
      writer.flush();
      writer.close();
  }	
}
