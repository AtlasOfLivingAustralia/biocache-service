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


import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.util.QueryFormatUtils;
import com.ctc.wstx.util.URLUtil;
import io.swagger.annotations.ApiParam;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.opengis.metadata.identification.CharacterSet;
import org.springdoc.api.annotations.ParameterObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.annotation.JsonInclude;
import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Controller for the "explore your area" page
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 * @author "Natasha Carter <natasha.carter@csiro.au>"
 */
@Controller
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExploreController {

    /**
     * Logger initialisation
     */
    private final static Logger logger = Logger.getLogger(ExploreController.class);
    public static final String ENDEMISM_CSV_HEADER = "Family,Scientific name,Common name,Taxon rank,LSID,# Occurrences";

    /**
     * Fulltext search DAO
     */
    @Inject
    protected SearchDAO searchDao;

    @Inject
    protected QueryFormatUtils queryFormatUtils;

    @Value("${species.subgroups.url:/data/biocache/config/subgroups.json}")
    protected String speciesSubgroupsUrl;
    private String speciesSubgroups = null;

    private String getSubgroupsConfig() {
        if (speciesSubgroups == null) {
            speciesSubgroups = getStringFromPath(speciesSubgroupsUrl);
        }

        return speciesSubgroups;
    }

    @Value("${species.groups.url:/data/biocache/config/groups.json}")
    protected String speciesGroupsUrl;
    private String speciesGroups = null;

    private String getGroupsConfig() {
        if (speciesGroups == null) {
            speciesGroups = getStringFromPath(speciesGroupsUrl);
        }

        return speciesGroups;
    }

    private String getStringFromPath(String path) {
        String result = null;
        try {
            if (path.startsWith("http")) {
                result = StreamUtils.copyToString(URLUtil.inputStreamFromURL(new URL(path)), CharacterSet.UTF_8.toCharset());
            } else {
                result = FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            logger.error("Exception reading from species.subgroups.url: " + speciesSubgroups, e);
        }
        return result;
    }

    /**
     * Mapping of radius in km to OpenLayers zoom level
     */
    public final static HashMap<Float, Integer> radiusToZoomLevelMap = new HashMap<Float, Integer>();

    static {
        radiusToZoomLevelMap.put(1f, 14);
        radiusToZoomLevelMap.put(5f, 12);
        radiusToZoomLevelMap.put(10f, 11);
        radiusToZoomLevelMap.put(50f, 9);
    }

    @Operation(summary = "Returns a hierarchical listing of all species groups", tags = "Explore")
    @Tag(name = "Explore", description = "Services for retrieval of all species groups")
    @RequestMapping(value = { "/explore/hierarchy"
//            , "/explore/hierarchy.json"
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public void getHierarchy(HttpServletResponse response) throws Exception {
        response.setContentType("application/json");
        try {
            OutputStream out = response.getOutputStream();
            out.write(getSubgroupsConfig().getBytes(StandardCharsets.UTF_8));
            out.flush();
            out.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Returns a hierarchical listing of species groups.
     * <p>
     * TODO push down to service implementation.
     *
     * @return
     * @throws Exception
     */
    @Operation(summary = "Returns a hierarchical listing of species groups", tags = "Explore")
    @Tag(name = "Explore", description = "Services for retrieval of Specimens & occurrence data based on species groups")
    @RequestMapping(value = { "/explore/hierarchy/groups"
//            , "/explore/hierarchy/groups.json*"
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody
    Collection<SpeciesGroupDTO> yourHierarchicalAreaView(
            @ParameterObject SpatialSearchRequestParams params, String speciesGroup) throws Exception {

        JSONArray ssgs = JSONArray.fromObject(getSubgroupsConfig());
        Map<String, SpeciesGroupDTO> parentGroupMap = new LinkedHashMap<String, SpeciesGroupDTO>();

        //create a parent lookup table
        Map<String, String> parentLookup = new HashMap<String, String>();
        ssgs.stream().forEach((Object sg) ->
            ((JSONArray) ((JSONObject) sg).get("taxa")).stream().forEach((Object ssg) ->
                    parentLookup.put(((JSONObject) ssg).getString("common").toLowerCase(), ((JSONObject) sg).getString("speciesGroup"))));

        //get the species group occurrence counts
        SpatialSearchRequestDTO requestParams = SpatialSearchRequestDTO.create(params);
        requestParams.setFormattedQuery(null);
        requestParams.setFacets(new String[]{OccurrenceIndex.SPECIES_SUBGROUP});
        requestParams.setFacet(true);
        requestParams.setPageSize(0);
        requestParams.setFlimit(-1);
        if (StringUtils.isNotBlank(speciesGroup)) {
            requestParams.setFq(new String[]{OccurrenceIndex.SPECIES_GROUP + ":\"" + speciesGroup + "\""});
        }

        //retrieve a list of subgroups with occurrences matching the query
        List<FacetResultDTO> speciesSubgroupCounts = searchDao.getFacetCounts(requestParams);
        Map<String, Long> occurrenceCounts = new HashMap<String, Long>();
        if (speciesSubgroupCounts.size() > 0) {
            for (FieldResultDTO fr : speciesSubgroupCounts.get(0).getFieldResult()) {
                occurrenceCounts.put(fr.getLabel(), fr.getCount());
            }
        }

        String taxonName = OccurrenceIndex.TAXON_NAME;

        //do a facet query for each species subgroup
        for (String ssg : occurrenceCounts.keySet()) {

            requestParams.setQ(OccurrenceIndex.SPECIES_SUBGROUP + ":\"" + ssg + "\"");
            requestParams.setFormattedQuery(null);
            requestParams.setFacets(new String[]{taxonName});

            long count = searchDao.estimateUniqueValues(requestParams, taxonName);
            if (count > 0) {
                String parentName = parentLookup.get(ssg.toLowerCase());
                SpeciesGroupDTO parentGroup = parentGroupMap.get(parentName);
                if (parentGroup == null) {
                    parentGroup = new SpeciesGroupDTO(parentName, 0, 0, 1);
                    parentGroupMap.put(parentName, parentGroup);
                }
                if (parentGroup != null) {
                    if (parentGroup.getChildGroups() == null) {
                        parentGroup.setChildGroups(new ArrayList<SpeciesGroupDTO>());
                    }
                    parentGroup.getChildGroups().add(new SpeciesGroupDTO(ssg, count, occurrenceCounts.get(ssg), 2));
                    parentGroup.setSpeciesCount(parentGroup.getSpeciesCount() + count);
                    parentGroup.setCount(parentGroup.getCount() + occurrenceCounts.get(ssg));
                } else {
                    logger.warn("Parent group lookup failed for: " + parentName + ", ssg: " + ssg);
                }
            }
        }

        //prune empty parents
        List<String> toRemove = new ArrayList<String>();
        for (String parentName : parentGroupMap.keySet()) {
            if (parentGroupMap.get(parentName).getChildGroups() == null || parentGroupMap.get(parentName).getChildGroups().size() == 0) {
                toRemove.add(parentName);
            }
        }
        for (String key : toRemove) {
            parentGroupMap.remove(key);
        }

        return parentGroupMap.values();
    }

    /**
     * Returns a list of species groups and counts that will need to be displayed.
     */
    @Operation(summary = "Returns a list of species groups and counts that will need to be displayed", tags = "Explore")
    @RequestMapping(value = { "/explore/groups"
//            , "/explore/groups.json*",
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody
    List<SpeciesGroupDTO> yourAreaView(@ParameterObject SpatialSearchRequestParams params) throws Exception {

        SpatialSearchRequestDTO requestParams = SpatialSearchRequestDTO.create(params);
        //now we want to grab all the facets to get the counts associated with the species groups
        JSONArray sgs = JSONArray.fromObject(getGroupsConfig());
        List<SpeciesGroupDTO> speciesGroups = new java.util.ArrayList<SpeciesGroupDTO>();
        SpeciesGroupDTO all = new SpeciesGroupDTO();
        String[] originalFqs = requestParams.getFq();
        all.setName("ALL_SPECIES");
        all.setLevel(0);
        Integer[] counts = getYourAreaCount(params, "ALL_SPECIES");
        all.setCount(counts[0]);
        all.setSpeciesCount(counts[1]);
        speciesGroups.add(all);

        String oldName = null;
        String kingdom = null;
        //set the counts an indent levels for all the species groups
        for (Object sg : sgs) {
            if (sg instanceof JSONObject
                    && ((JSONObject) sg).containsKey("name")) {

                String parent = null;
                if (((JSONObject) sg).containsKey("parent")) {
                    parent = ((JSONObject) sg).getString("parent");
                }

                String name = ((JSONObject) sg).getString("name");

                if (logger.isDebugEnabled()) {
                    logger.debug("name: " + name + " parent: " + parent);
                }

                int level = 3;
                SpeciesGroupDTO sdto = new SpeciesGroupDTO();
                sdto.setName(name);

                if (oldName != null && parent != null && parent.equals(kingdom)) {
                    level = 2;
                }

                oldName = name;
                if (StringUtils.isBlank(parent) || "null".equalsIgnoreCase(parent)) {
                    level = 1;
                    kingdom = name;
                }
                sdto.setLevel(level);
                //set the original query back to default to clean up after ourselves
                requestParams.setFq(originalFqs);
                //query per group
                counts = getYourAreaCount(params, name);
                sdto.setCount(counts[0]);
                sdto.setSpeciesCount(counts[1]);
                speciesGroups.add(sdto);
            }
        }
        return speciesGroups;
    }

    /**
     * Returns the number of records and distinct species in a particular species group
     *
     * @param group
     * @return
     * @throws Exception
     */
    @Operation(summary = "Returns the number of records and distinct species in a particular species group", tags = "Explore", description = "The first count is total number of occurrence, the second is the number of distinct species")
    @RequestMapping(value = { "/explore/counts/group/{group}"
//            , "/explore/counts/group/{group}.json*"
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody
    Integer[] getYourAreaCount(@ParameterObject SpatialSearchRequestParams params,
                               @PathVariable(value = "group") String group) throws Exception {

        SpatialSearchRequestDTO requestParams = SpatialSearchRequestDTO.create(params);
        addGroupFilterToQuery(requestParams, group);

        // find number of occurrences
        requestParams.setPageSize(0);
        requestParams.setFacet(false);
        SearchResultDTO results = searchDao.findByFulltextSpatialQuery(requestParams, false, null);

        // estimate number of species
        int speciesCount = (int) searchDao.estimateUniqueValues(requestParams, OccurrenceIndex.TAXON_NAME);

        return new Integer[]{(int) results.getTotalRecords(), speciesCount};
    }

    /**
     * Updates the requestParams to take into account the provided species group
     *
     * @param group
     */
    private void addGroupFilterToQuery(SpatialSearchRequestDTO requestParams, String group) {
        addFacetFilterToQuery(requestParams, OccurrenceIndex.SPECIES_GROUP, group);
    }

    /**
     * Updates the requestParams to take into account the provided species group
     *
     * @param facetValue
     */
    private void addFacetFilterToQuery(SpatialSearchRequestDTO requestParams, String facetName, String facetValue) {

        if (!facetValue.equals("ALL_SPECIES")) {
            queryFormatUtils.addFqs(new String [] {facetName + ":" + facetValue}, requestParams);
        }

        queryFormatUtils.addFqs(new String [] {OccurrenceIndex.TAXON_RANK_ID + ":[" + 7000 + " TO *]"}, requestParams);

        // reset formatted query
        requestParams.setFormattedQuery(null);
    }

    private void applyFacetForCounts(SpatialSearchRequestDTO requestParams, boolean useCommonName) {
        if (useCommonName)
            requestParams.setFacets(new String[]{OccurrenceIndex.COMMON_NAME_AND_LSID});
        else
            requestParams.setFacets(new String[]{OccurrenceIndex.NAMES_AND_LSID});
    }

    /**
     * Occurrence search page uses SOLR JSON to display results
     *
     * @return
     * @throws Exception
     */
    @Operation(summary = "Occurrence search page uses SOLR JSON to display results", tags = "Explore", description = "The first count is total number of occurrence, the second is the number of distinct species")
    @RequestMapping(value = { "/explore/group/{group}/download"
//            , "/explore/group/{group}/download.json*"
    } , method = RequestMethod.GET, produces = "application/vnd.ms-excel")
    public void yourAreaDownload(
            @ParameterObject DownloadRequestParams requestParams,
            @PathVariable(value = "group") String group,
            @RequestParam(value = "common", required = false, defaultValue = "false") boolean common,
            HttpServletRequest request,
            HttpServletResponse response) {

        DownloadRequestDTO dto = DownloadRequestDTO.create(requestParams, request);

        String filename = requestParams.getFile() != null ? requestParams.getFile() : "data";
        logger.debug("Downloading the species in your area... ");
        response.setHeader("Cache-Control", "must-revalidate");
        response.setHeader("Pragma", "must-revalidate");
        response.setHeader("Content-Disposition", "attachment;filename=" + filename);
        response.setContentType("application/vnd.ms-excel");

        addGroupFilterToQuery(dto, group);
        applyFacetForCounts(dto, common);

        try {
            ServletOutputStream out = response.getOutputStream();
            searchDao.writeSpeciesCountByCircleToStream(dto, group, out);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * JSON web service that returns a list of species and record counts for a given location search
     * and a higher taxa with rank.
     *
     * @throws Exception
     */
    @Operation(summary = "Returns a list of species and record counts for a given location search" +
            " and a higher taxa with rank.", tags = "Explore")
    @RequestMapping(value = {"/explore/group/{group}"
//            , "/explore/group/{group}.json*"
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiParam(value = "group", required = true)
    public void listSpeciesForHigherTaxa(
            @ParameterObject SpatialSearchRequestParams params,
            @PathVariable(value = "group") String group,
            @RequestParam(value = "common", required = false, defaultValue = "false") boolean common,
            @RequestParam(value = "includeRank", required = false, defaultValue = "true") boolean includeRank,
            HttpServletResponse response) throws Exception {

        SpatialSearchRequestDTO requestParams = SpatialSearchRequestDTO.create(params);

        addGroupFilterToQuery(requestParams, group);
        applyFacetForCounts(requestParams, common);

        // Legacy usage
        requestParams.setFlimit(requestParams.getPageSize());
        requestParams.setPageSize(0);
        requestParams.setFoffset(requestParams.getStart());
        requestParams.setFsort(requestParams.getSort());
        requestParams.setSort("");

        response.setContentType("application/json");

        searchDao.findAllSpeciesJSON(requestParams, includeRank, response.getOutputStream());
    }

    /**
     * Returns the number of distinct species that are in the supplied region.
     *
     * @param requestParams
     * @return
     * @throws Exception
     */
    @Operation(summary = "Returns the number of distinct species that are in the supplied region", tags = "Explore")
    @RequestMapping(value = {"/explore/counts/endemic"
//            , "/explore/counts/endemic.json*"
    }, method = RequestMethod.GET)
    public @ResponseBody
    int getSpeciesCountOnlyInWKT(@ParameterObject SpatialSearchRequestParams requestParams) throws Exception {

        List list = getSpeciesOnlyInWKT(requestParams);
        if (list != null)
            return list.size();
        return 0;
    }

    /**
     * Returns the species that only have occurrences in the supplied WKT.
     *
     * @return
     */
    @Operation(summary = "Returns the species that only have occurrences in the supplied WKT.", tags = "Endemism")
    @Tag(name = "Endemism", description = "Services for reports on endemism for an area")
    @RequestMapping(value = {"/explore/endemic/species"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody
    List<FieldResultDTO> getSpeciesOnlyInWKT(@ParameterObject SpatialSearchRequestParams requestParams) throws Exception {

        SpatialSearchRequestDTO requested = SpatialSearchRequestDTO.create(requestParams);
        SpatialSearchRequestDTO superset = new SpatialSearchRequestDTO();
        superset.setQ("decimalLongitude:[-180 TO 180]");
        superset.setFq(new String[] {"decimalLatitude:[-90 TO 90]"});

        // ensure that one facet is set
        prepareEndemicFacet(requested);
        prepareEndemicFacet(superset);

        return searchDao.getSubquerySpeciesOnly(requested, superset);
    }

    private void prepareEndemicFacet(SpatialSearchRequestDTO parentQuery) {
        if (parentQuery.getFacets() == null || parentQuery.getFacets().length != 1) {
            parentQuery.setFacets(new String[]{OccurrenceIndex.NAMES_AND_LSID});
        }
    }

    /**
     * Returns facet values that only occur in the supplied subQueryID
     * and not in the parentQuery.
     * <p>
     * The facet is defined in the parentQuery. Default facet is SearchDAOImpl.NAMES_AND_LSID
     * <p>
     * If no requestParams defined the default q=*:* is used.
     *
     * @return
     */
    @Operation(summary = "Returns facet values that only occur in the supplied subQueryID and not in the parentQuery.", tags = "Endemism")
    @RequestMapping(value = {"/explore/endemic/species/{subQueryID}"
//            , "/explore/endemic/species/{subQueryQid}.json*"
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiParam(value = "subQueryID", required = true)
    public @ResponseBody
    List<FieldResultDTO> getSpeciesOnlyInOneQuery(@ParameterObject SpatialSearchRequestParams parentQueryParams,
                                                  @PathVariable(value = "subQueryID") Long subQueryID)
            throws Exception {

        SpatialSearchRequestDTO parentQuery = SpatialSearchRequestDTO.create(parentQueryParams);
        SpatialSearchRequestDTO subQuery = new SpatialSearchRequestDTO();
        subQuery.setQ("qid:" + subQueryID);
        prepareEndemicFacet(parentQuery);

        subQuery.setFacets(parentQuery.getFacets());
        return searchDao.getSubquerySpeciesOnly(subQuery, parentQuery);
    }

    /**
     * Returns count of facet values that only occur in the supplied subQueryID
     * and not in the parentQuery.
     * <p>
     * The facet is defined in the parentQuery. Default facet is SearchDAOImpl.NAMES_AND_LSID
     * <p>
     * If no requestParams defined the default q=*:* is used.
     *
     * @return
     */
    @Operation(summary = "Returns count of facet values that only occur in the supplied " +
            "subQueryID and not in the parentQuery.", tags = "Endemism")
    @RequestMapping(value = {"/explore/endemic/speciescount/{subQueryID}"
//            , "/explore/endemic/speciescount/{subQueryID}.json*"
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiParam(value = "subQueryID", required = true)
    public @ResponseBody
    Map getSpeciesOnlyInOneCountQuery(@ParameterObject SpatialSearchRequestParams parentQuery,
                                      @PathVariable(value = "subQueryID") Long subQueryID)
            throws Exception {
        return Collections.singletonMap("count", getSpeciesOnlyInOneQuery(parentQuery, subQueryID).size());
    }

    /**
     * Returns the species that only have occurrences in the supplied WKT.
     *
     * @return
     */
    @Operation(summary = "Download a CSV of the species that only have occurrences in the supplied WKT"
            , tags = {"Endemism", "Download"}, description = "Download a CSV of the species that only have occurrences in the supplied WKT. Columns: \" + ENDEMISM_CSV_HEADER")
    @RequestMapping(value = "/explore/endemic/species.csv", method = RequestMethod.GET)
    public void getEndemicSpeciesCSV(@ParameterObject SpatialSearchRequestParams requestParams, HttpServletResponse response) throws Exception {
        requestParams.setFacets(new String[]{OccurrenceIndex.NAMES_AND_LSID});
        requestParams.setFq((String[]) ArrayUtils.add(requestParams.getFq(), OccurrenceIndex.SPECIESID + ":*"));

        // Cannot use getSpeciesOnlyInOneQueryCSV as the output columns differ
        List<FieldResultDTO> list = getSpeciesOnlyInWKT(requestParams);

        response.setCharacterEncoding("UTF-8");
        response.setContentType("text/plain");

        java.io.PrintWriter writer = response.getWriter();

        writer.write(ENDEMISM_CSV_HEADER);
        for (FieldResultDTO item : list) {
            String s = item.getLabel();
            if (s.startsWith("\"") && s.endsWith("\"") && s.length() > 2) s = s.substring(1, s.length() - 1);
            String[] values = s.split("\\|", 6);
            if (values.length >= 5) {
                writer.write("\n" + values[4] + ",\"" + values[0] + "\",\"" + values[2] + "\",," + values[1] + "," + item.getCount());
            }
        }
        writer.flush();
        writer.close();
    }

    /**
     * Returns facet values that only occur in the supplied subQueryID
     * and not in the parentQuery.
     * <p>
     * The facet is defined in the parentQuery. Default facet is SearchDAOImpl.NAMES_AND_LSID
     * <p>
     * If no requestParams defined the default q=*:* is used.
     *
     * @return
     */
    @Operation(summary = "Download a CSV of facet values that only occur in the supplied subQueryID and not in the parentQuery.", tags = {"Endemism", "Download"})
    @RequestMapping(value = "/explore/endemic/species/{subQueryID}.csv", method = RequestMethod.GET)
    @ApiParam(value = "subQueryID", required = true)
    public void getSpeciesOnlyInOneQueryCSV(@ParameterObject SpatialSearchRequestParams parentQueryParams,
                                            @PathVariable(value = "subQueryID") Long subQueryID,
                                            @RequestParam(value = "count", required = false, defaultValue = "false") boolean includeCount,
                                            @RequestParam(value = "lookup", required = false, defaultValue = "false") boolean lookupName,
                                            @RequestParam(value = "synonym", required = false, defaultValue = "false") boolean includeSynonyms,
                                            @RequestParam(value = "lists", required = false, defaultValue = "false") boolean includeLists,
                                            @RequestParam(value = "file", required = false, defaultValue = "") String file,
                                            HttpServletResponse response) {

        SpatialSearchRequestDTO parentQuery = SpatialSearchRequestDTO.create(parentQueryParams);
        SpatialSearchRequestDTO subQuery = new SpatialSearchRequestDTO();
        subQuery.setQ("qid:" + subQueryID);

        prepareEndemicFacet(parentQuery);
        prepareEndemicFacet(subQuery);

        String filename = StringUtils.isNotEmpty(file) ? file : parentQuery.getFacets()[0];
        response.setHeader("Cache-Control", "must-revalidate");
        response.setHeader("Pragma", "must-revalidate");
        response.setHeader("Content-Disposition", "attachment;filename=" + filename + ".csv");
        response.setContentType("text/csv");

        try {
            searchDao.writeEndemicFacetToStream(subQuery, parentQuery, includeCount, lookupName, includeSynonyms, includeLists, response.getOutputStream());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
