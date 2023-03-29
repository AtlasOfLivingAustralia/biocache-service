package au.org.ala.biocache.dao;
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

import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.stream.ProcessInterface;
import au.org.ala.biocache.util.LegendItem;
import au.org.ala.biocache.util.QidMissingException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

import javax.servlet.ServletOutputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DAO for searching occurrence records held in the biocache.
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>" .
 */
public interface SearchDAO {

    /**
     * Returns species that only occur in the supplied subQueryQid
     * and not in the requestParams query.
     *
     * @param subQuery
     * @param parentQuery
     * @return
     * @throws Exception
     */
    List<FieldResultDTO> getSubquerySpeciesOnly(SpatialSearchRequestDTO subQuery, SpatialSearchRequestDTO parentQuery) throws Exception;

    /**
     * Find all occurrences for a given (full text) query, latitude, longitude & radius (km). I.e.
     * a full-text spatial query.  The result will include the sensitive coordinates if available.
     *
     * @param requestParams
     * @return
     * @throws Exception
     */
    SearchResultDTO findByFulltextSpatialQuery(SpatialSearchRequestDTO requestParams, boolean includeSensitive, Map<String, String[]> extraParams) throws Exception;

    /**
     * Writes the species count in the specified circle to the output stream.
     *
     * @param requestParams
     * @param speciesGroup
     * @param out
     * @return
     * @throws Exception
     */
    int writeSpeciesCountByCircleToStream(SpatialSearchRequestDTO requestParams, String speciesGroup, ServletOutputStream out) throws Exception;

    /**
     * Writes the results of this query to the output stream using the index as a source of the data.
     *
     * @param downloadParams
     * @param out
     * @param uidStats
     * @param parallelQueryExecutor The ExecutorService to manage parallel query executions
     * @return
     * @throws Exception
     */
    DownloadHeaders writeResultsFromIndexToStream(DownloadRequestDTO downloadParams, OutputStream out, ConcurrentMap<String, AtomicInteger> uidStats, DownloadDetailsDTO dd, boolean checkLimit, ExecutorService parallelQueryExecutor) throws Exception;

    /**
     * Write coordinates out to the supplied stream.
     *
     * @param searchParams
     * @param out
     * @throws Exception
     */
    void writeCoordinatesToStream(SpatialSearchRequestDTO searchParams, OutputStream out) throws Exception;

    /**
     * Write facet content to supplied output stream
     *
     * @param searchParams
     * @param includeCount
     * @param lookupName
     * @param includeSynonyms
     * @param out
     * @param dd
     * @throws Exception
     */
    void writeFacetToStream(SpatialSearchRequestDTO searchParams, boolean includeCount, boolean lookupName, boolean includeSynonyms, boolean includeLists, OutputStream out, DownloadDetailsDTO dd) throws Exception;

    /**
     * Write endemic parentQuery.facets()[0] content to supplied output stream that only occur in the subQuery
     * and not in the parentQuery.
     *
     * @param subQuery
     * @param parentQuery
     * @param includeCount
     * @param lookupName
     * @param includeSynonyms
     * @param out
     * @throws Exception
     */
    void writeEndemicFacetToStream(SpatialSearchRequestDTO subQuery, SpatialSearchRequestDTO parentQuery, boolean includeCount, boolean lookupName, boolean includeSynonyms, boolean includeLists, OutputStream out) throws Exception;

    /**
     * Retrieve an OccurrencePoint (distinct list of points - lat-long to 4 decimal places) for a given search
     *
     * @param searchParams
     * @param pointType
     * @return
     * @throws Exception
     */
    @Deprecated
    List<OccurrencePoint> getFacetPoints(SpatialSearchRequestDTO searchParams, PointType pointType) throws Exception;

    /**
     * Get a list of occurrence points for a given lat/long and distance (radius)
     *
     * @param requestParams
     * @param pointType
     * @return
     * @throws Exception
     */
    @Deprecated
    List<OccurrencePoint> findRecordsForLocation(SpatialSearchRequestDTO requestParams, PointType pointType) throws Exception;

    /**
     * Refresh any caches in use to populate queries.
     */
    void refreshCaches();

    /**
     * Find all the sources for the supplied query
     *
     * @param searchParams
     * @return
     * @throws Exception
     */
    Map<String, Integer> getSourcesForQuery(SpatialSearchRequestDTO searchParams) throws Exception;

    /**
     * Calculate taxon breakdown.
     *
     * @param queryParams
     * @return
     * @throws Exception
     */
    TaxaRankCountDTO calculateBreakdown(BreakdownRequestDTO queryParams) throws Exception;

    /**
     * Returns the occurrence counts based on lft and rgt values for each of the supplied taxa.
     *
     * @param taxa
     * @return
     * @throws Exception
     */
    Map<String, Integer> getOccurrenceCountsForTaxa(List<String> taxa, String[] filterQueries) throws Exception;

    /**
     * Find all species (and counts) for a given query and stream as JSON
     *
     * @param requestParams
     * @return
     * @throws Exception
     */
    void findAllSpeciesJSON(SpatialSearchRequestDTO requestParams, OutputStream outputStream) throws Exception;

    /**
     * Find all species (and counts) for a given query and stream as CSV
     *
     * @param requestParams
     * @return
     * @throws Exception
     */
    void findAllSpeciesCSV(SpatialSearchRequestDTO requestParams, OutputStream outputStream) throws Exception;

    /**
     * Find all occurrences for a given query as SolrDocumentList
     *
     * @param searchParams
     * @return
     * @throws Exception
     */
    SolrDocumentList findByFulltext(SpatialSearchRequestDTO searchParams) throws Exception;

    /**
     * Get legend items for a query and specified facet.
     * <p>
     * Continuous variable cut-points can be specified.  Includes the minimum
     * and maximum values.
     * <p>
     * Returns an empty list if no valid values are found.
     *
     * @param searchParams
     * @param facet
     * @return
     * @throws Exception
     */
    List<LegendItem> getLegend(SpatialSearchRequestDTO searchParams, String facet, String[] cutpoints) throws Exception;

    /**
     * Get legend items for a query and specified facet.
     * <p>
     * Continuous variable cut-points can be specified.  Includes the minimum
     * and maximum values.
     * <p>
     * Includes the option to skipI18n facet value replacement
     * <p>
     * Returns an empty list if no valid values are found.
     *
     * @param searchParams
     * @param facet
     * @param cutpoints
     * @param skipI18n
     * @return
     * @throws Exception
     */
    List<LegendItem> getLegend(SpatialSearchRequestDTO searchParams, String facet, String[] cutpoints, boolean skipI18n) throws Exception;

    /**
     * Get a data provider list for a query.
     *
     * @param requestParams
     * @return
     * @throws Exception
     */
    List<DataProviderCountDTO> getDataProviderList(SpatialSearchRequestDTO requestParams) throws Exception;

    /**
     * Retrieve facet counts for this query
     *
     * @param searchParams
     * @return
     * @throws Exception
     */
    List<FacetResultDTO> getFacetCounts(SpatialSearchRequestDTO searchParams) throws Exception;

    /**
     * Perform grouped facet query.
     * <p>
     * facets is the list of grouped facets required
     * flimit restricts the number of groups returned
     * pageSize restricts the number of docs in each group returned
     * fl is the list of fields in the returned docs
     *
     * @param searchRequestParams
     * @return
     * @throws Exception
     */
    QueryResponse searchGroupedFacets(SpatialSearchRequestDTO searchRequestParams) throws Exception;

    /**
     * Perform one pivot facet query.
     * <p/>
     * facets is the pivot facet list
     *
     * @param searchParams
     * @return
     * @throws Exception
     */
    List<FacetPivotResultDTO> searchPivot(SpatialSearchRequestDTO searchParams) throws Exception;

    /**
     * Return statistics for a numerical field.
     * <p>
     * Supply an optional facet to return statistics for each value in the facet.
     *
     * @param searchParams
     * @param field
     * @param facet
     * @return
     * @throws Exception
     */
    List<FieldStatsItem> searchStat(SpatialSearchRequestDTO searchParams, String field, String facet,
                                    Collection<String> statType) throws Exception;

    /**
     * Return legend items for a query and facet.
     * <p>
     * Specifying the facet 'grid' will return a generic LegendItem list.
     * <p>
     * This will only use the first ColorUtil.colourList.length-1 items only.
     *
     * @param request    query
     * @param colourMode a facet or 'grid'
     * @return
     * @throws Exception
     */
    List<LegendItem> getColours(SpatialSearchRequestDTO request, String colourMode) throws Exception;

    /**
     * Get maxBooleanClauses value from SOLR
     */
    int getMaxBooleanClauses();

    /**
     * get bounding box for a query.
     *
     * @param requestParams
     * @return
     */
    double[] getBBox(SpatialSearchRequestDTO requestParams) throws Exception;

    /**
     * Get estimated number of unique values for a facet.
     *
     * @param requestParams
     * @param facet
     * @return
     * @throws Exception
     */
    long estimateUniqueValues(SpatialSearchRequestDTO requestParams, String facet) throws Exception;

    /**
     * list facets available to the search query
     *
     * @param searchParams
     * @return
     */
    List<String> listFacets(SpatialSearchRequestDTO searchParams) throws Exception;

    /**
     * Query for heatmaps.
     *
     * @param query
     * @param filterQueries
     * @param minx
     * @param miny
     * @param maxx
     * @param maxy
     * @param legend
     * @return
     * @throws Exception
     */
    HeatmapDTO getHeatMap(String query, String[] filterQueries, Double minx, Double miny, Double maxx, Double maxy,
                          List<LegendItem> legend,
                          int gridSize) throws Exception;

    /**
     * Retrieval of outlier stats for record.
     *
     * @param uuid
     * @return
     * @throws Exception
     */
    List<RecordJackKnifeStats> getOutlierStatsFor(String uuid) throws Exception;

    /**
     * Starts a streaming query.
     *
     * @param request
     * @param procSearch
     * @param procFacet
     * @return
     * @throws Exception
     */
    int streamingQuery(SpatialSearchRequestDTO request, ProcessInterface procSearch, ProcessInterface procFacet) throws Exception;

    SolrQuery initSolrQuery(SpatialSearchRequestDTO searchParams, boolean substituteDefaultFacetOrder, Map<String, String[]> extraSolrParams) throws QidMissingException;
}
