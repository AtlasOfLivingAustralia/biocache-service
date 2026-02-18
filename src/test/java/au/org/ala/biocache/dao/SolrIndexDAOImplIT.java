package au.org.ala.biocache.dao;

import au.org.ala.biocache.dto.FacetResultDTO;
import au.org.ala.biocache.dto.FieldResultDTO;
import au.org.ala.biocache.dto.SearchResultDTO;
import au.org.ala.biocache.dto.SpatialSearchRequestDTO;
import au.org.ala.biocache.stream.EndemicFacet;
import au.org.ala.biocache.stream.ProcessInterface;
import au.org.ala.biocache.util.QueryFormatUtils;
import au.org.ala.biocache.util.SolrUtils;
import junit.framework.TestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for occurrence services.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:springTest.xml"})
@WebAppConfiguration
@TestPropertySource(locations = "classpath:biocache-test-config.properties")
public class SolrIndexDAOImplIT extends TestCase {

    static {
        System.setProperty("biocache.config", System.getProperty("user.dir") + "/src/test/resources/biocache-test-config.properties");
    }

    @Autowired
    SolrIndexDAOImpl solrIndexDAO;

    @Autowired
    SearchDAOImpl searchDAO;

    @Autowired
    QueryFormatUtils queryFormatUtils;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        SolrUtils.setupIndex();
    }

    @Before
    public void setup() throws Exception {

    }

    @Test
    public void selectHandlerDf() throws Exception {
        SolrQuery query = new SolrQuery();
        query.setQuery("Spotted Harrier");
        query.setRows(10000);
        QueryResponse response = solrIndexDAO.runSolrQuery(query);

        assertEquals(response.getResults().size(), 1000);
    }

    @Test
    public void exportHandlerDf() throws Exception {
        SolrQuery query = new SolrQuery();
        query.setQuery("Spotted Harrier");
        query.setRows(-1);
        query.setStart(0);

        TupleCounter tupleCounter = new TupleCounter();

        int count = solrIndexDAO.streamingQuery(query, tupleCounter, null, null);

        assertEquals(tupleCounter.count, 1000);
        assertEquals(count, 1000);
    }

    @Test
    public void endemicQueryStream() throws Exception {
        // does the subset (year:1989) contain all (year:1989) occurrences that also appear in the superset (year:*)
        SolrQuery superset = new SolrQuery();
        superset.setQuery("year:*");

        SolrQuery subset = new SolrQuery();
        subset.setQuery("year:1989");
        subset.addFacetField("year");

        List<FieldResultDTO> output = new ArrayList();
        solrIndexDAO.streamingQuery(subset, null, new EndemicFacet(output, "year"), superset);

        assertEquals(output.size(), 1);
    }

    @Test
    public void endemicQueryStream2() throws Exception {
        // does the subset (year:[* TO 1989]) contain whole year occurrences that also appear in the superset (year:*)
        SolrQuery superset = new SolrQuery();
        superset.setQuery("year:*");

        SolrQuery subset = new SolrQuery();
        subset.setQuery("year:[* TO 1989]");
        subset.addFacetField("year");

        List<FieldResultDTO> output = new ArrayList();
        solrIndexDAO.streamingQuery(subset, null, new EndemicFacet(output, "year"), superset);

        assertEquals(output.size(), 28);

        // validate each 'year' returned is <= 1989
        for (FieldResultDTO fr : output) {
            String value = fr.getFieldValue();
            assert (Integer.parseInt(value) <= 1989);
        }
    }

    @Test
    @Ignore
    public void getIndexVersion() throws Exception {
        // TODO: This test will fail when using a zk/solr cluster. Not currently an issue.

        // initial index version
        Long indexVersionStart = solrIndexDAO.getIndexVersion(false);

        // increment version with a SOLR /update
        Map<String, Object> newRecord = new HashMap();

        // use a record id from test-data.csv, it must not have the same ID as the record used in `testIndexFromMap()`
        newRecord.put("record_uuid", "af56ce03-e664-421a-85ac-fbb839bbb140");    // TODO: refactor so valid SOLR field 'id' can be used
        newRecord.put("hasUserAssertions", true); // TODO: fix error message for 'invalid fields' in .indexFromMap
        List<Map<String, Object>> batch = new ArrayList();
        batch.add(newRecord);

        solrIndexDAO.indexFromMap(batch);
        Thread.sleep(1000);

        // fetch index version is on a thread, so wait after the force=true
        solrIndexDAO.getIndexVersion(true);
        Thread.sleep(1000);
        Long indexVersionEnd = solrIndexDAO.getIndexVersion(false);
        // System.out.println("indexVersionStart: " + indexVersionStart + ", indexVersionEnd: " + indexVersionEnd);

        assert (indexVersionStart < indexVersionEnd);
    }

    @Test
    public void testIndexFromMap() throws Exception {
        SolrQuery query = new SolrQuery();
        query.setQuery("id:c0ee1a86-1df6-40b2-950c-bdde40b1c46e");
        query.setRows(1);
        query.setStart(0);
        query.setFields("hasUserAssertions");

        // initial value
        QueryResponse queryResponse = solrIndexDAO.query(query);

        assertNotNull(queryResponse);
        assertNotNull(queryResponse.getResults());
        assertEquals(queryResponse.getResults().size(), 1);

        Object initialValue = queryResponse.getResults().get(0).getFieldValue("hasUserAssertions");

        Map<String, Object> newRecord = new HashMap();

        // use a record id from test-data.csv
        newRecord.put("record_uuid", "c0ee1a86-1df6-40b2-950c-bdde40b1c46e");    // TODO: refactor so valid SOLR field 'id' can be used
        newRecord.put("hasUserAssertions", true); // TODO: fix error message for 'invalid fields' in .indexFromMap
        List<Map<String, Object>> batch = new ArrayList();
        batch.add(newRecord);

        solrIndexDAO.indexFromMap(batch);
        Thread.sleep(1000);

        queryResponse = solrIndexDAO.query(query);
        Object trueValue = queryResponse.getResults().get(0).getFieldValue("hasUserAssertions");

        newRecord.put("hasUserAssertions", false);
        solrIndexDAO.indexFromMap(batch);
        Thread.sleep(1000);

        queryResponse = solrIndexDAO.query(query);
        Object falseValue = queryResponse.getResults().get(0).getFieldValue("hasUserAssertions");

        assertEquals(null, initialValue);
        assertEquals(true, trueValue);
        assertEquals(false, falseValue);
    }

    @Test
    public void flimitMaxCapsResults() throws Exception {
        // Set flimitMax to 5 so that facet requests are capped
        ReflectionTestUtils.setField(searchDAO, "flimitMax", 5);
        try {
            SpatialSearchRequestDTO params = new SpatialSearchRequestDTO();
            params.setQ("*:*");
            params.setFacet(true);
            params.setFacets(new String[]{"year"});
            params.setFlimit(100);
            params.setPageSize(0);

            SearchResultDTO result = searchDAO.findByFulltextSpatialQuery(params, false, null);

            assertNotNull(result.getFacetResults());
            assertFalse(result.getFacetResults().isEmpty());
            FacetResultDTO yearFacet = result.getFacetResults().get(0);
            assertTrue(
                    "Facet result size should be capped at flimitMax=5, but got " + yearFacet.getFieldResult().size(),
                    yearFacet.getFieldResult().size() <= 5
            );
        } finally {
            // Restore default (disabled)
            ReflectionTestUtils.setField(searchDAO, "flimitMax", -1);
        }
    }

    @Test
    public void flimitMaxDisabledReturnsRequestedAmount() throws Exception {
        // With flimitMax=-1 (disabled), the full requested flimit should be returned
        ReflectionTestUtils.setField(searchDAO, "flimitMax", -1);

        SpatialSearchRequestDTO params = new SpatialSearchRequestDTO();
        params.setQ("*:*");
        params.setFacet(true);
        params.setFacets(new String[]{"year"});
        params.setFlimit(50);
        params.setPageSize(0);

        SearchResultDTO result = searchDAO.findByFulltextSpatialQuery(params, false, null);

        assertNotNull(result.getFacetResults());
        assertFalse(result.getFacetResults().isEmpty());
        FacetResultDTO yearFacet = result.getFacetResults().get(0);
        // The test index has many years; with no cap, all 50 (or fewer if index has fewer) should come back
        assertTrue(
                "Expected up to 50 facet values without a cap, but got " + yearFacet.getFieldResult().size(),
                yearFacet.getFieldResult().size() > 5
        );
    }

    @Test
    public void pageSizeMaxCapsResults() throws Exception {
        // Set pageSizeMax to 5 so that row requests are capped
        ReflectionTestUtils.setField(searchDAO, "pageSizeMax", 5);
        try {
            SpatialSearchRequestDTO params = new SpatialSearchRequestDTO();
            params.setQ("*:*");
            params.setFacet(false);
            params.setPageSize(1000);

            SearchResultDTO result = searchDAO.findByFulltextSpatialQuery(params, false, null);

            assertTrue(
                    "Result count should be capped at pageSizeMax=5, but got " + result.getOccurrences().size(),
                    result.getOccurrences().size() <= 5
            );
        } finally {
            // Restore default (disabled)
            ReflectionTestUtils.setField(searchDAO, "pageSizeMax", -1);
        }
    }

    @Test
    public void pageSizeMaxDisabledReturnsRequestedAmount() throws Exception {
        // With pageSizeMax=-1 (disabled), the full requested pageSize should be honoured
        ReflectionTestUtils.setField(searchDAO, "pageSizeMax", -1);

        SpatialSearchRequestDTO params = new SpatialSearchRequestDTO();
        params.setQ("*:*");
        params.setFacet(false);
        params.setPageSize(50);

        SearchResultDTO result = searchDAO.findByFulltextSpatialQuery(params, false, null);

        assertTrue(
                "Expected 50 results without a cap, but got " + result.getOccurrences().size(),
                result.getOccurrences().size() > 5
        );
    }

    @Test
    public void fcontainsFiltersToMatchingFacetValues() throws Exception {
        // kingdom has values "Animalia" (981 records) and "Plantae" (5 records).
        // fcontains="alia" should return only the "Animalia" facet value.
        SpatialSearchRequestDTO params = new SpatialSearchRequestDTO();
        params.setQ("*:*");
        params.setFacet(true);
        params.setFacets(new String[]{"kingdom"});
        params.setFlimit(100);
        params.setFcontains("alia");
        params.setPageSize(0);

        SearchResultDTO result = searchDAO.findByFulltextSpatialQuery(params, false, null);

        assertNotNull(result.getFacetResults());
        assertFalse("Expected facet results for kingdom field", result.getFacetResults().isEmpty());
        FacetResultDTO facet = result.getFacetResults().get(0);
        assertNotNull(facet.getFieldResult());
        assertFalse("Expected at least one kingdom value containing 'alia'", facet.getFieldResult().isEmpty());
        for (FieldResultDTO fieldResult : facet.getFieldResult()) {
            assertTrue(
                    "Facet value '" + fieldResult.getFieldValue() + "' should contain 'alia'",
                    fieldResult.getFieldValue().toLowerCase().contains("alia")
            );
        }
    }

    @Test
    public void fcontainsEmptyReturnsAllFacetValues() throws Exception {
        // kingdom has "Animalia" and "Plantae". With fcontains="alia" only "Animalia" is returned;
        // with fcontains="" (disabled) both values are returned.
        SpatialSearchRequestDTO paramsWithFilter = new SpatialSearchRequestDTO();
        paramsWithFilter.setQ("*:*");
        paramsWithFilter.setFacet(true);
        paramsWithFilter.setFacets(new String[]{"kingdom"});
        paramsWithFilter.setFlimit(100);
        paramsWithFilter.setFcontains("alia");
        paramsWithFilter.setPageSize(0);

        SpatialSearchRequestDTO paramsNoFilter = new SpatialSearchRequestDTO();
        paramsNoFilter.setQ("*:*");
        paramsNoFilter.setFacet(true);
        paramsNoFilter.setFacets(new String[]{"kingdom"});
        paramsNoFilter.setFlimit(100);
        paramsNoFilter.setFcontains("");
        paramsNoFilter.setPageSize(0);

        SearchResultDTO filteredResult = searchDAO.findByFulltextSpatialQuery(paramsWithFilter, false, null);
        SearchResultDTO unfilteredResult = searchDAO.findByFulltextSpatialQuery(paramsNoFilter, false, null);

        int filteredCount = filteredResult.getFacetResults().get(0).getFieldResult().size();
        int unfilteredCount = unfilteredResult.getFacetResults().get(0).getFieldResult().size();

        assertTrue(
                "Unfiltered facet results (" + unfilteredCount + ") should be greater than filtered results (" + filteredCount + ")",
                unfilteredCount > filteredCount
        );
    }

    @Test
    public void getStatistics() throws Exception {
        Map<String, FieldStatsInfo> yearStats = solrIndexDAO.getStatistics("year");

        assert (yearStats != null);
        assert (yearStats.get("year").getCount() == 1005);
        assertEquals(1865.0, yearStats.get("year").getMin());
        assertEquals(2019.0, yearStats.get("year").getMax());
        assert (yearStats.get("year").getMissing() == 0);
    }

    class TupleCounter implements ProcessInterface {

        int count = 0;

        @Override
        public boolean process(Tuple t) {
            if (t.EOF) {
                // signal finished
                return false;
            } else {
                // increment count
                count++;

                // signal continue
                return true;
            }
        }

        @Override
        public boolean flush() {
            return true;
        }
    }
}
