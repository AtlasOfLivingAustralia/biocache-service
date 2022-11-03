package au.org.ala.biocache.dao;

import au.org.ala.biocache.dto.FieldResultDTO;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

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
public class SolrIndexDAOImplIT extends TestCase {

    static {
        System.setProperty("biocache.config", System.getProperty("user.dir") + "/src/test/resources/biocache-test-config.properties");
    }

    @Autowired
    SolrIndexDAOImpl solrIndexDAO;

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