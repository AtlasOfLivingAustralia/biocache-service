/**************************************************************************
 *  Copyright (C) 2011 Atlas of Living Australia
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

import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestContextManager;

import javax.inject.Inject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
         
/**
 * JUnit tests for SOLR Query formatting methods in SearchDAOImpl
 *
 * FIXME these tests need to be rewritten in a mocked fashion.
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
@Ignore
@RunWith(Theories.class)
@ContextConfiguration(locations={"classpath:springTest.xml"})
public class QueryFormatTest {

    private TestContextManager testContextManager;
    @Inject protected SearchDAOImpl searchDAO;
    private static final Logger logger = Logger.getLogger(QueryFormatTest.class);
    protected static String qid = "";

    /**
     * Setup the Spring application context (instead of @RunWith(SpringJUnit4ClassRunner.class))
     *
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        this.testContextManager = new TestContextManager( getClass() );
        this.testContextManager.prepareTestInstance( this );
    }

    /**
     * Load the test case DataPoints using the SearchQueryTester class
     *
     * @return
     */
    @DataPoints
    public static SearchQueryTester[] data() {
        return new SearchQueryTester[] {
                new SearchQueryTester("lsid:urn:lsid:biodiversity.org.au:afd.taxon:31a9b8b8-4e8f-4343-a15f-2ed24e0bf1ae", "lft:[", "species:", false),
                //new SearchQueryTester("lsid:urn:lsid:biodiversity.org.au:afd.taxon:7790064f-4ef7-4742-8112-6b0528d5f3fb", "lft:[", "species:", false),
                new SearchQueryTester("lsid:urn:lsid:biodiversity.org.au:afd.taxon:test0064f-4ef7-4742-8112-6b0528d5f3fb", "taxon_concept_lsid:urn\\:lsid\\:biodiversity.org.au\\:afd.taxon\\:test0064f\\-4ef7\\-4742\\-8112\\-6b0528d5f3fb","taxon_concept_lsid:urn:lsid:biodiversity.org.au:afd.taxon:test0064f-4ef7-4742-8112-6b0528d5f3fb", true),
                new SearchQueryTester("lsid:urn:lsid:biodiversity.org.au:afd.taxon:7790064f-4ef7-4742-8112-6b0528d5ftest OR lsid:urn:lsid:biodiversity.org.au:afd.taxon:test0064f-4ef7-4742-8112-6b0528d5f3fb", "taxon_concept_lsid:urn\\:lsid\\:biodiversity.org.au\\:afd.taxon\\:7790064f\\-4ef7\\-4742\\-8112\\-6b0528d5ftest OR taxon_concept_lsid:urn\\:lsid\\:biodiversity.org.au\\:afd.taxon\\:test0064f\\-4ef7\\-4742\\-8112\\-6b0528d5f3fb","taxon_concept_lsid:urn:lsid:biodiversity.org.au:afd.taxon:7790064f-4ef7-4742-8112-6b0528d5ftest OR taxon_concept_lsid:urn:lsid:biodiversity.org.au:afd.taxon:test0064f-4ef7-4742-8112-6b0528d5f3fb", true),
                new SearchQueryTester("(lsid:urn:lsid:biodiversity.org.au:afd.taxon:test0064f-4ef7-4742-8112-6b0528d5f3fb)", "(taxon_concept_lsid:urn\\:lsid\\:biodiversity.org.au\\:afd.taxon\\:test0064f\\-4ef7\\-4742\\-8112\\-6b0528d5f3fb)","(taxon_concept_lsid:urn:lsid:biodiversity.org.au:afd.taxon:test0064f-4ef7-4742-8112-6b0528d5f3fb)", true),
                new SearchQueryTester("geohash:\"Intersects(Circle(125.0 -14.0 d=0.9009009))\" AND *:*","Intersects(Circle","within", false),
                new SearchQueryTester("qid:"+ qid, "", "", false),
                new SearchQueryTester("water", "water", "water", true),
                new SearchQueryTester("basis_of_record:PreservedSpecimen", "basis_of_record:PreservedSpecimen", "Record type:PreservedSpecimen", true),
                new SearchQueryTester("state:\"New South Wales\"", "state:\"New\\ South\\ Wales\"", "State/Territory:\"New South Wales\"", true),
                new SearchQueryTester("state:New\\ South\\ Wales", "state:New\\\\ South\\\\ Wales", "State/Territory:New\\ South\\ Wales", true),
                new SearchQueryTester("text:water species_group:Animals","text:water species_group:Animals","text:water species_group:Animals", true),
                new SearchQueryTester("urn:lsid:biodiversity.org.au:afd.taxon:a7b69905-7163-4017-a2a2-e92ce5dffb84","urn\\:lsid\\:biodiversity.org.au\\:afd.taxon\\:a7b69905\\-7163\\-4017\\-a2a2\\-e92ce5dffb84","urn:lsid:biodiversity.org.au:afd.taxon:a7b69905-7163-4017-a2a2-e92ce5dffb84", true),
                new SearchQueryTester("species_guid:urn:lsid:biodiversity.org.au:apni.taxon:254666","species_guid:urn\\:lsid\\:biodiversity.org.au\\:apni.taxon\\:254666","species_guid:urn:lsid:biodiversity.org.au:apni.taxon:254666", true),
//                new SearchQueryTester("occurrence_year:[1990-01-01T12:00:00Z TO *]","occurrence_year:[1990-01-01T12:00\\:00Z TO *]","Date (by decade):[1990-01-01T12:00:00Z TO *]", true),
                new SearchQueryTester("matched_name:\"kangurus lanosus\"", "taxon_name:\"Macropus\\ rufus\"","Scientific name:\"kangurus lanosus\"", true),
                new SearchQueryTester("matched_name_children:\"kangurus lanosus\"", "lft:[", "species:", false),
                new SearchQueryTester("(matched_name_children:Mammalia OR matched_name_children:whales)", "lft:[", "class:", false),
                //new SearchQueryTester("collector_text:Latz AND matched_name_children:\"Pluchea tetranthera\"", "as","as",false)
        };
    }

    /**
     * Run the tests
     *
     * @param queryTest
     */
    @Theory
    public void testQueryFormatting(SearchQueryTester queryTest) {
        SpatialSearchRequestParams ssrp = new SpatialSearchRequestParams();
        ssrp.setQ(queryTest.query);
        searchDAO.formatSearchQuery(ssrp);
        logger.info("Testing query \"" + queryTest + "\" -> " + ssrp.getFormattedQuery());
        if (queryTest.exactMatch) {
            assertEquals("formattedQuery does not have expected exact match. ", ssrp.getFormattedQuery(), queryTest.formattedQuery);
            assertEquals("displayString does not have expected exact match. "+ ssrp.getDisplayString(), ssrp.getDisplayString(), queryTest.displayString);
        } else {
            assertTrue("formattedQuery does not have expected 'contains' match. " + ssrp.getFormattedQuery(), StringUtils.containsIgnoreCase(ssrp.getFormattedQuery(), queryTest.formattedQuery) );
            assertTrue("display query does not have expected 'contains' match. ", StringUtils.containsIgnoreCase(ssrp.getDisplayString(), queryTest.displayString) );
        }
    }

    /**
     * Inner "theory" class to hold test queries and expected output
     */
    public static class SearchQueryTester {
        // input query string
        String query;
        // Either the exact string or a substring of the formatted query produced by searchDAO.formatSearchQuery(String)
        String formattedQuery;
        // Either the exact string or a substring of the displayString produced by searchDAO.formatSearchQuery(String)
        String displayString;
        // whether to expect an exact string match for both formattedQuery & displayQuery (or use a containsIgnoreCase test instead)
        Boolean exactMatch = true;

        /**
         * Contructor
         * 
         * @param q
         * @param fq
         * @param ds
         * @param em
         */
        public SearchQueryTester(String q, String fq, String ds, Boolean em) {
            query = q;
            formattedQuery = fq;
            displayString = ds;
            exactMatch = em;
        }

        @Override
        public String toString() {
            return query;
        }
    }
}
