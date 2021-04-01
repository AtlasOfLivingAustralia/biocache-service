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

import au.org.ala.biocache.dto.FacetThemes;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.service.*;
import au.org.ala.biocache.util.*;
import au.org.ala.biocache.util.solr.FieldMappingUtil;
import au.org.ala.names.ws.api.NameUsageMatch;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.context.support.ReloadableResourceBundleMessageSource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * JUnit tests for SOLR Query formatting methods in SearchDAOImpl
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
@RunWith(MockitoJUnitRunner.class)
@ContextConfiguration(locations = {"classpath:springTest.xml"})
public class QueryFormatTest {

    @Mock
    protected DownloadFields downloadFields;

//    @Mock
//    protected SearchUtils searchUtils;

    @Mock
    protected CollectionsCache collectionCache;

//    @Mock
//    protected AbstractMessageSource messageSource;

    @Mock
    protected SpeciesLookupService speciesLookupService;

    @Mock
    protected AuthService authService;

    @Mock
    protected LayersService layersService;

    @Mock
    protected QidCacheDAO qidCacheDao;

    @Mock
    protected RangeBasedFacets rangeBasedFacets;

    @Mock
    protected SpeciesCountsService speciesCountsService;

    @Mock
    protected SpeciesImageService speciesImageService;

    @Mock
    protected ListsService listsService;

    @Mock
    protected SearchUtils searchUtils;

    @Mock DataQualityService dataQualityService;

    @InjectMocks
    QueryFormatUtils queryFormatUtils;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        Map<String, String> fieldMappings = new HashMap<String, String>() {{
            put("lsid", "taxonConceptID");
            put("basis_of_record", "basisOfRecord");
            put("species_guid", "speciesID");
            put("occurrence_year", "occurrenceYear");
        }};

        FieldMappingUtil fieldMappingUtil = new FieldMappingUtil();
        ReflectionTestUtils.setField(fieldMappingUtil, "fieldMappings", fieldMappings);


        NameUsageMatch nameUsageMatch = NameUsageMatch.builder()
                .success(true)
                .lft(0)
                .rgt(1)
                .rank("SPECIES")
                .build();
        when(speciesLookupService.getNameUsage(anyString())).thenReturn(nameUsageMatch);

        ReloadableResourceBundleMessageSource messageSource = new ReloadableResourceBundleMessageSource();
        messageSource.setDefaultEncoding("UTF-8");
        messageSource.setCacheSeconds(-1);
        messageSource.setBasenames("classpath:/messages");
        ReflectionTestUtils.setField(queryFormatUtils, "messageSource", messageSource);

        SearchUtils searchUtils = new SearchUtils();
        ReflectionTestUtils.setField(queryFormatUtils, "searchUtils", searchUtils);

        ReflectionTestUtils.setField(searchUtils, "collectionCache", collectionCache);
        ReflectionTestUtils.setField(searchUtils, "speciesLookupIndexService", speciesLookupService);
        ReflectionTestUtils.setField(searchUtils, "messageSource", messageSource);
        ReflectionTestUtils.setField(queryFormatUtils, "fieldMappingUtil", fieldMappingUtil);

        new FacetThemes("", null, 30, 30, true);
    }

    /**
     * Load the test case DataPoints using the SearchQueryTester class
     *
     * @return
     */
    @DataPoints
    public static SearchQueryTester[] data() {
        return new SearchQueryTester[] {
                new SearchQueryTester("lsid:urn:lsid:biodiversity.org.au:afd.taxon:31a9b8b8-4e8f-4343-a15f-2ed24e0bf1ae", "lft:[", "species", false),
                //new SearchQueryTester("lsid:urn:lsid:biodiversity.org.au:afd.taxon:7790064f-4ef7-4742-8112-6b0528d5f3fb", "lft:[", "species:", false),
                new SearchQueryTester("lsid:urn:lsid:biodiversity.org.au:afd.taxon:test0064f-4ef7-4742-8112-6b0528d5f3fb", "lft:[0 TO 1]","<span class='lsid' id='urn:lsid:biodiversity.org.au:afd.taxon:test0064f-4ef7-4742-8112-6b0528d5f3fb'>SPECIES: null</span>", true),
                new SearchQueryTester("lsid:urn:lsid:biodiversity.org.au:afd.taxon:7790064f-4ef7-4742-8112-6b0528d5ftest OR lsid:urn:lsid:biodiversity.org.au:afd.taxon:0064f-4ef7-4742-8112-6b0528d5f3fb", "lft:[0 TO 1] OR lft:[0 TO 1]","<span class='lsid' id='urn:lsid:biodiversity.org.au:afd.taxon:7790064f-4ef7-4742-8112-6b0528d5ftest'>SPECIES: null</span> OR <span class='lsid' id='urn:lsid:biodiversity.org.au:afd.taxon:0064f-4ef7-4742-8112-6b0528d5f3fb'>SPECIES: null</span>", true),
                new SearchQueryTester("(lsid:urn:lsid:biodiversity.org.au:afd.taxon:test0064f-4ef7-4742-8112-6b0528d5f3fb)", "(lft:[0 TO 1])","(<span class='lsid' id='urn:lsid:biodiversity.org.au:afd.taxon:test0064f-4ef7-4742-8112-6b0528d5f3fb'>SPECIES: null</span>)", true),
                new SearchQueryTester("geohash:\"Intersects(Circle(125.0 -14.0 d=0.9009009))\" AND *:*","Intersects(Circle","within", false),
                new SearchQueryTester("qid:"+ 1, "", "", false),
                new SearchQueryTester("water", "water", "water", true),
                new SearchQueryTester("basis_of_record:PreservedSpecimen", "basis_of_record:PreservedSpecimen", "Record type:Preserved specimen", true),
                new SearchQueryTester("state:\"New South Wales\"", "state:\"New\\ South\\ Wales\"", "State/Territory:\"New South Wales\"", true),
                new SearchQueryTester("text:water species_group:Animals","text:water species_group:Animals","text:water Lifeform:Animals", true),
                new SearchQueryTester("urn:lsid:biodiversity.org.au:afd.taxon:a7b69905-7163-4017-a2a2-e92ce5dffb84","urn\\:lsid\\:biodiversity.org.au\\:afd.taxon\\:a7b69905\\-7163\\-4017\\-a2a2\\-e92ce5dffb84","urn:lsid:biodiversity.org.au:afd.taxon:a7b69905-7163-4017-a2a2-e92ce5dffb84", true),
                new SearchQueryTester("species_guid:urn:lsid:biodiversity.org.au:apni.taxon:254666","species_guid:urn\\:lsid\\:biodiversity.org.au\\:apni.taxon\\:254666","Species:urn:lsid:biodiversity.org.au:apni.taxon:254666", true),
                new SearchQueryTester("occurrence_year:[1990-01-01T12:00:00Z TO *]","occurrence_year:[1990-01-01T12:00:00Z TO *]","Date (by decade):[1990-*]", true),
                //new SearchQueryTester("matched_name:\"kangurus lanosus\"", "taxon_name:\"kangurus\\ lanosus\"","Scientific name:\"kangurus lanosus\"", true),
                //new SearchQueryTester("matched_name_children:\"kangurus lanosus\"", "lft:[", "found", false),
                //new SearchQueryTester("(matched_name_children:Mammalia OR matched_name_children:whales)", "lft:[", "class:", false),
                //new SearchQueryTester("collector_text:Latz AND matched_name_children:\"Pluchea tetranthera\"", "as","as",false)
                new SearchQueryTester("spatial_list:dr123", "", "", false),
                new SearchQueryTester("month:03 month:04", "month:03 month:04", "Month:March Month:April", true),
                new SearchQueryTester("month:\"03\" month:\"04\"", "month:\"03\" month:\"04\"", "Month:\"March\" Month:\"April\"", true),
        };
    }

    /**
     * Run the tests
     */
    @Test
    public void testQueryFormatting() {
        for (SearchQueryTester sqt : data()) {
            SpatialSearchRequestParams ssrp = new SpatialSearchRequestParams();
            ssrp.setQ(sqt.query);
            queryFormatUtils.formatSearchQuery(ssrp, false);
            if (sqt.exactMatch) {
                assertEquals("formattedQuery does not have expected exact match. ", sqt.formattedQuery, ssrp.getFormattedQuery());
                assertEquals("displayString does not have expected exact match. " + ssrp.getDisplayString(), sqt.displayString, ssrp.getDisplayString());
            } else {
                assertTrue("formattedQuery does not have expected 'contains' match. " + ssrp.getFormattedQuery(), StringUtils.containsIgnoreCase(ssrp.getFormattedQuery(), sqt.formattedQuery));
                assertTrue("display query does not have expected 'contains' match. " + ssrp.getDisplayString(), StringUtils.containsIgnoreCase(ssrp.getDisplayString(), sqt.displayString));
            }
        }
    }

    /**
     * Run the tests with quality filters
     */
    @Test
    public void testQueryFormattingWithQualityFilters() {
        Map<String, String> filters = new LinkedHashMap();
        filters.put("first", "foo:bar");
        filters.put("second", "baz:qux");
        when(dataQualityService.getEnabledFiltersByLabel(any(SpatialSearchRequestParams.class))).thenReturn(filters);

        SpatialSearchRequestParams ssrp = new SpatialSearchRequestParams();
        ssrp.setQ("lsid:urn:lsid:biodiversity.org.au:afd.taxon:31a9b8b8-4e8f-4343-a15f-2ed24e0bf1ae"); //"lsid:urn:lsid:biodiversity.org.au:afd.taxon:31a9b8b8-4e8f-4343-a15f-2ed24e0bf1ae", "lft:[", "species", false
        queryFormatUtils.formatSearchQuery(ssrp, false);
        assertThat("filters are added", asList(ssrp.getFormattedFq()), containsInAnyOrder("foo:bar", "baz:qux"));

        ssrp = new SpatialSearchRequestParams();
        ssrp.setQ("lsid:urn:lsid:biodiversity.org.au:afd.taxon:31a9b8b8-4e8f-4343-a15f-2ed24e0bf1ae");
        ssrp.setFq(new String[]{"family:MACROPODIDAE"});
        queryFormatUtils.formatSearchQuery(ssrp, false);
        assertThat("filters are added", asList(ssrp.getFormattedFq()), containsInAnyOrder("family:MACROPODIDAE","foo:bar", "baz:qux"));
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
