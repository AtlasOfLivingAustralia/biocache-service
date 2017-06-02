package au.org.ala.biocache.dao;


import au.org.ala.biocache.dto.Facet;
import au.org.ala.biocache.dto.FacetThemes;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.service.SpeciesLookupService;
import au.org.ala.biocache.util.CollectionsCache;
import au.org.ala.biocache.util.QueryFormatUtils;
import au.org.ala.biocache.util.RangeBasedFacets;
import au.org.ala.biocache.util.SearchUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.MessageSource;
import org.springframework.context.support.ReloadableResourceBundleMessageSource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.CollectionUtils;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyList;

@RunWith(MockitoJUnitRunner.class)
public class FilterQueryParserTest {

    @Mock
    private SpeciesLookupService speciesLookupService;

    @Mock
    private CollectionsCache collectionCache;

    @Mock
    protected MessageSource messageSource;

    @InjectMocks
    QueryFormatUtils queryFormatUtils;

    protected Map<String, Facet> facetMap = null;

    String[] fqs = {"species_guid:urn:lsid:biodiversity.org.au:afd.taxon:2482313b-9d1e-4694-8f51-795213c8bb56",
            "collection_uid:co10",
            "institution_uid:in4 OR institution_uid:in22 OR institution_uid:in16 OR institution_uid:in6",
            "occurrence_year:[1940-01-01T00:00:00Z%20TO%201949-12-31T00:00:00Z]",
            "collector:\"Copland, S J\" OR collector:\"Sadlier, R.\" OR collector:\"Mcreaddie, W\" OR collector:\"Rollo, G\" OR collector:\"Harlow, Pete\"",
            "month:09 OR month:10 OR month:11"};

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        ReloadableResourceBundleMessageSource messageSource = new ReloadableResourceBundleMessageSource();
        messageSource.setDefaultEncoding("UTF-8");
        messageSource.setCacheSeconds(-1);
        messageSource.setBasenames("classpath:/messages");
        ReflectionTestUtils.setField(queryFormatUtils, "messageSource", messageSource);

        RangeBasedFacets rangeBasedFacets = new RangeBasedFacets();
        ReflectionTestUtils.setField(queryFormatUtils, "rangeBasedFacets", rangeBasedFacets);
        ReflectionTestUtils.setField(rangeBasedFacets, "messageSource", messageSource);

        SearchUtils searchUtils = new SearchUtils();
        ReflectionTestUtils.setField(queryFormatUtils, "searchUtils", searchUtils);

        ReflectionTestUtils.setField(searchUtils, "collectionCache", collectionCache);
        ReflectionTestUtils.setField(searchUtils, "speciesLookupService", speciesLookupService);
        ReflectionTestUtils.setField(searchUtils, "messageSource", messageSource);

        new FacetThemes("", null, 30, 30, true);

        final LinkedHashMap<String, String> collections =  new LinkedHashMap<String, String>();
        collections.put("co10", "found co10");
        collections.put("in4", "found in4");
        collections.put("in22", "found in22");
        collections.put("in16", "found in16");
        collections.put("in6", "found in6");

        Mockito.when(speciesLookupService.getNamesForGuids(anyList())).thenReturn(Arrays.asList(new String[] {"found guid"}));
        Mockito.when(collectionCache.getCollections()).thenReturn(collections);
        Mockito.when(collectionCache.getInstitutions()).thenReturn(collections);

        //update the collections cache - necessary because this is on a timer
        Set set = new java.util.HashSet<String>();
        CollectionUtils.mergeArrayIntoCollection("assertion_user_id,user_id,alau_user_id".split(","), set);
        SpatialSearchRequestParams query = new SpatialSearchRequestParams();
        query.setFq(fqs);
        facetMap = queryFormatUtils.formatSearchQuery(query, true);
    }

    @Test
    public void testFacetMapInit() {
        assertNotNull(facetMap);
    }

    @Test
    public void testAddFacetMap1() {
        Facet sp = facetMap.get(fqs[0]);
        assertNotNull(sp);
        assertTrue(StringUtils.containsIgnoreCase(sp.getValue(), "urn:lsid:biodiversity.org.au:afd.taxon:2482313b-9d1e-4694-8f51-795213c8bb56"));
        assertTrue("got: " + sp.getDisplayName(), StringUtils.containsIgnoreCase(sp.getDisplayName(), "Species:found guid"));
    }

    @Test
    public void testAddFacetMap2() {
        Facet in = facetMap.get(fqs[2]);
        assertNotNull(in);
        assertTrue(StringUtils.containsIgnoreCase(in.getValue(), "in4 OR institution_uid:in22 OR institution_uid:in16 OR institution_uid:in6"));
        assertTrue("got: " + in.getDisplayName(), StringUtils.containsIgnoreCase(in.getDisplayName(), "<span>Institution: found in4</span> OR <span>Institution: found in22</span> OR <span>Institution: found in16</span> OR <span>Institution: found in6</span>"));
    }

    @Test
    public void testAddFacetMap3() {
        Facet co = facetMap.get(fqs[1]);
        assertNotNull(co);
        assertTrue(StringUtils.containsIgnoreCase(co.getValue(), "co10"));
        assertTrue("got: " + co.getDisplayName(), StringUtils.containsIgnoreCase(co.getDisplayName(), "<span>Collection: found co10</span>"));
    }

    @Test
    public void testAddFacetMap4() {
        Facet od = facetMap.get(fqs[3]);
        assertNotNull(od);
        assertTrue(StringUtils.containsIgnoreCase(od.getValue(), "[1940-01-01T00:00:00Z%20TO%201949-12-31T00:00:00Z]"));
        assertTrue("got: " + od.getDisplayName(), StringUtils.containsIgnoreCase(od.getDisplayName(), "Date (by decade):1940-1949"));
    }

    @Test
    public void testAddFacetMap5() {
        Facet col = facetMap.get(fqs[4]);
        assertNotNull(col);
        assertTrue("got: " + col.getValue(), StringUtils.containsIgnoreCase(col.getValue(), "Copland, S J\" OR collector:\"Sadlier, R.\" OR collector:\"Mcreaddie, W\" OR collector:\"Rollo, G\" OR collector:\"Harlow, Pete"));
        assertTrue("got: " + col.getDisplayName(), StringUtils.containsIgnoreCase(col.getDisplayName(), "Collector:Copland, S J OR Collector:Sadlier, R. OR Collector:Mcreaddie, W OR Collector:Rollo, G OR Collector:Harlow, Pete"));
    }

    @Test
    public void testAddFacetMap6() {
        Facet month = facetMap.get(fqs[5]);
        assertNotNull(month);
        assertTrue("got: " + month.getValue(), StringUtils.containsIgnoreCase(month.getValue(), "09 OR month:10 OR month:11"));
        assertTrue("got: " + month.getDisplayName(), StringUtils.containsIgnoreCase(month.getDisplayName(), "Month:September OR Month:October OR Month:November"));
    }
}
