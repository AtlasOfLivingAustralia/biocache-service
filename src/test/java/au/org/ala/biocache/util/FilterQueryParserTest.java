package au.org.ala.biocache.util;


import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dao.SearchDAOImpl;
import au.org.ala.biocache.dto.Facet;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;

import javax.inject.Inject;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Ignore
@ContextConfiguration(locations = {"classpath:springTest.xml"})
public class FilterQueryParserTest {

    @Inject
    protected SearchUtils searchUtils;
    @Inject
    SearchDAO searchDao;

    protected Map<String, Facet> facetMap = null;

    @Before
    public void setUp() throws Exception {
        String[] fqs = {"species_guid:urn:lsid:biodiversity.org.au:afd.taxon:2482313b-9d1e-4694-8f51-795213c8bb56",
                "collection_uid:co10",
                "institution_uid:in4 OR institution_uid:in22 OR institution_uid:in16 OR institution_uid:in6",
                "occurrence_year:[1940-01-01T00:00:00Z%20TO%201949-12-31T00:00:00Z]",
                "collector:\"Copland, S J\" OR collector:\"Sadlier, R.\" OR collector:\"Mcreaddie, W\" OR collector:\"Rollo, G\" OR collector:\"Harlow, Pete\"",
                "month:09 OR month:10 OR month:11"};
        //update the collections cache - necessary because this is on a timer
//        collectionsCache.updateCache();
        facetMap = searchUtils.addFacetMap(fqs, ((SearchDAOImpl) searchDao).getAuthIndexFields());
    }

    @Test
    public void testFacetMapInit() {
        assertNotNull(facetMap);
    }

    @Test
    public void testAddFacetMap1() {
        Facet sp = facetMap.get("species_guid");
        assertNotNull(sp);
        assertTrue(StringUtils.containsIgnoreCase(sp.getValue(), "urn:lsid:biodiversity.org.au:afd.taxon:2482313b-9d1e-4694-8f51-795213c8bb56"));
        assertTrue("got: " + sp.getDisplayName(), StringUtils.containsIgnoreCase(sp.getDisplayName(), "Species:Pogona barbata"));
    }

    @Test
    public void testAddFacetMap2() {
        Facet in = facetMap.get("institution_uid");
        assertNotNull(in);
        assertTrue(StringUtils.containsIgnoreCase(in.getValue(), "in4 OR institution_uid:in22 OR institution_uid:in16 OR institution_uid:in6"));
        assertTrue("got: " + in.getDisplayName(), StringUtils.containsIgnoreCase(in.getDisplayName(), "Institution:Australian Museum"));
    }

    @Test
    public void testAddFacetMap3() {
        Facet co = facetMap.get("collection_uid");
        assertNotNull(co);
        assertTrue(StringUtils.containsIgnoreCase(co.getValue(), "co10"));
        assertTrue("got: " + co.getDisplayName(), StringUtils.containsIgnoreCase(co.getDisplayName(), "Collection:Australian Museum Herpetology Collection"));
    }

    @Test
    public void testAddFacetMap4() {
        Facet od = facetMap.get("occurrence_year");
        assertNotNull(od);
        assertTrue(StringUtils.containsIgnoreCase(od.getValue(), "[1940-01-01T00:00:00Z%20TO%201949-12-31T00:00:00Z]"));
        assertTrue("got: " + od.getDisplayName(), StringUtils.containsIgnoreCase(od.getDisplayName(), "Date (by decade):1940-1949"));
    }

    @Test
    public void testAddFacetMap5() {
        Facet col = facetMap.get("collector");
        assertNotNull(col);
        assertTrue("got: " + col.getValue(), StringUtils.containsIgnoreCase(col.getValue(), "Copland, S J\" OR collector:\"Sadlier, R.\" OR collector:\"Mcreaddie, W\" OR collector:\"Rollo, G\" OR collector:\"Harlow, Pete"));
        assertTrue("got: " + col.getDisplayName(), StringUtils.containsIgnoreCase(col.getDisplayName(), "Collector:Copland, S J OR Collector:Sadlier, R. OR Collector:Mcreaddie, W OR Collector:Rollo, G OR Collector:Harlow, Pete"));
    }

    @Test
    public void testAddFacetMap6() {
        Facet month = facetMap.get("month");
        assertNotNull(month);
        assertTrue("got: " + month.getValue(), StringUtils.containsIgnoreCase(month.getValue(), "09 OR month:10 OR month:11"));
        assertTrue("got: " + month.getDisplayName(), StringUtils.containsIgnoreCase(month.getDisplayName(), "Month:September OR Month:October OR Month:November"));
    }

    public void setSearchUtils(SearchUtils searchUtils) {
        this.searchUtils = searchUtils;
    }

    public void setSearchDao(SearchDAO searchDao) {
        this.searchDao = searchDao;
    }

    public void setFacetMap(Map<String, Facet> facetMap) {
        this.facetMap = facetMap;
    }
//
//    public void setCollectionsCache(CollectionsCache collectionsCache) {
//        this.collectionsCache = collectionsCache;
//    }
}
