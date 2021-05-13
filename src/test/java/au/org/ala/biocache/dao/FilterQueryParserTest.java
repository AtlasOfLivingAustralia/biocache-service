package au.org.ala.biocache.dao;


import au.org.ala.biocache.dto.Facet;
import au.org.ala.biocache.dto.FacetThemes;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.service.DataQualityService;
import au.org.ala.biocache.service.SpeciesLookupService;
import au.org.ala.biocache.util.*;
import au.org.ala.biocache.util.solr.FieldMappingUtil;
import au.org.ala.names.ws.api.NameUsageMatch;
import au.org.ala.names.ws.client.ALANameUsageMatchServiceClient;
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

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

//TODO: update hubs, then remove fqs[0].substring(0, fqs[0].indexOf(':'))
@RunWith(MockitoJUnitRunner.class)
public class FilterQueryParserTest {

    @Mock
    private SpeciesLookupService speciesLookupService;

    @Mock
    private CollectionsCache collectionCache;

    @Mock
    protected MessageSource messageSource;

    @Mock
    protected ALANameUsageMatchServiceClient nameUsageMatchService;

    @Mock
    private FieldMappingUtil fieldMappingUtil;

    @Mock
    DataQualityService dataQualityService;

    @InjectMocks
    QueryFormatUtils queryFormatUtils;

    protected Map<String, Facet> facetMap = null;

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
        ReflectionTestUtils.setField(searchUtils, "nameUsageMatchService", nameUsageMatchService);
        ReflectionTestUtils.setField(searchUtils, "messageSource", messageSource);
        ReflectionTestUtils.setField(queryFormatUtils, "fieldMappingUtil", fieldMappingUtil);

        Mockito.when(fieldMappingUtil.translateQueryFields(anyString())).thenAnswer(invocation -> invocation.getArgument(0));

        new FacetThemes("", null, 30, 30, true);
    }

    @Test
    public void testFacetMap() throws QidMissingException  {

        final LinkedHashMap<String, String> collections =  new LinkedHashMap<String, String>();
        collections.put("co10", "found co10");
        collections.put("in4", "found in4");
        collections.put("in22", "found in22");
        collections.put("in16", "found in16");
        collections.put("in6", "found in6");

//        Mockito.when(speciesLookupService.getNamesForGuids(anyList())).thenReturn(Arrays.asList(new String[] {"found guid"}));
        Mockito.when(nameUsageMatchService.get(anyString())).thenReturn(NameUsageMatch.builder().success(true).scientificName("found guid").build());

        Mockito.when(collectionCache.getCollections()).thenReturn(collections);
        Mockito.when(collectionCache.getInstitutions()).thenReturn(collections);
        Mockito.when(fieldMappingUtil.translateFieldName("speciesID")).thenReturn("speciesID");
        Mockito.when(fieldMappingUtil.translateFieldName("species_id")).thenReturn("speciesID");
        Mockito.when(fieldMappingUtil.translateFieldName("occurrenceYear")).thenReturn("occurrenceYear");
        Mockito.when(fieldMappingUtil.translateFieldName("occurrence_year")).thenReturn("occurrenceYear");
        Mockito.when(fieldMappingUtil.translateFieldName("month")).thenReturn("month");

        String[] fqs = {
                "speciesID:urn:lsid:biodiversity.org.au:afd.taxon:2482313b-9d1e-4694-8f51-795213c8bb56",
                "species_id:urn:lsid:biodiversity.org.au:afd.taxon:2482313b-9d1e-4694-8f51-795213c8bb56",
                "collectionUid:co10",
                "institutionUid:in4 OR institutionUid:in22 OR institutionUid:in16 OR institution_uid:in6",
                "occurrenceYear:[1940-01-01T00:00:00Z%20TO%201949-12-31T00:00:00Z]",
                "occurrence_year:[1940-01-01T00:00:00Z%20TO%201949-12-31T00:00:00Z]",
                "collector:\"Copland, S J\" OR collector:\"Sadlier, R.\" OR collector:\"Mcreaddie, W\" OR collector:\"Rollo, G\" OR collector:\"Harlow, Pete\"",
                "month:09 OR month:10 OR month:11"
        };

        SpatialSearchRequestParams query = new SpatialSearchRequestParams();
        query.setFq(fqs);
        facetMap = queryFormatUtils.formatSearchQuery(query, true)[0];

        Facet sp = facetMap.get("speciesID");
        assertNotNull(sp);
        assertTrue(StringUtils.containsIgnoreCase(sp.getValue(),
                "urn:lsid:biodiversity.org.au:afd.taxon:2482313b-9d1e-4694-8f51-795213c8bb56"));
        assertTrue("got: " + sp.getDisplayName(), StringUtils.containsIgnoreCase(sp.getDisplayName(), "Species:found guid"));

        sp = facetMap.get("species_id");
        assertNotNull(sp);
        assertTrue(StringUtils.containsIgnoreCase(sp.getValue(), "urn:lsid:biodiversity.org.au:afd.taxon:2482313b-9d1e-4694-8f51-795213c8bb56"));
        assertTrue("got: " + sp.getDisplayName(), StringUtils.containsIgnoreCase(sp.getDisplayName(), "Species:found guid"));

        Facet co = facetMap.get("collectionUid");
        assertNotNull(co);
        assertTrue(StringUtils.containsIgnoreCase(co.getValue(), "co10"));
        assertTrue("got: " + co.getDisplayName(), StringUtils.containsIgnoreCase(co.getDisplayName(), "<span>Collection: found co10</span>"));

        Facet in = facetMap.get("institutionUid");
        assertNotNull(in);
        assertTrue(StringUtils.containsIgnoreCase(in.getValue(), "in4 OR institutionUid:in22 OR institutionUid:in16 OR institution_uid:in6"));
        assertTrue("got: " + in.getDisplayName(), StringUtils.containsIgnoreCase(in.getDisplayName(), "<span>Institution: found in4</span> OR <span>Institution: found in22</span> OR <span>Institution: found in16</span> OR <span>Institution: found in6</span>"));

        Facet col = facetMap.get("collector");
        assertNotNull(col);
        assertTrue("got: " + col.getValue(), StringUtils.containsIgnoreCase(col.getValue(), "Copland, S J\" OR collector:\"Sadlier, R.\" OR collector:\"Mcreaddie, W\" OR collector:\"Rollo, G\" OR collector:\"Harlow, Pete"));
        assertTrue("got: " + col.getDisplayName(), StringUtils.containsIgnoreCase(col.getDisplayName(), "Collector:\"Copland, S J\" OR Collector:\"Sadlier, R.\" OR Collector:\"Mcreaddie, W\" OR Collector:\"Rollo, G\" OR Collector:\"Harlow, Pete\""));

        Facet od = facetMap.get("occurrenceYear");
        assertNotNull(od);
        assertTrue(StringUtils.containsIgnoreCase(od.getValue(), "[1940-01-01T00:00:00Z%20TO%201949-12-31T00:00:00Z]"));
        assertTrue("got: " + od.getDisplayName(), StringUtils.containsIgnoreCase(od.getDisplayName(), "Date (by decade):[1940-1949]"));

        od = facetMap.get("occurrence_year");
        assertNotNull(od);
        assertTrue(StringUtils.containsIgnoreCase(od.getValue(), "[1940-01-01T00:00:00Z%20TO%201949-12-31T00:00:00Z]"));
        assertTrue("got: " + od.getDisplayName(), StringUtils.containsIgnoreCase(od.getDisplayName(), "Date (by decade):[1940-1949]"));


        Facet month = facetMap.get("month");
        assertNotNull(month);
        assertTrue("got: " + month.getValue(), StringUtils.containsIgnoreCase(month.getValue(), "09 OR month:10 OR month:11"));
        assertTrue("got: " + month.getDisplayName(), StringUtils.containsIgnoreCase(month.getDisplayName(), "Month:September OR Month:October OR Month:November"));
    }

    @Test
    public void testFqFormat() throws QidMissingException {

        Mockito.when(fieldMappingUtil.translateFieldName("month")).thenReturn("month");

        SpatialSearchRequestParams query = new SpatialSearchRequestParams();
        String[][] fqs = new String[][] {
                {"-month:\"09\"", "-Month:\"September\""},
                {"-(month:\"09\")", "-(Month:\"September\")"},
                {"-(month:\"09\" OR month:\"10\")", "-(Month:\"September\" OR Month:\"October\")"},
                {"month:\"09\"", "Month:\"September\""},
                {"(month:\"09\")", "(Month:\"September\")"},
                {"month:\"09\" OR month:\"10\"", "Month:\"September\" OR Month:\"October\""},
                {"(month:\"09\" OR month:\"10\")", "(Month:\"September\" OR Month:\"October\")"}
        };

        for (String[] fq : fqs) {
            query.setFq(new String[] { fq[0] });
            Map<String, Facet> facetMap = queryFormatUtils.formatSearchQuery(query, true)[0];
            assertTrue(facetMap != null && facetMap.size() == 1);
            assertTrue(facetMap.get(facetMap.keySet().iterator().next()).getDisplayName().equals(fq[1]));
        }
    }

    @Test
    public void testActiveFacetObj_validfq() throws QidMissingException {
        // test valid fqs
        List<Facet> facetList = new ArrayList<>();
        facetList.add(new Facet("month", "Month:\"August\"", "month:\"08\""));
        facetList.add(new Facet("-month", "-Month:\"August\"", "-month:\"08\""));
        facetList.add(new Facet("month", "(Month:\"August\")", "(month:\"08\")"));
        facetList.add(new Facet("-month", "(-Month:\"August\")", "(-month:\"08\")"));
        facetList.add(new Facet("-month", "-(Month:\"August\")","-(month:\"08\")"));
        facetList.add(new Facet("month", "(Month:\"August\" OR Month:\"September\")", "(month:\"08\" OR month:\"09\")"));
        facetList.add(new Facet("month", "Month:\"August\" OR Month:\"September\"", "month:\"08\" OR month:\"09\""));
        facetList.add(new Facet("-month", "-Month:\"August\" OR -Month:\"September\"", "-month:\"08\" OR -month:\"09\""));
        facetList.add(new Facet("-month", "(-Month:\"August\" OR -Month:\"September\")", "(-month:\"08\" OR -month:\"09\")"));
        facetList.add(new Facet("-month", "-(Month:\"August\" OR Month:\"September\")", "-(month:\"08\" OR month:\"09\")"));
        facetList.add(new Facet("month", "Month:\"February\"", "month:\"02\""));
        facetList.add(new Facet("-month", "-(Month:\"November\" OR Month:\"December\")", "-(month:\"11\" OR month:\"12\")"));
        facetList.add(new Facet("-month", "-(Month:\"September\" OR Month:\"October\")", "-(month:\"09\" OR month:\"10\")"));
        facetList.add(new Facet("-month", "-(Month:\"July\" OR Month:\"August\")", "-(month:\"07\" OR month:\"08\")"));

        facetList.add(new Facet("occurrence_decade_i", "Decade:\"2010\"", "occurrence_decade_i:\"2010\""));
        facetList.add(new Facet("occurrence_decade_i", "(Decade:\"2010\" OR Decade:\"2000\")", "(occurrence_decade_i:\"2010\" OR occurrence_decade_i:\"2000\")"));
        facetList.add(new Facet("occurrence_decade_i", "(Decade:\"2010\" OR Decade:\"2000\" OR Decade:\"1990\" OR Decade:\"1980\")", "(occurrence_decade_i:\"2010\" OR occurrence_decade_i:\"2000\" OR occurrence_decade_i:\"1990\" OR occurrence_decade_i:\"1980\")"));

        facetList.add(new Facet("license", "license:\"CC BY NC\" license:\"CC BY NC 4.0 (Int)\"", "license:\"CC BY NC\" license:\"CC BY NC 4.0 (Int)\""));

        Mockito.when(fieldMappingUtil.translateFieldName("month")).thenReturn("month");
        Mockito.when(fieldMappingUtil.translateFieldName("occurrence_decade_i")).thenReturn("decade");

        runFqParsingTest(facetList);
    }

    private void runFqParsingTest(List<Facet> facets) throws QidMissingException {
        SpatialSearchRequestParams query = new SpatialSearchRequestParams();

        // collect values into fq list
        query.setFq(facets.stream().map(Facet::getValue).collect(Collectors.toList()).toArray(new String[0]));
        Map<String, List<Facet>> actualResult = queryFormatUtils.formatSearchQuery(query)[1];
        List<Facet> actualList = actualResult.values().stream().flatMap(List::stream).collect(Collectors.toList());
        assertThat("List equality without order", actualList, containsInAnyOrder(facets.toArray()));
    }

    @Test
    public void testActiveFacetObj_invalidfq() throws QidMissingException {
        SpatialSearchRequestParams query = new SpatialSearchRequestParams();
        // Construct fq
        query.setFq(new String[] {null, "", " ", "month", "   month  ", "(month", "month)", "(month:\"11\"", "month\"11\"", ":\"11\"", "    :\"11\"", "(:\"11\")", "(    :\"11\")", "month:", "month:   ",  "(month:   )", "-(month:   )"});
        assertTrue(queryFormatUtils.formatSearchQuery(query)[1].isEmpty());
    }
}
