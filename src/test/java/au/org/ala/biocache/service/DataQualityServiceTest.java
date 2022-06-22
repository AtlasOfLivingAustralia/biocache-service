package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.FacetThemes;
import au.org.ala.biocache.dto.SearchRequestDTO;
import au.org.ala.biocache.dto.SpatialSearchRequestDTO;
import au.org.ala.dataquality.api.QualityServiceRpcApi;
import okhttp3.MediaType;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.core.SubstringMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import retrofit2.HttpException;
import retrofit2.Response;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static retrofit2.mock.Calls.failure;
import static retrofit2.mock.Calls.response;

public class DataQualityServiceTest {

    @Mock QualityServiceRpcApi qualityServiceRpcApi;

    @InjectMocks DataQualityService dataQualityService;

    AutoCloseable mocks;

    @Before
    public void setup() {
        // Every application needs to explicitly initialise static fields in
        // FacetThemes by calling its constructor 🤮
        new FacetThemes();
        mocks = MockitoAnnotations.openMocks(this);
    }

    @After
    public void tearDown() throws Exception {
        mocks.close();
    }

    @Test
    public void testDisabled() {
        // given:
        dataQualityService.dataQualityEnabled = false;
        SearchRequestDTO params = new SearchRequestDTO();
        params.setDisableAllQualityFilters(false);
        params.setQualityProfile("default");
        params.setDisableQualityFilter(Collections.emptyList());
        //when
        Map<String, String> result = dataQualityService.getEnabledFiltersByLabel(params);
        //then
        verify(qualityServiceRpcApi, never()).getEnabledFiltersByLabel(any());
        assertThat("Result should be empty when data quality disabled", result.isEmpty());
    }

    @Test
    public void testEnabledBlankProfileIsDisabled() {
        // given a null profile:
        dataQualityService.dataQualityEnabled = true;
        SearchRequestDTO params = new SearchRequestDTO();
        params.setDisableAllQualityFilters(false);
        params.setQualityProfile(null);
        params.setDisableQualityFilter(Collections.emptyList());
        //when
        Map<String, String> result = dataQualityService.getEnabledFiltersByLabel(params);
        //then
        verify(qualityServiceRpcApi, never()).getEnabledFiltersByLabel(any());
        assertThat("Result should be empty when no profile provided", result.isEmpty());

        // given a blank profile:
        dataQualityService.dataQualityEnabled = true;
        params = new SearchRequestDTO();
        params.setDisableAllQualityFilters(false);
        params.setQualityProfile("");
        params.setDisableQualityFilter(Collections.emptyList());
        //when
        result = dataQualityService.getEnabledFiltersByLabel(params);
        //then
        verify(qualityServiceRpcApi, never()).getEnabledFiltersByLabel(any());
        assertThat("Result should be empty when no profile provided", result.isEmpty());

        // given a profile name:
        dataQualityService.dataQualityEnabled = true;
        params = new SearchRequestDTO();
        params.setDisableAllQualityFilters(false);
        params.setQualityProfile("default");
        params.setDisableQualityFilter(Collections.emptyList());
        Map<String,String> response = new LinkedHashMap<>();
        response.put("first", "foo:bar -baz:qux");
        response.put("second", "qux:baz -bar:foo");
        when(qualityServiceRpcApi.getEnabledFiltersByLabel("default")).thenReturn(response(response));
        //when
        result = dataQualityService.getEnabledFiltersByLabel(params);
        //then
        verify(qualityServiceRpcApi).getEnabledFiltersByLabel("default");
        assertThat("Result should contain filters", result.containsKey("first") && result.containsKey("second"));
    }

    @Test
    public void testGetEnabledFiltersByLabel() {
        ///
        /// given disable all quality filters is set:
        ///
        dataQualityService.dataQualityEnabled = true;
        SearchRequestDTO params = new SearchRequestDTO();
        params.setDisableAllQualityFilters(true);
        params.setQualityProfile("profile");
        params.setDisableQualityFilter(Collections.emptyList());
        //when
        Map<String, String> result = dataQualityService.getEnabledFiltersByLabel(params);
        //then
        verify(qualityServiceRpcApi, never()).getEnabledFiltersByLabel("profile");
        assertThat("Result should be empty", result.isEmpty());
    }

    @Test
    public void testGetEnabledFiltersByLabelWithDisabledFilter() {
        ///
        /// given a profile is provided and some filters are disabled:
        ///
        dataQualityService.dataQualityEnabled = true;
        SearchRequestDTO params = new SearchRequestDTO();
        params.setDisableAllQualityFilters(false);
        params.setQualityProfile("profile");
        params.setDisableQualityFilter(newArrayList("first"));
        Map<String,String> responseValue = new LinkedHashMap<>();
        responseValue.put("first", "foo:bar -baz:qux");
        responseValue.put("second", "qux:baz -bar:foo");

        when(qualityServiceRpcApi.getEnabledFiltersByLabel("profile")).thenReturn(response(responseValue));
        //when
        Map<String, String> result = dataQualityService.getEnabledFiltersByLabel(params);
        //then
        verify(qualityServiceRpcApi).getEnabledFiltersByLabel("profile");
        assertThat("Result should contain only enabled filters", result, allOf(
                hasEntry("second", "qux:baz -bar:foo"),
                not(hasKey("first"))
        ));
    }

    @Test
    public void testGetEnabledFiltersByLabelErrorResponses() {
        dataQualityService.dataQualityEnabled = true;

        ///
        /// IOException
        ///

        when(qualityServiceRpcApi.getEnabledFiltersByLabel("profile")).thenReturn(failure(new IOException()));

        RuntimeException e = assertThrows("Should throw a RuntimeException wrapping the IO Exception", RuntimeException.class, () ->
            dataQualityService.getEnabledFiltersByLabel("profile")
        );

        assertThat("Cause should be an IOException", e.getCause(), isA(IOException.class));

        ///
        /// HTTP error response
        ///

        when(qualityServiceRpcApi.getEnabledFiltersByLabel("profile")).thenReturn(response(Response.error(500, ResponseBody.create(MediaType.parse("application/json"), "{\"error\":true}"))));

        HttpException httpException = assertThrows("Should throw an HttpException on a non successful response", HttpException.class, () ->
                dataQualityService.getEnabledFiltersByLabel("profile")
        );

        assertThat("HttpException should be code 500", httpException.code(), equalTo(500));
    }

    @Test
    public void testConvertDataQualityParameters() {
        dataQualityService.dataQualityEnabled = true;

        Map<String, String> filters = newLinkedHashMap();
        filters.put("first", "foo:bar");
        filters.put("second", "-baz:qux");

        String searchUrl = "https://example.org/something/else/?q=*%3A*&fq=month%3A%2207%22&qualityProfile=test&disableQualityFilter=third&disableQualityFilter=fourth&disableQualityFilters=false";

        String result = dataQualityService.convertDataQualityParameters(searchUrl, filters);
        assertThat("fqs added", result, allOf(
                Matchers.startsWith("https://example.org/something/else/?"),
                containsString("q=*%3A*"),
                containsString("disableAllQualityFilters=true"),
                containsString("fq=foo%3Abar"),
                containsString("fq=-baz%3Aqux"),
                containsString("fq=month%3A%2207%22"),
                not(containsString("qualityProfile=")),
                not(containsString("disableQualityFilter="))
        ));



        searchUrl = "https://example.org/something/else/?q=*%3A*&fq=month%3A%2207%22&fq=hello:kitty&qualityProfile=test&disableQualityFilters=true";

        result = dataQualityService.convertDataQualityParameters(searchUrl, filters);
        assertThat("fqs added", result, allOf(
                containsString("q=*%3A*"),
                containsString("disableAllQualityFilters=true"),
                containsString("fq=foo%3Abar"),
                containsString("fq=-baz%3Aqux"),
                containsString("fq=month%3A%2207%22"),
                containsString("fq=hello:kitty"),
                not(containsString("qualityProfile=")),
                not(containsString("disableQualityFilter="))
        ));
    }

    @Test
    public void testConvertDataQualityParametersWithPlusAsSpace() {
        dataQualityService.dataQualityEnabled = true;

        Map<String, String> filters = newLinkedHashMap();
        filters.put("first", "foo:bar AND a:b +c:d -d:e");
        filters.put("second", "-baz:qux");

        String searchUrl = "https://example.org/something/else/?q=*%3A*&fq=month%3A%2207%22+year%3A2020&qualityProfile=test&disableQualityFilter=third&disableQualityFilter=fourth&disableQualityFilters=false";

        String result = dataQualityService.convertDataQualityParameters(searchUrl, filters);
        assertThat("fqs added", result, allOf(
                Matchers.startsWith("https://example.org/something/else/?"),
                containsString("q=*%3A*"),
                containsString("disableAllQualityFilters=true"),
                containsString("fq=foo%3Abar%20AND%20a%3Ab%20%2Bc%3Ad%20-d%3Ae"), // spaces in existing query params are converted to %20s
                containsString("fq=-baz%3Aqux"),
                containsString("fq=month%3A%2207%22%20year%3A2020"),
                not(containsString("qualityProfile=")),
                not(containsString("disableQualityFilter="))
        ));
    }

    @Test
    public void testConvertDataQualityParametersFiltersAlreadyPresent() {
        dataQualityService.dataQualityEnabled = true;

        Map<String, String> filters = newLinkedHashMap();
        filters.put("first", "foo:bar");
        filters.put("second", "-baz:qux");

        String searchUrl = "https://example.org/something/else/?q=*%3A*&fq=month%3A%2207%22&fq=foo%3Abar&fq=-baz%3Aqux&disableAllQualityFilters=true";

        String result = dataQualityService.convertDataQualityParameters(searchUrl, filters);
        assertThat("fqs added", result, allOf(
                Matchers.startsWith("https://example.org/something/else/?"),
                containsStringOnce("q=*%3A*"),
                containsStringOnce("disableAllQualityFilters=true"),
                containsStringOnce("fq=foo%3Abar"),
                containsStringOnce("fq=-baz%3Aqux"),
                containsStringOnce("fq=month%3A%2207%22"),
                not(containsString("qualityProfile=")),
                not(containsString("disableQualityFilter="))
        ));

        searchUrl = "https://example.org/something/else/?q=*%3A*&fq=month%3A%2207%22&fq=foo%3Abar&fq=-baz%3Aqux&qualityProfile=test&disableQualityFilter=first";
        result = dataQualityService.convertDataQualityParameters(searchUrl, filters);
        assertThat("fqs added", result, allOf(
                Matchers.startsWith("https://example.org/something/else/?"),
                containsStringOnce("q=*%3A*"),
                containsStringOnce("disableAllQualityFilters=true"),
                containsStringOnce("fq=foo%3Abar"),
                containsStringOnce("fq=-baz%3Aqux"),
                containsStringOnce("fq=month%3A%2207%22"),
                not(containsString("qualityProfile=")),
                not(containsString("disableQualityFilter="))
        ));
    }

    @Test
    public void testConvertDataQualityParametersFiltersWhenDqFiltersAreDisabled() {
        dataQualityService.dataQualityEnabled = false;

        Map<String, String> filters = newLinkedHashMap();

        String searchUrl = "https://example.org/something/else/?q=*%3A*&fq=month%3A%2207%22&fq=foo%3Abar&fq=-baz%3Aqux";

        String result = dataQualityService.convertDataQualityParameters(searchUrl, filters);
        assertThat("search URL is untouched", result, equalTo(searchUrl));
    }

    @Test
    public void testConvertDataQualityParametersFiltersWhenDqFiltersAreDisabledInRequest() {
        dataQualityService.dataQualityEnabled = false;

        Map<String, String> filters = newLinkedHashMap();

        String searchUrl = "https://example.org/something/else/?q=*%3A*&fq=month%3A%2207%22&fq=foo%3Abar&fq=-baz%3Aqux&disableAllQualityFilters=true";

        String result = dataQualityService.convertDataQualityParameters(searchUrl, filters);
        assertThat("search URL is untouched", result, equalTo(searchUrl));
    }

    @Test
    public void testConvertDataQualityParametersFiltersWhenDqFiltersAreEmpty() {
        dataQualityService.dataQualityEnabled = true;

        Map<String, String> filters = newLinkedHashMap();

        String searchUrl = "https://example.org/something/else/?q=*%3A*&fq=month%3A%2207%22&fq=foo%3Abar&fq=-baz%3Aqux&qualityProfile=test";

        String result = dataQualityService.convertDataQualityParameters(searchUrl, filters);
        assertThat("searchUrl is the same with dq filters disabled", result, allOf(
                Matchers.startsWith("https://example.org/something/else/?"),
                containsStringOnce("q=*%3A*"),
                containsStringOnce("disableAllQualityFilters=true"),
                containsStringOnce("fq=foo%3Abar"),
                containsStringOnce("fq=-baz%3Aqux"),
                not(containsString("qualityProfile=")),
                not(containsString("disableQualityFilter="))
        ));
    }

    @Test
    public void testConvertDataQualityParametersFiltersWhenNoProfilePresent() {
        dataQualityService.dataQualityEnabled = true;

        Map<String, String> filters = newLinkedHashMap();

        String searchUrl = "https://example.org/something/else/?q=*%3A*&fq=month%3A%2207%22&fq=foo%3Abar&fq=-baz%3Aqux";

        String result = dataQualityService.convertDataQualityParameters(searchUrl, filters);
        assertThat("searchUrl is the same with dq filters disabled", result, equalTo(searchUrl));
    }

    @Test
    public void testGenerateCombinedFqsWithDisableAll() {
        dataQualityService.dataQualityEnabled = true;

        // setup filters response
        Map<String, String> responseValue = new LinkedHashMap<>();
        responseValue.put("first", "foo:bar -baz:qux");
        responseValue.put("second", "qux:baz -bar:foo");

        when(qualityServiceRpcApi.getEnabledFiltersByLabel("profile")).then((invocation) -> response(responseValue));

        // given a request with disable all quality filters
        SpatialSearchRequestDTO ssrp = new SpatialSearchRequestDTO();
        ssrp.setFq(new String[]{"a:b", "d:e"});
        ssrp.setDisableAllQualityFilters(true);
        ssrp.setQualityProfile("profile");

        // when
        String[] fqs = dataQualityService.generateCombinedFqs(ssrp);

        // then dq filters aren't applied
        verify(qualityServiceRpcApi, times(0)).getEnabledFiltersByLabel("profile");
        assertThat(fqs, equalTo(new String[]{"a:b", "d:e"}));
    }

    public void testGenerateCombinedFqsWithAProfile() {
        dataQualityService.dataQualityEnabled = true;

        // setup filters response
        Map<String, String> responseValue = new LinkedHashMap<>();
        responseValue.put("first", "foo:bar -baz:qux");
        responseValue.put("second", "qux:baz -bar:foo");

        when(qualityServiceRpcApi.getEnabledFiltersByLabel("profile")).then((invocation) -> response(responseValue));

        // given a request with a quality profile
        SpatialSearchRequestDTO ssrp = new SpatialSearchRequestDTO();
        ssrp.setFq(new String[]{"a:b", "d:e"});
        ssrp.setDisableAllQualityFilters(false);
        ssrp.setQualityProfile("profile");

        // when
        String[] fqs = dataQualityService.generateCombinedFqs(ssrp);

        // then
        verify(qualityServiceRpcApi).getEnabledFiltersByLabel("profile");
        assertThat(fqs, arrayContainingInAnyOrder("a:b", "d:e", "foo:bar -baz:qux", "qux:baz -bar:foo"));
    }
    @Test
    public void testGenerateCombinedFqsWithAProfileWithDisableFilterList() {
        dataQualityService.dataQualityEnabled = true;

        // setup filters response
        Map<String, String> responseValue = new LinkedHashMap<>();
        responseValue.put("first", "foo:bar -baz:qux");
        responseValue.put("second", "qux:baz -bar:foo");

        when(qualityServiceRpcApi.getEnabledFiltersByLabel("profile")).then((invocation) -> response(responseValue));

        // given a request with a quality profile and some disabled filters
        SpatialSearchRequestDTO ssrp = new SpatialSearchRequestDTO();
        ssrp.setFq(new String[]{ "a:b", "d:e" });
        ssrp.setDisableAllQualityFilters(false);
        ssrp.setQualityProfile("profile");
        ssrp.setDisableQualityFilter(newArrayList("first"));

        // when
        String[] fqs = dataQualityService.generateCombinedFqs(ssrp);

        verify(qualityServiceRpcApi).getEnabledFiltersByLabel("profile");
        assertThat(fqs, arrayContainingInAnyOrder("a:b", "d:e", "qux:baz -bar:foo"));
    }

    @Test
    public void testGenerateCombinedFqsWithDqDisabled() {
        dataQualityService.dataQualityEnabled = false;
        SpatialSearchRequestDTO ssrp = new SpatialSearchRequestDTO();
        ssrp.setFq(new String[]{ "a:b", "d:e" });
        ssrp.setQualityProfile("profile");

        String[] fqs = dataQualityService.generateCombinedFqs(ssrp);

        verify(qualityServiceRpcApi, times(0)).getEnabledFiltersByLabel(any());

        assertThat(fqs, arrayContainingInAnyOrder("a:b", "d:e"));

        ssrp.setFq(new String[]{ });

        fqs = dataQualityService.generateCombinedFqs(ssrp);

        verify(qualityServiceRpcApi, times(0)).getEnabledFiltersByLabel(any());

        assertThat(fqs, emptyArray());

        ssrp.setFq(null);

        fqs = dataQualityService.generateCombinedFqs(ssrp);

        verify(qualityServiceRpcApi, times(0)).getEnabledFiltersByLabel(any());

        assertThat(fqs, emptyArray());

    }

    public static Matcher<String> containsStringOnce(String substring) {
        return new StringContainsExactlyOnce(substring);
    }


    /**
     * Tests if the argument is a string that contains a substring exactly once.
     */
    public static class StringContainsExactlyOnce extends SubstringMatcher {
        public StringContainsExactlyOnce(String substring) {
            super("containing only once", false, substring);
        }

        @Override
        protected boolean evalSubstringOf(String s) {
            return StringUtils.countMatches(s, substring) == 1;
        }
    }
}
