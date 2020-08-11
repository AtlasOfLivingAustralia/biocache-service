package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.FacetThemes;
import au.org.ala.biocache.dto.SearchRequestParams;
import au.org.ala.dataquality.api.QualityServiceRpcApi;
import com.google.common.collect.Lists;
import okhttp3.MediaType;
import okhttp3.ResponseBody;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import retrofit2.HttpException;
import retrofit2.Response;
import retrofit2.mock.Calls;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
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
        // FacetThemes by calling its constructor
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
        dataQualityService.dataQualityBlankProfileIsDefault = false;
        SearchRequestParams params = new SearchRequestParams();
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
        dataQualityService.dataQualityBlankProfileIsDefault = false;
        SearchRequestParams params = new SearchRequestParams();
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
        dataQualityService.dataQualityBlankProfileIsDefault = false;
        params = new SearchRequestParams();
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
        dataQualityService.dataQualityBlankProfileIsDefault = false;
        params = new SearchRequestParams();
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
    public void testEnabledBlankProfileIsDefault() {
        // given a profile name:
        dataQualityService.dataQualityEnabled = true;
        dataQualityService.dataQualityBlankProfileIsDefault = true;
        SearchRequestParams params = new SearchRequestParams();
        params.setDisableAllQualityFilters(false);
        params.setQualityProfile(null);
        params.setDisableQualityFilter(Collections.emptyList());
        Map<String,String> response = new LinkedHashMap<>();
        response.put("first", "foo:bar -baz:qux");
        response.put("second", "qux:baz -bar:foo");
        when(qualityServiceRpcApi.getEnabledFiltersByLabel(null)).thenReturn(response(response));
        //when
        Map<String, String> result = dataQualityService.getEnabledFiltersByLabel(params);
        //then
        verify(qualityServiceRpcApi).getEnabledFiltersByLabel(null);
        assertThat("Result should contain filters", result.containsKey("first") && result.containsKey("second"));
    }

    @Test
    public void testGetEnabledFiltersByLabel() {
        ///
        /// given disable all quality filters is set:
        ///
        dataQualityService.dataQualityEnabled = true;
        dataQualityService.dataQualityBlankProfileIsDefault = false;
        SearchRequestParams params = new SearchRequestParams();
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
        dataQualityService.dataQualityBlankProfileIsDefault = false;
        SearchRequestParams params = new SearchRequestParams();
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
        dataQualityService.dataQualityBlankProfileIsDefault = false;

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
}
