package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.SearchRequestParams;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.dataquality.api.QualityServiceRpcApi;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;
import retrofit2.Call;
import retrofit2.HttpException;
import retrofit2.Response;

import javax.annotation.CheckForNull;
import javax.inject.Inject;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.newLinkedHashSet;
import static java.util.stream.Collectors.toList;

@Component("dataQualityService")
public class DataQualityService {
    private static final Logger logger = Logger.getLogger(DataQualityService.class);
    public static final String DISABLE_ALL_QUALITY_FILTERS_PARAM_NAME = "disableAllQualityFilters";
    public static final String QUALITY_PROFILE_PARAM_NAME = "qualityProfile";
    public static final String DISABLE_QUALITY_FILTER_PARAM_NAME = "disableQualityFilter";

    @Inject
    private QualityServiceRpcApi qualityServiceRpcApi;

    /**
     * Whether data quality filters are enabled
     */
    @Value("${dataquality.enabled:false}")
    @VisibleForTesting
    protected boolean dataQualityEnabled;

    /**
     * Get all enabled filters by label for the search params.  This method will take into account whether
     * the request explicitly disables quality filters or disables individual filters.
     *
     * @param searchRequestParams The search request params
     * @return The enabled filters for this request
     * @throws HttpException if an http error code is returned from the service
     * @throws RuntimeException if a network error occurs
     */
    public Map<String, String> getEnabledFiltersByLabel(SearchRequestParams searchRequestParams) {
        if (searchRequestParams.isDisableAllQualityFilters()) {
            return new LinkedHashMap<>();
        }

        Map<String, String> filtersByLabel = getEnabledFiltersByLabel(searchRequestParams.getQualityProfile());
        filtersByLabel.keySet().removeAll(searchRequestParams.getDisableQualityFilter());
        return filtersByLabel;
    }

    /**
     * Gets the enabled filters for by label for a given quality profile.
     * @param qualityProfile The quality profile to use, may be blank for the default profile.
     * @return The Map of filter labels to filters.
     * @throws HttpException if an http error code is returned from the service
     * @throws RuntimeException if a network error occurs
     */
    public Map<String, String> getEnabledFiltersByLabel(String qualityProfile) {
        if (!dataQualityEnabled) {
            return new LinkedHashMap<>();
        }
        if (StringUtils.isBlank(qualityProfile)) {
            return new LinkedHashMap<>();
        }

        Call<Map<String, String>> enabledFiltersByLabelCall = qualityServiceRpcApi.getEnabledFiltersByLabel(qualityProfile);
        return responseValueOrThrow(enabledFiltersByLabelCall);
    }

    private <T> T responseValueOrThrow(Call<T> call) {
        Response<T> response = null;
        try {
            response = call.execute();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (response.isSuccessful()) {
            return response.body();
        } else {
            throw new HttpException(response);
        }
    }

    /**
     * Convert a URL with data quality filter parameters to one with the equivalent fqs applied and all data
     * quality filters disabled.  This is to support the offline downloads.
     *
     * This creates a URL that allows a user's exact search to be repeated even if the underlying data quality filters
     * change.
     *
     * No sanity checking is done to ensure the caller's searchURL and the enabledQualityFiltersByLabel use the same
     * quality filters.
     *
     * @param searchUrl The searchUrl provided
     * @param enabledQualityFiltersByLabel The data quality filters to apply
     * @return The searchUrl with any data quality paramters fixed.
     */
    public String convertDataQualityParameters(String searchUrl, Map<String, String> enabledQualityFiltersByLabel) {

        // If DQ filters aren't enabled we can skip the rest of this method.
        if (!dataQualityEnabled) {
            return searchUrl;
        }

        URI uri;
        try {
            uri = new URI(searchUrl);
        } catch (URISyntaxException e) {
            logger.warn("Search URL " + searchUrl + " can't be parsed, dq filters won't be fixed in DOI!");
            return searchUrl;
        }
        UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUri(uri);
        // convert query param + with %20 to make UriComponentsBuilder happy
        uriBuilder.replaceQuery(uri.getRawQuery().replace("+","%20"));
        MultiValueMap<String, String> params = uriBuilder.build(true).getQueryParams();
        if (Boolean.parseBoolean(params.getFirst(DISABLE_ALL_QUALITY_FILTERS_PARAM_NAME))) {
            // data quality filters are already disabled so we can skip this step.
            return searchUrl;
        }

        /** If no profile was provided then this is effectively disabled */
        String profile = params.getFirst(QUALITY_PROFILE_PARAM_NAME);
        if (StringUtils.isBlank(profile)) {
            return searchUrl;
        }

        uriBuilder.replaceQueryParam(QUALITY_PROFILE_PARAM_NAME);
        uriBuilder.replaceQueryParam(DISABLE_QUALITY_FILTER_PARAM_NAME);

        uriBuilder.queryParam(DISABLE_ALL_QUALITY_FILTERS_PARAM_NAME, "true");
        Set<String> fqs;
        if (params.containsKey("fq")) {
            fqs = newLinkedHashSet(params.get("fq"));
        } else {
            fqs = newLinkedHashSet();
        }
        fqs.addAll(enabledQualityFiltersByLabel.values().stream().map(DataQualityService::encode).collect(toList()));
        uriBuilder.replaceQueryParam("fq", fqs.toArray());

        return uriBuilder.build(true).encode().toUriString();
    }

    /**
     * Encode a param value and convert the checked exception that should never be thrown to an unchecked exception
     * @param paramValue The value to encode
     * @return The encoded value
     */
    private static String encode(String paramValue) {
        try {
            // Theoretically we should use this as it makes only the
            // minimal encoding necessary
//            return UriUtils.encodeQueryParam(paramValue, "UTF-8");
            // but to match the client encoding as it's implemented now (2020-08-14)
            // we use URLEncoder but replace "+" with "%20" to make Spring UriComponentsBuilder happy.
            return URLEncoder.encode(paramValue, "UTF-8").replace("+","%20");
        } catch (UnsupportedEncodingException e) { throw new RuntimeException(e); }// UTF-8 always exists
    }

    /**
     * Combine the fqs from a request with the fqs from the data quality profile, if any, for the QidCacheDAOImpl service
     * @param requestParams The params
     * @return The combined fqs
     */
    public String[] generateCombinedFqs(SpatialSearchRequestParams requestParams) {
        String[] fqs = requestParams.getFq();
        if (fqs == null) {
            fqs = new String[0];
        }
        int fqsLength = fqs.length;
        Collection<String> qualityFilters = this.getEnabledFiltersByLabel(requestParams).values();

        fqs = Arrays.copyOf(fqs, fqsLength + qualityFilters.size());
        System.arraycopy(qualityFilters.toArray(new String[0]), 0, fqs, fqsLength, qualityFilters.size());
        return fqs;
    }
}
