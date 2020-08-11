package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.SearchRequestParams;
import au.org.ala.dataquality.api.QualityServiceRpcApi;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import retrofit2.Call;
import retrofit2.HttpException;
import retrofit2.Response;

import javax.inject.Inject;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

@Component("dataQualityService")
public class DataQualityService {
    private static final Logger logger = Logger.getLogger(DataQualityService.class);

    @Inject
    private QualityServiceRpcApi qualityServiceRpcApi;

    /**
     * Whether data quality filters are enabled
     */
    @Value("${dataquality.enabled:false}")
    @VisibleForTesting
    protected boolean dataQualityEnabled;

    /**
     * Whether an empty data quality profile parameter uses the default profile (true) or no data quality filters (false)
     */
    @Value("${dataquality.blankProfileIsDefault:false}")
    @VisibleForTesting
    protected boolean dataQualityBlankProfileIsDefault;

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
        if (!dataQualityBlankProfileIsDefault && StringUtils.isBlank(qualityProfile)) {
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
}
