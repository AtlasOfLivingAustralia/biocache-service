/*
 * Copyright (C) 2014 Atlas of Living Australia
 * All Rights Reserved.
 *
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 */
package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.DownloadDoiDTO;
import au.org.ala.doi.*;
import com.google.common.net.UrlEscapers;
import okhttp3.*;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component("doiService")
public class DoiService {

    private static final Logger logger = Logger.getLogger(DoiService.class);
    public static final String DISPLAY_TEMPLATE = "biocache";

    @Value("${doi.service.url:https://devt.ala.org.au/doi-service/api/}")
    private String doiServiceUrl;

    @Value("${doi.service.apiKey:Provide a valid key}")
    private String doiServiceApiKey;

    // By default wait 15 min before hearing any response from the server
    // (Useful for long file uploads)
    @Value("${doi.service.readTimeout:900000}")
    private long doiServiceReadTimeout;

    @Value("${doi.service.writeTimeout:30000}")
    private long doiServiceWriteTimeout;

    @Value("${doi.author:Atlas Of Living Australia}")
    private String doiAuthor;

    @Value("${doi.description:ALA occurrence record download}")
    private String doiDescription;

    @Value("${doi.resourceText:Species information}")
    private String doiResourceText;

    private DoiApiService doiApiService;

    @PostConstruct
    public void init() {

        OkHttpClient.Builder httpClient = new OkHttpClient.Builder();
        httpClient.addInterceptor(new Interceptor() {
            @Override
            public okhttp3.Response intercept(Interceptor.Chain chain) throws IOException {
                Request original = chain.request();

                Request request = original.newBuilder()
                        .header("apiKey", doiServiceApiKey)
                        .method(original.method(), original.body())
                        .build();

                return chain.proceed(request);
            }
        });

        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(doiServiceUrl)
                .addConverterFactory(GsonConverterFactory.create())
                .client(
                        httpClient.writeTimeout(doiServiceWriteTimeout, TimeUnit.MILLISECONDS)
                        .readTimeout(doiServiceReadTimeout, TimeUnit.MILLISECONDS).build())
                .build();

        doiApiService = retrofit.create(DoiApiService.class);

    }

    /**
     * Get the {@link Doi} instance for the given doi number
     * @param doi The Doi number
     * @return The {@link Doi} instance or null if not found or there was an error reported by the actual DOI service backend
     * @throws IOException If unable to connect to the DOI service backend
     */
    public Doi getDoi(String doi) throws IOException {
        String doiStr = UrlEscapers.urlPathSegmentEscaper().escape(doi);
        Call<Doi> doiCall = doiApiService.getEncoded(doiStr);
        Response<Doi> response = doiCall.execute();

        if(response.isSuccessful()) {
            return response.body();
        } else {
            logger.error("Error while getting doi " +doi + ":" + response.errorBody().string());
            return null;
        }
    }

    /**
     * Mint a new DOI
     * @param request The metadata for the DOI
     * @return The {@link CreateDoiResponse} instance with the DOI number and additional metadata or null if not found or there was an error reported by the actual DOI service backend
     * @throws IOException If unable to connect to the DOI service backend
     */
    public CreateDoiResponse mintDoi(CreateDoiRequest request) throws IOException {
        request.setProvider(Provider.ANDS.name());

        Response<CreateDoiResponse> response = doiApiService.create(request).execute();

        if(response.isSuccessful()) {
            return response.body();
        } else {
            logger.error("Error creating DOI for request " + request + ":" + response.errorBody().string());
            throw new RuntimeException("Unable to mint DOI for " + request.getApplicationUrl());
        }
    }

    /**
     * Mint a new DOI
     *
     * @param downloadInfo
     * @throws IOException If unable to connect to the DOI service backend
     */
    public CreateDoiResponse mintDoi(DownloadDoiDTO downloadInfo) throws IOException {
        CreateDoiRequest request = new CreateDoiRequest();

        request.setAuthors(doiAuthor);
        request.setTitle(downloadInfo.getTitle());
        request.setApplicationUrl(downloadInfo.getApplicationUrl());
        request.setDescription(doiDescription);
        request.setLicence(downloadInfo.getLicence());
        request.setUserId(downloadInfo.getRequesterId());
        request.setAuthorisedRoles(downloadInfo.getAuthorisedRoles());

        request.setProvider(Provider.ANDS.name());
        request.setFileUrl(downloadInfo.getFileUrl());
        request.setDisplayTemplate(DISPLAY_TEMPLATE);


        Map<String, Object> providerMetadata = generateProviderMetadataPayload(downloadInfo);

        request.setProviderMetadata(providerMetadata);

        Map<String, Object> applicationMetadata = generateApplicationMetadataPayload(downloadInfo);
        request.setApplicationMetadata(applicationMetadata);

        request.setActive(false);
        return mintDoi(request);
    }

    private Map<String, Object> generateApplicationMetadataPayload(DownloadDoiDTO downloadInfo) {
        Map<String, Object> applicationMetadata = new HashMap<>();
        if (downloadInfo.getApplicationMetadata() != null) {
            applicationMetadata.putAll(downloadInfo.getApplicationMetadata());
        }
        applicationMetadata.put("searchUrl", downloadInfo.getApplicationUrl());
        applicationMetadata.put("datasets", downloadInfo.getDatasetMetadata());
        applicationMetadata.put("requestedOn", downloadInfo.getRequestTime());
        applicationMetadata.put("recordCount", Long.toString(downloadInfo.getRecordCount()));
        applicationMetadata.put("queryTitle", downloadInfo.getQueryTitle());
        return applicationMetadata;
    }

    private Map<String, Object> generateProviderMetadataPayload(DownloadDoiDTO downloadInfo) {
        Map<String, Object> providerMetadata = new HashMap<>();

        List<String> authorsList = new ArrayList<>();
        authorsList.add(doiAuthor);

        providerMetadata.put("authors", authorsList);
        providerMetadata.put("publisher", doiAuthor);
        providerMetadata.put("title", downloadInfo.getTitle());

        providerMetadata.put("resourceType", "Text");
        providerMetadata.put("resourceText", doiResourceText);

        List<Map<String, Object>> contributorsList = new ArrayList<>();
        Map<String, Object> contributorMap = new HashMap<>();
        contributorMap.put("name", downloadInfo.getRequesterName());
        contributorMap.put("type", "Distributor");

        contributorsList.add(contributorMap);
        providerMetadata.put("contributors", contributorsList);

        List<Map<String, Object>> descriptionsList = new ArrayList<>();
        Map<String, Object> descriptionMap = new HashMap<>();
        descriptionMap.put("text", doiDescription);
        descriptionMap.put("type", "Other");

        descriptionsList.add(descriptionMap);
        providerMetadata.put("descriptions", descriptionsList);

        List<Map<String, String>> creators = new ArrayList<>();

        for(Map<String, String> datasetProvider: downloadInfo.getDatasetMetadata()) {
            Map<String, String> creator = new HashMap<>();
            creator.put("name", datasetProvider.get("name"));
            creator.put("type", "Producer" );
            creators.add(creator);
        }

        providerMetadata.put("creator", creators);

        return providerMetadata;
    }

    public Doi updateFile(String id, String fileUrl) throws IOException {
        UpdateDoiRequest updateRequest = new UpdateDoiRequest();

        updateRequest.setFileUrl(fileUrl);
        updateRequest.setActive(true);

        //pass it like this
        File file = new File(fileUrl);
        RequestBody requestFile =
                RequestBody.create(MediaType.parse("multipart/form-data"), file);

        // MultipartBody.Part is used to send also the actual file name
        MultipartBody.Part body =
                MultipartBody.Part.createFormData("file", file.getName(), requestFile);

        Response<Doi> updateResponse = doiApiService.updateMultipart(id, body, updateRequest).execute();

        if(updateResponse.isSuccessful()) {
            return updateResponse.body();
        } else {
            logger.error("Error updating DOI for id " + id + ":" + updateResponse.errorBody().string());
            throw new RuntimeException("Unable to update file for DOI uuid: " + id +", with URL: " + fileUrl);
        }

    }

}
