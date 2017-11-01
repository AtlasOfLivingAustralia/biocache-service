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

import au.org.ala.doi.*;
import com.google.common.net.UrlEscapers;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Component("doiService")
public class DoiService {

    private static final Logger logger = Logger.getLogger(DoiService.class);

    @Value("${doi.service.url:https://devt.ala.org.au/doi-service/api/}")
    private String doiServiceUrl;

    @Value("${doi.author:Atlas Of Living Australia}")
    private String doiAuthor;

    @Value("${doi.title:Biocache Download}")
    private String doiTitle;

    private DoiApiService doiApiService;

    @PostConstruct
    private void init() {
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(doiServiceUrl)
                .addConverterFactory(GsonConverterFactory.create())
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
        String doiStr = UrlEscapers.urlPathSegmentEscaper().escape("10.1000/3056cd6f-6ed4-4fd0-a35b-08dbc032c316");
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
            return null;
        }

    }


    /**
     * Mint a new DOI
     * @param request The metadata for the DOI
     * @return The {@link CreateDoiResponse} instance with the DOI number and additional metadata or null if not found or there was an error reported by the actual DOI service backend
     * @throws IOException If unable to connect to the DOI service backend
     */
    public CreateDoiResponse mintDoi(String applicationUrl, String query, String fileUrl) throws IOException {
        CreateDoiRequest request = new CreateDoiRequest();

        request.setAuthors(doiAuthor);
        request.setTitle(doiTitle);
        request.setApplicationUrl(applicationUrl);
        request.setDescription(query);

        request.setProvider(Provider.ANDS.name());
        request.setFileUrl(fileUrl);

        Map providerMetadata = new HashMap<String, String>();
        providerMetadata.put("authors", doiAuthor);
        providerMetadata.put("title", doiTitle);

        request.setProviderMetadata(providerMetadata);

        return mintDoi(request);
    }

}
