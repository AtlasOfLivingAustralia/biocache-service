package au.org.ala.doi.service;

import au.org.ala.doi.*;
import com.google.common.net.UrlEscapers;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;

import okhttp3.Request;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DoiApiTest {

    DoiApiService doiApiService;

    @Before
    public void setup(){

        OkHttpClient.Builder httpClient = new OkHttpClient.Builder();
        httpClient.addInterceptor(new Interceptor() {
            @Override
            public okhttp3.Response intercept(Interceptor.Chain chain) throws IOException {
                Request original = chain.request();

                Request request = original.newBuilder()
                        .header("apiKey", "INSERT YOUR API MEY HERE")
                        .method(original.method(), original.body())
                        .build();

                return chain.proceed(request);
            }
        });

        Retrofit retrofit = new Retrofit.Builder()
//                .baseUrl("https://devt.ala.org.au/doi-service/api/")
                .baseUrl("https://doi-test.ala.org.au/api/")
                .addConverterFactory(GsonConverterFactory.create())
                .client(httpClient.build())
                .build();

        doiApiService = retrofit.create(DoiApiService.class);
    }

    @Test
    @Ignore
    public void testGetDoi() throws IOException {

        String doiStr = UrlEscapers.urlPathSegmentEscaper().escape("INSERT AN EXISTING DOI HERE");
        Call<Doi> doiCall = doiApiService.getEncoded(doiStr);
        Response<Doi> doiResponse = doiCall.execute();

        assertTrue(doiResponse.isSuccessful());
        assertNotNull(doiResponse.body());
        assertNotNull(doiResponse.body().getDoi());

//        System.out.println(doiResponse.body());
    }

    @Test
    @Ignore
    public void mintAndUpdateDoi() throws IOException {

        CreateDoiRequest createRequest = new CreateDoiRequest();

        createRequest.setAuthors("ALA");
        createRequest.setTitle("Full Integration Test");
        createRequest.setApplicationUrl("https://devt.ala.org.au/ala-hub/");
        createRequest.setDescription("Excercising DOI Service API");
        createRequest.setLicence("Licence");
        createRequest.setUserId("UserId");

        createRequest.setProvider(Provider.ANDS.name());
//        createRequest.setFileUrl("https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/homepage-channel-image-lionfish.jpg");

        Map providerMetadata = new HashMap<String, String>();
        providerMetadata.put("authors", "ALA");
        providerMetadata.put("title", "Integration Test");

        createRequest.setProviderMetadata(providerMetadata);

        Response<CreateDoiResponse> createResponse = doiApiService.create(createRequest).execute();

        assertTrue(createResponse.isSuccessful());
        assertNotNull(createResponse.body());
        assertNotNull(createResponse.body().getDoi());
        String uuid = createResponse.body().getUuid();
        assertNotNull(uuid);

        Call<Doi> doiCall = doiApiService.get(uuid);
        Response<Doi> doiResponse = doiCall.execute();

        assertTrue(doiResponse.isSuccessful());
        Doi doi = doiResponse.body();
        assertNotNull(doi);
        assertNotNull(doi.getDoi());

        UpdateDoiRequest updateRequest = new UpdateDoiRequest();

        updateRequest.setFileUrl("https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/homepage-channel-image-lionfish.jpg");

        Response<Doi> updateResponse = doiApiService.update(uuid, updateRequest).execute();
        assertTrue(updateResponse.isSuccessful());
        Doi updateDoi = updateResponse.body();
        assertNotNull(updateDoi);
        assertNotNull(doi.getDoi());

        assertEquals("ALA", updateDoi.getAuthors());
        assertEquals("Full Integration Test", updateDoi.getTitle());
        assertEquals("https://devt.ala.org.au/ala-hub/", updateDoi.getApplicationUrl());
        assertEquals("Excercising DOI Service API", updateDoi.getDescription());
        assertEquals("Licence", updateDoi.getLicence());
        assertEquals("UserId", updateDoi.getUserId());

        System.out.println(updateDoi);
    }
}
