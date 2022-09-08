package au.org.ala.doi.service;

import au.org.ala.doi.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import com.google.common.net.UrlEscapers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

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
                .baseUrl("https://devt.ala.org.au/doi-service/api/")
//                .baseUrl("https://doi-test.ala.org.au/api/")
                .addConverterFactory(GsonConverterFactory.create())
                .client(httpClient.writeTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS).build())
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

        final String author = "ALA";
        createRequest.setAuthors(author);
        createRequest.setTitle("Full Integration Test");
        createRequest.setApplicationUrl("https://devt.ala.org.au/ala-hub/");
        final String description = "Excercising DOI Service API";
        createRequest.setDescription(description);
        List<String> licence = Lists.asList("Licence 1", new String[]{"Licence 2"});
        createRequest.setLicence(licence);
        createRequest.setUserId("UserId");

        Set<String> authorisedRoles = Sets.newHashSet("ROLE 1", "ROLE 2");
        createRequest.setAuthorisedRoles(authorisedRoles);

        createRequest.setProvider("DATACITE");
//        createRequest.setFileUrl("https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/homepage-channel-image-lionfish.jpg");

        Map providerMetadata = new HashMap<String, String>();

        List<String> authorsList = new ArrayList<>();
        authorsList.add(author);

        providerMetadata.put("authors", authorsList);
        providerMetadata.put("title", "Integration Test");
        providerMetadata.put("resourceType", "Text");
        providerMetadata.put("resourceText", "Species information");
        providerMetadata.put("publisher", "Atlas Of Living Australia");

        List<Map> descriptionsList = new ArrayList<>();
        Map <String, String> descriptionMap = new HashMap<>();
        descriptionMap.put("text", description);
        descriptionMap.put("type", "Other");

        descriptionsList.add(descriptionMap);
        providerMetadata.put("descriptions", descriptionsList);


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
        assertEquals(Lists.asList("Licence 1", new String[]{"Licence 2"}), updateDoi.getLicence());
        assertEquals("UserId", updateDoi.getUserId());

        System.out.println(updateDoi);
    }
}
