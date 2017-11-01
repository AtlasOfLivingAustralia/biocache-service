package au.org.ala.doi.service;

import au.org.ala.doi.*;
import com.google.common.net.UrlEscapers;
import org.junit.Before;
import org.junit.Test;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DoiApiTest {

    DoiApiService doiApiService;

    @Before
    public void setup(){
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl("https://devt.ala.org.au/doi-service/api/")
                .addConverterFactory(GsonConverterFactory.create())
                .build();

        doiApiService = retrofit.create(DoiApiService.class);
    }

    @Test
//    @Ignore
    public void testGetDoi() throws IOException {

        String doiStr = UrlEscapers.urlPathSegmentEscaper().escape("10.1000/3056cd6f-6ed4-4fd0-a35b-08dbc032c316");
        Call<Doi> doiCall = doiApiService.getEncoded(doiStr);
        Response<Doi> doiResponse = doiCall.execute();

        assertTrue(doiResponse.isSuccessful());
        assertNotNull(doiResponse.body());
        assertNotNull(doiResponse.body().getDoi());

//        System.out.println(doiResponse.body());
    }

    @Test
    public void mintDoi() throws IOException {


        CreateDoiRequest request = new CreateDoiRequest();

        request.setAuthors("ALA");
        request.setTitle("Integration Test");
        request.setApplicationUrl("https://devt.ala.org.au/ala-hub/");
        request.setDescription("Excercising DOI Service API");

        request.setProvider(Provider.ANDS.name());
        request.setFileUrl("https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/homepage-channel-image-lionfish.jpg");

        Map providerMetadata = new HashMap<String, String>();
        providerMetadata.put("authors", "ALA");
        providerMetadata.put("title", "Integration Test");

        request.setProviderMetadata(providerMetadata);

         Response<CreateDoiResponse> response = doiApiService.create(request).execute();

        assertTrue(response.isSuccessful());
        assertNotNull(response.body());
        assertNotNull(response.body().getDoi());

        System.out.println(response.body());
    }
}
