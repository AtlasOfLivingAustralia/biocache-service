package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.DownloadDoiDTO;
import au.org.ala.doi.CreateDoiRequest;
import au.org.ala.doi.CreateDoiResponse;
import au.org.ala.doi.DoiApiService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import retrofit2.Call;
import retrofit2.Response;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Test for {@link DoiService}
 */
public class DoiServiceTest {

    private DoiService doiService;
    private DoiApiService doiApiService;

    @Before
    public void setUp() throws Exception {
        doiService = new DoiService();
        doiApiService = mock(DoiApiService.class);

        // The DoiApiService is a private field that is initialised in a PostConstruct method.
        Field field = DoiService.class.getDeclaredField("doiApiService");
        field.setAccessible(true);
        field.set(doiService, doiApiService);
    }

    @Test
    /**
     * Here we are ensuring that when we make a web service call to the doi service we are
     * including the doi application metadata supplied.
     */
    public void testMintDoiIncludesTheDoiApplicationMetadata() throws Exception {
        DownloadDoiDTO downloadDoiDTO = new DownloadDoiDTO();
        Map<String, String> applicationMetadata = new HashMap<String, String>();
        applicationMetadata.put("key1", "value1");
        applicationMetadata.put("key2", "value2");
        downloadDoiDTO.setApplicationMetadata(applicationMetadata);
        downloadDoiDTO.setDatasetMetadata(new ArrayList<Map<String, String>>());

        Call<CreateDoiResponse> call = mock(Call.class);
        when(doiApiService.create(any())).thenReturn(call);

        Response<CreateDoiResponse> response = Response.success(new CreateDoiResponse());
        when(call.execute()).thenReturn(response);

        doiService.mintDoi(downloadDoiDTO);

        ArgumentCaptor<CreateDoiRequest> argument = ArgumentCaptor.forClass(CreateDoiRequest.class);
        verify(doiApiService).create(argument.capture());
        verify(call).execute();

        CreateDoiRequest request = argument.getValue();

        Map outputApplicationMetadata = request.getApplicationMetadata();
        assertTrue(outputApplicationMetadata.entrySet().containsAll(outputApplicationMetadata.entrySet()));

        // Now make sure null metadata doesn't break anything.
        downloadDoiDTO.setApplicationMetadata(null);
        reset(call);
        when(call.execute()).thenReturn(response);

        reset(doiApiService);
        when(doiApiService.create(any())).thenReturn(call);

        doiService.mintDoi(downloadDoiDTO);

        verify(doiApiService).create(any());
        verify(call).execute();

        assertTrue(outputApplicationMetadata.entrySet().size() > 0);
    }


}
