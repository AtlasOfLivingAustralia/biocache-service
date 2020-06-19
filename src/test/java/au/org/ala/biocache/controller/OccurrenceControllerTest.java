package au.org.ala.biocache.controller;

import au.org.ala.biocache.service.DownloadService;
import au.org.ala.biocache.service.LoggerService;
import junit.framework.TestCase;
import org.ala.client.model.LogEventVO;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

/**
 * Integration tests for occurrence services.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:springTest.xml"})
@WebAppConfiguration
public class OccurrenceControllerTest extends TestCase {

    static {
        System.setProperty("biocache.config", System.getProperty("user.dir") + "/src/test/resources/biocache-test-config.properties");
    }

    public final int TEST_INDEX_SIZE = 1000;
    public final int DEFAULT_SEARCH_PAGE_SIZE = 10;
    public final int INDEXED_FIELD_SIZE = 377;

    @Autowired
    DownloadService downloadService;

    LoggerService loggerService;

    @Autowired
    WebApplicationContext wac;

    MockMvc mockMvc;

    @Before
    public void setup() {

        loggerService = mock(LoggerService.class);
        ReflectionTestUtils.setField(downloadService, "loggerService", loggerService);

        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
    }

    @Test
    public void getRecordTest() throws Exception {
        this.mockMvc.perform(get("/occurrence/41fcf3f2-fa7b-4ba6-a88c-4ac5240c8aab")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.raw.rowKey").value("41fcf3f2-fa7b-4ba6-a88c-4ac5240c8aab"));
    }

    @Test
    public void getRecordAssertionsTest() throws Exception {
        this.mockMvc.perform(get("/occurrences/41fcf3f2-fa7b-4ba6-a88c-4ac5240c8aab/assertions")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    public void allRecordsSearchTest() throws Exception {
        this.mockMvc.perform(get("/occurrences/search")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalRecords").value(TEST_INDEX_SIZE))
                .andExpect(jsonPath("$.occurrences.length()").value(DEFAULT_SEARCH_PAGE_SIZE));
    }

    @Test
    public void getIndexFieldsTest() throws Exception {
        this.mockMvc.perform(get("/index/fields")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(INDEXED_FIELD_SIZE));
    }

    @Test
    public void batchSearchTest() throws Exception {
        this.mockMvc.perform(post("/occurrences/batchSearch")
                .param("action", "Search")
                .param("queries", "Circus")
                .param("field", "taxon_name")
                .param("separator", "title")
                .param("redirectBase", "http://localhost:8080/biocache-service/occurrences/search")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is3xxRedirection());
    }

    @Test
    public void emptyTaxaCountTest() throws Exception {
        this.mockMvc.perform(post("/occurrences/taxaCount")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }

    @Test
    public void taxaCountTest() throws Exception {


        MvcResult result = this.mockMvc.perform(post("/occurrences/taxaCount")
                .param("guids", "urn:lsid:biodiversity.org.au:afd.taxon:75801261-975f-436f-b1c7-d395a06dc067")
                .contentType(MediaType.APPLICATION_JSON)).andReturn();

        System.out.println("TAXACOUNT:" + result.getResponse().getContentAsString());

        this.mockMvc.perform(post("/occurrences/taxaCount")
                .param("guids", "urn:lsid:biodiversity.org.au:afd.taxon:75801261-975f-436f-b1c7-d395a06dc067")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$['urn:lsid:biodiversity.org.au:afd.taxon:75801261-975f-436f-b1c7-d395a06dc067']").value(1000));
    }

    @Test
    public void downloadTest() throws Exception {

        this.mockMvc.perform(get("/occurrences/download*")
                .header("user-agent", "test User-Agent")
                .param("reasonTypeId", "10"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/zip"));

        ArgumentCaptor<LogEventVO> argument = ArgumentCaptor.forClass(LogEventVO.class);
        verify(loggerService).logEvent(argument.capture());

        LogEventVO logEventVO = argument.getValue();
        assertEquals(logEventVO.getUserAgent(), "test User-Agent");
    }
}