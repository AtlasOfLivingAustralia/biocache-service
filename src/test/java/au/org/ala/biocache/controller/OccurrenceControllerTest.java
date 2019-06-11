package au.org.ala.biocache.controller;

import au.org.ala.biocache.web.OccurrenceController;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.net.URL;

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
    WebApplicationContext wac;

    @Autowired
    OccurrenceController occurrenceController;

    MockMvc mockMvc;

    @Before
    public void setup() throws Exception {

        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
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
}
