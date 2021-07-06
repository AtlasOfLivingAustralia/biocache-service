package au.org.ala.biocache.controller;

import au.org.ala.biocache.util.QueryFormatUtils;
import au.org.ala.biocache.util.SolrUtils;
import au.org.ala.biocache.web.ExploreController;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Integration tests for occurrence services.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:springTest.xml"})
@WebAppConfiguration
public class ExploreControllerIT extends TestCase {

    static {
        System.setProperty("biocache.config", System.getProperty("user.dir") + "/src/test/resources/biocache-test-config.properties");
    }

    public final int TEST_INDEX_SIZE = 1000;
    public final int DEFAULT_SEARCH_PAGE_SIZE = 10;
    public final int INDEXED_FIELD_SIZE = 21;

    @Autowired
    ExploreController exploreController;

    @Autowired
    QueryFormatUtils queryFormatUtils;

    @Autowired
    WebApplicationContext wac;

    MockMvc mockMvc;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        SolrUtils.setupIndex();
    }

    @Before
    public void setup() throws Exception {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
    }

    @Test
    public void getExploreHierarchy() throws Exception {

        this.mockMvc.perform(get("/explore/hierarchy")
                .header("user-agent", "test User-Agent")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.length()").value(10))
                .andExpect(jsonPath("$.[?(@.speciesGroup=='Birds')].taxa.length()").value(21));
    }

    @Test
    public void getExploreHierarchyGroups1() throws Exception {

        this.mockMvc.perform(get("/explore/hierarchy/groups?speciesGroup=Birds")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0].name").value("Birds"))
                .andExpect(jsonPath("$.[0].childGroups.length()").value(1))
                .andExpect(jsonPath("$.[0].childGroups.[0].count").value(1000));
    }

    @Test
    public void getExploreHierarchyGroups2() throws Exception {

        this.mockMvc.perform(get("/explore/hierarchy/groups?speciesGroup=Fungi")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    public void getExploreGroups1() throws Exception {

        this.mockMvc.perform(get("/explore/groups")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(23))
                .andExpect(jsonPath("$.[?(@.name=='Birds')].count").value(1000))
                .andExpect(jsonPath("$.[?(@.name=='Birds')].speciesCount").value(1));
    }

    @Test
    public void getExploreGroups2() throws Exception {

        this.mockMvc.perform(get("/explore/groups?q=-*:*")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.length()").value(23))
                .andExpect(jsonPath("$.[?(@.name=='Birds')].count").value(0))
                .andExpect(jsonPath("$.[?(@.name=='Birds')].speciesCount").value(0));
    }

    @Test
    public void getExploreCountsGroup1() throws Exception {
        this.mockMvc.perform(get("/explore/counts/group/Birds")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.[0]]").value(1000))
                .andExpect(jsonPath("$.[1]]").value(1));
    }

    @Test
    public void getExploreCountsGroup2() throws Exception {
        this.mockMvc.perform(get("/explore/counts/group/Birds?q=-*:*")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.[0]]").value(0))
                .andExpect(jsonPath("$.[1]]").value(0));
    }

    @Test
    public void getExploreGroupDownload1() throws Exception {
        MvcResult result = this.mockMvc.perform(get("/explore/group/Birds/download"))
                .andExpect(status().isOk()).andReturn();
        String response = result.getResponse().getContentAsString();

        // response is TSV
        assertTrue(response.length() > 0);
        assertTrue(response.split("\n").length == 2);
        assertTrue(response.split("\n",2)[1].split("\t")[5].trim().equals("\"1000\""));
    }

    @Test
    public void getExploreGroupDownload2() throws Exception {
        MvcResult result = this.mockMvc.perform(get("/explore/group/Birds/download?q=-*:*"))
                .andExpect(status().isOk()).andReturn();
        String response = result.getResponse().getContentAsString();

        assertTrue(response.length() > 0);
        assertTrue(response.split("\n").length == 1);
    }

    @Test
    public void getExploreGroup1() throws Exception {
        this.mockMvc.perform(get("/explore/group/Birds?pageSize=10&start=0"))
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$.[0].count").value(1000));
    }

    @Test
    public void getExploreGroup2() throws Exception {
        this.mockMvc.perform(get("/explore/group/Birds?q=-*:*&sort=count"))
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    public void getExploreGroup3() throws Exception {
        this.mockMvc.perform(get("/explore/group/Birds?pageSize=0&start=0&sort=count"))
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    public void getExploreGroup4() throws Exception {
        this.mockMvc.perform(get("/explore/group/Birds?pageSize=10&start=1&sort=count"))
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    public void getExploreGroup5() throws Exception {
        this.mockMvc.perform(get("/explore/group/Birds?pageSize=10&start=0&sort=taxa"))
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$.[0].count").value(1000));
    }

    @Test
    public void getExploreGroup6() throws Exception {
        this.mockMvc.perform(get("/explore/group/Birds?pageSize=10&start=0&sort=index"))
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$.[0].count").value(1000));
    }

    String wkt = "MULTIPOLYGON+(((150.1171875+-26.543080020962417,+154.86328125+-26.543080020962417,+154.86328125+-24.16053726999624,+150.1171875+-24.16053726999624,+150.1171875+-26.543080020962417)))";

    @Ignore
    @Test
    public void getExploreCountsEndemic() throws Exception {
        this.mockMvc.perform(get("/explore/counts/endemic?wkt=" + wkt)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }

    @Ignore
    @Test
    public void getExploreEndemic() throws Exception {
        this.mockMvc.perform(get("/explore/endemic/species?wkt=" + wkt)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }

    @Ignore
    @Test
    public void getExploreEndemicCSV() throws Exception {
        this.mockMvc.perform(get("/explore/endemic/species.csv?wkt=" + wkt)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }

    String qid = "1";

    @Ignore
    @Test
    public void getExploreEndemicGeneral() throws Exception {
        this.mockMvc.perform(get("/explore/endemic/species/" + qid + "?wkt=" + wkt)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }

    @Ignore
    @Test
    public void getExploreEndemicSpeciescountGeneral() throws Exception {
        this.mockMvc.perform(get("/explore/endemic/speciescount/" + qid + "?q=*:*")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }

    @Ignore
    @Test
    public void getExploreEndemicGeneralCSV() throws Exception {
        this.mockMvc.perform(get("/explore/endemic/species/" + qid + ".csv?q=*:*")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }

}