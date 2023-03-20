package au.org.ala.biocache.controller;

import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.IndexFieldDTO;
import au.org.ala.biocache.util.QueryFormatUtils;
import au.org.ala.biocache.util.SolrUtils;
import au.org.ala.biocache.util.solr.FieldMappingUtil;
import au.org.ala.biocache.web.ScatterplotController;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.HashSet;
import java.util.Set;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Integration tests for scatterplot controller.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:springTest.xml"})
@WebAppConfiguration
@TestPropertySource(locations = "classpath:biocache-test-config.properties")
public class ScatterplotControllerIT extends TestCase {

    static {
        System.setProperty("biocache.config", System.getProperty("user.dir") + "/src/test/resources/biocache-test-config.properties");
    }

    @Autowired
    ScatterplotController scatterplotController;

    @Autowired
    SearchDAO searchDAO;

    @Autowired
    QueryFormatUtils queryFormatUtils;

    @Autowired
    FieldMappingUtil fieldMappingUtil;

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
    public void createScatterplot() throws Exception {

        this.mockMvc.perform(get("/scatterplot?x=decimalLongitude&y=decimalLatitude"))
                .andExpect(status().isOk());
    }

    @Test
    public void intersectScatterplot() throws Exception {

        this.mockMvc.perform(get("/scatterplot/point?x=decimalLongitude&y=decimalLatitude&pointx1=100&pointy1=100&pointx2=110&pointy2=110")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.xaxis").value("decimalLongitude"))
                .andExpect(jsonPath("$.yaxis").value("decimalLatitude"))

                // TODO: Find out why travis and local env give different values for x/yaxis_range
//                .andExpect(jsonPath("$.xaxis_range[0]").value(lessThan(123.0)))
//                .andExpect(jsonPath("$.xaxis_range[1]").value(greaterThan(123.0)))
//                .andExpect(jsonPath("$.yaxis_range[0]").value(lessThan(-24.0)))
//                .andExpect(jsonPath("$.yaxis_range[1]").value(greaterThan(-24.0)))

                .andExpect(jsonPath("$.xaxis_pixel_selection[0]").value(100))
                .andExpect(jsonPath("$.xaxis_pixel_selection[1]").value(110))
                .andExpect(jsonPath("$.xaxis_pixel_selection[0]").value(100))
                .andExpect(jsonPath("$.xaxis_pixel_selection[1]").value(110));
    }

    @Test
    public void fieldValidation() throws Exception {
        Set<IndexFieldDTO> list = new HashSet<>();

        IndexFieldDTO field = new IndexFieldDTO();
        field.setDataType("string");
        field.setDescription("description");
        field.setName("invalidType");
        list.add(field);

        field = new IndexFieldDTO();
        field.setDataType("float");
        field.setName("invalidProperties");
        field.setDescription("description");
        field.setDocvalue(false);
        field.setStored(false);
        list.add(field);

        field = new IndexFieldDTO();
        field.setDataType("float");
        field.setName("valid");
        field.setDescription("validField");
        field.setDocvalue(true);
        field.setStored(false);
        list.add(field);

        Exception exception = null;
        try {
            scatterplotController.getFieldDescription("invalidType", list);
        } catch (Exception e) {
            exception = e;
        }
        assert (exception != null);

        exception = null;
        try {
            scatterplotController.getFieldDescription("invalidName", list);
        } catch (Exception e) {
            exception = e;
        }
        assert (exception != null);

        exception = null;
        try {
            scatterplotController.getFieldDescription("invalidProperties", list);
        } catch (Exception e) {
            exception = e;
        }
        assert (exception != null);

        assert ("validField".equals(scatterplotController.getFieldDescription("valid", list)));
    }
}
