package au.org.ala.biocache.controller;

import au.org.ala.biocache.web.WMSOSGridController;
import org.junit.Before;
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

import javax.servlet.http.HttpServletResponse;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:springTest.xml"})
@WebAppConfiguration
public class WMSOSGridControllerTest {
    static {
        System.setProperty("biocache.config", System.getProperty("user.dir") + "/src/test/resources/biocache-test-config.properties");
    }

    @Autowired
    WMSOSGridController wmsosGridController;

    @Autowired
    WebApplicationContext wac;
    MockMvc mockMvc;

    @Before
    public void setup() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
    }

    // test normal controller handle "text/html", "*/*" in case of exception
    @Test
    public void testNormalControllerHTMLException() throws Exception {
        String[] acceptTypes = new String[] { "text/html", "*/*" };
        String[][] urls = new String[][] {{"/osgrid/wms/reflect", "/WEB-INF/jsp/error/general.jsp"}};

        for (String[] url : urls) {
            for (String type : acceptTypes) {
                // this request generates an exception
                MvcResult mvcResult = this.mockMvc.perform(get(url[0]).header("Accept", type)
                        .param("BBOX", "0,0,100,100"))
                        .andExpect(status().is(HttpServletResponse.SC_INTERNAL_SERVER_ERROR))
                        .andReturn();
                assert (mvcResult.getResponse().getForwardedUrl().equals(url[1]));
            }
        }
    }

    // test normal controller handle "application/json" in case of exception
    @Test
    public void testNormalControllerJSONException() throws Exception {
        String acceptType = "application/json";
        String url = "/osgrid/wms/reflect";

        // this request generates an exception
        MvcResult mvcResult = this.mockMvc.perform(get(url).header("Accept", acceptType)
                .param("BBOX", "0,0,100,100"))
                .andExpect(status().is(HttpServletResponse.SC_INTERNAL_SERVER_ERROR))
                .andExpect(jsonPath("errorType").exists()).andReturn();

        assert (mvcResult.getResponse().getContentType().contains(MediaType.APPLICATION_JSON_VALUE));
    }
}
