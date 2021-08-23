package au.org.ala.biocache.controller;

import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.util.QidMissingException;
import au.org.ala.biocache.util.SolrUtils;
import au.org.ala.biocache.web.WMSOSGridController;
import junit.framework.TestCase;
import org.apache.solr.common.SolrException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
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

import javax.servlet.http.HttpServletResponse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:springTest.xml"})
@WebAppConfiguration
public class WMSOSGridControllerIT extends TestCase {
    static {
        System.setProperty("biocache.config", System.getProperty("user.dir") + "/src/test/resources/biocache-test-config.properties");
    }

    @Autowired
    WMSOSGridController wmsosGridController;

    SearchDAO searchDAO;

    @Autowired
    WebApplicationContext wac;
    MockMvc mockMvc;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        SolrUtils.setupIndex();
    }

    @Before
    public void setup() throws Exception {
        searchDAO = mock(SearchDAO.class);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
    }

    private Object backupSearchDAO() {
        Object searchDAOOrig = ReflectionTestUtils.getField(wmsosGridController, "searchDAO");
        ReflectionTestUtils.setField(wmsosGridController, "searchDAO", searchDAO);
        return searchDAOOrig;
    }

    private void restoreSearchDAO(Object object) {
        ReflectionTestUtils.setField(wmsosGridController, "searchDAO", object);
    }

    // test normal controller handle "text/html", "*/*" in case of QidMissingException exception
    @Test
    public void testNormalControllerHTMLQidMissingException() throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        String[] acceptTypes = new String[] { "text/html", "*/*" };
        String[][] urls = new String[][] {{"/osgrid/wms/reflect", "/WEB-INF/jsp/error/general.jsp"}};

        when(this.searchDAO.findByFulltextSpatialQuery(Mockito.any(), Mockito.anyBoolean(), Mockito.anyMap())).thenThrow(new QidMissingException("qidmissing"));
        for (String[] url : urls) {
            for (String type : acceptTypes) {
                // this request generates an exception
                MvcResult mvcResult = this.mockMvc.perform(get(url[0]).header("Accept", type)
                        .param("BBOX", "0,0,100,100"))
                        .andExpect(status().is(HttpServletResponse.SC_BAD_REQUEST))
                        .andReturn();
                assert (mvcResult.getResponse().getForwardedUrl().equals(url[1]));
            }
        }
        restoreSearchDAO(searchDAOOrig);
    }

    // test normal controller handle "text/html", "*/*" in case of SolrException exception
    @Test
    public void testNormalControllerHTMLSolrException() throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        String[] acceptTypes = new String[] { "text/html", "*/*" };
        String[][] urls = new String[][] {{"/osgrid/wms/reflect", "/WEB-INF/jsp/error/general.jsp"}};

        when(this.searchDAO.findByFulltextSpatialQuery(Mockito.any(), Mockito.anyBoolean(), Mockito.anyMap())).thenThrow(new SolrException(SolrException.ErrorCode.NOT_FOUND, "qidmissing"));
        for (String[] url : urls) {
            for (String type : acceptTypes) {
                // this request generates an exception
                MvcResult mvcResult = this.mockMvc.perform(get(url[0]).header("Accept", type)
                        .param("BBOX", "0,0,100,100"))
                        .andExpect(status().is(HttpServletResponse.SC_NOT_FOUND))
                        .andReturn();
                assert (mvcResult.getResponse().getForwardedUrl().equals(url[1]));
            }
        }
        restoreSearchDAO(searchDAOOrig);
    }

    // test normal controller handle "application/json" in case of QidMissingException exception
    @Test
    public void testNormalControllerJSONQidMissingException() throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        String acceptType = "application/json";
        String url = "/osgrid/wms/reflect";
        when(this.searchDAO.findByFulltextSpatialQuery(Mockito.any(), Mockito.anyBoolean(), Mockito.anyMap())).thenThrow(new QidMissingException("qidmissing"));

        // this request generates an exception
        MvcResult mvcResult = this.mockMvc.perform(get(url).header("Accept", acceptType)
                .param("BBOX", "0,0,100,100"))
                .andExpect(status().is(HttpServletResponse.SC_BAD_REQUEST))
                .andExpect(jsonPath("errorType").exists()).andReturn();

        assert (mvcResult.getResponse().getContentType().contains(MediaType.APPLICATION_JSON_VALUE));
        restoreSearchDAO(searchDAOOrig);
    }

    // test normal controller handle "application/json" in case of SolrException exception
    @Test
    public void testNormalControllerJSONSolrException() throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        String acceptType = "application/json";
        String url = "/osgrid/wms/reflect";
        when(this.searchDAO.findByFulltextSpatialQuery(Mockito.any(), Mockito.anyBoolean(), Mockito.anyMap())).thenThrow(new SolrException(SolrException.ErrorCode.SERVER_ERROR, "qidmissing"));

        // this request generates an exception
        MvcResult mvcResult = this.mockMvc.perform(get(url).header("Accept", acceptType)
                .param("BBOX", "0,0,100,100"))
                .andExpect(status().is(HttpServletResponse.SC_INTERNAL_SERVER_ERROR))
                .andExpect(jsonPath("errorType").exists()).andReturn();

        assert (mvcResult.getResponse().getContentType().contains(MediaType.APPLICATION_JSON_VALUE));
        restoreSearchDAO(searchDAOOrig);
    }
}
