package au.org.ala.biocache.controller;

import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.service.DownloadService;
import au.org.ala.biocache.service.LoggerService;
import au.org.ala.biocache.util.QueryFormatUtils;
import au.org.ala.biocache.web.OccurrenceController;
import junit.framework.TestCase;
import org.ala.client.model.LogEventVO;
import org.apache.solr.common.SolrException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.TransactionTimedOutException;
import org.springframework.validation.Validator;
import org.springframework.web.context.WebApplicationContext;

import javax.servlet.http.HttpServletResponse;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.SolrException.ErrorCode.FORBIDDEN;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration tests for occurrence services.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:springTest.xml"})
@WebAppConfiguration
public class OccurrenceControllerMockedDAOTestIT extends TestCase {

    static {
        System.setProperty("biocache.config", System.getProperty("user.dir") + "/src/test/resources/biocache-test-config.properties");
    }

    public final int TEST_INDEX_SIZE = 1000;
    public final int DEFAULT_SEARCH_PAGE_SIZE = 10;
    public final int INDEXED_FIELD_SIZE = 435;

    @Autowired
    OccurrenceController occurrenceController;

    @Autowired
    QueryFormatUtils queryFormatUtils;

    @Autowired
    DownloadService downloadService;

    LoggerService loggerService;
    SearchDAO searchDAO;

    @Autowired
    WebApplicationContext wac;


    MockMvc mockMvc;
//
//    @BeforeClass
//    public static void setupBeforeClass() throws Exception {
//        SolrUtils.setupIndex();
//    }

    @Before
    public void setup() throws Exception {
        loggerService = mock(LoggerService.class);
        searchDAO = mock(SearchDAO.class);
        Validator validator = mock(Validator.class);

        ReflectionTestUtils.setField(occurrenceController, "loggerService", loggerService);
        ReflectionTestUtils.setField(downloadService, "loggerService", loggerService);
        ReflectionTestUtils.setField(occurrenceController, "rateLimitCount", 5);
//        ReflectionTestUtils.setField(occurrenceController, "validator", validator);

        when(validator.supports(any())).thenReturn(true);

        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
    }

    private Object backupSearchDAO() {
        Object searchDAOOrig = ReflectionTestUtils.getField(occurrenceController, "searchDAO");
        ReflectionTestUtils.setField(occurrenceController, "searchDAO", searchDAO);
        return searchDAOOrig;
    }

    private void restoreSearchDAO(Object object) {
        ReflectionTestUtils.setField(occurrenceController, "searchDAO", object);
    }

    // Accept */* -> REST controller -> returns json when succeed
    // Accept application/json -> REST controller -> returns json when succeed
    @Test
    public void testRESTControllerCompatibleFormat() throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        when(this.searchDAO.getMaxBooleanClauses()).thenReturn((int) 1234);
        String[] acceptTypes = new String[]{
                "application/json",
                "*/*"};
        for (String type : acceptTypes) {
            MvcResult mvcResult = this.mockMvc.perform(get("/index/maxBooleanClauses").header("Accept", type))
                    .andExpect(status().is(HttpServletResponse.SC_OK))
                    .andExpect(jsonPath("maxBooleanClauses").value(1234))
                    .andReturn();

            assert (mvcResult.getResponse().getContentType().contains(MediaType.APPLICATION_JSON_VALUE));
        }
        restoreSearchDAO(searchDAOOrig);
    }

    // Accept text/html -> REST controller -> returns 406 Not Acceptable (not compatible header), contentType = text/html
    @Test
    public void testRESTControllerNONCompatibleFormat() throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        when(searchDAO.getMaxBooleanClauses()).thenReturn((int) 1234);
        String[] acceptTypes = new String[]{
                "text/plain",
                "text/html, text/plain"};
        for (String type : acceptTypes) {
            this.mockMvc.perform(get("/index/maxBooleanClauses").header("Accept", type))
                    .andExpect(status().is(HttpServletResponse.SC_NOT_ACCEPTABLE));

            // no content type set so we can't test it
            //assert(mvnResult.getResponse().getContentType().contains(MediaType.TEXT_HTML_VALUE));
        }
        restoreSearchDAO(searchDAOOrig);
    }

    private void validateErrorPageReturned() throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        String[] acceptTypes = new String[]{
                "*/*",
                "text/html"
        };

        for (String type : acceptTypes) {
            MvcResult mvcResult = this.mockMvc.perform(get("/index/maxBooleanClauses").header("Accept", type))
                    .andExpect(status().is(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)).andReturn();
            assert (mvcResult.getResponse().getForwardedUrl().equals("/WEB-INF/jsp/error/dataAccessFailure.jsp"));
        }
        restoreSearchDAO(searchDAOOrig);
    }

    // Accept */*       -> REST controller -> returns error page in case of an DataAccessException thrown
    // Accept text/html -> REST controller -> returns error page in case of an DataAccessException thrown
    @Test
    public void testRESTControllerHTMLFormatDataAccessException() throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        when(searchDAO.getMaxBooleanClauses()).thenThrow(new QueryTimeoutException("test-QueryTimeoutException"));
        validateErrorPageReturned();
        restoreSearchDAO(searchDAOOrig);
    }

    // Accept */*       -> REST controller -> returns error page in case of an TransactionException thrown
    // Accept text/html -> REST controller -> returns error page in case of an TransactionException thrown
    @Test
    public void testRESTControllerHTMLFormatTransactionException() throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        when(searchDAO.getMaxBooleanClauses()).thenThrow(new TransactionTimedOutException("test-TransactionTimedOutException"));
        validateErrorPageReturned();
        restoreSearchDAO(searchDAOOrig);
    }

    private void validateJSONErrorReturned(int expectedError) throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        MvcResult mvcResult = this.mockMvc.perform(get("/index/maxBooleanClauses")
                        .header("Accept", "application/json")
                )
                .andExpect(status().is(expectedError))
                .andExpect(jsonPath("message").exists())
                .andExpect(jsonPath("errorType").exists()).andReturn();

        assert (mvcResult.getResponse().getContentType().contains(MediaType.APPLICATION_JSON_VALUE));
        restoreSearchDAO(searchDAOOrig);
    }

    // Accept application/json -> REST controller -> returns JSON with status 500 in case of an DataAccessException thrown
    @Test
    public void testRESTControllerJSONFormatDataAccessException() throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        when(searchDAO.getMaxBooleanClauses()).thenThrow(new QueryTimeoutException("test-QueryTimeoutException"));
        validateJSONErrorReturned(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        restoreSearchDAO(searchDAOOrig);
    }

    // Accept application/json -> REST controller -> returns JSON with status 500 in case of an TransactionException thrown
    @Test
    public void testRESTControllerJSONFormatTransactionException() throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        when(searchDAO.getMaxBooleanClauses()).thenThrow(new TransactionTimedOutException("test-TransactionTimedOutException"));
        validateJSONErrorReturned(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        restoreSearchDAO(searchDAOOrig);
    }

    // Accept application/json -> REST controller -> returns JSON with status 400 in case of an SolrException (bad request) thrown
    @Test
    public void testRESTControllerJSONFormatSolrException400() throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        when(searchDAO.getMaxBooleanClauses()).thenThrow(new SolrException(BAD_REQUEST, "test-SolrException"));
        validateJSONErrorReturned(HttpServletResponse.SC_BAD_REQUEST);
        restoreSearchDAO(searchDAOOrig);
    }

    // Accept application/json -> REST controller -> returns JSON with status 400 in case of an SolrException (bad request) thrown
    @Test
    public void testRESTControllerJSONFormatSolrException403() throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        when(searchDAO.getMaxBooleanClauses()).thenThrow(new SolrException(FORBIDDEN, "test-SolrException"));
        validateJSONErrorReturned(HttpServletResponse.SC_FORBIDDEN);
        restoreSearchDAO(searchDAOOrig);
    }
}