package au.org.ala.biocache.controller;

import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.service.DownloadService;
import au.org.ala.biocache.service.LoggerService;
import au.org.ala.biocache.util.QueryFormatUtils;
import au.org.ala.biocache.util.SolrUtils;
import au.org.ala.biocache.web.OccurrenceController;
import junit.framework.TestCase;
import org.ala.client.model.LogEventVO;
import org.apache.solr.common.SolrException;
import org.junit.Before;
import org.junit.BeforeClass;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration tests for occurrence services.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:springTest.xml"})
@WebAppConfiguration
public class OccurrenceControllerIT extends TestCase {

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

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        SolrUtils.setupIndex();
    }

    @Before
    public void setup() throws Exception {
        loggerService = mock(LoggerService.class);
        searchDAO = mock(SearchDAO.class);
        Validator validator = mock(Validator.class);

        ReflectionTestUtils.setField(occurrenceController, "loggerService", loggerService);
        ReflectionTestUtils.setField(downloadService, "loggerService", loggerService);
        ReflectionTestUtils.setField(occurrenceController, "rateLimitCount", 5);
        ReflectionTestUtils.setField(occurrenceController, "validator", validator);

        when(validator.supports(any())).thenReturn(true);

        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
    }

    @Test
    public void getRecordTest() throws Exception {

        ReflectionTestUtils.setField(occurrenceController, "occurrenceLogEnabled", true);

        this.mockMvc.perform(get("/occurrence/41fcf3f2-fa7b-4ba6-a88c-4ac5240c8aab")
                .header("user-agent", "test User-Agent")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.raw.rowKey").value("41fcf3f2-fa7b-4ba6-a88c-4ac5240c8aab"));

        ArgumentCaptor<LogEventVO> argument = ArgumentCaptor.forClass(LogEventVO.class);
        verify(loggerService).logEvent(argument.capture());

        LogEventVO logEventVO = argument.getValue();
        assertEquals(logEventVO.getUserAgent(), "test User-Agent");
    }

    @Test
    public void getRecordTestWithoutLogging() throws Exception {

        ReflectionTestUtils.setField(occurrenceController, "occurrenceLogEnabled", false);

        this.mockMvc.perform(get("/occurrence/41fcf3f2-fa7b-4ba6-a88c-4ac5240c8aab")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.raw.rowKey").value("41fcf3f2-fa7b-4ba6-a88c-4ac5240c8aab"));

        // log event should never be called
        verify(loggerService, never()).logEvent(any());
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

        //FIXME
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

    @Test
    public void downloadValidEmailTest() throws Exception {

        this.mockMvc.perform(get("/occurrences/download*")
                .header("user-agent", "test User-Agent")
                .param("reasonTypeId", "10")
                .param("email", "test@test.com"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/zip"));
    }

    @Test
    public void downloadInvalidEmailTest() throws Exception {

        // need to set rate limit count to 0 to cause failure on first attempt
        ReflectionTestUtils.setField(occurrenceController, "rateLimitCount", 0);

        this.mockMvc.perform(get("/occurrences/download*")
                .param("reasonTypeId", "10")
                .param("email", ""))
                .andExpect(status().is(HttpServletResponse.SC_FORBIDDEN));

        this.mockMvc.perform(get("/occurrences/download*")
                .param("reasonTypeId", "10"))
                .andExpect(status().is(HttpServletResponse.SC_FORBIDDEN));

        this.mockMvc.perform(get("/occurrences/download*")
                .param("reasonTypeId", "10")
                .param("email", "test"))
                .andExpect(status().is(HttpServletResponse.SC_FORBIDDEN));
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
        String[] acceptTypes = new String[]{
                "application/json"
        };

        for (String type : acceptTypes) {
            MvcResult mvcResult = this.mockMvc.perform(get("/index/maxBooleanClauses").header("Accept", type))
                    .andExpect(status().is(expectedError))
                    .andExpect(jsonPath("message").exists())
                    .andExpect(jsonPath("errorType").exists()).andReturn();

            assert (mvcResult.getResponse().getContentType().contains(MediaType.APPLICATION_JSON_VALUE));
        }
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

    // below we test '/' and '/oldapi' controller which returns a view name

    // Accept */*       -> normal controller -> returns text/html when succeed
    // Accept text/html -> normal controller -> returns text/html when succeed
    @Test
    public void testNormalControllerCompatibleFormat() throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        String[] acceptTypes = new String[]{"text/html", "*/*"};
        String[][] urls = new String[][]{{"/", "/WEB-INF/jsp/homePage.jsp"}, {"/oldapi", "/WEB-INF/jsp/oldapi.jsp"}};

        for (String[] url : urls) {
            for (String type : acceptTypes) {
                MvcResult mvcResult = this.mockMvc.perform(get(url[0]).header("Accept", type))
                        .andExpect(status().is(HttpServletResponse.SC_OK))
                        .andReturn();

                assert (mvcResult.getResponse().getForwardedUrl().equals(url[1]));
            }
        }
        restoreSearchDAO(searchDAOOrig);
    }

    // Accept application/json -> normal controller -> returns JSON (because contentNegotiatingViewResolvers maps model to json in this case)
    @Test
    public void testNormalControllerNONCompatibleFormat() throws Exception {
        Object searchDAOOrig = backupSearchDAO();
        String[] acceptTypes = new String[]{"application/json"};
        String[] urls = new String[]{"/", "/oldapi"};
        for (String url : urls) {
            for (String type : acceptTypes) {
                MvcResult mvcResult =
                        this.mockMvc.perform(get(url).header("Accept", type)).andExpect(status().is(HttpServletResponse.SC_OK)).andReturn();
                assert (mvcResult.getResponse().getContentType().contains(MediaType.APPLICATION_JSON_VALUE));
            }
        }
        restoreSearchDAO(searchDAOOrig);
    }
}