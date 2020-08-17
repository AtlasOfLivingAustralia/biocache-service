/**
 * 
 */
package au.org.ala.biocache.service;

import au.org.ala.biocache.dao.JsonPersistentQueueDAOImpl;
import au.org.ala.biocache.dao.PersistentQueueDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dao.SearchDAOImpl;
import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import au.org.ala.biocache.util.thread.DownloadCreator;
import au.org.ala.doi.CreateDoiResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharSource;
import com.google.common.io.Files;
import org.ala.client.model.LogEventVO;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.context.support.ReloadableResourceBundleMessageSource;
import org.springframework.core.io.ClassPathResource;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newConcurrentMap;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

/**
 * Test for {@link DownloadService}
 * 
 * @author Peter Ansell
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Files.class)
public class DownloadServiceTest {

    @Rule
    public Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private Path testCacheDir;
    private Path testDownloadDir;

    private PersistentQueueDAO persistentQueueDAO;

    private DownloadService testService;

    /**
     * This latch is used to reliably simulate stalled and successful downloads.
     */
    private CountDownLatch testLatch;

    @Before
    public void setUp() throws Exception {
        testCacheDir = tempDir.newFolder("downloadcontrolthreadtest-cache").toPath();
        testDownloadDir = tempDir.newFolder("downloadcontrolthreadtest-destination").toPath();
        persistentQueueDAO = new JsonPersistentQueueDAOImpl() {
            @Override
            public void init() {
                cacheDirectory = testCacheDir.toAbsolutePath().toString();
                biocacheDownloadDir = testDownloadDir.toAbsolutePath().toString();
                super.init();
            }
        };
        persistentQueueDAO.init();

        // Every application needs to explicitly initialise static fields in
        // FacetThemes by calling its constructor
        new FacetThemes();

        SearchDAOImpl searchDAO = new SearchDAOImpl();

        testLatch = new CountDownLatch(1);

        testService = new DownloadService() {
            {
                sensitiveAccessRoles = "{}";
                concurrentDownloadsJSON = "[]";
            }

            protected DownloadCreator getNewDownloadCreator() {
                return new DownloadCreator() {
                    @Override
                    public Callable<DownloadDetailsDTO> createCallable(final DownloadDetailsDTO nextDownload,
                            final long executionDelay, final Semaphore capacitySemaphore,
                            final ExecutorService parallelQueryExecutor) {
                        return new Callable<DownloadDetailsDTO>() {
                            @Override
                            public DownloadDetailsDTO call() throws Exception {
                                try {
                                    // Reliably test the sequence by waiting
                                    // here
                                    // The latch must be already at 0 before
                                    // test to avoid a
                                    // wait here


                                    testLatch.await();
                                    Thread.sleep(executionDelay + Math.round(Math.random() * executionDelay));
                                    return nextDownload;
                                } finally {
                                    capacitySemaphore.release();
                                }
                            }
                        };
                    }
                };
            }
        };
        testService.dataQualityService = mock(DataQualityService.class);
        testService.downloadQualityFiltersTemplate = new ClassPathResource("download-email-quality-filter-snippet.html");
        testService.biocacheDownloadDir = testDownloadDir.toAbsolutePath().toString();
        testService.persistentQueueDAO = persistentQueueDAO;
        testService.searchDAO = searchDAO;
    }

    @After
    public void tearDown() throws Exception {
        // Ensure we are not stuck on the countdown latch if it failed to be
        // called in test as expected
        testLatch.countDown();
        persistentQueueDAO.shutdown();
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#afterInitialisation()}.
     */
    @Test
    public final void testAfterInitialisation() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        Thread testThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    latch.await();
                    testService.afterInitialisation();
                } catch (InterruptedException e) {
                    fail("Interruption occurred: " + e.getMessage());
                }
            }
        });
        testThread.start();
        testService.init();

        latch.countDown();
        testService.afterInitialisation();
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#init()}.
     */
    @Test
    public final void testInit() throws Exception {
        testService.init();
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#getNewDownloadCreator()}.
     */
    @Test
    public final void testGetNewDownloadCreator() throws Exception {
        // TODO: This method is overriden to avoid referencing classes that are
        // not yet setup for testing
        // Just verifies that the method continues to exist
        testService.getNewDownloadCreator();
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#onApplicationEvent(org.springframework.context.event.ContextClosedEvent)}.
     */
    @Test
    public final void testOnApplicationEvent() throws Exception {
        testService.init();
        // Check that this method completes reliably
        testService.onApplicationEvent(new ContextClosedEvent(new GenericApplicationContext()));
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#registerDownload(DownloadRequestParams requestParams, String ip, String userAgent, DownloadDetailsDTO.DownloadType type)}.
     */
    @Test
    public final void testRegisterDownload() throws Exception {
        testService.init();
        DownloadDetailsDTO registerDownload = testService.registerDownload(new DownloadRequestParams(), "::1", "",
                DownloadType.RECORDS_INDEX);
        assertNotNull(registerDownload);
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#unregisterDownload(au.org.ala.biocache.dto.DownloadDetailsDTO)}.
     */
    @Test
    public final void testUnregisterDownload() throws Exception {
        testService.init();
        DownloadDetailsDTO registerDownload = testService.registerDownload(new DownloadRequestParams(), "::1", "",
                DownloadType.RECORDS_INDEX);
        assertNotNull(registerDownload);
        Thread.sleep(5000);
        testService.unregisterDownload(registerDownload);
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#unregisterDownload(au.org.ala.biocache.dto.DownloadDetailsDTO)}.
     */
    @Test
    public final void testUnregisterDownloadWithoutDownloadLatchWait() throws Exception {
        testLatch.countDown();
        testService.init();
        DownloadDetailsDTO registerDownload = testService.registerDownload(new DownloadRequestParams(), "::1", "",
                DownloadType.RECORDS_INDEX);
        assertNotNull(registerDownload);
        Thread.sleep(5000);
        testService.unregisterDownload(registerDownload);
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#unregisterDownload(au.org.ala.biocache.dto.DownloadDetailsDTO)}.
     */
    @Test
    public final void testUnregisterDownloadMultipleWithDownloadLatchWaitOn() throws Exception {
        testService.init();
        DownloadDetailsDTO registerDownload = testService.registerDownload(new DownloadRequestParams(), "::1", "",
                DownloadType.RECORDS_INDEX);
        assertNotNull(registerDownload);
        Thread.sleep(5000);
        testService.unregisterDownload(registerDownload);
        testService.unregisterDownload(registerDownload);
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#unregisterDownload(au.org.ala.biocache.dto.DownloadDetailsDTO)}.
     */
    @Test
    public final void testUnregisterDownloadMultipleWithDownloadLatchWaitOnNoSleep() throws Exception {
        testService.init();
        DownloadDetailsDTO registerDownload = testService.registerDownload(new DownloadRequestParams(), "::1", "",
                DownloadType.RECORDS_INDEX);
        assertNotNull(registerDownload);
        testService.unregisterDownload(registerDownload);
        testService.unregisterDownload(registerDownload);
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#getCurrentDownloads()}.
     */
    @Test
    public final void testGetCurrentDownloadsWithDownloadLatchWaitOn() throws Exception {
        testService.init();
        List<DownloadDetailsDTO> emptyDownloads = testService.getCurrentDownloads();
        assertEquals(0, emptyDownloads.size());
        DownloadDetailsDTO registerDownload = testService.registerDownload(new DownloadRequestParams(), "::1", "",
                DownloadType.RECORDS_INDEX);
        assertNotNull(registerDownload);
        Thread.sleep(5000);
        List<DownloadDetailsDTO> notEmptyDownloads = testService.getCurrentDownloads();
        assertNotNull(notEmptyDownloads);
        // testLatch has not been called, so download should be still in queue
        assertEquals(1, notEmptyDownloads.size());
        testService.unregisterDownload(registerDownload);
        assertEquals(0, emptyDownloads.size());
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#getCurrentDownloads()}.
     */
    @Test
    public final void testGetCurrentDownloadsWithoutDownloadLatchWaitOn() throws Exception {
        // Not verifying the individual stage transitions in this test, just
        // verifying the complete workflow, so switching off the control latch
        testLatch.countDown();
        testService.init();
        List<DownloadDetailsDTO> emptyDownloads = testService.getCurrentDownloads();
        assertEquals(0, emptyDownloads.size());
        DownloadDetailsDTO registerDownload = testService.registerDownload(new DownloadRequestParams(), "::1", "",
                DownloadType.RECORDS_INDEX);
        assertNotNull(registerDownload);
        Thread.sleep(5000);
        List<DownloadDetailsDTO> notEmptyDownloads = testService.getCurrentDownloads();
        assertNotNull(notEmptyDownloads);
        // Can't rely on the download still being in the queue if the latch is
        // not active, so not testing here
        // assertEquals(1, notEmptyDownloads.size());
        testService.unregisterDownload(registerDownload);
        assertEquals(0, emptyDownloads.size());
    }

    /**
     * This test is to ensure the DoiApplicationMetadata supplied to the DownloadRequestParams is copied
     * correctly to the DownloadDetailsDTO before invoking the doiService.mintDoi method.
     * There is a fair bit of mocking/stubbing required to be able to test this.
     */
    @Test
    public final void testDoiApplicationMetadataIsPassedToTheDoiService() throws Exception {

        // Initialisation - if we don't do this the tests will not run.
        testLatch.countDown();
        testService.init();

        // Setup mocks and stubs - could be in setup but I don't want to interfere with the other tests.
        DoiService doiService = mock(DoiService.class);
        testService.doiService = doiService;
        SearchDAO searchDAO = mock(SearchDAO.class);
        testService.searchDAO = searchDAO;
        testService.loggerService = mock(LoggerService.class);
        AbstractMessageSource messageSource = mock(AbstractMessageSource.class);
        testService.messageSource = messageSource;
        AuthService authService = mock(AuthService.class);
        testService.authService = authService;

        testService.biocacheDownloadDoiReadmeTemplate = "/tmp/readme.txt";

        // Setup method parameters
        OutputStream out = new ByteArrayOutputStream();
        List<CreateDoiResponse> doiResponseList = new ArrayList<CreateDoiResponse>();

        DownloadRequestParams downloadRequestParams = new DownloadRequestParams();
        downloadRequestParams.setMintDoi(true);
        downloadRequestParams.setDisplayString("");
        Map<String, String> doiApplicationMetadata = new HashMap<String, String>();
        doiApplicationMetadata.put("key1", "value1");
        doiApplicationMetadata.put("key2", "value2");

        DownloadDetailsDTO downloadDetailsDTO = new DownloadDetailsDTO(downloadRequestParams, "192.168.0.1", "", DownloadType.RECORDS_INDEX);

        when(searchDAO.writeResultsFromIndexToStream(any(), any(), anyBoolean(), any(), anyBoolean(), any())).thenReturn(new ConcurrentHashMap<String, AtomicInteger>());
        when(doiService.mintDoi(isA(DownloadDoiDTO.class))).thenReturn(new CreateDoiResponse());
        String doiSearchUrl = "https://biocache-test.ala.org.au/occurrences/search?q=lsid%3Aurn%3Alsid%3Abiodiversity.org.au%3Aafd.taxon%3Ae6aff6af-ff36-4ad5-95f2-2dfdcca8caff&disableAllQualityFilters=true&fq=month%3A%2207%22&foo%3Abar&baz%3Aqux";
        when(testService.dataQualityService.convertDataQualityParameters(anyString(), any())).thenReturn(doiSearchUrl);
        testService.writeQueryToStream(
                downloadDetailsDTO,
                downloadRequestParams,
                downloadDetailsDTO.getIpAddress(),
                out,
                false, true, true, false, (ExecutorService)null, doiResponseList);

        ArgumentCaptor<DownloadDoiDTO> argument = ArgumentCaptor.forClass(DownloadDoiDTO.class);
        verify(doiService).mintDoi(argument.capture());

        DownloadDoiDTO downloadDoiDTO = argument.getValue();
        assertEquals(downloadDoiDTO.getApplicationMetadata(), downloadRequestParams.getDoiMetadata());
    }

    @Test
    public final void testUserAgentPassedToLoggerService() throws Exception {

        // Initialisation - if we don't do this the tests will not run.
        testLatch.countDown();
        testService.init();

        // Setup mocks and stubs - could be in setup but I don't want to interfere with the other tests.
        DoiService doiService = mock(DoiService.class);
        testService.doiService = doiService;
        SearchDAO searchDAO = mock(SearchDAO.class);
        testService.searchDAO = searchDAO;
        LoggerService loggerService = mock(LoggerService.class);
        testService.loggerService = loggerService;
        AbstractMessageSource messageSource = mock(AbstractMessageSource.class);
        testService.messageSource = messageSource;
        AuthService authService = mock(AuthService.class);
        testService.authService = authService;

        testService.biocacheDownloadDoiReadmeTemplate = "/tmp/readme.txt";

        // Setup method parameters
        OutputStream out = new ByteArrayOutputStream();
        List<CreateDoiResponse> doiResponseList = new ArrayList<CreateDoiResponse>();

        DownloadRequestParams downloadRequestParams = new DownloadRequestParams();
        downloadRequestParams.setMintDoi(true);
        downloadRequestParams.setDisplayString("");

        DownloadDetailsDTO downloadDetailsDTO = new DownloadDetailsDTO(downloadRequestParams, "192.168.0.1", "test User-Agent", DownloadType.RECORDS_INDEX);

        when(searchDAO.writeResultsFromIndexToStream(any(), any(), anyBoolean(), any(), anyBoolean(), any())).thenReturn(new ConcurrentHashMap<String, AtomicInteger>());
        when(doiService.mintDoi(isA(DownloadDoiDTO.class))).thenReturn(new CreateDoiResponse());
        when(testService.dataQualityService.convertDataQualityParameters(any(), any())).thenAnswer(returnsFirstArg());
        testService.writeQueryToStream(
                downloadDetailsDTO,
                downloadRequestParams,
                downloadDetailsDTO.getIpAddress(),
                out,
                false, true, true, false, (ExecutorService)null, doiResponseList);

        ArgumentCaptor<LogEventVO> argument = ArgumentCaptor.forClass(LogEventVO.class);
        verify(loggerService).logEvent(argument.capture());

        LogEventVO logEventVO = argument.getValue();
        assertEquals(logEventVO.getUserAgent(), "test User-Agent");
    }

    @Test
    public final void testOfflineDownload() throws Exception {

        testService = createDownloadServiceForOfflineTest();

        // mock the reading of the downloadEmailTemplate
        mockStatic(Files.class);
        given(Files.asCharSource(any(), eq(StandardCharsets.UTF_8))).willReturn(CharSource.wrap(""));

        testService.biocacheDownloadEmailTemplate = "/tmp/download-email.html";
        testService.biocacheDownloadDoiReadmeTemplate = "/tmp/readme.txt";

        testService.init();
        List<DownloadDetailsDTO> emptyDownloads = testService.getCurrentDownloads();
        assertEquals(0, emptyDownloads.size());

        DownloadRequestParams requestParams = new DownloadRequestParams();
        requestParams.setDisplayString("[all records]");

        DownloadDetailsDTO registerDownload = testService.registerDownload(requestParams, "::1", "", DownloadType.RECORDS_INDEX);
        assertNotNull(registerDownload);
        testService.persistentQueueDAO.addDownloadToQueue(registerDownload);
        Thread.sleep(5000);

        verify(testService.emailService, times(1)).sendEmail(any(), any(), any());
    }

    @Test
    public final void testOfflineDownloadWithQualityFiltersAndDoi() throws Exception {

        testService = createDownloadServiceForOfflineTest();

        // mock the reading of the downloadEmailTemplate
        mockStatic(Files.class);
        given(Files.asCharSource(any(), eq(StandardCharsets.UTF_8))).willReturn(CharSource.wrap(""));

        testService.biocacheDownloadEmailTemplate = "/tmp/download-email.html";
        testService.biocacheDownloadDoiEmailTemplate = "/tmp/download-email.html";
        testService.biocacheDownloadDoiReadmeTemplate = "/tmp/readme.txt";

        testService.init();
        List<DownloadDetailsDTO> emptyDownloads = testService.getCurrentDownloads();
        assertEquals(0, emptyDownloads.size());

        DownloadRequestParams requestParams = new DownloadRequestParams();
        requestParams.setDisplayString("[all records]");

        //
        // verify with data quality results
        //
        requestParams.setQualityProfile("default");
        requestParams.setDisableAllQualityFilters(false);
        requestParams.setMintDoi(true);
        requestParams.setEmailTemplate("doi");
        requestParams.setEmail("example@example.org");
        requestParams.setReason("testing");
        requestParams.setReasonTypeId(1);
        requestParams.setSourceTypeId(2);

        Map<String, String> filters = new LinkedHashMap<>();
        filters.put("first", "foo:bar");
        filters.put("second", "baz:qux");

        when(testService.dataQualityService.getEnabledFiltersByLabel(any(DownloadRequestParams.class))).thenReturn(filters);

        // Return first argument, because in this case our searchUrl will be generated by biocache service and won't be need to be
        // munged by the DataQualityService
        when(testService.dataQualityService.convertDataQualityParameters(any(), eq(filters))).thenAnswer(returnsFirstArg());

        CreateDoiResponse createDoiResponse = new CreateDoiResponse();
        createDoiResponse.setDoi("10.5555/12345678");
        createDoiResponse.setUuid("ac2ca7ca-9f3a-42af-a840-9c9bd99066b7");
        createDoiResponse.setLandingPage("https://example.org/");
        createDoiResponse.setDoiServiceLandingPage("https://doi.example.org/");
        when(testService.doiService.mintDoi(any(DownloadDoiDTO.class))).thenReturn(createDoiResponse);

        ConcurrentMap<String, AtomicInteger> uidStats = newConcurrentMap();
        when(testService.searchDAO.writeResultsFromIndexToStream(any(), any(), anyBoolean(), any(), anyBoolean(), any())).thenReturn(uidStats);

        DownloadDetailsDTO registerDownload = testService.registerDownload(requestParams, "::1", "", DownloadType.RECORDS_INDEX);
        assertNotNull(registerDownload);
        testService.persistentQueueDAO.addDownloadToQueue(registerDownload);
        Thread.sleep(5000);

        verify(testService.emailService).sendEmail(requestParams.getEmail(), "ALA Occurrence Download Complete - data", "");

        verify(testService.dataQualityService, times(2)).getEnabledFiltersByLabel(requestParams);

        ArgumentCaptor<DownloadDoiDTO> acDoi = ArgumentCaptor.forClass(DownloadDoiDTO.class);

        verify(testService.doiService).mintDoi(acDoi.capture());
        DownloadDoiDTO value = acDoi.getValue();
        assertThat("Mint DOI call was captured", value, notNullValue());
        assertThat("Mint DOI call contained filters info",
                value.getQualityFilters(),
                containsInAnyOrder(
                        new QualityFilterDTO("first", "foo:bar"),
                        new QualityFilterDTO("second", "baz:qux")
                )
        );
        assertThat("Mint DOI call contains search URL with filters as fqs",
                value.getApplicationUrl(), containsString("&disableAllQualityFilters=true&fq=foo%3Abar&fq=baz%3Aqux"));

        // TODO verify LogEventVO, requires .equals/.hashCode on LogEventVO?
        verify(testService.loggerService).logEvent(any(LogEventVO.class));

    }

    @Test
    public final void testOfflineDownloadWithQualityFiltersAndDoiAndProvidedSearchUrl() throws Exception {

        testService = createDownloadServiceForOfflineTest();

        // mock the reading of the downloadEmailTemplate
        mockStatic(Files.class);
        given(Files.asCharSource(any(), eq(StandardCharsets.UTF_8))).willReturn(CharSource.wrap(""));

        testService.biocacheDownloadEmailTemplate = "/tmp/download-email.html";
        testService.biocacheDownloadDoiEmailTemplate = "/tmp/download-email.html";
        testService.biocacheDownloadDoiReadmeTemplate = "/tmp/readme.txt";

        testService.init();
        List<DownloadDetailsDTO> emptyDownloads = testService.getCurrentDownloads();
        assertEquals(0, emptyDownloads.size());

        DownloadRequestParams requestParams = new DownloadRequestParams();
        requestParams.setDisplayString("[all records]");

        //
        // verify with data quality results and a provided searchUrl
        //
        String searchUrl = "https://biocache-test.ala.org.au/occurrences/search?q=lsid%3Aurn%3Alsid%3Abiodiversity.org.au%3Aafd.taxon%3Ae6aff6af-ff36-4ad5-95f2-2dfdcca8caff&qualityProfile=default&disableQualityFilter=dates-post-1700&fq=month%3A%2207%22";
        requestParams.setSearchUrl(searchUrl);
        requestParams.setQualityProfile("default");
        requestParams.setDisableQualityFilter(newArrayList("dates-post-1700"));
        requestParams.setMintDoi(true);
        requestParams.setEmailTemplate("doi");
        requestParams.setEmail("example@example.org");
        requestParams.setReason("testing");
        requestParams.setReasonTypeId(1);
        requestParams.setSourceTypeId(2);

        Map<String, String> filters = new LinkedHashMap<>();
        filters.put("first", "foo:bar");
        filters.put("second", "baz:qux");

        when(testService.dataQualityService.getEnabledFiltersByLabel(any(DownloadRequestParams.class))).thenReturn(filters);

        String doiSearchUrl = "https://biocache-test.ala.org.au/occurrences/search?q=lsid%3Aurn%3Alsid%3Abiodiversity.org.au%3Aafd.taxon%3Ae6aff6af-ff36-4ad5-95f2-2dfdcca8caff&disableAllQualityFilters=true&fq=month%3A%2207%22&foo%3Abar&baz%3Aqux";
        when(testService.dataQualityService.convertDataQualityParameters(eq(searchUrl), eq(filters))).thenReturn(doiSearchUrl);

        CreateDoiResponse createDoiResponse = new CreateDoiResponse();
        createDoiResponse.setDoi("10.5555/12345678");
        createDoiResponse.setUuid("ac2ca7ca-9f3a-42af-a840-9c9bd99066b7");
        createDoiResponse.setLandingPage("https://example.org/");
        createDoiResponse.setDoiServiceLandingPage("https://doi.example.org/");
        when(testService.doiService.mintDoi(any(DownloadDoiDTO.class))).thenReturn(createDoiResponse);

        ConcurrentMap<String, AtomicInteger> uidStats = newConcurrentMap();
        when(testService.searchDAO.writeResultsFromIndexToStream(any(), any(), anyBoolean(), any(), anyBoolean(), any())).thenReturn(uidStats);

        DownloadDetailsDTO registerDownload = testService.registerDownload(requestParams, "::1", "", DownloadType.RECORDS_INDEX);
        assertNotNull(registerDownload);
        testService.persistentQueueDAO.addDownloadToQueue(registerDownload);
        Thread.sleep(5000);

        verify(testService.emailService).sendEmail(requestParams.getEmail(), "ALA Occurrence Download Complete - data", "");

        verify(testService.dataQualityService).getEnabledFiltersByLabel(requestParams);

        ArgumentCaptor<DownloadDoiDTO> acDoi = ArgumentCaptor.forClass(DownloadDoiDTO.class);

        verify(testService.doiService).mintDoi(acDoi.capture());
        DownloadDoiDTO value = acDoi.getValue();
        assertThat("Mint DOI call was captured", value, notNullValue());
        assertThat("Mint DOI call contained filters info",
                value.getQualityFilters(),
                containsInAnyOrder(
                        new QualityFilterDTO("first", "foo:bar"),
                        new QualityFilterDTO("second", "baz:qux")
                )
        );
        assertThat("Mint DOI call contains search URL with filters as fqs",
                value.getApplicationUrl(), equalTo(doiSearchUrl));

        // TODO verify LogEventVO, requires .equals/.hashCode on LogEventVO?
        verify(testService.loggerService).logEvent(any(LogEventVO.class));

    }

    @Test
    public final void testOfflineDownloadNoEmailNotify() throws Exception {

        testService = createDownloadServiceForOfflineTest();

        testService.biocacheDownloadDoiReadmeTemplate = "/tmp/readme.txt";

        testService.init();
        List<DownloadDetailsDTO> emptyDownloads = testService.getCurrentDownloads();
        assertEquals(0, emptyDownloads.size());

        DownloadRequestParams requestParams = new DownloadRequestParams();
        requestParams.setEmailNotify(false);
        requestParams.setDisplayString("[all records]");

        DownloadDetailsDTO registerDownload = testService.registerDownload(requestParams, "::1", "", DownloadType.RECORDS_INDEX);
        assertNotNull(registerDownload);
        testService.persistentQueueDAO.addDownloadToQueue(registerDownload);
        Thread.sleep(5000);

        verify(testService.emailService, times(0)).sendEmail(any(), any(), any());
    }


    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#writeQueryToStream(
     *                                                              DownloadDetailsDTO dd,
     *                                                              DownloadRequestParams requestParams,
     *                                                              String ip,
     *                                                              OutputStream out,
     *                                                              boolean includeSensitive,
     *                                                              boolean fromIndex,
     *                                                              boolean limit,
     *                                                              boolean zip,
     *                                                              ExecutorService parallelExecutor,
     *                                                              List<CreateDoiResponse> doiResponseList)}.
     */
    @Ignore("TODO: Implement me")
    @Test
    public final void testWriteQueryToStreamDownloadDetailsDTODownloadRequestParamsStringOutputStreamBooleanBooleanBooleanBoolean()
            throws Exception {
        fail("Not yet implemented"); // TODO
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#writeQueryToStream(
     *                                                              DownloadRequestParams requestParams,
     *                                                              HttpServletResponse response,
     *                                                              String ip,
     *                                                              String userAgent,
     *                                                              OutputStream out,
     *                                                              boolean includeSensitive,
     *                                                              boolean fromIndex,
     *                                                              boolean zip,
     *                                                              ExecutorService parallelQueryExecutor)}.
     */
    @Ignore("TODO: Implement me")
    @Test
    public final void testWriteQueryToStreamDownloadRequestParamsHttpServletResponseStringServletOutputStreamBooleanBooleanBoolean()
            throws Exception {
        fail("Not yet implemented"); // TODO
    }

    /**
     * Test method for
     * {@link DownloadService#getCitations(java.util.concurrent.ConcurrentMap, java.io.OutputStream, char, char, List, List)}.
     */
    @Ignore("TODO: Implement me")
    @Test
    public final void testGetCitations() throws Exception {
        fail("Not yet implemented"); // TODO
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#getHeadings(java.util.concurrent.ConcurrentMap, java.io.OutputStream, au.org.ala.biocache.dto.DownloadRequestParams, java.lang.String[])}.
     */
    @Ignore("TODO: Implement me")
    @Test
    public final void testGetHeadings() throws Exception {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public final void testDataQualityResourceTemplate() throws Exception {
        List<QualityFilterDTO> qualityFilters = new ArrayList<>();
        qualityFilters.add(new QualityFilterDTO("test", "asdf"));
        qualityFilters.add(new QualityFilterDTO("test2", "fdas"));

        String result = testService.getDataQualityFiltersString(qualityFilters);

        assertThat(result, equalTo("<p>Quality Filters applied:</p>\n" +
                "\n" +
                "<ul>\n" +
                " <li>test: asdf</li>\n" +
                " <li>test2: fdas</li>\n" +
                "</ul>"));
    }

    private DownloadService createDownloadServiceForOfflineTest() {
        DownloadService testService = new DownloadService() {
            {
                sensitiveAccessRoles = "{}";
                concurrentDownloadsJSON = "[]";
            }
        };

        testService.downloadQualityFiltersTemplate = new ClassPathResource("download-email-quality-filter-snippet.html");
        testService.biocacheDownloadDir = testDownloadDir.toAbsolutePath().toString();
        testService.persistentQueueDAO = persistentQueueDAO;

        testService.doiService = mock(DoiService.class);
        testService.searchDAO = mock(SearchDAO.class);
        testService.objectMapper = new ObjectMapper();
        testService.loggerService = mock(LoggerService.class);
        AbstractMessageSource messageSource = new ReloadableResourceBundleMessageSource();
        messageSource.setUseCodeAsDefaultMessage(true);
        testService.messageSource = messageSource;
        testService.authService = mock(AuthService.class);
        EmailService emailService = mock(EmailService.class);
        testService.emailService = emailService;
        testService.dataQualityService = mock(DataQualityService.class);

        return testService;
    }
}
