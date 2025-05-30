/**
 *
 */
package au.org.ala.biocache.service;

import au.org.ala.biocache.dao.IndexDAO;
import au.org.ala.biocache.dao.JsonPersistentQueueDAOImpl;
import au.org.ala.biocache.dao.PersistentQueueDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import au.org.ala.biocache.util.QueryFormatUtils;
import au.org.ala.doi.CreateDoiResponse;
import au.org.ala.ws.security.profile.AlaUserProfile;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.ala.client.model.LogEventVO;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.context.support.ReloadableResourceBundleMessageSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.Principal;
import java.util.*;
import java.util.concurrent.*;

import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

/**
 * Test for {@link DownloadService}
 *
 * @author Peter Ansell
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "org.w3c.*", "javax.net.ssl.*" })
@PrepareForTest(FileUtils.class)
@ContextConfiguration(locations = {"classpath:springTest.xml"})
@TestPropertySource(locations = "classpath:biocache-test-config.properties")
public class DownloadServiceTest {

    @Rule
    public Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private Path testCacheDir;
    private Path testDownloadDir;

    private PersistentQueueDAO persistentQueueDAO;

    DownloadService testService;

    @Autowired
    QueryFormatUtils queryFormatUtils;

    @Autowired
    IndexDAO indexDAO;

    final static AlaUserProfile TEST_USER = new AlaUserProfile() {

        @Override
        public String getId() {
            return null;
        }

        @Override
        public void setId(String id) {

        }

        @Override
        public String getTypedId() {
            return null;
        }

        @Override
        public String getUsername() {
            return null;
        }

        @Override
        public Object getAttribute(String name) {
            return null;
        }

        @Override
        public Map<String, Object> getAttributes() {
            return null;
        }

        @Override
        public boolean containsAttribute(String name) {
            return false;
        }

        @Override
        public void addAttribute(String key, Object value) {

        }

        @Override
        public void removeAttribute(String key) {

        }

        @Override
        public void addAuthenticationAttribute(String key, Object value) {

        }

        @Override
        public void removeAuthenticationAttribute(String key) {

        }

        @Override
        public void addRole(String role) {

        }

        @Override
        public void addRoles(Collection<String> roles) {

        }

        @Override
        public Set<String> getRoles() {
            return Collections.emptySet();
        }

        @Override
        public void addPermission(String permission) {

        }

        @Override
        public void addPermissions(Collection<String> permissions) {

        }

        @Override
        public Set<String> getPermissions() {
            return Collections.emptySet();
        }

        @Override
        public boolean isRemembered() {
            return false;
        }

        @Override
        public void setRemembered(boolean rme) {

        }

        @Override
        public String getClientName() {
            return null;
        }

        @Override
        public void setClientName(String clientName) {

        }

        @Override
        public String getLinkedId() {
            return null;
        }

        @Override
        public void setLinkedId(String linkedId) {

        }

        @Override
        public boolean isExpired() {
            return false;
        }

        @Override
        public Principal asPrincipal() {
            return null;
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public String getUserId() {
            return "Tester";
        }

        @Override
        public String getEmail() {
            return "test@test.com";
        }

        @Override
        public String getGivenName() {
            return null;
        }

        @Override
        public String getFamilyName() {
            return null;
        }
    };

    final static AlaUserProfile TEST_USER2 = new AlaUserProfile() {

        @Override
        public String getId() {
            return null;
        }

        @Override
        public void setId(String id) {

        }

        @Override
        public String getTypedId() {
            return null;
        }

        @Override
        public String getUsername() {
            return null;
        }

        @Override
        public Object getAttribute(String name) {
            return null;
        }

        @Override
        public Map<String, Object> getAttributes() {
            return null;
        }

        @Override
        public boolean containsAttribute(String name) {
            return false;
        }

        @Override
        public void addAttribute(String key, Object value) {

        }

        @Override
        public void removeAttribute(String key) {

        }

        @Override
        public void addAuthenticationAttribute(String key, Object value) {

        }

        @Override
        public void removeAuthenticationAttribute(String key) {

        }

        @Override
        public void addRole(String role) {

        }

        @Override
        public void addRoles(Collection<String> roles) {

        }

        @Override
        public Set<String> getRoles() {
            return Collections.emptySet();
        }

        @Override
        public void addPermission(String permission) {

        }

        @Override
        public void addPermissions(Collection<String> permissions) {

        }

        @Override
        public Set<String> getPermissions() {
            return Collections.emptySet();
        }

        @Override
        public boolean isRemembered() {
            return false;
        }

        @Override
        public void setRemembered(boolean rme) {

        }

        @Override
        public String getClientName() {
            return null;
        }

        @Override
        public void setClientName(String clientName) {

        }

        @Override
        public String getLinkedId() {
            return null;
        }

        @Override
        public void setLinkedId(String linkedId) {

        }

        @Override
        public boolean isExpired() {
            return false;
        }

        @Override
        public Principal asPrincipal() {
            return null;
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public String getUserId() {
            return "Tester2";
        }

        @Override
        public String getEmail() {
            return "test2@test.com";
        }

        @Override
        public String getGivenName() {
            return null;
        }

        @Override
        public String getFamilyName() {
            return null;
        }
    };

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

        testLatch = new CountDownLatch(1);

        testService = new DownloadService() {

            protected DownloadRunnable getDownloadRunnable(DownloadDetailsDTO dd) {
                return new DownloadRunnable(dd) {
                    @Override
                    public void run() {
                        try {
                            // each download will take 2500ms
                            Thread.sleep(2500);
                        } catch (InterruptedException e) {

                        }
                    }
                };
            }
        };

        indexDAO = mock(IndexDAO.class);

        testService.dataQualityService = mock(DataQualityService.class);
        testService.downloadQualityFiltersTemplate = new ClassPathResource("download-email-quality-filter-snippet.html");
        testService.biocacheDownloadDir = testDownloadDir.toAbsolutePath().toString();
        testService.persistentQueueDAO = persistentQueueDAO;
        testService.indexDao = indexDAO;
    }

    @After
    public void tearDown() throws Exception {
        // Ensure we are not stuck on the countdown latch if it failed to be
        // called in test as expected
        if (testLatch != null) {
            testLatch.countDown();
        }

        FileUtils.deleteDirectory(testCacheDir.toFile());
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#init()}.
     */
    @Test
    public final void testInit() throws Exception {
        testService.init();
        Thread.sleep(500);
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#onApplicationEvent(org.springframework.context.event.ContextClosedEvent)}.
     */
    @Test
    public final void testOnApplicationEvent() throws Exception {
        testService.init();
        Thread.sleep(500);
        // Check that this method completes reliably
        testService.onApplicationEvent(new ContextClosedEvent(new GenericApplicationContext()));
    }

    /**
     * Test method for
     * {@link DownloadService#add(DownloadDetailsDTO)}
     */
    @Test
    public final void testAdd() throws Exception {
        testService.init();
        Thread.sleep(500);
        testService.add(new DownloadDetailsDTO(new DownloadRequestDTO(), TEST_USER, "::1", "", DownloadType.RECORDS_INDEX));
        assertEquals(persistentQueueDAO.getAllDownloads().size(), 1);
        assertEquals(testService.userExecutors.size(), 1);
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#cancel(DownloadDetailsDTO)}.
     */
    @Test
    public final void testCancel() throws Exception {
        testService.init();
        Thread.sleep(500);
        DownloadDetailsDTO dd = new DownloadDetailsDTO(new DownloadRequestDTO(), TEST_USER, "::1", "", DownloadType.RECORDS_INDEX);
        testService.add(dd);

        // sleep for a little, but not enough that the tasks will finish
        Thread.sleep(500);

        assertEquals(persistentQueueDAO.getAllDownloads().size(), 1);
        assertEquals(testService.userExecutors.size(), 1);
        testService.cancel(dd);
        Thread.sleep(500);
        assertEquals(persistentQueueDAO.getAllDownloads().size(), 0);
        assertEquals(testService.userExecutors.size(), 1);
    }

    private DownloadRequestDTO getParams(String query){
        DownloadRequestDTO d = new DownloadRequestDTO();
        d.setQ(query);
        d.setFile("Testing");
        d.setEmail("natasha.carter@csiro.au");
        return d;
    }

    /**
    * Test method for
    * {@link au.org.ala.biocache.service.DownloadService#cancel(DownloadDetailsDTO)}.
    */
    @Test
    public final void testCancelSingleUser() throws Exception {
        testService.init();
        Thread.sleep(500);
        DownloadDetailsDTO dd = new DownloadDetailsDTO(new DownloadRequestDTO(), TEST_USER, "::1", "", DownloadType.RECORDS_INDEX);
        testService.add(dd);
        DownloadDetailsDTO dd2 = new DownloadDetailsDTO(getParams("test2"), TEST_USER,"127.0.0.1", "", DownloadType.FACET);
        testService.add(dd2);

        // sleep for a little, but not enough that the tasks will finish
        Thread.sleep(500);

        assertEquals(persistentQueueDAO.getAllDownloads().size(), 2);
        assertEquals(testService.userExecutors.size(), 1);
        testService.cancel(dd);
        Thread.sleep(500);
        assertEquals(persistentQueueDAO.getAllDownloads().size(), 1);
        assertEquals(testService.userExecutors.size(), 1);
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#cancel(DownloadDetailsDTO)}.
     */
    @Test
    public final void testCancelMultipleUser() throws Exception {
        testService.init();
        Thread.sleep(500);
        DownloadDetailsDTO dd = new DownloadDetailsDTO(new DownloadRequestDTO(), TEST_USER, "::1", "", DownloadType.RECORDS_INDEX);
        testService.add(dd);
        DownloadDetailsDTO dd2 = new DownloadDetailsDTO(getParams("test2"), TEST_USER2,"127.0.0.1", "", DownloadType.FACET);
        testService.add(dd2);

        // sleep for a little, but not enough that the tasks will finish
        Thread.sleep(500);

        assertEquals(persistentQueueDAO.getAllDownloads().size(), 2);
        assertEquals(testService.userExecutors.size(), 2);
        testService.cancel(dd);
        Thread.sleep(500);
        assertEquals(persistentQueueDAO.getAllDownloads().size(), 1);
        assertEquals(testService.userExecutors.size(), 2);
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
        Thread.sleep(500);

        // Setup mocks and stubs - could be in setup but I don't want to interfere with the other tests.
        DoiService doiService = mock(DoiService.class);
        testService.doiService = doiService;
        SearchDAO searchDAO = mock(SearchDAO.class);
        testService.searchDAO = searchDAO;
        testService.loggerService = mock(LoggerService.class);
        AbstractMessageSource messageSource = mock(AbstractMessageSource.class);
        testService.messageSource = messageSource;

        testService.biocacheDownloadDoiReadmeTemplate = "/tmp/readme.txt";

        // Setup method parameters
        OutputStream out = new ByteArrayOutputStream();
        List<CreateDoiResponse> doiResponseList = new ArrayList<CreateDoiResponse>();

        DownloadRequestDTO downloadRequestDTO = new DownloadRequestDTO();
        downloadRequestDTO.setMintDoi(true);
        downloadRequestDTO.setDisplayString("");
        Map<String, String> doiApplicationMetadata = new HashMap<String, String>();
        doiApplicationMetadata.put("key1", "value1");
        doiApplicationMetadata.put("key2", "value2");

        DownloadDetailsDTO downloadDetailsDTO = new DownloadDetailsDTO(downloadRequestDTO, TEST_USER, "192.168.0.1", "", DownloadType.RECORDS_INDEX);

        when(searchDAO.writeResultsFromIndexToStream(any(), any(), any(), any(), anyBoolean(), any())).thenReturn(new DownloadHeaders(new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}));
        when(doiService.mintDoi(isA(DownloadDoiDTO.class))).thenReturn(new CreateDoiResponse());
        String doiSearchUrl = "https://biocache-test.ala.org.au/occurrences/search?q=lsid%3Aurn%3Alsid%3Abiodiversity.org.au%3Aafd.taxon%3Ae6aff6af-ff36-4ad5-95f2-2dfdcca8caff&disableAllQualityFilters=true&fq=month%3A%2207%22&foo%3Abar&baz%3Aqux";
        when(testService.dataQualityService.convertDataQualityParameters(anyString(), any())).thenReturn(doiSearchUrl);
        testService.writeQueryToStream(
                downloadDetailsDTO,
                out,
                 true, false, (ExecutorService) null, doiResponseList);

        ArgumentCaptor<DownloadDoiDTO> argument = ArgumentCaptor.forClass(DownloadDoiDTO.class);
        verify(doiService).mintDoi(argument.capture());

        DownloadDoiDTO downloadDoiDTO = argument.getValue();
        assertEquals(downloadDoiDTO.getApplicationMetadata(), downloadRequestDTO.getDoiMetadata());
    }

    /**
     * This test is to ensure the DoiApplicationMetadata supplied to the DownloadRequestParams is copied
     * correctly to the DownloadDetailsDTO before invoking the doiService.mintDoi method.
     * There is a fair bit of mocking/stubbing required to be able to test this.
     */
    @Test
    public final void testDoiDTOContainsProfileFullNameWhenDataProfileIsProvded() throws Exception {

        // Initialisation - if we don't do this the tests will not run.
        testLatch.countDown();
        testService.init();
        Thread.sleep(500);

        // Setup mocks and stubs - could be in setup but I don't want to interfere with the other tests.
        DoiService doiService = mock(DoiService.class);
        testService.doiService = doiService;
        SearchDAO searchDAO = mock(SearchDAO.class);
        testService.searchDAO = searchDAO;
        testService.loggerService = mock(LoggerService.class);
        AbstractMessageSource messageSource = mock(AbstractMessageSource.class);
        testService.messageSource = messageSource;

        testService.biocacheDownloadDoiReadmeTemplate = "/tmp/readme.txt";

        // Setup method parameters
        OutputStream out = new ByteArrayOutputStream();
        List<CreateDoiResponse> doiResponseList = new ArrayList<CreateDoiResponse>();

        DownloadRequestDTO downloadRequestDTO = new DownloadRequestDTO();
        downloadRequestDTO.setMintDoi(true);
        downloadRequestDTO.setDisplayString("");
        downloadRequestDTO.setQualityProfile("short-name");

        DownloadDetailsDTO downloadDetailsDTO = new DownloadDetailsDTO(downloadRequestDTO, TEST_USER,  "192.168.0.1", "", DownloadType.RECORDS_INDEX);
        final String profileFullName = "Full Name";

        when(searchDAO.writeResultsFromIndexToStream(any(), any(), any(),  any(), anyBoolean(), any())).thenReturn(new DownloadHeaders(new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}));
        when(doiService.mintDoi(isA(DownloadDoiDTO.class))).thenReturn(new CreateDoiResponse());
        String doiSearchUrl = "https://biocache-test.ala.org.au/occurrences/search?q=lsid%3Aurn%3Alsid%3Abiodiversity.org.au%3Aafd.taxon%3Ae6aff6af-ff36-4ad5-95f2-2dfdcca8caff&disableAllQualityFilters=true&fq=month%3A%2207%22&foo%3Abar&baz%3Aqux";
        when(testService.dataQualityService.convertDataQualityParameters(anyString(), any())).thenReturn(doiSearchUrl);
        when(testService.dataQualityService.getProfileFullName(downloadRequestDTO.getQualityProfile())).thenReturn(profileFullName);
        testService.writeQueryToStream(
                downloadDetailsDTO,
                out,
                 true, false, (ExecutorService)null, doiResponseList);

        ArgumentCaptor<DownloadDoiDTO> argument = ArgumentCaptor.forClass(DownloadDoiDTO.class);
        verify(doiService).mintDoi(argument.capture());

        DownloadDoiDTO downloadDoiDTO = argument.getValue();
        assertEquals(downloadDoiDTO.getDataProfile(), profileFullName);
    }

    /**
     * This test is to ensure the DoiApplicationMetadata supplied to the DownloadRequestParams is copied
     * correctly to the DownloadDetailsDTO before invoking the doiService.mintDoi method.
     * There is a fair bit of mocking/stubbing required to be able to test this.
     */
    @Test
    public final void testDoiDTOContainsNoProfileNameWhenNoneProvided() throws Exception {

        // Initialisation - if we don't do this the tests will not run.
        testLatch.countDown();
        testService.init();
        Thread.sleep(500);

        // Setup mocks and stubs - could be in setup but I don't want to interfere with the other tests.
        DoiService doiService = mock(DoiService.class);
        SearchDAO searchDAO = mock(SearchDAO.class);
        AbstractMessageSource messageSource = mock(AbstractMessageSource.class);

        testService.doiService = doiService;
        testService.searchDAO = searchDAO;
        testService.loggerService = mock(LoggerService.class);
        testService.messageSource = messageSource;
        testService.biocacheDownloadDoiReadmeTemplate = "/tmp/readme.txt";

        // Setup method parameters
        OutputStream out = new ByteArrayOutputStream();
        List<CreateDoiResponse> doiResponseList = new ArrayList<CreateDoiResponse>();

        DownloadRequestDTO downloadRequestDTO = new DownloadRequestDTO();
        downloadRequestDTO.setMintDoi(true);
        downloadRequestDTO.setDisplayString("");
        downloadRequestDTO.setQualityProfile("");

        DownloadDetailsDTO downloadDetailsDTO = new DownloadDetailsDTO(downloadRequestDTO, TEST_USER,  "192.168.0.1", "", DownloadType.RECORDS_INDEX);

        when(searchDAO.writeResultsFromIndexToStream(any(), any(), any(), any(), anyBoolean(), any())).thenReturn(new DownloadHeaders(new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}));
        when(doiService.mintDoi(isA(DownloadDoiDTO.class))).thenReturn(new CreateDoiResponse());
        String doiSearchUrl = "https://biocache-test.ala.org.au/occurrences/search?q=lsid%3Aurn%3Alsid%3Abiodiversity.org.au%3Aafd.taxon%3Ae6aff6af-ff36-4ad5-95f2-2dfdcca8caff&disableAllQualityFilters=true&fq=month%3A%2207%22&foo%3Abar&baz%3Aqux";
        when(testService.dataQualityService.convertDataQualityParameters(anyString(), any())).thenReturn(doiSearchUrl);
        verify(testService.dataQualityService, never()).getProfileFullName(anyString());
        testService.writeQueryToStream(
                downloadDetailsDTO,
                out,
                 true, false, (ExecutorService) null, doiResponseList);

        ArgumentCaptor<DownloadDoiDTO> argument = ArgumentCaptor.forClass(DownloadDoiDTO.class);
        verify(doiService).mintDoi(argument.capture());

        DownloadDoiDTO downloadDoiDTO = argument.getValue();
        assertEquals(downloadDoiDTO.getDataProfile(), null);
    }

    @Test
    public final void testUserAgentPassedToLoggerService() throws Exception {

        // Initialisation - if we don't do this the tests will not run.
        testLatch.countDown();
        testService.init();
        Thread.sleep(500);

        // Setup mocks and stubs - could be in setup but I don't want to interfere with the other tests.
        DoiService doiService = mock(DoiService.class);
        testService.doiService = doiService;
        SearchDAO searchDAO = mock(SearchDAO.class);
        testService.searchDAO = searchDAO;
        LoggerService loggerService = mock(LoggerService.class);
        testService.loggerService = loggerService;
        AbstractMessageSource messageSource = mock(AbstractMessageSource.class);
        testService.messageSource = messageSource;

        testService.biocacheDownloadDoiReadmeTemplate = "/tmp/readme.txt";

        // Setup method parameters
        OutputStream out = new ByteArrayOutputStream();
        List<CreateDoiResponse> doiResponseList = new ArrayList<CreateDoiResponse>();

        DownloadRequestDTO downloadRequestDTO = new DownloadRequestDTO();
        downloadRequestDTO.setMintDoi(true);
        downloadRequestDTO.setDisplayString("");

        DownloadDetailsDTO downloadDetailsDTO = new DownloadDetailsDTO(downloadRequestDTO, TEST_USER,  "192.168.0.1", "test User-Agent", DownloadType.RECORDS_INDEX);

        when(searchDAO.writeResultsFromIndexToStream(any(), any(), any(), any(), anyBoolean(), any())).thenReturn(new DownloadHeaders(new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}));
        when(doiService.mintDoi(isA(DownloadDoiDTO.class))).thenReturn(new CreateDoiResponse());
        when(testService.dataQualityService.convertDataQualityParameters(any(), any())).thenAnswer(returnsFirstArg());
        testService.writeQueryToStream(
                downloadDetailsDTO,
                out,
                 true, false, (ExecutorService) null, doiResponseList);

        ArgumentCaptor<LogEventVO> argument = ArgumentCaptor.forClass(LogEventVO.class);
        verify(loggerService).logEvent(argument.capture());

        LogEventVO logEventVO = argument.getValue();
        assertEquals(logEventVO.getUserAgent(), "test User-Agent");
    }

    @Test
    public final void testOfflineDownload() throws Exception {

        testService = createDownloadServiceForOfflineTest();

        mockStatic(FileUtils.class);
        given(FileUtils.readFileToString(any(), eq(StandardCharsets.UTF_8))).willReturn("");
        given(FileUtils.openOutputStream(any())).willCallRealMethod();
        given(FileUtils.openOutputStream(any(), anyBoolean())).willCallRealMethod();

        when(testService.searchDAO.writeResultsFromIndexToStream(any(), any(), any(),  any(), anyBoolean(), any())).thenReturn(new DownloadHeaders(new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}));
        when(testService.dataQualityService.convertDataQualityParameters(any(), any())).thenAnswer(returnsFirstArg());

        testService.support = "support@ala.org.au";
        testService.myDownloadsUrl = "https://dev.ala.org.au/myDownloads";
        testService.biocacheDownloadUrl = "http://dev.ala.org.au/biocache-download";
        testService.biocacheDownloadEmailTemplate = "/tmp/download-email.html";
        testService.biocacheDownloadReadmeTemplate = "/tmp/readme.txt";

        testService.init();
        Thread.sleep(500);
        List<DownloadDetailsDTO> emptyDownloads = testService.getCurrentDownloads();
        assertEquals(0, emptyDownloads.size());

        DownloadRequestDTO requestParams = new DownloadRequestDTO();
        requestParams.setDisplayString("[all records]");

        DownloadDetailsDTO dd = new DownloadDetailsDTO(requestParams, null, "::1", "", DownloadType.RECORDS_INDEX);
        testService.add(dd);
        Thread.sleep(5000);

        verify(testService.emailService, times(1)).sendEmail(any(), any(), any());
    }

    @Test
    public final void testOfflineDownloadWithQualityFiltersAndDoi() throws Exception {

        testService = createDownloadServiceForOfflineTest();

        // mock the reading of the downloadEmailTemplate
        mockStatic(FileUtils.class);
        given(FileUtils.readFileToString(any(), eq(StandardCharsets.UTF_8))).willReturn("");
        given(FileUtils.openOutputStream(any())).willCallRealMethod();
        given(FileUtils.openOutputStream(any(), anyBoolean())).willCallRealMethod();

        testService.support = "support@ala.org.au";
        testService.myDownloadsUrl = "https://dev.ala.org.au/myDownloads";
        testService.biocacheDownloadUrl = "http://dev.ala.org.au/biocache-download";
        testService.biocacheDownloadEmailTemplate = "/tmp/download-email.html";
        testService.biocacheDownloadDoiEmailTemplate = "/tmp/download-email.html";
        testService.biocacheDownloadDoiReadmeTemplate = "/tmp/readme.txt";


        testService.init();
        Thread.sleep(500);
        List<DownloadDetailsDTO> emptyDownloads = testService.getCurrentDownloads();
        assertEquals(0, emptyDownloads.size());

        DownloadRequestDTO requestParams = new DownloadRequestDTO();
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

        when(testService.dataQualityService.getEnabledFiltersByLabel(any(DownloadRequestDTO.class))).thenReturn(filters);

        // Return first argument, because in this case our searchUrl will be generated by biocache service and won't be need to be
        // munged by the DataQualityService
        when(testService.dataQualityService.convertDataQualityParameters(any(), eq(filters))).thenAnswer(returnsFirstArg());

        CreateDoiResponse createDoiResponse = new CreateDoiResponse();
        createDoiResponse.setDoi("10.5555/12345678");
        createDoiResponse.setUuid("ac2ca7ca-9f3a-42af-a840-9c9bd99066b7");
        createDoiResponse.setLandingPage("https://example.org/");
        createDoiResponse.setDoiServiceLandingPage("https://doi.example.org/");
        when(testService.doiService.mintDoi(any(DownloadDoiDTO.class))).thenReturn(createDoiResponse);

        when(testService.searchDAO.writeResultsFromIndexToStream(any(), any(), any(),  any(), anyBoolean(), any())).thenReturn(new DownloadHeaders(new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}));

        DownloadDetailsDTO dd = new DownloadDetailsDTO(requestParams, null, "::1", "", DownloadType.RECORDS_INDEX);
        testService.add(dd);
        Thread.sleep(5000);

        // One of the other tests puts something into the download.cache.dir so it ends up doing 2 downloads. Unsure how to fix.
        // verify(testService.emailService).sendEmail(requestParams.getEmail(), "ALA Occurrence Download Complete - data", "");
        //verify(testService.dataQualityService, times(2)).getEnabledFiltersByLabel(requestParams);

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
        mockStatic(FileUtils.class);
        given(FileUtils.readFileToString(any(), eq(StandardCharsets.UTF_8))).willReturn("");
        given(FileUtils.openOutputStream(any())).willCallRealMethod();
        given(FileUtils.openOutputStream(any(), anyBoolean())).willCallRealMethod();

        testService.support = "support@dev.ala.org.au";
        testService.myDownloadsUrl = "https://dev.ala.org.au/myDownloads";
        testService.biocacheDownloadUrl = "http://dev.ala.org.au/biocache-download";
        testService.biocacheDownloadEmailTemplate = "/tmp/download-email.html";
        testService.biocacheDownloadDoiEmailTemplate = "/tmp/download-email.html";
        testService.biocacheDownloadDoiReadmeTemplate = "/tmp/readme.txt";

        testService.init();
        Thread.sleep(500);
        List<DownloadDetailsDTO> emptyDownloads = testService.getCurrentDownloads();
        assertEquals(0, emptyDownloads.size());

        DownloadRequestDTO requestParams = new DownloadRequestDTO();
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

        when(testService.dataQualityService.getEnabledFiltersByLabel(any(DownloadRequestDTO.class))).thenReturn(filters);

        String doiSearchUrl = "https://biocache-test.ala.org.au/occurrences/search?q=lsid%3Aurn%3Alsid%3Abiodiversity.org.au%3Aafd.taxon%3Ae6aff6af-ff36-4ad5-95f2-2dfdcca8caff&disableAllQualityFilters=true&fq=month%3A%2207%22&foo%3Abar&baz%3Aqux";
        when(testService.dataQualityService.convertDataQualityParameters(eq(searchUrl), eq(filters))).thenReturn(doiSearchUrl);

        CreateDoiResponse createDoiResponse = new CreateDoiResponse();
        createDoiResponse.setDoi("10.5555/12345678");
        createDoiResponse.setUuid("ac2ca7ca-9f3a-42af-a840-9c9bd99066b7");
        createDoiResponse.setLandingPage("https://example.org/");
        createDoiResponse.setDoiServiceLandingPage("https://doi.example.org/");
        when(testService.doiService.mintDoi(any(DownloadDoiDTO.class))).thenReturn(createDoiResponse);

        when(testService.searchDAO.writeResultsFromIndexToStream(any(), any(), any(),  any(), anyBoolean(), any())).thenReturn(new DownloadHeaders(new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}));

        DownloadDetailsDTO dd = new DownloadDetailsDTO(requestParams, null, "::1", "", DownloadType.RECORDS_INDEX);
        testService.add(dd);
        Thread.sleep(5000);

        verify(testService.emailService).sendEmail(
                requestParams.getEmail(),
                "ALA Occurrence Download Complete - data",
                "");

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

        when(testService.searchDAO.writeResultsFromIndexToStream(any(), any(), any(),  any(), anyBoolean(), any())).thenReturn(new DownloadHeaders(new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}, new String[] {}));
        when(testService.dataQualityService.convertDataQualityParameters(any(), any())).thenAnswer(returnsFirstArg());

        testService.support = "support@ala.org.au";
        testService.myDownloadsUrl = "https://dev.ala.org.au/myDownloads";
        testService.biocacheDownloadUrl = "http://dev.ala.org.au/biocache-download";
        testService.biocacheDownloadEmailTemplate = "/tmp/download-email.html";
        testService.biocacheDownloadDoiReadmeTemplate = "/tmp/readme.txt";

        // delete download cache
        File cache = new File("/tmp/cache");
        if (cache.exists()) {
            FileUtils.deleteDirectory(cache);
        }

        testService.init();
        Thread.sleep(500);
        List<DownloadDetailsDTO> emptyDownloads = testService.getCurrentDownloads();
        assertEquals(0, emptyDownloads.size());

        DownloadRequestDTO requestParams = new DownloadRequestDTO();
        requestParams.setEmailNotify(false);
        requestParams.setDisplayString("[all records]");

        DownloadDetailsDTO dd = new DownloadDetailsDTO(requestParams, null, "::1", "", DownloadType.RECORDS_INDEX);
        testService.add(dd);
        Thread.sleep(5000);

        verify(testService.emailService, times(0)).sendEmail(any(), any(), any());
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
        // empty cached download directory, in case it is messing up tests
        try {
            File dir = new File(testCacheDir.toAbsolutePath().toString());
            if (dir.exists() && dir.isDirectory()) {
                for (File f : dir.listFiles()) {
                    f.delete();
                }
            }
        } catch (Exception ignored) {
        }

        DownloadService testService = new DownloadService() {
        };

        testService.downloadQualityFiltersTemplate = new ClassPathResource("download-email-quality-filter-snippet.html");
        testService.biocacheDownloadDir = testDownloadDir.toAbsolutePath().toString();
        testService.persistentQueueDAO = persistentQueueDAO;

        testService.doiService = mock(DoiService.class);
        testService.searchDAO = mock(SearchDAO.class);
        testService.indexDao = mock(IndexDAO.class);
        testService.objectMapper = new ObjectMapper();
        testService.loggerService = mock(LoggerService.class);
        AbstractMessageSource messageSource = new ReloadableResourceBundleMessageSource();
        messageSource.setUseCodeAsDefaultMessage(true);
        testService.messageSource = messageSource;
        EmailService emailService = mock(EmailService.class);
        testService.emailService = emailService;
        testService.dataQualityService = mock(DataQualityService.class);

        return testService;
    }
}
