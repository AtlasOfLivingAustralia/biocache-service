/**
 * 
 */
package au.org.ala.biocache.service;

import static org.junit.Assert.*;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.support.GenericApplicationContext;

import au.org.ala.biocache.dao.JsonPersistentQueueDAOImpl;
import au.org.ala.biocache.dao.PersistentQueueDAO;
import au.org.ala.biocache.dao.SearchDAOImpl;
import au.org.ala.biocache.dto.DownloadDetailsDTO;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import au.org.ala.biocache.dto.DownloadRequestParams;
import au.org.ala.biocache.dto.FacetThemes;
import au.org.ala.biocache.util.thread.DownloadCreator;

/**
 * Test for {@link DownloadService}
 * 
 * @author Peter Ansell
 */
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
     * {@link au.org.ala.biocache.service.DownloadService#registerDownload(au.org.ala.biocache.dto.DownloadRequestParams, java.lang.String, au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType)}.
     */
    @Test
    public final void testRegisterDownload() throws Exception {
        testService.init();
        DownloadDetailsDTO registerDownload = testService.registerDownload(new DownloadRequestParams(), "::1",
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
        DownloadDetailsDTO registerDownload = testService.registerDownload(new DownloadRequestParams(), "::1",
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
        DownloadDetailsDTO registerDownload = testService.registerDownload(new DownloadRequestParams(), "::1",
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
        DownloadDetailsDTO registerDownload = testService.registerDownload(new DownloadRequestParams(), "::1",
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
        DownloadDetailsDTO registerDownload = testService.registerDownload(new DownloadRequestParams(), "::1",
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
        DownloadDetailsDTO registerDownload = testService.registerDownload(new DownloadRequestParams(), "::1",
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
        DownloadDetailsDTO registerDownload = testService.registerDownload(new DownloadRequestParams(), "::1",
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
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#writeQueryToStream(au.org.ala.biocache.dto.DownloadDetailsDTO, au.org.ala.biocache.dto.DownloadRequestParams, java.lang.String, java.io.OutputStream, boolean, boolean, boolean, boolean)}.
     */
    @Ignore("TODO: Implement me")
    @Test
    public final void testWriteQueryToStreamDownloadDetailsDTODownloadRequestParamsStringOutputStreamBooleanBooleanBooleanBoolean()
            throws Exception {
        fail("Not yet implemented"); // TODO
    }

    /**
     * Test method for
     * {@link au.org.ala.biocache.service.DownloadService#writeQueryToStream(au.org.ala.biocache.dto.DownloadRequestParams, javax.servlet.http.HttpServletResponse, java.lang.String, javax.servlet.ServletOutputStream, boolean, boolean, boolean)}.
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

}
