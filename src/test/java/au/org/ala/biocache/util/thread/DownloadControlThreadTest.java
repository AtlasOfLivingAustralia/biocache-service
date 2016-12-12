package au.org.ala.biocache.util.thread;

import static org.junit.Assert.*;

import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import au.org.ala.biocache.dao.JsonPersistentQueueDAOImpl;
import au.org.ala.biocache.dao.PersistentQueueDAO;
import au.org.ala.biocache.dto.DownloadDetailsDTO;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import au.org.ala.biocache.dto.DownloadRequestParams;
import au.org.ala.biocache.dto.FacetThemes;

public class DownloadControlThreadTest {

    @Rule
    public Timeout timeout = new Timeout(30, TimeUnit.SECONDS);
    
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();
    
    private Path testCacheDir;
    private Path testDownloadDir;
    
    private PersistentQueueDAO persistentQueueDAO;
    private DownloadControlThread testDownloadControlThread;

    private Thread testRunningThread;

    @Before
    public void setUp() throws Exception {
        testCacheDir = tempDir.newFolder("downloadcontrolthread-cache").toPath();
        testDownloadDir = tempDir.newFolder("downloadcontrolthread-destination").toPath();        
        persistentQueueDAO = new JsonPersistentQueueDAOImpl() {
            @Override
            public void init() {
                cacheDirectory = testCacheDir.toAbsolutePath().toString();
                biocacheDownloadDir = testDownloadDir.toAbsolutePath().toString();
                super.init();
            }
        };
        persistentQueueDAO.init();
        
        // Every application needs to explicitly initialise static fields in FacetThemes by calling its constructor
        new FacetThemes();
    }

    @After
    public void tearDown() throws Exception {
        try {
            persistentQueueDAO.shutdown();
        } finally {
            try {
                if (testDownloadControlThread != null) {
                    testDownloadControlThread.shutdown();
                }
            } finally {
                if(testRunningThread.isAlive()) {
                    testRunningThread.interrupt();
                }
            }
        }
    }

    @Test
    public final void testRun() throws Exception {
        final CountDownLatch testLatch = new CountDownLatch(1);
        
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 1;
        Long pollDelayMs = 0L;
        Long executionDelayMs = 50L;
        Integer threadPriority = Thread.NORM_PRIORITY;
        final Queue<DownloadDetailsDTO> currentDownloads = new LinkedBlockingQueue<>();
        
        DownloadCreator downloadCreator = new DownloadCreator() {
            @Override
            public Callable<DownloadDetailsDTO> createCallable(final DownloadDetailsDTO nextDownload,
                    final long executionDelay) {
                return new Callable<DownloadDetailsDTO>() {

                    @Override
                    public DownloadDetailsDTO call() throws Exception {
                        testLatch.await();
                        Thread.sleep(executionDelay);
                        // Signal download complete by removing from queue
                        currentDownloads.remove(nextDownload);
                        persistentQueueDAO.removeDownloadFromQueue(nextDownload);
                        return nextDownload;
                    }
                };
            }
        };
        testDownloadControlThread = new DownloadControlThread(maxRecords, downloadType, concurrencyLevel, pollDelayMs,
                executionDelayMs, threadPriority, currentDownloads, downloadCreator, persistentQueueDAO);
        
        testRunningThread = new Thread(testDownloadControlThread);
        testRunningThread.start();
        DownloadDetailsDTO nextDownload = new DownloadDetailsDTO("does-not-exist", "127.0.0.1", DownloadType.RECORDS_DB);
        nextDownload.setEmail("test@csiro.au.example");
        DownloadRequestParams requestParams = new DownloadRequestParams();
        requestParams.setFile("my-download.txt");
        nextDownload.setRequestParams(requestParams);
        persistentQueueDAO.addDownloadToQueue(nextDownload);
        // Sleep to verify that it doesn't drop through immediately
        Thread.sleep(1000);
        assertFalse(persistentQueueDAO.getAllDownloads().isEmpty());
        // Then let it drop through after a short execution delay
        testLatch.countDown();
        Thread.sleep(1000);
        // Verify that it completed and is now off the queue
        assertTrue(persistentQueueDAO.getAllDownloads().isEmpty());
    }

    @Ignore
    @Test
    public final void testShutdown() {
        fail("Not yet implemented"); // TODO
    }

}
