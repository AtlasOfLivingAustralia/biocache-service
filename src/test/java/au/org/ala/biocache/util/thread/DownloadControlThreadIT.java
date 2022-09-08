package au.org.ala.biocache.util.thread;

import au.org.ala.biocache.dao.JsonPersistentQueueDAOImpl;
import au.org.ala.biocache.dao.PersistentQueueDAO;
import au.org.ala.biocache.dto.DownloadDetailsDTO;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import au.org.ala.biocache.dto.DownloadRequestDTO;
import au.org.ala.biocache.dto.FacetThemes;
import au.org.ala.ws.security.AlaUser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class DownloadControlThreadIT {

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
                if (testRunningThread.isAlive()) {
                    testRunningThread.interrupt();
                }
            }
        }
    }

    @Test
    public final void testRunSingle() throws Exception {
        int count = 1;
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 1;
        Long pollDelayMs = 0L;
        Long executionDelayMs = 5L;
        Integer threadPriority = Thread.NORM_PRIORITY;
        boolean randomlyFail = false;

        runTestInternal(count, maxRecords, downloadType, concurrencyLevel, pollDelayMs, executionDelayMs,
                threadPriority, randomlyFail);
    }

    @Test
    public final void testRunMultipleWithSingleConcurrency() throws Exception {
        int count = 50;
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 1;
        Long pollDelayMs = 0L;
        Long executionDelayMs = 5L;
        Integer threadPriority = Thread.NORM_PRIORITY;
        boolean randomlyFail = false;

        runTestInternal(count, maxRecords, downloadType, concurrencyLevel, pollDelayMs, executionDelayMs,
                threadPriority, randomlyFail);
    }

    @Test
    public final void testRunMultipleWithMultipleConcurrency() throws Exception {
        int count = 50;
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 4;
        Long pollDelayMs = 0L;
        Long executionDelayMs = 5L;
        Integer threadPriority = Thread.NORM_PRIORITY;
        boolean randomlyFail = false;

        runTestInternal(count, maxRecords, downloadType, concurrencyLevel, pollDelayMs, executionDelayMs,
                threadPriority, randomlyFail);
    }

    @Test
    public final void testRunMultipleWithMultipleConcurrencyMinPriority() throws Exception {
        int count = 50;
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 4;
        Long pollDelayMs = 0L;
        Long executionDelayMs = 5L;
        Integer threadPriority = Thread.MIN_PRIORITY;
        boolean randomlyFail = false;

        runTestInternal(count, maxRecords, downloadType, concurrencyLevel, pollDelayMs, executionDelayMs,
                threadPriority, randomlyFail);
    }

    @Test
    public final void testRunMultipleWithMultipleConcurrencyRandomFailures() throws Exception {
        int count = 50;
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 4;
        Long pollDelayMs = 0L;
        Long executionDelayMs = 5L;
        Integer threadPriority = Thread.NORM_PRIORITY;
        boolean randomlyFail = true;

        runTestInternal(count, maxRecords, downloadType, concurrencyLevel, pollDelayMs, executionDelayMs,
                threadPriority, randomlyFail);
    }

    @Test
    public final void testRunMultipleWithMultipleHighConcurrency() throws Exception {
        int count = 50;
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 40;
        Long pollDelayMs = 0L;
        Long executionDelayMs = 5L;
        Integer threadPriority = Thread.NORM_PRIORITY;
        boolean randomlyFail = false;

        runTestInternal(count, maxRecords, downloadType, concurrencyLevel, pollDelayMs, executionDelayMs,
                threadPriority, randomlyFail);
    }

    @Test
    public final void testRunMultipleWithMultipleHighConcurrencyRandomFailures() throws Exception {
        int count = 50;
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 40;
        Long pollDelayMs = 0L;
        Long executionDelayMs = 5L;
        Integer threadPriority = Thread.NORM_PRIORITY;
        boolean randomlyFail = true;

        runTestInternal(count, maxRecords, downloadType, concurrencyLevel, pollDelayMs, executionDelayMs,
                threadPriority, randomlyFail);
    }

    @Test
    public final void testRunMultipleWithMultipleOverEngineeredConcurrency() throws Exception {
        int count = 50;
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 100;
        Long pollDelayMs = 0L;
        Long executionDelayMs = 5L;
        Integer threadPriority = Thread.NORM_PRIORITY;
        boolean randomlyFail = false;

        runTestInternal(count, maxRecords, downloadType, concurrencyLevel, pollDelayMs, executionDelayMs,
                threadPriority, randomlyFail);
    }

    @Test
    public final void testRunMultipleWithMultipleOverEngineeredConcurrencyRandomFailures() throws Exception {
        int count = 50;
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 100;
        Long pollDelayMs = 0L;
        Long executionDelayMs = 5L;
        Integer threadPriority = Thread.NORM_PRIORITY;
        boolean randomlyFail = true;

        runTestInternal(count, maxRecords, downloadType, concurrencyLevel, pollDelayMs, executionDelayMs,
                threadPriority, randomlyFail);
    }

    @Test
    public final void testRunMultipleWithMultipleOverEngineeredConcurrencyPollDelay() throws Exception {
        int count = 50;
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 100;
        Long pollDelayMs = 100L;
        Long executionDelayMs = 5L;
        Integer threadPriority = Thread.NORM_PRIORITY;
        boolean randomlyFail = false;

        runTestInternal(count, maxRecords, downloadType, concurrencyLevel, pollDelayMs, executionDelayMs,
                threadPriority, randomlyFail);
    }

    @Test
    public final void testRunMultipleWithMultipleOverEngineeredConcurrencyPollDelayRandomFailures() throws Exception {
        int count = 50;
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 100;
        Long pollDelayMs = 100L;
        Long executionDelayMs = 5L;
        Integer threadPriority = Thread.NORM_PRIORITY;
        boolean randomlyFail = true;

        runTestInternal(count, maxRecords, downloadType, concurrencyLevel, pollDelayMs, executionDelayMs,
                threadPriority, randomlyFail);
    }

    @Test
    public final void testRunMultipleWithMultipleConcurrencyPollDelay() throws Exception {
        int count = 50;
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 4;
        Long pollDelayMs = 100L;
        Long executionDelayMs = 5L;
        Integer threadPriority = Thread.NORM_PRIORITY;
        boolean randomlyFail = false;

        runTestInternal(count, maxRecords, downloadType, concurrencyLevel, pollDelayMs, executionDelayMs,
                threadPriority, randomlyFail);
    }

    @Test
    public final void testRunMultipleWithMultipleConcurrencyPollDelayRandomFailures() throws Exception {
        int count = 50;
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 4;
        Long pollDelayMs = 100L;
        Long executionDelayMs = 5L;
        Integer threadPriority = Thread.NORM_PRIORITY;
        boolean randomlyFail = true;

        runTestInternal(count, maxRecords, downloadType, concurrencyLevel, pollDelayMs, executionDelayMs,
                threadPriority, randomlyFail);
    }

    @Test
    public final void testRunMultipleWithMultipleConcurrencyPollDelayExecutionDelay() throws Exception {
        int count = 50;
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 4;
        Long pollDelayMs = 100L;
        Long executionDelayMs = 50L;
        Integer threadPriority = Thread.NORM_PRIORITY;
        boolean randomlyFail = false;

        runTestInternal(count, maxRecords, downloadType, concurrencyLevel, pollDelayMs, executionDelayMs,
                threadPriority, randomlyFail);
    }

    @Test
    public final void testRunMultipleWithMultipleConcurrencyPollDelayExecutionDelayRandomFailures() throws Exception {
        int count = 50;
        Integer maxRecords = null;
        DownloadType downloadType = null;
        int concurrencyLevel = 4;
        Long pollDelayMs = 100L;
        Long executionDelayMs = 50L;
        Integer threadPriority = Thread.NORM_PRIORITY;
        boolean randomlyFail = true;

        runTestInternal(count, maxRecords, downloadType, concurrencyLevel, pollDelayMs, executionDelayMs,
                threadPriority, randomlyFail);
    }

    private void runTestInternal(int count, Integer maxRecords, DownloadType downloadType, int concurrencyLevel,
            Long pollDelayMs, Long executionDelayMs, Integer threadPriority, final boolean randomlyFail)
            throws InterruptedException {
        final CountDownLatch beforeLatch = new CountDownLatch(1);
        final CountDownLatch afterLatch = new CountDownLatch(count);
        final Queue<DownloadDetailsDTO> currentDownloads = new LinkedBlockingQueue<>();

        DownloadCreator downloadCreator = new DownloadCreator() {
            @Override
            public Callable<DownloadDetailsDTO> createCallable(final DownloadDetailsDTO nextDownload,
                    final long executionDelay, final Semaphore capacitySemaphore,
                    ExecutorService parallelQueryExecutor) {
                return new Callable<DownloadDetailsDTO>() {
                    boolean willFail = randomlyFail ? Math.random() > 0.5 : false;

                    @Override
                    public DownloadDetailsDTO call() throws Exception {
                        try {
                            beforeLatch.await();
                            Thread.sleep(executionDelay + Math.round(Math.random() * executionDelay));

                            if (willFail) {
                                throw new Exception("Download failed, but this must not fail to have it removed");
                            }
                            return nextDownload;
                        } finally {
                            try {
                                // Signal download complete by removing from
                                // queue
                                currentDownloads.remove(nextDownload);
                                persistentQueueDAO.removeDownloadFromQueue(nextDownload);
                            } finally {
                                try {
                                    capacitySemaphore.release();
                                } finally {
                                    afterLatch.countDown();
                                }
                            }
                        }
                    }
                };
            }
        };
        ExecutorService parallelQueryExecutor = Executors.newSingleThreadExecutor();
        testDownloadControlThread = new DownloadControlThread("download-test-thread", maxRecords, downloadType, concurrencyLevel, pollDelayMs,
                executionDelayMs, threadPriority, currentDownloads, downloadCreator, persistentQueueDAO,
                parallelQueryExecutor);

        testRunningThread = new Thread(testDownloadControlThread);
        testRunningThread.start();
        for (int i = 0; i < count; i++) {
            DownloadDetailsDTO nextDownload = new DownloadDetailsDTO(
                    new DownloadRequestDTO(),
                    new AlaUser(),
                    "127.0.0.1",
                    "",
                    DownloadType.RECORDS_INDEX);
            DownloadRequestDTO requestParams = new DownloadRequestDTO();
            requestParams.setEmail("test@csiro.au.example");
            requestParams.setFile("my-download-!@#$%^&*()_+{}|:\"\'\\/" + i + ".txt");
            nextDownload.setRequestParams(requestParams);
            persistentQueueDAO.addDownloadToQueue(nextDownload);
        }
        // Sleep to verify that it doesn't drop through immediately
        Thread.sleep(100);
        assertFalse(persistentQueueDAO.getAllDownloads().isEmpty());
        assertEquals(count, persistentQueueDAO.getAllDownloads().size());
        // Then let it drop through after a short execution delay
        beforeLatch.countDown();
        afterLatch.await();
        Thread.sleep(100);
        // Verify that all completed and are now off the queue
        assertTrue(persistentQueueDAO.getAllDownloads().isEmpty());
    }

}
