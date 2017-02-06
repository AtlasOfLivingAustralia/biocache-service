package au.org.ala.biocache.util.thread;

import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import au.org.ala.biocache.dao.PersistentQueueDAO;
import au.org.ala.biocache.dto.DownloadDetailsDTO;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;

/**
 * A Runnable that can be used to schedule records dumps for a particular class of download.
 * 
 * @author Peter Ansell
 */
public class DownloadControlThread implements Runnable {
    private final Integer maxRecords;
    private final DownloadType downloadType;
    private final int concurrencyLevel;
    private final long pollDelay;
    private final long executionDelay;
    private final int threadPriority;
    
    private final DownloadServiceExecutor downloadServiceExecutor;
    private final AtomicBoolean shutdownFlag = new AtomicBoolean(false);
    private final Queue<DownloadDetailsDTO> currentDownloads;
    private final DownloadCreator downloadCreator;
    private final PersistentQueueDAO persistentQueueDAO;
    
    public DownloadControlThread(Integer maxRecords, DownloadType downloadType, int concurrencyLevel, Long pollDelayMs, Long executionDelayMs, Integer threadPriority, Queue<DownloadDetailsDTO> currentDownloads, DownloadCreator downloadCreator, PersistentQueueDAO persistentQueueDAO) {
        this.maxRecords = maxRecords;
        this.downloadType = downloadType;
        this.concurrencyLevel = concurrencyLevel > 0 ? concurrencyLevel : 1;
        this.pollDelay = pollDelayMs != null && pollDelayMs >= 0L ? pollDelayMs : 10L;
        this.executionDelay = executionDelayMs != null && executionDelayMs >= 0L ? executionDelayMs : 0L;
        this.threadPriority = threadPriority != null && threadPriority >= Thread.MIN_PRIORITY && threadPriority <= Thread.MAX_PRIORITY ? threadPriority : Thread.NORM_PRIORITY;
        this.currentDownloads = currentDownloads;
        this.downloadCreator = downloadCreator;
        this.persistentQueueDAO = persistentQueueDAO;
        // Create a dedicated ExecutorService for this thread
        this.downloadServiceExecutor = createExecutor();
    }

    protected DownloadServiceExecutor createExecutor() {
        return new DownloadServiceExecutor(this.maxRecords, this.downloadType, 
                                           this.concurrencyLevel, this.executionDelay, 
                                           this.threadPriority, this.downloadCreator);
    }

    @Override
    public void run() {
        DownloadDetailsDTO currentDownload = null;
        try {
            // Runs until the thread is interrupted, then shuts down all of the downloads under its control before returning
            while (true) {
                if(shutdownFlag.get() || Thread.currentThread().isInterrupted()) {
                    break;
                }
                // Busy wait polling
                // TODO: Convert PersistentQueueDAO to a
                // blocking/interruptible wait to avoid busy wait
                Thread.sleep(pollDelay);
                if(shutdownFlag.get() || Thread.currentThread().isInterrupted()) {
                    break;
                }
                downloadServiceExecutor.reserveCapacity();
                if(shutdownFlag.get() || Thread.currentThread().isInterrupted()) {
                    break;
                }
                currentDownload = persistentQueueDAO.getNextDownload(maxRecords, downloadType);
                if (currentDownload != null) {
                    downloadServiceExecutor.submitDownload(currentDownload);
                } else {
                    // We need to return the capacity we reserved because we don't need to use it
                    downloadServiceExecutor.returnCapacity();
                }
            }
        } catch (InterruptedException | RejectedExecutionException e) {
            Thread.currentThread().interrupt();
        } finally {
            try {
                downloadServiceExecutor.shutdown();
                downloadServiceExecutor.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // Reset the interrupt status before returning
                Thread.currentThread().interrupt();
            } finally {
                downloadServiceExecutor.shutdownNow();
            }
        }
    }
    
    /**
     * Set a flag to indicate that we need to shutdown.
     */
    public void shutdown() {
        shutdownFlag.set(true);
    }
    
    /**
     * A thread to abstract away the details of the ExecutorService/Callable being used. 
     * 
     * @author Peter Ansell
     */
    public class DownloadServiceExecutor {
    
        private final Integer maxRecords;
        private final DownloadType downloadType;
        private final ExecutorService executor;
        private final long executionDelay;
        private final int priority;
        private final DownloadCreator downloadCreator;
        private final Semaphore mySemaphore;
    
        public DownloadServiceExecutor(Integer maxRecords, DownloadType downloadType, int concurrencyLevel, long executionDelay, int threadPriority, DownloadCreator downloadCreator) {
            this.maxRecords = maxRecords;
            this.downloadType = downloadType;
            // Customise the name so they can be identified easily on thread traces in debuggers
            String nameFormat = "biocachedownload-pool-";
            nameFormat += (maxRecords == null ? "nolimit" : maxRecords.toString()) + "-";
            nameFormat += (downloadType == null ? "alltypes" : downloadType.name()) + "-";
            // This will be replaced by a number to identify the individual threads
            nameFormat += "%d";
            this.executionDelay = executionDelay;
            this.priority = threadPriority;
            this.downloadCreator = downloadCreator;
            this.executor = Executors.newFixedThreadPool(concurrencyLevel,
                    new ThreadFactoryBuilder().setNameFormat(nameFormat).setPriority(priority).build());
            this.mySemaphore = new Semaphore(concurrencyLevel);
        }
    
        public void submitDownload(DownloadDetailsDTO nextDownload) throws RejectedExecutionException {
            executor.submit(downloadCreator.createCallable(nextDownload, executionDelay, mySemaphore));
        }
    
        public void shutdown() {
            executor.shutdown();
        }
        
        public void shutdownNow() {
            executor.shutdownNow();
        }
        
        public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            executor.awaitTermination(timeout, unit);
        }
        
        /**
         * This method must be called exactly once before each call to {@link #submitDownload(DownloadDetailsDTO)} or {@link #returnCapacity()}.
         * @throws InterruptedException If the thread is interrupted while waiting.
         */
        public void reserveCapacity() throws InterruptedException {
            mySemaphore.acquire();
        }
        
        /**
         * This method must be called exactly once after a call to {@link #reserveCapacity()} is abandoned without a call to {@link #submitDownload(DownloadDetailsDTO)}.
         */
        public void returnCapacity() {
            mySemaphore.release();
        }
    }
}
