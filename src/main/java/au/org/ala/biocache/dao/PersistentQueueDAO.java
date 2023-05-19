package au.org.ala.biocache.dao;

import au.org.ala.biocache.dto.DownloadDetailsDTO;

import java.io.IOException;
import java.util.List;
import java.util.Queue;

/**
 * A DAO for a persistent FIFO QUEUE.  To be used to persist downloads
 * independent to the service running.
 *
 * @author Natasha Carter (natasha.carter@csiro.au)
 */
public interface PersistentQueueDAO {

    /**
     * Adds the supplied download to the offline queue
     * @param download The download to add
     */
    void add(DownloadDetailsDTO download) throws IOException;

    /**
     * Removes the supplied download from the queue
     * @param download The download to remove from the queue
     */
    boolean remove(DownloadDetailsDTO download);

    /**
     * Gets a list of all the offline downloads in the order in which they were requested.
     * @return A list of all the downloads in the queue
     */
    List<DownloadDetailsDTO> getAllDownloads();

    /**
     * Refreshes the list from the persistent data store
     *
     * @return
     */
    Queue<DownloadDetailsDTO> refreshFromPersistent();

    /**
     * Check if the given download is still in the queue.
     *
     * @param dd The download to check
     * @return A copy of the download if it is in the queue or <tt>null</tt> otherwise.
     */
    DownloadDetailsDTO isInQueue(DownloadDetailsDTO dd);

    /**
     * Initialises the queue before use, to allow it to setup in-memory caches and start paused downloads.
     */
    void init();
}
