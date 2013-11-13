package org.ala.biocache.dao;

import java.util.List;

import org.ala.biocache.dto.DownloadDetailsDTO;

/**
 * A DAO for a persistent FIFO QUEUE.  To be used to persist downloads
 * independent to the service running.
 * 
 * @author Natasha Carter (natasha.carter@csiro.au)
 */
public interface PersistentQueueDAO {
    /**
     * Adds the supplied download to the offline queue
     * @param download
     */
    void addDownloadToQueue(DownloadDetailsDTO download);
    /**
     * Return the next offline download from the queue. Leaving it on the
     * queue until a remove is called.
     * @return
     */
    DownloadDetailsDTO getNextDownload();
    /**
     * Returns the total number of download that are on the queue
     * @return
     */
    int getTotalDownloads();
    /**
     * Removes the supplied download from the queue
     * @param download
     */
    void removeDownloadFromQueue(DownloadDetailsDTO download);
    /**
     * Returns a list of all the offline downloads in the order in which they were requested.
     * @return
     */
    List<DownloadDetailsDTO> getAllDownloads();
    /**
     * Refreshes the list from the persistent data store
     */
    void refreshFromPersistent();
}
