package au.org.ala.biocache.dao;

import java.util.List;

import au.org.ala.biocache.dto.DownloadDetailsDTO;

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
     * Limited by the optional maxRecords and type, return the next offline download from the queue. Leaving it on the
     * queue until a remove is called.
     *
     * @param maxRecords null to ignore
     * @param type null to ignore
     * @return
     */
    DownloadDetailsDTO getNextDownload(Integer maxRecords, DownloadDetailsDTO.DownloadType type);

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

    DownloadDetailsDTO isInQueue(DownloadDetailsDTO dd);
}
