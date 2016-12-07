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
     * @param download The download to add
     */
    void addDownloadToQueue(DownloadDetailsDTO download);
    
    /**
     * Get the next offline download from the queue. Leaving it on the
     * queue until a remove is called.<br>
     * A non-null returned download will contain a non-null {@link DownloadDetailsDTO#getFileLocation()}.
     * @return A {@link DownloadDetailsDTO} or null if the queue was empty
     */
    DownloadDetailsDTO getNextDownload();

    /**
     * Limited by the optional maxRecords and type, return the next offline download from the queue. Leaving it on the
     * queue until a remove is called.<br>
     * A non-null returned download will contain a non-null {@link DownloadDetailsDTO#getFileLocation()}.
     *
     * @param maxRecords An {@link Integer} specifying the maximum records in a download 
     *                   to have it selected, or null to ignore the number of records in a download
     * @param type A {@link DownloadDetailsDTO.DownloadType} to specify a particular type of download, or null to ignore
     * @return A {@link DownloadDetailsDTO} or null if no downloads matched the criteria.
     */
    DownloadDetailsDTO getNextDownload(Integer maxRecords, DownloadDetailsDTO.DownloadType type);

    /**
     * Gets the total number of downloads that are on the queue
     * @return The number of downloads that are currently in the queue
     */
    int getTotalDownloads();
    
    /**
     * Removes the supplied download from the queue
     * @param download The download to remove from the queue
     */
    void removeDownloadFromQueue(DownloadDetailsDTO download);
    
    /**
     * Gets a list of all the offline downloads in the order in which they were requested.
     * @return A list of all the downloads in the queue
     */
    List<DownloadDetailsDTO> getAllDownloads();
    
    /**
     * Refreshes the list from the persistent data store
     */
    void refreshFromPersistent();

    /**
     * Check if the given download is still in the queue.
     * 
     * @param dd The download to check
     * @return A copy of the download if it is in the queue or <tt>null</tt> otherwise.
     */
    DownloadDetailsDTO isInQueue(DownloadDetailsDTO dd);
    
    /**
     * Stops the queue from accepting new downloads using the {@link #addDownloadToQueue(DownloadDetailsDTO)} 
     * method, and returns immediately even if there are still downloads outstanding.<br>
     * Already existing downloads in the queue can still be requested using the accessor methods after shutdown.
     */
    void shutdown();
}
