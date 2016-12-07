/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
 *  All Rights Reserved.
 * 
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 * 
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.biocache.dao;

import au.org.ala.biocache.dto.DownloadDetailsDTO;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A queue that stores the Downloads as JSON files in the supplied directory
 * 
 * @author Natasha Carter (natasha.carter@csiro.au)
 */
@Component("persistentQueueDao")
public class JsonPersistentQueueDAOImpl implements PersistentQueueDAO {
	
    /** log4 j logger */
    private static final Logger logger = Logger.getLogger(JsonPersistentQueueDAOImpl.class);
    private String cacheDirectory="/data/cache/downloads";
    private String FILE_PREFIX = "offline";

    @Value("${download.dir:/data/biocache-download}")
    protected String biocacheDownloadDir;

    private final ObjectMapper jsonMapper = new ObjectMapper();
    
    private final Queue<DownloadDetailsDTO> offlineDownloadList = new LinkedBlockingQueue<>();

    private final Object listLock = new Object();
    
    @PostConstruct
    public void init() {
        synchronized (listLock) {
            jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            File file = new File(cacheDirectory);
            try {
                FileUtils.forceMkdir(file);
            } catch (IOException e) {
                logger.error("Unable to construct cache directory.", e);
            }

            refreshFromPersistent();
        }
    }
    /**
     * Returns a file object that represents the a persisted download on the queue
     * @param key
     * @return
     */
    private File getFile(long key) {
        return new File(cacheDirectory +File.separator+ FILE_PREFIX + key + ".json");
    }
    /**
     * @see au.org.ala.biocache.dao.PersistentQueueDAO#addDownloadToQueue(DownloadDetailsDTO)
     */
    @Override
    public void addDownloadToQueue(DownloadDetailsDTO download) {
        synchronized (listLock) {
            offlineDownloadList.add(download);
            File f = getFile(download.getStartTime());
            try {
                jsonMapper.writeValue(f, download);
            } catch (Exception e) {
                logger.error("Unable to cache the download", e);
            }
        }
    }
    /**
     * @see au.org.ala.biocache.dao.PersistentQueueDAO#getNextDownload()
     */
    @Override
    public DownloadDetailsDTO getNextDownload() {
        synchronized (listLock) {
            for (DownloadDetailsDTO dd : offlineDownloadList) {
                if (dd.getFileLocation() == null) {
                    //give a place for the downlaod
                    dd.setFileLocation(biocacheDownloadDir + File.separator + UUID.nameUUIDFromBytes(dd.getEmail().getBytes()) + File.separator + dd.getStartTime() + File.separator + dd.getRequestParams().getFile() + ".zip");
                    return dd;
                }
            }
        }
        
        //if we reached here all of the downloads have started or there are no downloads on the list
        return null;
    }

    /**
     * @see au.org.ala.biocache.dao.PersistentQueueDAO#getNextDownload(Integer maxRecords, au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType type)
     */
    @Override
    public DownloadDetailsDTO getNextDownload(Integer maxRecords, DownloadDetailsDTO.DownloadType type) {
        synchronized (listLock) {
            for (DownloadDetailsDTO dd : offlineDownloadList) {
                if (dd.getFileLocation() == null &&
                        (maxRecords == null || dd.getTotalRecords() <= maxRecords) &&
                        (type == null || dd.getDownloadType().equals(type))) {
                    //give a place for the downlaod
                    dd.setFileLocation(biocacheDownloadDir + File.separator + UUID.nameUUIDFromBytes(dd.getEmail().getBytes()) + File.separator + dd.getStartTime() + File.separator + dd.getRequestParams().getFile() + ".zip");
                    return dd;
                }
            }
        }

        //if we reached here all of the downloads have started or there are no downloads on the list
        return null;
    }
    
    /**
     * @see au.org.ala.biocache.dao.PersistentQueueDAO#getTotalDownloads()
     */
    @Override
    public int getTotalDownloads() {
        synchronized (listLock) {
            return offlineDownloadList.size();
        }
    }
    
    /**
     * @see au.org.ala.biocache.dao.PersistentQueueDAO#removeDownloadFromQueue(au.org.ala.biocache.dto.DownloadDetailsDTO)
     */
    @Override
    public void removeDownloadFromQueue(DownloadDetailsDTO download) {
        synchronized (listLock) {
            logger.debug("Removing the download from the queue");
            // delete it from the directory
            File f = getFile(download.getStartTime());
            logger.info("Deleting " + f.getAbsolutePath() + " " + f.exists());
            FileUtils.deleteQuietly(f);
            offlineDownloadList.remove(download);
            //add the download JSON String to the download directory
        }
        
    }
    
    /**
     * @see au.org.ala.biocache.dao.PersistentQueueDAO#getAllDownloads()
     */
    @Override
    public List<DownloadDetailsDTO> getAllDownloads() {
        List<DownloadDetailsDTO> result = new ArrayList<>(offlineDownloadList);
        return Collections.unmodifiableList(result);
    }
    
    /**
     * @see au.org.ala.biocache.dao.PersistentQueueDAO#refreshFromPersistent()
     */
    @Override
    public void refreshFromPersistent() {
        synchronized (listLock) {
            offlineDownloadList.clear();
            File file = new File(cacheDirectory);
            //load the list with the available downloads ordering by the least recently modified
            File[] files = file.listFiles();
            if (files != null) {
                Arrays.sort(files, new Comparator<File>() {
                    @Override
                    public int compare(File o1, File o2) {
                        return (int) (((File) o1).lastModified() - ((File) o2).lastModified());
                    }
                });

                //value = jsonMapper.readValue(file, ParamsCacheObject.class);
                for (File f : files) {
                    if (f.isFile()) {
                        try {
                            DownloadDetailsDTO dd = jsonMapper.readValue(f, DownloadDetailsDTO.class);
                            offlineDownloadList.add(dd);
                        } catch (Exception e) {
                            logger.error("Unable to load cached download " + f.getAbsolutePath(), e);
                        }
                    }
                }
            }
        }
    }

    /**
     * @see au.org.ala.biocache.dao.PersistentQueueDAO#isInQueue(au.org.ala.biocache.dto.DownloadDetailsDTO dd)
     */
    @Override
    public DownloadDetailsDTO isInQueue(DownloadDetailsDTO dd) {
        synchronized (listLock) {
            for (DownloadDetailsDTO d : offlineDownloadList) {
                if (d.getEmail().equalsIgnoreCase(d.getEmail()) &&
                        d.getDownloadParams().equalsIgnoreCase(dd.getDownloadParams())) {
                    return d;
                }
            }
        }

        //if we reached here it was not found
        return null;
    }
}
