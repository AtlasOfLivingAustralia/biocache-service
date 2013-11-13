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
package org.ala.biocache.dao;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.ala.biocache.dto.DownloadDetailsDTO;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.stereotype.Component;

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
    private String downloadDirectory="/data/biocache-download";

    private final ObjectMapper jsonMapper = new ObjectMapper();
    
    private List<DownloadDetailsDTO> offlineDownloadList;
    
    @PostConstruct
    public void init(){
        offlineDownloadList = Collections.synchronizedList(new ArrayList<DownloadDetailsDTO>());
        File file = new File(cacheDirectory);
        jsonMapper.configure(org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);                
                
        try {
            FileUtils.forceMkdir(file);
        } catch (IOException e) {
            logger.error("Unable to construct cache directory.",e);
        }
        refreshFromPersistent();
        
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
     * @see org.ala.biocache.dao.PersistentQueueDAO#addDownloadToQueue(DownloadDetailsDTO)
     */
    @Override
    public void addDownloadToQueue(DownloadDetailsDTO download) {
        offlineDownloadList.add(download);
        File f = getFile(download.getStartTime());
        try{
            jsonMapper.writeValue(f, download);
        } catch(Exception e){
            logger.error("Unable to cache the download", e);
        }
        
    }
    /**
     * @see org.ala.biocache.dao.PersistentQueueDAO#getNextDownload()
     */
    @Override
    public DownloadDetailsDTO getNextDownload() {
        if(offlineDownloadList.size()>0){
            for(DownloadDetailsDTO dd: offlineDownloadList){
                if(dd.getFileLocation() == null){
                    //give a place for the downlaod
                    dd.setFileLocation(downloadDirectory+File.separator+UUID.nameUUIDFromBytes(dd.getEmail().getBytes())+File.separator +dd.getStartTime()+File.separator+dd.getRequestParams().getFile()+".zip");
                    return dd;
                }
            }
        }
        
        //if we reached here all of the downloads have started or there are no downloads on the list
        return null;
    }
    
    /**
     * @see org.ala.biocache.dao.PersistentQueueDAO#getTotalDownloads()
     */
    @Override
    public int getTotalDownloads() {
        return offlineDownloadList.size();
    }
    
    /**
     * @see org.ala.biocache.dao.PersistentQueueDAO#removeDownloadFromQueue(org.ala.biocache.dto.DownloadDetailsDTO)
     */
    @Override
    public void removeDownloadFromQueue(DownloadDetailsDTO download) {
        logger.debug("Removing the download from the queue");
        // delete it from the directory
        File f = getFile(download.getStartTime());
        logger.info("Deleting " + f.getAbsolutePath() + " " + f.exists());        
        FileUtils.deleteQuietly(f);
        offlineDownloadList.remove(download);
        //add the download JSON String to the download directory
        
    }
    
    /**
     * @see org.ala.biocache.dao.PersistentQueueDAO#getAllDownloads()
     */
    @Override
    public List<DownloadDetailsDTO> getAllDownloads() {
        return offlineDownloadList;
    }
    
    /**
     * @see org.ala.biocache.dao.PersistentQueueDAO#refreshFromPersistent()
     */
    @Override
    public void refreshFromPersistent() {
        offlineDownloadList.clear();
        File file = new File(cacheDirectory);
      //load the list with the available downloads ordering by the least recently modified
        File[] files = file.listFiles();
        Arrays.sort(files, new Comparator(){

            @Override
            public int compare(Object o1, Object o2) {
                return (int)(((File)o1).lastModified() - ((File)o2).lastModified());
            }
            
        });
        //value = jsonMapper.readValue(file, ParamsCacheObject.class);
        for(File f :files){
            if(f.isFile()){
                try {
                    DownloadDetailsDTO dd = jsonMapper.readValue(f, DownloadDetailsDTO.class);
                    offlineDownloadList.add(dd);
                } catch(Exception e){
                    logger.error("Unable to load cached downlaod " + f.getAbsolutePath(), e);
                }                
            }
        }        
    }
}
