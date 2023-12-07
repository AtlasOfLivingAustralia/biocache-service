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
import au.org.ala.biocache.dto.DownloadRequestDTO;
import au.org.ala.biocache.util.AlaUserProfileDeserializer;
import au.org.ala.ws.security.profile.AlaUserProfile;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

    @Value("${download.cache.dir:/data/cache/downloads}")
    protected String cacheDirectory="/data/cache/downloads";

    private static final String FILE_PREFIX = "offline";

    @Value("${download.dir:/data/biocache-download}")
    protected String biocacheDownloadDir;

    private final ObjectMapper jsonMapper = new ObjectMapper();

    private final Queue<DownloadDetailsDTO> offlineDownloadList = new LinkedBlockingQueue<>();


    @PostConstruct
    @Override
    public void init() {
        jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // add deserializer for AlaUserProfile
        SimpleModule module = new SimpleModule();
        module.addDeserializer(AlaUserProfile.class, new AlaUserProfileDeserializer());
        jsonMapper.registerModule(module);

        File file = new File(cacheDirectory);
        try {
            FileUtils.forceMkdir(file);
        } catch (IOException e) {
            logger.error("Unable to construct cache directory with correct permissions.", e);
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
     * @see au.org.ala.biocache.dao.PersistentQueueDAO#add(DownloadDetailsDTO)
     */
    @Override
    public void add(DownloadDetailsDTO download) throws IOException {
        File f = getFile(download.getStartTime());
        jsonMapper.writeValue(f, download);

        //give a place for the downlaod
        UUID emailUUID = UUID.nameUUIDFromBytes(download.getRequestParams().getEmail().getBytes(StandardCharsets.UTF_8));
        long startTime = download.getStartTime();
        DownloadRequestDTO requestParams = download.getRequestParams();
        String file = requestParams.getFile();
        download.setFileLocation(biocacheDownloadDir + File.separator + emailUUID + File.separator + startTime + File.separator + file + ".zip");

        offlineDownloadList.add(download);
    }

    /**
     * @see au.org.ala.biocache.dao.PersistentQueueDAO#remove(au.org.ala.biocache.dto.DownloadDetailsDTO)
     */
    @Override
    public boolean remove(DownloadDetailsDTO download) {
        logger.debug("Removing the download from the queue");

        try {
            // delete it from memory
            offlineDownloadList.remove(download);

            // delete it from the directory
            File f = getFile(download.getStartTime());
            if (f.exists()) {
                if (logger.isInfoEnabled()) {
                    logger.info("Deleting " + f.getAbsolutePath() + " " + f.exists());
                }
                FileUtils.deleteQuietly(f);

                return true;
            }
        } catch (Exception e) {
            logger.error("failed to delete: " + download.getStartTime() + ", " + e.getMessage());
        }
        return false;
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
     * @return
     * @see au.org.ala.biocache.dao.PersistentQueueDAO#refreshFromPersistent()
     */
    @Override
    public Queue<DownloadDetailsDTO> refreshFromPersistent() {
        Queue<DownloadDetailsDTO> fromPersistent = new LinkedBlockingQueue<>();

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
                        // Ensure that previously partially downloaded files get their downloads
                        // reattempted by making them available for download again and removing
                        // any partial files that already exist for it
                        String previousFileLocation = dd.getFileLocation();
                        dd.setFileLocation(null);
                        if (previousFileLocation != null) {
                            FileUtils.deleteQuietly(new File(previousFileLocation));
                        }
                        fromPersistent.add(dd);
                    } catch (Exception e) {
                        logger.error("Unable to load cached download " + f.getAbsolutePath(), e);
                    }
                }
            }
        }
        return fromPersistent;
    }

    /**
     * @see au.org.ala.biocache.dao.PersistentQueueDAO#isInQueue(au.org.ala.biocache.dto.DownloadDetailsDTO dd)
     */
    @Override
    public DownloadDetailsDTO isInQueue(DownloadDetailsDTO dd) {
        for (DownloadDetailsDTO d : offlineDownloadList) {
            if (d.getRequestParams().getEmail().equalsIgnoreCase(d.getRequestParams().getEmail()) &&
                    d.getRequestParams().toString().equalsIgnoreCase(dd.getRequestParams().toString())) {
                return d;
            }
        }

        // if we reached here it was not found
        return null;
    }
}
