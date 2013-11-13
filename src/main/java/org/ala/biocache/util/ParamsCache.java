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
package org.ala.biocache.util;

import java.io.File;
import java.io.FileFilter;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Manage cache of POST'ed search parameter q in memory and on disk.
 *
 * @author Adam
 */
public class ParamsCache {

    private final static Logger logger = Logger.getLogger(ParamsCache.class);
    //max size of cached params in bytes
    static long MAX_CACHE_SIZE = 104857600;
    //min size of cached params in bytes
    static long MIN_CACHE_SIZE = 52428800;
    //max single cacheable object size
    static long LARGEST_CACHEABLE_SIZE = 5242880;
    //file store for params removed from cache
    static String TEMP_FILE_PATH = System.getProperty("java.io.tmpdir");
    //stored file prefix
    final static String FILE_PREFIX = "qid_";
    //max age of stored files, in ms
    static long MAX_FILE_AGE = 24 * 60 * 60 * 1000;
    //in memory store of params
    static ConcurrentHashMap<Long, ParamsCacheObject> cache = new ConcurrentHashMap<Long, ParamsCacheObject>();
    //last used storage key    
    static Long lastKey = 0L;
    //counter and lock
    final static Object counterLock = new Object();
    static long cacheSize;
    static CountDownLatch counter;
    static long triggerCleanSize = MIN_CACHE_SIZE + (MAX_CACHE_SIZE - MIN_CACHE_SIZE) / 2;
    //thread for cache size limitation
    final static Thread cacheCleaner;
    //settable properties file
    static Properties qidProperties;
    //for saving/loading ParamsCacheObject
    final static ObjectMapper jsonMapper = new ObjectMapper();
    
    public static Pattern qidPattern = Pattern.compile("qid:(\")?[0-9]*(\")?");

    static {
        counter = new CountDownLatch(1);

        cacheCleaner = new Thread() {

            @Override
            public void run() {
                try {
                    while (true) {
                        if(counter != null) counter.await();

                        synchronized (counterLock) {
                            cacheSize = MIN_CACHE_SIZE;
                            counter = new CountDownLatch(1);
                        }

                        cleanCache();

                        deleteOldParamFiles();
                    }
                } catch (InterruptedException e) {
                } catch (Exception e) {
                    logger.error("params cache cleaner stopping", e);
                }
            }
        };
        cacheCleaner.start();

        try {
            qidProperties = new Properties();
            InputStream is = ParamsCache.class.getResourceAsStream("/qid.properties");
            qidProperties.load(is);

            MAX_CACHE_SIZE = Long.parseLong(qidProperties.getProperty("MAX_CACHE_SIZE", String.valueOf(MAX_CACHE_SIZE)));
            MIN_CACHE_SIZE = Long.parseLong(qidProperties.getProperty("MIN_CACHE_SIZE", String.valueOf(MIN_CACHE_SIZE)));
            LARGEST_CACHEABLE_SIZE = Long.parseLong(qidProperties.getProperty("LARGEST_CACHEABLE_SIZE", String.valueOf(LARGEST_CACHEABLE_SIZE)));
            updateTriggerCleanSize();

            TEMP_FILE_PATH = qidProperties.getProperty("TEMP_FILE_PATH", TEMP_FILE_PATH);

            File tfp = new File(TEMP_FILE_PATH);
            if (!tfp.exists() && !tfp.mkdirs()) {
                logger.error("cannot find or create directory for qid files: " + TEMP_FILE_PATH);
            }

            logger.info("MAX_CACHE_SIZE > " + MAX_CACHE_SIZE);
            logger.info("MIN_CACHE_SIZE > " + MIN_CACHE_SIZE);
            logger.info("TEMP_FILE_PATH > " + TEMP_FILE_PATH);
        } catch (Exception e) {
            logger.error("cannot load qid.properties", e);
        }
    }

    /**
     * Store search params and return key.
     *
     * @param q Search parameter q to store.
     * @param displayQ Search display q to store.
     * @return id to retrieve stored value as long.
     */
    public static long put(String q, String displayQ, String wkt, double[] bbox, String[]fqs) throws ParamsCacheSizeException {
        long key = getNextKey();
        ParamsCacheObject pco = new ParamsCacheObject(key, q, displayQ, wkt, bbox, fqs);

        if(pco.getSize() > LARGEST_CACHEABLE_SIZE) {
            throw new ParamsCacheSizeException(pco.getSize());
        }

        save(key, pco);

        while(!put(key, pco)){
            //cache cleaner has been run, safe to try again
        }

        return key;
    }

    /**
     * after adding an object to the cache update the cache size.
     *
     * @param pco
     * @return true if successful. 
     */
    static boolean put(long key, ParamsCacheObject pco) {
        boolean runCleaner = false;
        synchronized (counterLock) {
            logger.debug("new cache size: " + cacheSize);
            if(cacheSize + pco.getSize() > MAX_CACHE_SIZE) {
                //run outside of counterLock
                runCleaner = true;
            } else {
                if (cacheSize + pco.getSize() > triggerCleanSize) {
                    counter.countDown();
                }
                
                cacheSize += pco.getSize();
                cache.put(key, pco);
            }
        }

        if(runCleaner) {
            cleanCache();
            return false;
        }

        return true;
    }

    /**
     * Retrive search parameter object
     * @param key id returned by put as long.
     * @return search parameter q as String, or null if not in memory
     * or in file storage.
     */
    public static ParamsCacheObject get(long key) throws ParamsCacheMissingException {
        ParamsCacheObject obj = cache.get(key);

        if (obj == null) {
            obj = load(key);
        }

        if (obj != null) {
            obj.lastUse = System.currentTimeMillis();
        }
        
        if(obj == null) {
            throw new ParamsCacheMissingException(key);
        }

        return obj;
    }
    /**
     * Retrieves the ParamsCacheObject based on the supplied query string.
     * @param query
     * @return
     * @throws Exception
     */
    public static ParamsCacheObject getParamCacheObjectFromQuery(String query) throws ParamsCacheMissingException{
      if(query.contains("qid:")) {            
          Matcher matcher = ParamsCache.qidPattern.matcher(query);
          long qid = 0;
          if(matcher.find()){              
              String value = matcher.group();              
              qid = Long.parseLong(value.substring(4));
              ParamsCacheObject pco =ParamsCache.get(qid);
              return pco;
          }
      }
      return null;      
    }

    /**
     * get a unique cache record key.
     *
     * @return
     */
    static synchronized long getNextKey() {
        long nextKey = System.currentTimeMillis();

        if (nextKey <= lastKey) {
            nextKey = lastKey + 1;
        }

        return lastKey = nextKey;
    }

    /**
     * delete records from the cache to get cache size <= MIN_CACHE_SIZE
     */
    static synchronized void cleanCache() {
        if(cacheSize < triggerCleanSize) {
            return;
        }
        
        List<Entry<Long, ParamsCacheObject>> entries = new ArrayList(cache.entrySet());

        //sort ascending by last use time
        Collections.sort(entries, new Comparator<Entry<Long, ParamsCacheObject>>() {

            @Override
            public int compare(Entry<Long, ParamsCacheObject> o1, Entry<Long, ParamsCacheObject> o2) {
                long c = o1.getValue().lastUse - o2.getValue().lastUse;
                return (c < 0) ? -1 : ((c > 0) ? 1 : 0);
            }
        });

        long size = 0;
        int numberRemoved = 0;
        for (int i = 0; i < entries.size(); i++) {            
            if (size + entries.get(i).getValue().getSize() > MIN_CACHE_SIZE) {
                long key = entries.get(i).getKey();
                cache.remove(key);
                numberRemoved++;
            } else {
                size += entries.get(i).getValue().getSize();
            }
        }
        //adjust output size correctly
        synchronized(counterLock) {
            cacheSize = cacheSize - (MIN_CACHE_SIZE - size);
            size = cacheSize;
        }
        logger.debug("removed " + numberRemoved + " cached qids, new cache size " + size);
    }

    /**
     * delete save params cache objects that are older than MAX_FILE_AGE.
     *
     */
    static public void deleteOldParamFiles() {
        try {
            File dir = new File(TEMP_FILE_PATH);
            FileFilter ff = new FileFilter() {

                @Override
                public boolean accept(File pathname) {
                    if (pathname.getName().startsWith(FILE_PREFIX)) {
                        try {
                            long l = Long.parseLong(pathname.getName().replace(FILE_PREFIX, "").replace(".json",""));
                            if ((System.currentTimeMillis() - l) > MAX_FILE_AGE) {
                                return true;
                            }
                        } catch (Exception e) {
                        }
                    }
                    return false;
                }
            };
            File[] filesToRemove = dir.listFiles(ff);
            for (File f : filesToRemove) {
                FileUtils.deleteQuietly(f);
                logger.info("removing cached query file: " + f.getName());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * get the filename for a ParamsCacheObject.
     * 
     * @param key
     * @return
     */
    static File getFile(long key) {
        return new File(TEMP_FILE_PATH + File.separator + FILE_PREFIX + key + ".json");
    }

    /**
     * save a ParamsCacheObject to disk
     *
     * @param key
     * @param value
     */
    static void save(long key, ParamsCacheObject value) {
        try {
            File file = getFile(key);
            jsonMapper.writeValue(file, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * load disk stored ParamsCacheObject
     *
     * @param key
     * @return
     * @throws ParamsCacheMissingException
     */
    static ParamsCacheObject load(long key) throws ParamsCacheMissingException {
        ParamsCacheObject value = null;
        File file = getFile(key);
        if(file.exists()) {
            try {
                value = jsonMapper.readValue(file, ParamsCacheObject.class);

                if (value != null && value.getSize() < LARGEST_CACHEABLE_SIZE) {
                    file.setLastModified(System.currentTimeMillis());

                    while(!put(key, value)){
                        //cache cleaner has been run, safe to try again
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            throw new ParamsCacheMissingException(key);
        }

        return value;
    }

    static public void setMaxCacheSize(long sizeInBytes) {
        MAX_CACHE_SIZE = sizeInBytes;
        updateTriggerCleanSize();
    }

    static public long getMaxCacheSize() {
        return MAX_CACHE_SIZE;
    }

    static public void setMinCacheSize(long sizeInBytes) {
        MIN_CACHE_SIZE = sizeInBytes;
        updateTriggerCleanSize();
    }

    static public long getMinCacheSize() {
        return MIN_CACHE_SIZE;
    }

    static public void setLargestCacheableSize(long sizeInBytes) {
        LARGEST_CACHEABLE_SIZE = sizeInBytes;
    }

    static public long getLargestCacheableSize() {
        return LARGEST_CACHEABLE_SIZE;
    }

    static long getSize() {
        return cacheSize;
    }

    static public void setTempFilePath(String path) {
        TEMP_FILE_PATH = path;
    }

    static public String getTempFilePath() {
        return TEMP_FILE_PATH;
    }

    /**
     * cache cleaner is triggered when the size of the cache is
     * half way between the min and max size.
     */
    static void updateTriggerCleanSize() {
        triggerCleanSize = MIN_CACHE_SIZE + (MAX_CACHE_SIZE - MIN_CACHE_SIZE) / 2;
        logger.debug("triggerCleanSize=" + triggerCleanSize + " MIN_CACHE_SIZE=" + MIN_CACHE_SIZE + " MAX_CACHE_SIZE=" + MAX_CACHE_SIZE);
    }

    static public void setMaxFileAge(long ageInMs) {
        MAX_FILE_AGE = ageInMs;
    }

    static public long getMaxFileAge() {
        return MAX_FILE_AGE;
    }
}
