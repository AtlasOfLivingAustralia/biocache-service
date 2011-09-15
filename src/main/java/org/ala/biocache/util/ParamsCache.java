package org.ala.biocache.util;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * Manage cache of POST'ed search parameter q in memory and on disk.
 *
 * @author Adam
 */
public class ParamsCache {
    private final static Logger logger = Logger.getLogger(ParamsCache.class);

    //max number of cached params
    final static int MAX_CACHE_SIZE = 10;
    //number of cached params to remove when reducing the size
    final static int MIN_CACHE_SIZE = 5;
    //file store for params removed from cache
    final static String TEMP_FILE_PATH = System.getProperty("java.io.tmpdir");
    //stored file prefix
    final static String FILE_PREFIX = "params_";
    //max age of stored files, in ms
    final static long MAX_FILE_AGE = 24*60*60*1000;
    //in memory store of params
    static ConcurrentHashMap<Long, ParamsCacheObject> cache = new ConcurrentHashMap<Long, ParamsCacheObject>();
    //last used storage key    
    static Long lastKey = 0L;
    //counter and lock
    static Object counterLock = new Object();
    static CountDownLatch counter = new CountDownLatch(MAX_CACHE_SIZE);
    //thread for cache size limitation
    static Thread cacheCleaner = new Thread() {

        @Override
        public void run() {
            try {
                while(true) {
                    counter.await();

                    synchronized(counterLock) {
                        counter = new CountDownLatch(MAX_CACHE_SIZE - MIN_CACHE_SIZE);
                    }

                    cleanCache();

                    removeOldParamFiles();
                }
            } catch (InterruptedException e) {
                
            }
        }
    };

    /**
     * Store search params and return key.
     *
     * @param q Search parameter q to store.
     * @param displayQ Search display q to store.
     * @return id to retrieve stored value as long.
     */
    public static long put(String q, String displayQ, String wkt, double [] bbox) {
        long key = getNextKey();
        ParamsCacheObject pco = new ParamsCacheObject(key, q, displayQ, wkt, bbox);

        cache.put(key, pco);
        save(key, pco);

        synchronized(counterLock) {
            counter.countDown();
        }

        return key;
    }

    /**
     * Retrive search parameter object
     * @param key id returned by put as long.
     * @return search parameter q as String, or null if not in memory
     * or in file storage.
     */
    public static ParamsCacheObject get(long key) {
        ParamsCacheObject obj = cache.get(key);

        if (obj == null) {
            obj = load(key);
        }

        if (obj != null) {
            obj.lastUse = System.currentTimeMillis();
        }

        return obj;
    }

    static synchronized long getNextKey() {
        long nextKey = System.currentTimeMillis();

        if (nextKey <= lastKey) {
            nextKey = lastKey + 1;
        }

        return lastKey = nextKey;
    }

    static void cleanCache() {
        if (cache.size() > MAX_CACHE_SIZE) {
            List<Entry<Long, ParamsCacheObject>> entries = new ArrayList(cache.entrySet());

            //sort ascending by last use time
            Collections.sort(entries, new Comparator<Entry<Long, ParamsCacheObject>>() {

                @Override
                public int compare(Entry<Long, ParamsCacheObject> o1, Entry<Long, ParamsCacheObject> o2) {
                    long c = o1.getValue().lastUse - o2.getValue().lastUse;
                    return (c < 0) ? -1 : ((c > 0) ? 1 : 0);
                }
            });

            for (int i = MIN_CACHE_SIZE - 1; i >= 0; i--) {
                long key = entries.get(i).getKey();
                cache.remove(key);
            }
        }
    }

    static void removeOldParamFiles() {
        try {
            File dir = new File(TEMP_FILE_PATH);
            FileFilter ff = new FileFilter() {

                @Override
                public boolean accept(File pathname) {
                    if(pathname.getName().startsWith(FILE_PREFIX)) {
                        try {
                            long l = Long.parseLong(pathname.getName().replace(FILE_PREFIX, ""));
                            if((System.currentTimeMillis() - l) > MAX_FILE_AGE) {
                                return true;
                            }
                        } catch (Exception e) {
                        }
                    }
                    return false;
                }
            
            };
            File [] filesToRemove = dir.listFiles(ff);
            for(File f : filesToRemove) {
                FileUtils.deleteQuietly(f);
                logger.info("removing cached query file: " + f.getName());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void save(long key, ParamsCacheObject value) {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(TEMP_FILE_PATH + File.separator + FILE_PREFIX + key));
            oos.writeObject(value.getQ());
            oos.writeObject(value.getDisplayString());
            oos.writeObject(value.getWkt());
            oos.writeObject(value.getBbox());
            oos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static ParamsCacheObject load(long key) {
        ParamsCacheObject value = null;
        try {
            ObjectInputStream oos = new ObjectInputStream(new FileInputStream(TEMP_FILE_PATH + File.separator + FILE_PREFIX + key));
            String q = null;
            String displayString = null;
            String wkt = null;
            double [] bbox = null;
            q = (String) oos.readObject();
            displayString = (String) oos.readObject();
            wkt = (String) oos.readObject();
            bbox = (double []) oos.readObject();

            oos.close();
            
            if(q != null) {
                value = new ParamsCacheObject(key, q, displayString, wkt, bbox);
                cache.put(key, value);
                synchronized(counterLock) {
                    counter.countDown();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return value;
    }
}
