/**************************************************************************
 *  Copyright (C) 2013 Atlas of Living Australia
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
package au.org.ala.biocache.util;

import au.org.ala.biocache.dto.PointType;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * A cache of points and colours for WMS.
 *
 * Cache size defaults can overridden in biocache-config.properties or directly at runtime.
 *
 * Management of the cache size not exact.
 *
 * @author Adam
 */
@Component("WMSCache")
public class WMSCache {

    private final Logger logger = Logger.getLogger(WMSCache.class);
    //max size of cached params in bytes

    @Value("${wms.cache.size.max:104857600}")
    long maxCacheSize;
    //min size of cached params in bytes
    @Value("${wms.cache.size.min:52428800}")
    long minCacheSize;
    //max age of any one object in the cache in ms
    @Value("${wms.cache.age.max:3600000}")
    long maxAge;
    //in memory store of params
    ConcurrentHashMap<String, WMSTile> cache = new ConcurrentHashMap<String, WMSTile>();
    //cache size management
    final Object counterLock = new Object();
    long cacheSize;
    CountDownLatch counter;
    //thread for cache size limitation
    final Thread cacheCleaner;
    //lock on get operation
    final Object getLock = new Object();
    //cache size before cleaner is triggered
    long triggerCleanSize = minCacheSize + (maxCacheSize - minCacheSize) / 2;

    {
        counter = new CountDownLatch(1);

        cacheCleaner = new Thread() {

            @Override
            public void run() {
                try {
                    while (true) {
                        counter.await();

                        synchronized (counterLock) {
                            cacheSize = minCacheSize;
                            counter = new CountDownLatch(1);
                        }

                        cleanCache();
                    }
                } catch (InterruptedException e) {
                } catch (Exception e) {
                    logger.error("wms cache cleaner stopping unexpectedly", e);
                }
            }
        };
        cacheCleaner.start();

        logger.info("maxCacheSize > " + maxCacheSize);
        logger.info("minCacheSize > " + minCacheSize);
        logger.info("maxAge > " + maxAge);
    }

    /**
     * Store search params and return key.
     *
     * @param q Search url params to store as String.
     * @param colourMode to store as String
     * @param pointType resolution of data to store as PointType
     * @param wco data to store as WMSTile
     * @return true when successfully added to the cache.  WMSCache must be 
     * enabled, not full.  wco must be not too large and not cause the cache
     * to exceed max size when added.
     */
    public boolean put(String q, String colourMode, PointType pointType, WMSTile wco) {
        if (isFull() || !isEnabled()) {
            return false;
        }

        wco.updateSize();

        synchronized (counterLock) {
            if (cacheSize + wco.getSize() > maxCacheSize) {
                return false;
            }
            cache.put(getKey(q, colourMode, pointType), wco);
            cacheSize += wco.getSize();
            logger.debug("new cache size: " + cacheSize);
            updateTriggerCleanSize();
            if (cacheSize > triggerCleanSize) {
                counter.countDown();
            }
        }

        wco.setCached(true);

        return true;
    }

    /**
     * cache key built from query, colourmode and point type.
     *
     * @param query
     * @param colourmode
     * @param pointType
     * @return cache key as String
     */
    public String getKey(String query, String colourmode, PointType pointType) {
        return query + "|" + colourmode + "|" + pointType.getLabel();
    }

    /**
     * Get a WMSTile or an empty lockable WMSTile. Used to avoid index queries for the same information.
     *
     * @param query Search url params to store as String.
     * @param colourmode colourmode as String
     * @param pointType data resolution as PointType
     * @return WMSTile that can be in varying states:
     * - being filled when !getCached() and isCacheable() and is locked
     * - will not be filled when !isCacheable()
     * - ready to use when getCached()
     */
    public WMSTile get(String query, String colourmode, PointType pointType) {
        String key = getKey(query, colourmode, pointType);
        WMSTile obj = null;
        synchronized (getLock) {
            obj = cache.get(key);

            if (obj != null && obj.getCreated() + maxAge < System.currentTimeMillis()) {
                cache.remove(key);
                obj = null;
            }

            if (obj == null) {
                obj = new WMSTile();
                cache.put(key, obj);
            }
        }

        if (obj != null) {
            obj.lastUse = System.currentTimeMillis();
        }

        return obj;
    }

    /**
     * Get a WMSTile without returning an empty, lockable WMSTile. 
     *
     * @param query
     * @param colourmode
     * @param pointType
     * @return null if no tile found
     */
    public WMSTile getTest(String query, String colourmode, PointType pointType) {
        synchronized (getLock) {
            return cache.get(getKey(query, colourmode, pointType));
        }
    }

    /**
     * empty the cache to <= minCacheSize
     */
    void cleanCache() {
        updateTriggerCleanSize();
                
        List<Entry<String, WMSTile>> entries = new ArrayList<>(cache.entrySet());

        //sort ascending by last use time
        Collections.sort(entries, new Comparator<Entry<String, WMSTile>>() {

            @Override
            public int compare(Entry<String, WMSTile> o1, Entry<String, WMSTile> o2) {
                long c = o1.getValue().lastUse - o2.getValue().lastUse;
                return (c < 0) ? -1 : ((c > 0) ? 1 : 0);
            }
        });

        long size = 0;
        int numberRemoved = 0;
        for (int i = 0; i < entries.size(); i++) {
            if (size + entries.get(i).getValue().getSize() > minCacheSize) {
                String key = entries.get(i).getKey();
                cache.remove(key);
                numberRemoved++;
            } else {
                size += entries.get(i).getValue().getSize();
            }
        }

        synchronized (counterLock) {
            cacheSize -= (minCacheSize - size);
            size = cacheSize;
        }
        logger.debug("removed " + numberRemoved + " cached wms points, new cache size " + size);
    }

    /**
     * WMSCache is enabled
     *
     * @return true when WMSCache is enabled
     */
    public boolean isEnabled() {
        return maxCacheSize > 0;
    }

    /**
     * empty the WMSCache
     */
    public void empty() {
        synchronized (counterLock) {
            cacheSize = 0;
            counter = new CountDownLatch(1);
            cache.clear();
        }
    }

    /**
     * remove a specific cache entry.
     *
     * @param q Search url params to store as String.
     * @param colourMode to store as String
     * @param pointType resolution of data to store as PointType
     */
    public void remove(String q, String colourMode, PointType pointType) {
        cache.remove(getKey(q, colourMode, pointType));
    }

    /**
     * Test if cache is full.
     *
     * Note: all put requests into the cache will fail should it be full.
     *
     * @return
     */
    public boolean isFull() {
        return cacheSize >= maxCacheSize;
    }

    public void setMaxCacheSize(long sizeInBytes) {
        maxCacheSize = sizeInBytes;
        updateTriggerCleanSize();
    }

    public long getMaxCacheSize() {
        return maxCacheSize;
    }

    public void setMinCacheSize(long sizeInBytes) {
        minCacheSize = sizeInBytes;
        updateTriggerCleanSize();
    }

    public long getMinCacheSize() {
        return minCacheSize;
    }

    long getSize() {
        return cacheSize;
    }

    public long getMaxCacheAge() {
        return maxAge;
    }

    public void setMaxCacheAge(long maxCacheAge) {
        maxAge = maxCacheAge;
    }


    /**
     * cache cleaner is triggered when the size of the cache is
     * half way between the min and max size.
     */
    void updateTriggerCleanSize() {
        triggerCleanSize = minCacheSize + (maxCacheSize - minCacheSize) / 2;
        logger.debug("triggerCleanSize=" + triggerCleanSize + " minCacheSize=" + minCacheSize + " maxCacheSize=" + maxCacheSize);
    }
}
