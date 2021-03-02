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

import au.org.ala.biocache.dto.Qid;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.service.DataQualityService;
import au.org.ala.biocache.util.QidMissingException;
import au.org.ala.biocache.util.QidSizeException;
import au.org.ala.biocache.util.SpatialUtils;
import com.googlecode.ehcache.annotations.Cacheable;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;

/**
 * Manage cache of POST'ed search parameter q in memory and in db.
 *
 * @author Adam
 */
@Component("qidCacheDao")
public class QidCacheDAOImpl implements QidCacheDAO {

    private final Logger logger = Logger.getLogger(QidCacheDAOImpl.class);
    
    /**
     * max size of cached params in bytes
     */
    @Value("${qid.cache.size.max:104857600}")
    long maxCacheSize;
    
    /**
     * min size of cached params in bytes
     */
    @Value("${qid.cache.size.min:52428800}")
    long minCacheSize;
    
    /**
     * max single cacheable object size
     */
    @Value("${qid.cache.largestCacheableSize:5242880}")
    long largestCacheableSize;
    
    /**
     * Limit WKT complexity to reduce index query time for qids.
     */
    @Value("${qid.wkt.maxPoints:5000}")
    private int maxWktPoints;

    /**
     * The simplification factor used to iteratively reduce WKT complexity to reduce index query time for qids between the initial and maximum precisions
     */
    @Value("${qid.wkt.simplification.factor:2.0}")
    private double wktSimplificationFactor;

    /**
     * The initial distance precision value to attempt when reducing WKT complexity to reduce index query time for qids.
     */
    @Value("${qid.wkt.simplification.initialprecision:0.0001}")
    private double wktSimplificationInitialPrecision;

    /**
     * The maximum distance precision value before giving up on iteratively reducing WKT complexity to reduce index query time for qids.
     */
    @Value("${qid.wkt.simplification.maxprecision:10.0}")
    private double wktSimplificationMaxPrecision;

    @Inject
    private DataQualityService dataQualityService;

    /**
     * in memory store of params
     */
    private ConcurrentMap<String, Qid> cache = new ConcurrentHashMap<String, Qid>();
    
    /**
     * counter and lock
     */
    final private Object counterLock = new Object();

    private long cacheSize;
    
    private CountDownLatch counter;

    private long triggerCleanSize = minCacheSize + (maxCacheSize - minCacheSize) / 2;

    /**
     * thread for cache size limitation
     */
    private Thread cacheCleaner;

    @Inject
    private SearchDAO searchDAO;

    @Inject
    private StoreDAO storeDao;

    //protected QidDAO qidDao = (QidDAO) au.org.ala.biocache.Config.getInstance(QidDAO.class);

    /**
     * init
     */
    public QidCacheDAOImpl() {
        counter = new CountDownLatch(1);

        cacheCleaner = new Thread() {

            @Override
            public void run() {
                try {
                    while (true) {
                        if (counter != null) counter.await();

                        synchronized (counterLock) {
                            cacheSize = minCacheSize;
                            counter = new CountDownLatch(1);
                        }

                        cleanCache();
                    }
                } catch (InterruptedException e) {
                } catch (Exception e) {
                    logger.error("params cache cleaner stopping", e);
                }
            }
        };
        cacheCleaner.setName("qid-cache-cleaner");
        cacheCleaner.start();

        try {
            updateTriggerCleanSize();

            logger.info("maxCacheSize > " + maxCacheSize);
            logger.info("minCacheSize > " + minCacheSize);
        } catch (Exception e) {
            logger.error("cannot load qid.properties", e);
        }
    }

    /**
     * Store search params and return key.
     *
     * @param q            Search parameter q to store.
     * @param displayQ     Search display q to store.
     * @param wkt          wkt to store
     * @param bbox         bounding box to store as double array [min longitude, min latitude, max longitude, max latitude]
     * @param fqs          fqs to store
     * @param maxAge       -1 or expected qid life in ms
     * @param source       name of app that created this qid
     * @return id to retrieve stored value as long.
     */
    public String put(String q, String displayQ, String wkt, double[] bbox, String[] fqs, long maxAge, String source) throws QidSizeException {
        Qid qid = new Qid(null, q, displayQ, wkt, bbox, 0L, fqs, maxAge, source);

        if (qid.getSize() > largestCacheableSize) {
            throw new QidSizeException(qid.getSize());
        }

        save(qid);

        while (!put(qid)) {
            //cache cleaner has been run, safe to try again
        }

        return qid.getRowKey();
    }

    /**
     * after adding an object to the cache update the cache size.
     *
     * @param qid
     * @return true if successful.
     */
    boolean put(Qid qid) {
        boolean runCleaner = false;
        synchronized (counterLock) {
            logger.debug("new cache size: " + cacheSize);
            if (cacheSize + qid.getSize() > maxCacheSize) {
                //run outside of counterLock
                runCleaner = true;
                logger.debug("not putting qid");
            } else {
                if (cacheSize + qid.getSize() > triggerCleanSize) {
                    counter.countDown();
                }

                cacheSize += qid.getSize();
                logger.debug("putting qid");
                cache.put(qid.getRowKey(), qid);
            }
        }

        if (runCleaner) {
            logger.debug("cleaning qid cache");
            cleanCache();
            return false;
        }

        return true;
    }

    /**
     * Retrive search parameter object
     *
     * @param key id returned by put as long.
     * @return search parameter q as String, or null if not in memory
     * or in file storage.
     */
    public Qid get(String key) throws QidMissingException {
        Qid obj = cache.get(key);

        if (obj == null) {
            obj = load(key);

            if (obj != null) {
                cache.put(key, obj);

                // remove SOLR escaping of older qid
                if (obj.getQ() != null && obj.getQ().indexOf('\\') >= 0) {
                    obj.setQ(removeSolrEscaping(obj.getQ()));
                }
            }
        }

        if (obj == null) {
            throw new QidMissingException(key);
        } else {
            obj.setLastUse(System.currentTimeMillis());
        }

        return obj;
    }

    private String removeSolrEscaping(String s) {
        if (s == null || s.length() == 0) {
            return s;
        }

        StringBuilder sb = new StringBuilder();

        char a, c = ' ';
        int len = s.length() - 1;
        for(int i = 0; i < len; i++) {
            a = s.charAt(i);
            c = s.charAt(i + 1);
            if(a != '\\' ||
                    !(c == 92 || c == 43 || c == 45 || c == 33 || c == 40 || c == 41 || c == 58 || c == 94 || c == 91 ||
                            c == 93 || c == 34 || c == 123 || c == 125 || c == 126 || c == 42 || c == 63 || c == 124 ||
                            c == 38 || c == 59 || c == 47 || Character.isWhitespace(c))) {
                sb.append(a);
            }
        }
        sb.append(c);

        return sb.toString();
    }

    /**
     * Retrieves the ParamsCacheObject based on the supplied query string.
     *
     * @param query
     * @return
     * @throws Exception
     */
    public Qid getQidFromQuery(String query) throws QidMissingException {
        Qid qid = null;
        if (query.contains("qid:")) {
            Matcher matcher = QidCacheDAOImpl.qidPattern.matcher(query);

            if (matcher.find()) {
                String value = matcher.group();
                qid = get(value.substring(4));
            }
        }
        return qid;
    }

    /**
     * delete records from the cache to get cache size <= minCacheSize
     */
    synchronized void cleanCache() {
        updateTriggerCleanSize();
                
        if (cacheSize < triggerCleanSize) {
            return;
        }

        List<Entry<String, Qid>> entries = new ArrayList<>(cache.entrySet());

        //sort ascending by last use time
        Collections.sort(entries, new Comparator<Entry<String, Qid>>() {

            @Override
            public int compare(Entry<String, Qid> o1, Entry<String, Qid> o2) {
                long c = o1.getValue().getLastUse() - o2.getValue().getLastUse();
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

        //adjust output size correctly
        synchronized (counterLock) {
            cacheSize = cacheSize - (minCacheSize - size);
            size = cacheSize;
        }
        logger.debug("removed " + numberRemoved + " cached qids, new cache size " + size);
    }

    /**
     * save a Qid to db
     *
     * @param value
     */
    void save(Qid value) {
        value.setRowKey(String.valueOf(nextId()));
        try {
            storeDao.put(value.getRowKey(), value);
        } catch (Exception e) {
            logger.error("faild to save qid to db", e);
        }
    }

    /**
     * load db stored Qid
     *
     * @param key
     * @return
     * @throws au.org.ala.biocache.util.QidMissingException
     */
    Qid load(String key) throws QidMissingException {
        try {
            return storeDao.get(Qid.class, key);
        } catch (Exception e) {
            logger.error("failed to find qid:" + key, e);
            throw new QidMissingException(key);
        }
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

    public void setLargestCacheableSize(long sizeInBytes) {
        largestCacheableSize = sizeInBytes;
    }

    public long getLargestCacheableSize() {
        return largestCacheableSize;
    }

    public long getSize() {
        return cacheSize;
    }

    /**
     * cache cleaner is triggered when the size of the cache is
     * half way between the min and max size.
     */
    void updateTriggerCleanSize() {
        triggerCleanSize = minCacheSize + (maxCacheSize - minCacheSize) / 2;
        logger.debug("triggerCleanSize=" + triggerCleanSize + " minCacheSize=" + minCacheSize + " maxCacheSize=" + maxCacheSize);
    }

    public String[] getFq(SpatialSearchRequestParams requestParams) {
        int requestParamsFqLength = requestParams.getFq() != null ? requestParams.getFq().length : 0;

        String[] qidFq = null;
        int qidFqLength = 0;
        String q = requestParams.getQ();
        if (q.startsWith("qid:")) {
            try {
                qidFq = get(q.substring(4)).getFqs();
                if (qidFq != null) {
                    qidFqLength = qidFq.length;
                }
            } catch (Exception e) {
            }
        }

        if (requestParamsFqLength + qidFqLength == 0) {
            return null;
        }

        String[] allFqs = new String[requestParamsFqLength + qidFqLength];

        if (requestParamsFqLength > 0) {
            System.arraycopy(requestParams.getFq(), 0, allFqs, 0, requestParamsFqLength);
        }

        if (qidFqLength > 0) {
            System.arraycopy(qidFq, 0, allFqs, requestParamsFqLength, qidFqLength);
        }

        return allFqs;
    }

    @Cacheable(cacheName = "qidGeneration")
    @Override
    public String generateQid(SpatialSearchRequestParams requestParams, String bbox, String title, Long maxage, String source) {
        try {
            //simplify wkt
            String wkt = requestParams.getWkt();
            if (wkt != null && wkt.length() > 0) {
                //TODO: Is this too slow? Do not want to send large WKT to SOLR.
                wkt = fixWkt(wkt);

                if (wkt == null) {
                    //wkt too large and simplification failed, do not produce qid
                    return null;
                }

                //set wkt
                requestParams.setWkt(wkt);
            }

            //get bbox (also cleans up Q)
            double[] bb = null;
            if (bbox != null && bbox.equals("true")) {
                bb = searchDAO.getBBox(requestParams);
            } else {
                //get a formatted Q by running a query
                requestParams.setPageSize(0);
                requestParams.setFacet(false);
                searchDAO.findByFulltext(requestParams);
            }

            //store the title if necessary
            if (title == null) {
                title = requestParams.getDisplayString();
            }
            String[] fqs = dataQualityService.generateCombinedFqs(requestParams);
            if (fqs.length == 0 || (fqs.length == 1 && fqs[0].length() == 0)) {
                fqs = null;
            }
            String qid = put(requestParams.getQ(), title, requestParams.getWkt(), bb, fqs, maxage, source);

            return qid;
        } catch (Exception e) {
            logger.error("Error generating QID for q = " + requestParams.getQ() + ", fq = " + requestParams.getFq(), e);
        }
        return null;
    }

    @Cacheable(cacheName = "fixWkt")
    private String fixWkt(String wkt) {
        return SpatialUtils.simplifyWkt(wkt, maxWktPoints, wktSimplificationFactor, wktSimplificationInitialPrecision, wktSimplificationMaxPrecision);
    }

    /**
     * qid's had numeric ids (long), want to keep the same so nothing breaks
     */
    Object idLock = new Object();
    long lastId = 0;

    private long nextId() {
        synchronized (idLock) {
            long id = System.currentTimeMillis();
            if (id == lastId) id = id + 1;
            lastId = id;
            return id;
        }
    }
}
