package org.ala.biocache.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import junit.framework.TestCase;

public class ParamsCacheTest extends TestCase {

    /**
     * test put, get, delete old cache files
     */
    public void testPutGet() throws ParamsCacheMissingException, ParamsCacheSizeException {
        configPath();

        //test a cache put returns a valid ordered key
        long key1 = ParamsCache.put("q1", "displayQ", "wkt", new double[]{1, 2, 3, 4});
        long key2 = ParamsCache.put("q2", "displayQ", "wkt", new double[]{1, 2, 3, 4});
        assertTrue(key1 >= 0);
        assertTrue((key1 - key2) < 0);

        //test get returns the the correct object
        ParamsCacheObject pco = ParamsCache.get(key1);
        assertNotNull(pco);
        if (pco != null) {
            assertEquals(pco.getQ(), "q1");
            assertEquals(pco.getDisplayString(), "displayQ");
            assertEquals(pco.getWkt(), "wkt");
            assertTrue(pco.getSize() > 0);

            double[] bbox = pco.getBbox();
            assertNotNull(bbox);
            if (bbox != null) {
                assertTrue(bbox.length == 4);
                if (bbox.length == 4) {
                    assertTrue(bbox[0] == 1);
                    assertTrue(bbox[1] == 2);
                    assertTrue(bbox[2] == 3);
                    assertTrue(bbox[3] == 4);
                }
            }
        }

        //get from cache an object that does not exist throws the correct error
        try {
            pco = ParamsCache.get(-1);
            assertTrue(false);
        } catch (ParamsCacheMissingException e) {
            System.out.println(e.getMessage());
            assertTrue(true);
        }

        //put very large object into cache throws an error
        try {
            ParamsCache.setLargestCacheableSize(10000);
            StringBuilder largeString = new StringBuilder();
            for (int i = 0; i < 10000; i++) {
                largeString.append("123");
            }
            ParamsCache.put("large q", "displayString", largeString.toString(), null);
            assertTrue(false);
        } catch (ParamsCacheSizeException e) {
            System.out.println(e.getMessage());
            assertTrue(true);
        }

        //test cached file on disk exists
        File file = ParamsCache.getFile(key1);
        assertTrue(file.exists());

        //test deletion of old cache files
        ParamsCache.setMaxFileAge(0);
        ParamsCache.deleteOldParamFiles();
        assertTrue(!file.exists());

        deleteFiles();
    }

    /**
     * test cache size management
     * 1. put more than maxcachesize causes a drop to mincachesize
     * 2. after drop all puts are still retrievable, from disk
     */
    public void testSizeManagement() throws ParamsCacheMissingException, ParamsCacheSizeException {
        configPath();

        //setup
        ParamsCache.setMaxCacheSize(1000);
        ParamsCache.setMinCacheSize(100);
        ArrayList<ParamsCacheObject> pcos = new ArrayList<ParamsCacheObject>();
        ArrayList<Long> keys = new ArrayList<Long>();
        double[] defaultbbox = {1, 2, 3, 4};
        long putSize = 0;
        int cacheSizeDropCount = 0;
        for (int i = 0; i < 1000; i++) {
            long beforeSize = ParamsCache.getSize();
            keys.add(ParamsCache.put("q" + i, "displayString", "wkt", defaultbbox));
            long afterSize = ParamsCache.getSize();

            if (beforeSize > afterSize) {
                //test cache size is significantly reduced after a cacheclean
                //cachecleaner is on a thread
                assertTrue(afterSize <= 500);
                cacheSizeDropCount++;
            }

            pcos.add(ParamsCache.get(keys.get(keys.size() - 1)));

            putSize += pcos.get(pcos.size() - 1).getSize();
        }

        //test size calcuations are operating
        assertTrue(putSize > 10000);

        //test cache cleaner was run more than once
        assertTrue(cacheSizeDropCount > 1);

        //test gets
        for (int i = 0; i < pcos.size(); i++) {
            ParamsCacheObject getpco = ParamsCache.get(keys.get(i));
            ParamsCacheObject putpco = pcos.get(i);

            //compare getpco and putpco
            assertNotNull(getpco);
            if (getpco != null) {
                assertEquals(getpco.getQ(), putpco.getQ());
                assertEquals(getpco.getDisplayString(), putpco.getDisplayString());
                assertEquals(getpco.getWkt(), putpco.getWkt());
                assertTrue(getpco.getSize() > 0);

                double[] getbbox = getpco.getBbox();
                double[] putbbox = putpco.getBbox();
                assertNotNull(getbbox);
                if (getbbox != null) {
                    assertTrue(getbbox.length == 4);
                    if (getbbox.length == 4) {
                        assertTrue(getbbox[0] == putbbox[0]);
                        assertTrue(getbbox[1] == putbbox[1]);
                        assertTrue(getbbox[2] == putbbox[2]);
                        assertTrue(getbbox[3] == putbbox[3]);
                    }
                }
            }
        }

        deleteFiles();
    }

    /**
     * test that cache does operate with concurrent requests
     * 1. perform many puts and gets on multiple threads
     */
    public void testConcurrency() throws ParamsCacheMissingException, InterruptedException {
        configPath();

        //setup
        ParamsCache.setMaxCacheSize(100000);
        ParamsCache.setMinCacheSize(10000);
        double[] defaultbbox = {1, 2, 3, 4};

        final ArrayList<ParamsCacheObject> pcos = new ArrayList<ParamsCacheObject>();
        final ArrayList<ParamsCacheObject> getpcos = new ArrayList<ParamsCacheObject>();
        final ArrayList<Long> keys = new ArrayList<Long>();
        final LinkedBlockingQueue<Integer> idxs = new LinkedBlockingQueue<Integer>();

        Collection<Callable<Integer>> tasks = new ArrayList<Callable<Integer>>();

        for (int i = 0; i < 30000; i++) {
            pcos.add(new ParamsCacheObject(0, "q" + i, "displayString", "wkt", defaultbbox));
            getpcos.add(null);
            keys.add(-1L);
            idxs.put(i);

            //put and get task
            tasks.add(new Callable<Integer>() {

                public Integer call() throws Exception {
                    //put
                    int i = idxs.take();
                    ParamsCacheObject pco = pcos.get(i);
                    keys.set(i, ParamsCache.put(pco.getQ(), pco.getDisplayString(), pco.getWkt(), pco.getBbox()));

                    //get
                    getpcos.set(i, (ParamsCache.get(keys.get(i))));
                    return i;
                }
            });
        }
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        executorService.invokeAll(tasks);

        //test cache cleaner operated correctly
        assertTrue(ParamsCache.getSize() <= ParamsCache.getMaxCacheSize());

        //test get objects match put objects
        for (int i = 0; i < pcos.size(); i++) {
            ParamsCacheObject getpco = getpcos.get(i);
            ParamsCacheObject putpco = pcos.get(i);

            //compare getpco and putpco
            assertNotNull(getpco);
            if (getpco != null) {
                assertEquals(getpco.getQ(), putpco.getQ());
                assertEquals(getpco.getDisplayString(), putpco.getDisplayString());
                assertEquals(getpco.getWkt(), putpco.getWkt());
                assertTrue(getpco.getSize() > 0);

                double[] getbbox = getpco.getBbox();
                double[] putbbox = putpco.getBbox();
                assertNotNull(getbbox);
                if (getbbox != null) {
                    assertTrue(getbbox.length == 4);
                    if (getbbox.length == 4) {
                        assertTrue(getbbox[0] == putbbox[0]);
                        assertTrue(getbbox[1] == putbbox[1]);
                        assertTrue(getbbox[2] == putbbox[2]);
                        assertTrue(getbbox[3] == putbbox[3]);
                    }
                }
            }
        }

        deleteFiles();
    }

    void configPath() {
        File dir = new File(System.getProperty("java.io.tmpdir") + File.separator + "test_qids");
        dir.mkdir();
        ParamsCache.setTempFilePath(System.getProperty("java.io.tmpdir") + File.separator + "test_qids");
    }

    void deleteFiles() {
        File dir = new File(System.getProperty("java.io.tmpdir") + File.separator + "test_qids");
        File[] files = dir.listFiles();
        for (int i = 0; i < files.length; i++) {
            if (files[i].isFile()) {
                files[i].deleteOnExit();
            }
        }
        dir.delete();
    }
}
