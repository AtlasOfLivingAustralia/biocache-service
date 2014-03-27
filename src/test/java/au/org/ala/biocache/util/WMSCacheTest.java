package au.org.ala.biocache.util;

import au.org.ala.biocache.dto.PointType;
import junit.framework.TestCase;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This test isnt written in a fashion that can be executed as part of a build.
 */
@Ignore
public class WMSCacheTest extends TestCase {

    /**
     * test put and get
     */
    public void testPutGetWMS() {
        //test a cache put and get
        WMSTile wco1 = getDefaultWMSCacheObject("q1", 100, true);
        WMSCache.put(wco1.getQuery(), wco1.getColourmode(), PointType.POINT_001, wco1);

        WMSTile wco2 = getDefaultWMSCacheObject("q2", 100, true);
        WMSCache.put(wco2.getQuery(), wco2.getColourmode(), PointType.POINT_001, wco2);

        //test get returns the correct object
        assertTrue(compareWMSObjects(WMSCache.get(wco1.getQuery(), wco1.getColourmode(), PointType.POINT_001), wco1));
        assertTrue(compareWMSObjects(WMSCache.get(wco2.getQuery(), wco2.getColourmode(), PointType.POINT_001), wco2));

        //get from cache an object that does not exist returns a placeholder
        WMSTile wcop = WMSCache.get("", "", PointType.POINT_00001);
        assertNotNull(wcop);
        assertTrue(!wcop.getCached());

        //put very large object into cache returns null
        WMSCache.setLargestCacheableSize(10000);
        assertTrue(!WMSCache.put("q1", "c", PointType.POINT_00001, getDefaultWMSCacheObject("q1", 10000, true)));
    }

    /**
     * test cache size management
     * 1. when > cache size records put, nulls will be returned until
     * cachecleaner has done its job.
     */
    public void testSizeManagementWMS() {
        //setup
        WMSCache.setMaxCacheSize(50000);
        WMSCache.setMinCacheSize(5000);
        ArrayList<WMSTile> wcos = new ArrayList<WMSTile>();
        long putSize = 0;
        long maxSize = 0;
        int cacheSizeDropCount = 0;
        for (int i = 0; i < 1000; i++) {
            WMSTile wco = getDefaultWMSCacheObject("q" + i, 50, true);
            putSize += wco.getSize();

            wcos.add(wco);
            long size = WMSCache.getSize();

            if (size < maxSize) {
                maxSize = size;
                cacheSizeDropCount++;
            } else {
                maxSize = size;
            }

            boolean result = WMSCache.put(wco.getQuery(), wco.getColourmode(), PointType.POINT_1, wco);

            //test if cache is full the put was unsuccessful
            //allow for cachecleaner to have reduced the size between put and test
            boolean test = (WMSCache.getSize() + wco.getSize() > WMSCache.getMaxCacheSize()) == !result;
            if (test == false) {
                assertTrue((size + wco.getSize() > WMSCache.getMaxCacheSize()) == !result);
            }

            assertTrue(WMSCache.getSize() <= WMSCache.getMaxCacheSize());
        }

        //test size calcuations are operating
        assertTrue(putSize > 10000);

        //test cache cleaner was run more than once
        assertTrue(cacheSizeDropCount > 1);

        //test cache size is under max
        assertTrue(WMSCache.getSize() <= WMSCache.getMaxCacheSize());

        //test gets.  Anything that is a placeholder will be null.
        int cachedCount = 0;
        for (int i = 0; i < wcos.size(); i++) {
            WMSTile getwco = WMSCache.get(wcos.get(i).getQuery(), wcos.get(i).getColourmode(), PointType.POINT_1);
            if (getwco.getCached()) {
                assertTrue(compareWMSObjects(wcos.get(i), getwco));
                cachedCount++;
            }
        }

        //test if at least 2 objects were cached at the end
        assertTrue(cachedCount > 1);
    }

    /**
     * test that cache does operate with concurrent requests
     * 1. perform many puts and gets on multiple threads
     */
    public void testConcurrency() throws InterruptedException {
        //setup
        WMSCache.setMaxCacheSize(50000);
        WMSCache.setMinCacheSize(5000);

        final ArrayList<WMSTile> wcos = new ArrayList<WMSTile>();
        final ArrayList<Integer> test = new ArrayList<Integer>();
        final LinkedBlockingQueue<Integer> idxs = new LinkedBlockingQueue<Integer>();

        Collection<Callable<Integer>> tasks = new ArrayList<Callable<Integer>>();

        for (int i = 0; i < 30000; i++) {
            wcos.add(getDefaultWMSCacheObject("q" + i, 50, true));
            test.add(-1);
            idxs.put(i);

            //put and get task
            tasks.add(new Callable<Integer>() {

                public Integer call() throws Exception {
                    //put
                    int i = idxs.take();
                    boolean ok = WMSCache.put(wcos.get(i).getQuery(), wcos.get(i).getColourmode(), PointType.POINT_1, wcos.get(i));

                    //get
                    if (ok) {
                        WMSTile wco = WMSCache.get(wcos.get(i).getQuery(), wcos.get(i).getColourmode(), PointType.POINT_1);
                        if (wco.getCached()) {
                            if (compareWMSObjects(wco, wcos.get(i))) {
                                test.set(i, 1);
                            } else {
                                test.set(i, 0);
                            }
                        }
                    }
                    return i;
                }
            });
        }
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        executorService.invokeAll(tasks);

        //test cache cleaner operated correctly
        assertTrue(WMSCache.getSize() <= WMSCache.getMaxCacheSize());

        //test for presence of invalid test comparisons
        int invalid = 0;
        int valid = 0;
        for (int i = 0; i < test.size(); i++) {
            if (test.get(i) == 0) {
                invalid++;
            } else if (test.get(i) == 1) {
                valid++;
            }
        }
        assertTrue(valid > 0);
        assertTrue(invalid == 0);
    }

    WMSTile getDefaultWMSCacheObject(String name, int points, boolean counts) {
        double[] defaultBbox = {1, 2, 3, 4};

        ArrayList<float[]> defaultPoints = new ArrayList<float[]>();
        float[] f = new float[points];
        for (int i = 0; i < points; i++) {
            f[i] = (float) Math.random();
        }
        defaultPoints.add(f);

        ArrayList<int[]> defaultCounts = null;
        if (counts) {
            defaultCounts = new ArrayList<int[]>();
            int[] c = new int[points];
            for (int i = 0; i < points; i++) {
                c[i] = (int) (Math.random() * (255 * 255 * 255));
            }
            defaultCounts.add(c);
        }

        ArrayList<Integer> defaultColours = new ArrayList<Integer>();
        defaultColours.add(0xffffff00);

        return new WMSTile(name, "colourmode", defaultPoints, defaultCounts, defaultColours, defaultBbox);
    }

    private boolean compareWMSObjects(WMSTile wco1, WMSTile wco2) {
        boolean status = true;

        status = status && wco1.getQuery().equals(wco2.getQuery());
        status = status && wco1.getColourmode().equals(wco2.getColourmode());

        List<float[]> p1 = wco1.getPoints();
        List<float[]> p2 = wco2.getPoints();
        status = status && (p1.size() == p2.size());
        for (int i = 0; i < p1.size(); i++) {
            float[] f1 = p1.get(i);
            float[] f2 = p2.get(i);
            status = status && (f1.length == f2.length);
            for (int j = 0; j < f1.length; j++) {
                status = status && (f1[j] == f2[j]);
            }
        }

        List<int[]> c1 = wco1.getCounts();
        List<int[]> c2 = wco2.getCounts();
        if (c1 != null && c2 != null) {
            status = status && (c1.size() == c2.size());
            for (int i = 0; i < c1.size(); i++) {
                int[] f1 = c1.get(i);
                int[] f2 = c2.get(i);
                status = status && (f1.length == f2.length);
                for (int j = 0; j < f1.length; j++) {
                    status = status && (f1[j] == f2[j]);
                }
            }
        } else if ((c1 == null) != (c2 == null)) {
            status = false;
        }

        List<Integer> i1 = wco1.getColours();
        List<Integer> i2 = wco2.getColours();
        status = status && (i1.size() == i2.size());
        for (int i = 0; i < i1.size(); i++) {
            status = status && i1.get(i).equals(i2.get(i));
        }

        double[] b1 = wco1.getBbox();
        double[] b2 = wco2.getBbox();
        if (b1 != null && b2 != null) {
            status = status && b1[0] == b2[0] && b1[1] == b2[1]
                    && b1[2] == b2[2] && b1[3] == b2[3];
        } else if ((b1 == null) != (b2 == null)) {
            status = false;
        }

        return status;
    }
}
