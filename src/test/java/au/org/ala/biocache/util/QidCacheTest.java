package au.org.ala.biocache.util;

import au.org.ala.biocache.dao.QidCacheDAO;
import au.org.ala.biocache.dao.QidDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.model.Qid;
import junit.framework.TestCase;
import org.junit.Ignore;
import org.springframework.test.context.ContextConfiguration;

import javax.inject.Inject;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This test isnt written in a fashion that can be executed as part of a build.
 */
@Ignore
public class QidCacheTest extends TestCase {

    @Inject
    QidCacheDAO qidCacheDao;

    @Inject
    QidDAO qidDao;

    /**
     * test put, get, delete old cache files
     */
    public void testPutGet() throws QidMissingException, QidSizeException {

        //test a cache put returns a valid ordered key
        String key1 = qidCacheDao.put("q1", "displayQ", "wkt", new double[]{1, 2, 3, 4},null, -1, null);
        String key2 = qidCacheDao.put("q2", "displayQ", "wkt", new double[]{1, 2, 3, 4},null, -1, null);
        assertTrue(Long.parseLong(key1) >= 0);
        assertTrue((Long.parseLong(key1) - Long.parseLong(key2)) < 0);

        //test get returns the the correct object
        Qid qid = qidCacheDao.get(String.valueOf(key1));
        assertNotNull(qid);
        if (qid != null) {
            assertEquals(qid.getQ(), "q1");
            assertEquals(qid.getDisplayString(), "displayQ");
            assertEquals(qid.getWkt(), "wkt");
            assertTrue(qid.size() > 0);

            double[] bbox = qid.getBbox();
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
            qid = qidCacheDao.get("-1");
            assertTrue(false);
        } catch (QidMissingException e) {
            System.out.println(e.getMessage());
            assertTrue(true);
        }

        //put very large object into cache throws an error
        try {
            qidCacheDao.setLargestCacheableSize(10000);
            StringBuilder largeString = new StringBuilder();
            for (int i = 0; i < 10000; i++) {
                largeString.append("123");
            }
            qidCacheDao.put("large q", "displayString", largeString.toString(), null, null, -1, null);
            assertTrue(false);
        } catch (QidSizeException e) {
            System.out.println(e.getMessage());
            assertTrue(true);
        }

        //test cached file on disk exists
        Qid qidFromDb = qidDao.get(String.valueOf(key1));
        assertTrue(qidFromDb != null);
    }

    /**
     * test cache size management
     * 1. put more than maxcachesize causes a drop to mincachesize
     * 2. after drop all puts are still retrievable, from disk
     */
    public void testSizeManagement() throws QidMissingException, QidSizeException {

        //setup
        qidCacheDao.setMaxCacheSize(1000);
        qidCacheDao.setMinCacheSize(100);
        ArrayList<Qid> pcos = new ArrayList<Qid>();
        ArrayList<String> keys = new ArrayList<String>();
        double[] defaultbbox = {1, 2, 3, 4};
        long putSize = 0;
        int cacheSizeDropCount = 0;
        for (int i = 0; i < 1000; i++) {
            long beforeSize = qidCacheDao.getSize();
            keys.add(qidCacheDao.put("q" + i, "displayString", "wkt", defaultbbox, null, -1, null));
            long afterSize = qidCacheDao.getSize();

            if (beforeSize > afterSize) {
                //test cache size is significantly reduced after a cacheclean
                //cachecleaner is on a thread
                assertTrue(afterSize <= 500);
                cacheSizeDropCount++;
            }

            pcos.add(qidCacheDao.get(String.valueOf(keys.get(keys.size() - 1))));

            putSize += pcos.get(pcos.size() - 1).size();
        }

        //test size calcuations are operating
        assertTrue(putSize > 10000);

        //test cache cleaner was run more than once
        assertTrue(cacheSizeDropCount > 1);

        //test gets
        for (int i = 0; i < pcos.size(); i++) {
            Qid getqid = qidCacheDao.get(String.valueOf(keys.get(i)));
            Qid putqid = pcos.get(i);

            //compare getpco and putpco
            assertNotNull(getqid);
            if (getqid != null) {
                assertEquals(getqid.getQ(), putqid.getQ());
                assertEquals(getqid.getDisplayString(), putqid.getDisplayString());
                assertEquals(getqid.getWkt(), putqid.getWkt());
                assertTrue(getqid.size() > 0);

                double[] getbbox = getqid.getBbox();
                double[] putbbox = putqid.getBbox();
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
    }

    /**
     * test that cache does operate with concurrent requests
     * 1. perform many puts and gets on multiple threads
     */
    public void testConcurrency() throws QidMissingException, InterruptedException {

        //setup
        qidCacheDao.setMaxCacheSize(100000);
        qidCacheDao.setMinCacheSize(10000);
        double[] defaultbbox = {1, 2, 3, 4};

        final ArrayList<Qid> qids = new ArrayList<Qid>();
        final ArrayList<Qid> getqids = new ArrayList<Qid>();
        final ArrayList<String> keys = new ArrayList<String>();
        final LinkedBlockingQueue<Integer> idxs = new LinkedBlockingQueue<Integer>();

        Collection<Callable<Integer>> tasks = new ArrayList<Callable<Integer>>();

        for (int i = 0; i < 30000; i++) {
            qids.add(new Qid(null, "q" + i, "displayString", "wkt", defaultbbox,-1,null, -1, null));
            getqids.add(null);
            keys.add("-1");
            idxs.put(i);

            //put and get task
            tasks.add(new Callable<Integer>() {

                public Integer call() throws Exception {
                    //put
                    int i = idxs.take();
                    Qid qid = qids.get(i);
                    keys.set(i, qidCacheDao.put(qid.getQ(), qid.getDisplayString(), qid.getWkt(), qid.getBbox(),null, -1, null));

                    //get
                    getqids.set(i, (qidCacheDao.get(String.valueOf(keys.get(i)))));
                    return i;
                }
            });
        }
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        executorService.invokeAll(tasks);

        //test cache cleaner operated correctly
        assertTrue(qidCacheDao.getSize() <= qidCacheDao.getMaxCacheSize());

        //test get objects match put objects
        for (int i = 0; i < qids.size(); i++) {
            Qid getqid = getqids.get(i);
            Qid putqid = qids.get(i);

            //compare getqid and putqid
            assertNotNull(getqid);
            if (getqid != null) {
                assertEquals(getqid.getQ(), putqid.getQ());
                assertEquals(getqid.getDisplayString(), putqid.getDisplayString());
                assertEquals(getqid.getWkt(), putqid.getWkt());
                assertTrue(getqid.size() > 0);

                double[] getbbox = getqid.getBbox();
                double[] putbbox = putqid.getBbox();
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
    }
}
