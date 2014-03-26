package au.org.ala.names.search;

import au.org.ala.names.model.NameSearchResult;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.io.LineIterator;

import java.io.BufferedReader;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests different list of names for existence in the ALA names
 *
 * @author Natasha Carter
 */
public class NameListTest {

    private static ALANameSearcher searcher, searcherOld;

    @org.junit.BeforeClass
    public static void init() {
        try {
            //namematchingv1_1
            //namematching_v13
            searcher = new ALANameSearcher("/data/lucene/namematching_v13");
            searcherOld = new ALANameSearcher("/data/lucene/namematchingv1_1");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //@Test
    public void testWeirdName() {
        String name = "Chalcopteroides sp";
        try {
            System.out.println(searcher.searchForRecord(name, null).getRankClassification());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBiocacheAnimalCoLName2012() {
        assertEquals(0, testFile("biocache_animal_col.txt"));
    }

    //@Test
    public void testBANames() {
        assertEquals(0, testFile("ba_names.txt"));
    }

    // @Test
    public void testBiocacheAustBirds() {
        assertEquals(0, testFile("bio_aust_birds.txt"));
    }

    //@Test
    public void testBirds() {
        assertEquals(0, testFile("birds.txt"));
    }

    //@Test
    public void testCAABNames() {
        assertEquals(0, testFile("caab_fish.txt"));
    }

    //@Test
    public void testSpatialDistributionNames() {
        assertEquals(0, testFile("spatial-distribution-names.txt"));
    }

    private int testFile(String filename) {
        boolean retValue = true;
        int missing = 0, exception = 0, total = 0;
        try {
            LineIterator lines = new LineIterator(new BufferedReader(new java.io.FileReader(new java.io.File(getClass().getResource(filename).toURI()))));
            while (lines.hasNext()) {
                String name = lines.next();
                total++;
                try {
                    String lsid = searcher.searchForLSID(name, true);
                    if (StringUtils.isBlank(lsid)) {
                        retValue = false;
                        NameSearchResult nsr = searcherOld.searchForRecord(name, null, true);
                        if (nsr != null) {
                            System.out.println("NEW MISSING: " + name + "," + nsr.getLsid() + "," + nsr.getRankClassification().toCSV(','));
                            //System.out.println("CoL");
                        } else {
                            System.out.println("MISSING: " + name);
                            //System.out.println("no");
                        }
                        missing++;
                    } else {
//                        System.out.println("yes");
                        // System.out.println(lsid);
                    }
                } catch (SearchResultException sre) {
                    //System.out.println("Exception");
                    //sre.printStackTrace();
                    exception++;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("There are " + missing + " names. With " + exception + " exception names. Total : " + total);
        return missing;
    }
}
