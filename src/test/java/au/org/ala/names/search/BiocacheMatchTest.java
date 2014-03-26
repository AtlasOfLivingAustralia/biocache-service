package au.org.ala.names.search;

import au.org.ala.names.model.MetricsResultDTO;
import au.org.ala.names.model.RankType;
import au.org.ala.names.model.MatchType;
import au.org.ala.names.model.ErrorType;
import au.org.ala.names.model.LinnaeanRankClassification;
import org.gbif.ecat.voc.NameType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * The set of test associated with performing correct matches of biocache names
 * - includes tests for error/issues types
 * - matches based on higher classification etc...
 * <p/>
 * TODO Need to add more test cases to this class
 *
 * @author Natasha Carter
 */
public class BiocacheMatchTest {

    private static ALANameSearcher searcher;

    @org.junit.BeforeClass
    public static void init() {
        try {
            searcher = new ALANameSearcher("/data/lucene/namematching_v13");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testCrossRankHomonym(){
        try{
            //test unresolved
            LinnaeanRankClassification cl = new LinnaeanRankClassification();
            cl.setScientificName("Symphyta");
            cl.setFamily("LASIOCAMPIDAE");
            MetricsResultDTO metrics =searcher.searchForRecordMetrics(cl, true);
            assertTrue("Failed the cross rank homonym test",metrics.getErrors().contains(ErrorType.HOMONYM));
            //test resolved
            cl.setGenus("Symphyta");
            metrics = searcher.searchForRecordMetrics(cl, true);
            assertFalse("Cross rank homonym should have been resolved",metrics.getErrors().contains(ErrorType.HOMONYM));
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    // @Test
    public void testTibicentibicen() {
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification();
            cl.setScientificName("Tibicen tibicen");
            //don't want Tibicen tibicen to match to Tibicen (?) blah
            MetricsResultDTO metrics = searcher.searchForRecordMetrics(cl, true);
            assertNull("Result should be null: " + metrics.getResult(), metrics.getResult());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSPNovName() {
        try {
            String name = "Eremophila sp.nov.";
            String genus = "Eremophila";
            String family = "Myoporaceae";
            String spEp = "sp.nov.";
            LinnaeanRankClassification cl = new LinnaeanRankClassification();
            cl.setScientificName(name);
            cl.setGenus(genus);
            cl.setFamily(family);
            cl.setSpecificEpithet(spEp);
            MetricsResultDTO metrics = searcher.searchForRecordMetrics(cl, true);
            //System.out.println(metrics.getResult());
            assertEquals("urn:lsid:biodiversity.org.au:apni.taxon:399930", metrics.getResult().getAcceptedLsid());
            assertTrue(metrics.getErrors().contains(ErrorType.HOMONYM));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testAlternatePhraseName() {
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification();
            cl.setScientificName("Senna form taxon 'petiolaris'");
            MetricsResultDTO metrics = searcher.searchForRecordMetrics(cl, true);
            System.out.println(metrics);
        } catch (Exception e) {

        }
    }

//    @Test public void testKing(){
//        try{
//            LinnaeanRankClassification cl = new LinnaeanRankClassification();
//            cl.setScientificName("Howea");
//            cl.setKingdom("animalia");
//            searcher.searchForAcceptedLsidDefaultHandling(cl, true);
//        }
//        catch(Exception e){
//            e.printStackTrace();
//        }
//    }

    @Test
    public void genericIssueTest() {
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification();
            cl.setAuthorship("L.");
            cl.setScientificName("Echium vulgare L.");
            cl.setKlass("Equisetopsida");
            cl.setPhylum("Streptophyta");
            cl.setKingdom("Plantae");
            cl.setGenus("EcHium");
            cl.setOrder("[Boraginales]");
            MetricsResultDTO metrics = searcher.searchForRecordMetrics(cl, true);
            assertEquals("urn:lsid:biodiversity.org.au:apni.taxon:308506", metrics.getResult().getLsid());
            //System.out.println(metrics);
            //System.out.println(metrics.getLastException());
            //System.out.println(metrics.getErrors());
            //System.out.println(metrics.getResult());
            //metrics.getLastException().printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testHomonym() {
        try {
            System.out.println(searcher.searchForRecord("Terebratella", null));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void commonName() {
        try {
            //System.out.println(searcher.searchForCommonName("Red Kangaroo"));
            // searcher.searchForLSID("Centropogon australis");
            searcher.searchForRecord("Dexillus muelleri", null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMisappliedNames() {
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification();
            cl.setScientificName("Tephrosia savannicola");
            MetricsResultDTO metrics = searcher.searchForRecordMetrics(cl, true);
            assertTrue(metrics.getErrors().contains(ErrorType.MATCH_MISAPPLIED));
            assertEquals("urn:lsid:biodiversity.org.au:apni.taxon:549612", metrics.getResult().getLsid());
            cl = new LinnaeanRankClassification();
            cl.setScientificName("Myosurus minimus");
            metrics = searcher.searchForRecordMetrics(cl, true);
            assertTrue(metrics.getErrors().contains(ErrorType.MISAPPLIED));
            assertEquals("urn:lsid:biodiversity.org.au:apni.taxon:319672", metrics.getResult().getLsid());
        } catch (Exception e) {
            fail("No exception shoudl occur");
            e.printStackTrace();
        }
    }

    @Test
    public void testAuthorsProvidedInName() {
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification();
            cl.setScientificName("Acanthastrea bowerbanki Edwards & Haime, 1857");
            MetricsResultDTO metrics = searcher.searchForRecordMetrics(cl, true);
            assertEquals(NameType.wellformed, metrics.getNameType());
        } catch (Exception e) {
            e.printStackTrace();
            fail("No exception should  occur");
        }
    }
    //test this one for a bunchof homonyms that are synonyms of another concept...
    //Acacia retinodes

    @Test
    public void testAffCfSpecies() {
        LinnaeanRankClassification cl = new LinnaeanRankClassification();
        try {

            cl.setScientificName("Zabidius novemaculeatus");
            MetricsResultDTO metrics = searcher.searchForRecordMetrics(cl, true);
            System.out.println(metrics);


            cl.setScientificName("Climacteris affinis");
            //this on should match with parent child synonym issue
            metrics = searcher.searchForRecordMetrics(cl, true);
            assertTrue(metrics.getErrors().contains(ErrorType.PARENT_CHILD_SYNONYM));
            cl = new LinnaeanRankClassification();
            cl.setScientificName("Acacia aff. retinodes");
            metrics = searcher.searchForRecordMetrics(cl, true);
            //aff. species need to match to the genus
            assertTrue(metrics.getErrors().contains(ErrorType.AFFINITY_SPECIES));
            assertEquals(RankType.GENUS, metrics.getResult().getRank());
            cl.setScientificName("Acacia aff.");
            metrics = searcher.searchForRecordMetrics(cl, true);
            //aff. species need to match to the genus
            assertTrue(metrics.getErrors().contains(ErrorType.AFFINITY_SPECIES));
            cl = new LinnaeanRankClassification();
            cl.setScientificName("Acanthastrea cf. bowerbanki Edwards & Haime, 1857");
            metrics = searcher.searchForRecordMetrics(cl, true);
            assertTrue(metrics.getErrors().contains(ErrorType.CONFER_SPECIES));
        } catch (Exception e) {
            fail("No excpetion shoudl occur");
        }
    }

    @Test
    public void testQuestionSpecies() {
        String name = "Lepidosperma ? sp. Mt Short (S. Kern et al. LCH 17510)";
        LinnaeanRankClassification cl = new LinnaeanRankClassification();
        cl.setScientificName(name);
        try {
            //test a one where the species does not exists
            //ensures that higher matches work in this case
            MetricsResultDTO metrics = searcher.searchForRecordMetrics(cl, true);
            assertEquals(NameType.doubtful, metrics.getNameType());
            assertTrue(metrics.getErrors().contains(ErrorType.QUESTION_SPECIES));
            assertEquals(MatchType.RECURSIVE, metrics.getResult().getMatchType());
            //Cardium media ?
            cl = new LinnaeanRankClassification();
            cl.setScientificName("Macropus rufus ?");
            metrics = searcher.searchForRecordMetrics(cl, true);
            assertEquals(NameType.doubtful, metrics.getNameType());
            assertTrue(metrics.getErrors().contains(ErrorType.QUESTION_SPECIES));
            assertEquals(MatchType.EXACT, metrics.getResult().getMatchType());
            cl = new LinnaeanRankClassification();
            cl.setScientificName("Macropus ?");

            //testing an empty result with errors on mname
            metrics = searcher.searchForRecordMetrics(cl, true);
            assertTrue(metrics.getErrors().contains(ErrorType.HOMONYM));
            assertTrue(metrics.getErrors().contains(ErrorType.QUESTION_SPECIES));
            assertTrue(metrics.getResult() == null);

            //System.out.println(metrics);
        } catch (Exception e) {
            e.printStackTrace();
            fail("No exception should occur");
        }
    }
}
