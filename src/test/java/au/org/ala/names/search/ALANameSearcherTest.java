

package au.org.ala.names.search;

import au.org.ala.names.model.MatchType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import au.org.ala.names.model.LinnaeanRankClassification;
import org.junit.Test;


import au.org.ala.names.model.NameSearchResult;
import au.org.ala.names.model.RankType;
import org.gbif.ecat.model.ParsedName;
import org.gbif.ecat.parser.NameParser;

/**
 * @author Natasha, Tommy
 */
public class ALANameSearcherTest {
    private static ALANameSearcher searcher;

    @org.junit.BeforeClass
    public static void init() {
        try {
            searcher = new ALANameSearcher("/data/lucene/namematching_v13");
            //searcher = new ALANameSearcher("/data/lucene/merge_namematching");
            //searcher = new ALANameSearcher("/data/lucene/col_namematching");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testMisappliedNames() {
        try {
            //test to ensure that the accepted name is returned when it also exists as a misapplied name.
            String lsid = searcher.searchForLSID("Tephrosia savannicola");
            fail("A misapplied exception should be thrown");
            //assertEquals("urn:lsid:biodiversity.org.au:apni.taxon:549612",lsid);
        } catch (MisappliedException e) {
            assertEquals("urn:lsid:biodiversity.org.au:apni.taxon:549612", e.getMatchedResult().getLsid());
            assertEquals("urn:lsid:biodiversity.org.au:apni.taxon:685259", e.getMisappliedResult().getLsid());
        } catch (Exception e) {
            fail("No other exceptions should occur");
        }
        //test a misapplied name that does not have an accepted concept
        try {
            String lsid = searcher.searchForLSID("Myosurus minimus");
            fail("a misapplied expcetption shoudl be thrown.");
        } catch (MisappliedException e) {
            assertEquals("urn:lsid:biodiversity.org.au:apni.taxon:303525", e.getMatchedResult().getLsid());
            assertTrue(e.getMisappliedResult() == null);
        } catch (Exception e) {
            fail("no other exceptions should occur.");
        }
    }

    @Test
    public void testSynonymAsHomonym() {
        try {
            searcher.searchForLSID("Terebratella");
            fail("This test should throw a homonym for a matched synonym");
        } catch (Exception e) {
            assertTrue(e instanceof HomonymException);
        }
        try {
            String lsid = searcher.searchForLSID("ISOPTERA", RankType.ORDER);
            assertTrue(lsid != null);
        } catch (Exception e) {
            fail("When supplied with a higher order rank no homonym exception should be thrown");
        }
    }


    @Test
    public void catchAllSpeciesTest() {

        String name = "sp";
        try {
            String lsid = searcher.searchForLSID(name);
            fail("A rank marker should not match to a name");
        } catch (Exception e) {
            assertEquals("Supplied scientific name is a rank marker.", e.getMessage());
        }
    }

    @Test
    public void npeInAuthorTest() {
        String name = "Sphacelaria Lynbye";
        try {
            searcher.searchForLSID(name);
        } catch (Exception e) {
            e.printStackTrace();
            fail("No exception shoul occur");
        }
    }

    @Test
    public void parserBlackList() {
        //Bettongia lesueur unnamed subsp. - this name should NOT throw a NPE
        String name = "Bettongia lesueur unnamed subsp.";
        try {
            searcher.searchForLSID(name, true);
        } catch (Exception e) {
            e.printStackTrace();
            fail("No Exception should occur.");
        }
    }

    @Test
    public void testRescursiveSearch() {
        String name = "Varanus timorensis";
        LinnaeanRankClassification cl = new LinnaeanRankClassification();
        cl.setScientificName(name);
        cl.setGenus("Varanus");
        cl.setFamily("Varanidae");
        cl.setSpecificEpithet("timorensis");
        try {
            NameSearchResult nsr = searcher.searchForRecord(cl, true, true);
            System.out.println(nsr);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testSpeciesSplitSynonym() {
        String name = "Petaurus australis";
        try {
            List<NameSearchResult> results = searcher.searchForRecords(name, null, true);
            fail("An exception should have been thrown");
        } catch (Exception e) {
            assertTrue(e instanceof ParentSynonymChildException);
            ParentSynonymChildException psce = (ParentSynonymChildException) e;
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:ca722c6d-6d53-4de6-b296-310621eeffa8", psce.getParentResult().getLsid());
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:18ee79c4-3822-4d95-908c-f5c61f50b87f", psce.getChildResult().getLsid());
        }

    }

    @Test
    public void testEmbeddedRankMarker() {
        String name = "Flueggea virosa subsp. melanthesoides";
        try {
            String lsid = searcher.searchForLSID(name, true);
            assertEquals("urn:lsid:biodiversity.org.au:apni.taxon:575277", lsid);
        } catch (Exception e) {
            fail("An exception shoudl not occur in embedded rank marker");
            e.printStackTrace();
        }
    }

    @Test
    public void testExcludedNames() {
        String afdExcludedName = "Cyrtodactylus louisiadensis";
        try {
            searcher.searchForLSID(afdExcludedName);
        } catch (Exception e) {
            assertTrue(e instanceof ExcludedNameException);
            ExcludedNameException ene = (ExcludedNameException) e;
            assertEquals("urn:lsid:biodiversity.org.au:afd.name:450112", ene.getExcludedName().getLsid());
        }

        String apcExcludedName = "Parestia elegans";
        try {
            searcher.searchForLSID(apcExcludedName);
        } catch (Exception e) {
            assertTrue(e instanceof ExcludedNameException);
            ExcludedNameException ene = (ExcludedNameException) e;
            assertTrue(ene.getNonExcludedName() == null);
        }
        //Test to ensure that Exception identifies a non excluded version of a name (if one exists).
        apcExcludedName = "Callistemon pungens";
        try {
            searcher.searchForLSID(apcExcludedName);
        } catch (Exception e) {
            assertTrue(e instanceof ExcludedNameException);
            ExcludedNameException ene = (ExcludedNameException) e;
            assertEquals("urn:lsid:biodiversity.org.au:apni.taxon:445680", ene.getNonExcludedName().getLsid());
        }
    }

    @Test
    public void testHomonymsWithAuthorResolution() {
        LinnaeanRankClassification cl = new LinnaeanRankClassification();
        NameSearchResult nsr = null;
        cl.setScientificName("Thalia");
        try {
            nsr = searcher.searchForRecord("Thalia", null, true);
            fail("Thalia should throw a homonym without kingdom or author");
        } catch (Exception e) {
            assertTrue(e instanceof HomonymException);
        }
        cl.setKingdom("Animalia");
        cl.setPhylum("Chordata");
        try {
            nsr = searcher.searchForRecord(cl, false);
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:52c68649-47d5-4f2e-9730-417fc54fb080", nsr.getLsid());
        } catch (Exception e) {
            fail("Homonym should be resolved via the Kingdom");
        }
        cl.setKingdom(null);
        cl.setPhylum(null);
        cl.setAuthorship("Blumenbach, 1798");
        try {
            nsr = searcher.searchForRecord(cl, false);
        } catch (Exception e) {
            fail("Author should identify homonym value to use");
        }

    }

    @Test
    public void testBiocacheName() {
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification();
            cl.setScientificName("Darwinia acerosa?");
            cl.setKingdom("Plantae");
            NameSearchResult nsr = searcher.searchForRecord(cl, true, true);
            System.out.println(nsr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testsStrMarker() {
        try {

            LinnaeanRankClassification cl = new LinnaeanRankClassification();
            cl.setKingdom("Plantae");
            cl.setGenus("Test");
            cl.setScientificName("Macropus rufus");
            System.out.println(searcher.searchForRecord(cl, true));

            String name = "Pterodroma arminjoniana s. str.";
            NameSearchResult nsr = searcher.searchForRecord(name, null);
            name = "Stennella longirostris longirostris";
            nsr = searcher.searchForRecord(name, null, true);
            name = "Aplonis fusca hulliana";
            nsr = searcher.searchForRecord(name, null);

            nsr = searcher.searchForRecord("Cryphaea tenella", null);

            nsr = searcher.searchForRecord("Grevillea 'White Wings'", null);
            System.out.println(nsr);
            nsr = searcher.searchForRecord("Siganus nebulosus", null, true);
            nsr = searcher.searchForRecord("Anabathron contabulatum", null, true);

        } catch (Exception e) {

        }
    }

    @Test
    public void testSpeciesConstructFromClassification() {
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification();

        } catch (Exception e) {

        }
    }

    @Test
    public void testQuestionSpeciesMatch() {
        try {
            String name = "Corymbia ?hendersonii K.D.Hill & L.A.S.Johnson";
            LinnaeanRankClassification cl = new LinnaeanRankClassification();
            cl.setKingdom("Plantae");
            cl.setScientificName(name);
            NameSearchResult nsr = searcher.searchForRecord(cl, true);
            assertEquals("urn:lsid:biodiversity.org.au:apni.taxon:252213", nsr.getLsid());
            //assertEquals(ErrorType.QUESTION_SPECIES, nsr.getErrorType());
            System.out.println(nsr);
            name = "Cacatua leadbeateri";
            //name = "Acacia bartleana ms";

            //test the "name based" synonym "has generic combination"
            nsr = searcher.searchForRecord("Cacatua leadbeateri", null);
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:1365807d-927b-4219-97bf-7e619afa5f72", nsr.getAcceptedLsid());

            name = "Zieria smithii";
            nsr = searcher.searchForRecord(name, null);
            //Cycas media subsp. banksii - C.media subsp. media
            //Boronia crenulata subsp. crenulata var. angustifolia
            //Dendrobium kingianum subsp. kingianum
            //Dendrobium speciosum subsp. capricornicum
            //Dendrobium speciosum subsp. grandiflorum
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSpMarker() {
        try {
            String name = "Thelymitra sp. adorata";
            NameSearchResult nsr = null;
            nsr = searcher.searchForRecord(name, null);
            assertEquals("urn:lsid:biodiversity.org.au:apni.taxon:572459", nsr.getLsid());
            name = "Grevillea brachystylis subsp. Busselton (G.J.Keighery s.n. 28/6/1985)";
            nsr = searcher.searchForRecord(name, null);
            System.out.println(nsr);
            name = "Pterodroma arminjoniana s. str.";
            nsr = searcher.searchForRecord(name, null, true);
            System.out.println(nsr);
        } catch (Exception e) {
            e.printStackTrace();
            fail("No exception should occur");
        }
    }

    @Test
    public void testPhraseMatch() {
        try {
            String name = "Elaeocarpus sp. Rocky Creek";
            NameSearchResult nsr = null;
            try {
                nsr = searcher.searchForRecord(name, null);
                assertEquals("urn:lsid:biodiversity.org.au:apni.taxon:331597", nsr.getLsid());
            } catch (Exception e) {
                //not necessary anymore as the homonym results point to the same accepted concept
//                    if(e instanceof HomonymException){
//                        assertEquals(2, ((HomonymException)e).getResults().size());
//                    }
//                    else{
//                        fail("There should be homonyms for Elaeocarpus sp. Rocky Creek");
//                    }
            }

            name = "Elaeocarpus sp. Rocky Creek (Hunter s.n., 16 Sep 1993)";
            nsr = searcher.searchForRecord(name, null);
            assertEquals("urn:lsid:biodiversity.org.au:apni.taxon:285662", nsr.getLsid());

            name = " Pultenaea sp. Olinda";
            nsr = searcher.searchForRecord(name, null);
            System.out.println(nsr);
            name = "Thelymitra sp. adorata";
            nsr = searcher.searchForRecord(name, null);
            System.out.println(nsr);
            name = "Asterolasia sp. \"Dungowan Creek\"";
            nsr = searcher.searchForRecord(name, null);
            //System.out.println(nsr);
            assertEquals(nsr.getLsid(), "urn:lsid:biodiversity.org.au:apni.taxon:270800");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testSynonymWithoutRank() {
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification();
            cl.setKingdom("Animalia");
            cl.setScientificName("Gymnorhina tibicen");
            NameSearchResult nsr = searcher.searchForRecord(cl, true, true);
            assertEquals("Gymnorhina tibicen", nsr.getRankClassification().getScientificName());
            nsr = searcher.searchForRecord("Cracticus tibicen", RankType.SPECIES);
            assertEquals("Cracticus tibicen", nsr.getRankClassification().getScientificName());
            nsr = searcher.searchForRecord("Cracticus tibicen", RankType.GENUS);
            assertEquals(null, nsr);
        } catch (Exception e) {

        }
    }

    @Test
    public void testRecordSearchWithoutScientificName() {
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification(null, null, null, "Hemiptera", "Pentatomidae", null, null);
            System.out.println(searcher.searchForRecord(cl, true));
            System.out.println("Lilianae::: " + searcher.searchForRecord("Lilianae", null));
            System.out.println("Leptospermum: " + searcher.searchForRecord("Leptospermum", null));
            //searcher.searchForLSID("Pulex (Pulex)");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInfragenricAndSoundEx() {
        String nameDifferentEnding = "Phylidonyris pyrrhopterus";
        String nameWithInfraGenric = "Phylidonyris (Phylidonyris) pyrrhoptera (Latham, 1801)";
        String nameDiffEndInfraGeneric = "Phylidonyris (Phylidonyris) pyrrhopterus";
        try {
            NameSearchResult nsr = searcher.searchForRecord(nameDifferentEnding, null, true);
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:f3871d29-1201-49eb-bd23-70f2bbc616fe", nsr.getLsid());
            assertEquals(nsr.getMatchType(), MatchType.SOUNDEX);
            nsr = searcher.searchForRecord(nameWithInfraGenric, null, true);
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:f3871d29-1201-49eb-bd23-70f2bbc616fe", nsr.getLsid());
            assertEquals(nsr.getMatchType(), MatchType.CANONICAL);
            nsr = searcher.searchForRecord(nameDiffEndInfraGeneric, null, true);
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:f3871d29-1201-49eb-bd23-70f2bbc616fe", nsr.getLsid());
            assertEquals(nsr.getMatchType(), MatchType.SOUNDEX);

            System.out.println(searcher.searchForRecord("Latrodectus hasseltii", null, true));
            System.out.println(searcher.searchForRecord("Latrodectus hasselti", null, true));
            System.out.println(searcher.searchForRecord("Elseya belli", null, true));
            System.out.println(searcher.searchForRecord("Grevillea brachystylis subsp. Busselton (G.J.Keighery s.n. 28/6/1985)", null));
            System.out.println(searcher.searchForRecord("Prostanthera sp. Bundjalung Nat. Pk. (B.J.Conn 3471)", null));



        } catch (Exception e) {
            e.printStackTrace();
            fail("testInfragenericAndSoundEx failed " + e.getMessage());
        }
    }

    // @Test
    public void testSoundExMatch() {
        String name = "Argyrotegium nitidulus";
        try {
            System.out.println(searcher.searchForRecord(name, null, true));
        } catch (Exception e) {

        }
    }

    //@Test
    public void testSubspeciesSynonym() {
        try {
            String name = "Turnix castanota magnifica";
            System.out.println(searcher.searchForRecord(name, null));
            System.out.println(searcher.searchForRecord("Baeckea sp. Baladjie (PJ Spencer 24)", null));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //@Test
    public void testVirusName() {
        try {
            String name = "Cucumovirus cucumber mosaic";
            NameParser parser = new NameParser();
            ParsedName cn = parser.parse(name);
            System.out.println(cn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //@Test
    public void testPhraseNames() {
        //All the names below need to map to the same concept
        try {
            String name1 = "Goodenia sp. Bachsten Creek (M.D. Barrett 685) WA Herbarium";
            String name2 = "Goodenia sp. Bachsten Creek (M.D.Barrett 685) WA Herbarium";
            String name3 = "Goodenia sp. Bachsten Creek";
            String name4 = "Goodenia sp. Bachsten Creek (M.D. Barrett 685)";
            String name5 = "Goodenia sp. Bachsten Creek M.D. Barrett 685";
            NameParser parser = new NameParser();
            ParsedName cn = parser.parse(name1);
            System.out.println(cn + "##" + cn.canonicalName());
            cn = parser.parse(name1);
            System.out.println(cn);
            System.out.println(parser.parse("Macropus sp. rufus"));
            System.out.println(parser.parse("Macropus rufus subsp. rufus"));
            System.out.println(parser.parse("Allocasuarina spinosissima subsp. Short spine (D.L.Serventy & A.R.Main s.n., 25 Aug. 1960) WA Herbarium"));

            System.out.println(searcher.searchForRecord(name1, null));

            System.out.println(searcher.searchForRecord("Baeckea sp. Bungalbin Hill (BJ Lepschi, LA Craven 4586)", null));

            System.out.println(searcher.searchForRecord("Baeckea sp. Calingiri (F Hort 1710)", null));

            System.out.println(searcher.searchForRecord("Baeckea sp. Calingiri (F Hort 1710)", null));

            System.out.println(searcher.searchForRecord("Acacia sp. Goodlands (BR Maslin 7761) [aff. resinosa]", null));

            System.out.println(searcher.searchForRecord("Acacia sp. Manmanning (BR Maslin 7711) [aff. multispicata]", null));

            //System.out.println(parser.parseExtended(name1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //@Test
    public void testBadCommonName() {
        try {
            System.out.println("Higher_sulfur_oxides: " + searcher.searchForCommonName("Higher sulfur oxides"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //@Test
    public void testSpeciesSynonymOfSubspecies() {
        LinnaeanRankClassification cl = new LinnaeanRankClassification("Animalia", "Chordata", "Aves", "Charadriiformes", "Laridae", "Larus", "Larus novaehollandiae");
        try {
            System.out.println(searcher.searchForRecord(cl, true));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testNoRank() {
        try {
            String lsid = searcher.searchForLSID("Animalia");
            System.out.println("testNoRank: " + lsid);

            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:4647863b-760d-4b59-aaa1-502c8cdf8d3c", lsid);
            lsid = searcher.searchForLSID("Bacteria");
            System.out.println("testNoRank: " + lsid);
            assertEquals("urn:lsid:catalogueoflife.org:taxon:d755c2e0-29c1-102b-9a4a-00304854f820:col20120124", lsid);
        } catch (SearchResultException e) {
            e.printStackTrace();
            fail("testNoRank failed");
        }
    }

    @Test
    public void testGetPrimaryLsid() {
        try {
            String primaryLsid = searcher.getPrimaryLsid("urn:lsid:biodiversity.org.au:afd.taxon:00d9e076-b619-4a65-bd9e-8538d958817a");
            System.out.println("testGetPrimaryLsid: " + primaryLsid);
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:1f81ef20-e651-4d27-909e-a4d0be1b2782", primaryLsid);
            primaryLsid = searcher.getPrimaryLsid("urn:lsid:biodiversity.org.au:afd.taxon:1f81ef20-e651-4d27-909e-a4d0be1b2782");
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:1f81ef20-e651-4d27-909e-a4d0be1b2782", primaryLsid);
        } catch (Exception e) {
            e.printStackTrace();
            fail("testGetPrimaryLsid failed");
        }
    }

    @Test
    public void testSearchForRecordByLsid() {
        try {
            NameSearchResult nsr = searcher.searchForRecordByLsid("urn:lsid:biodiversity.org.au:afd.taxon:00d9e076-b619-4a65-bd9e-8538d958817a");
            System.out.println("testSearchForRecordByLsid: " + nsr);
        } catch (Exception e) {
            e.printStackTrace();
            fail("testSearchForRecordByLsid failed");
        }
    }


    private void printAllResults(String prefix, List<NameSearchResult> results) {
        System.out.println("## " + prefix + " ##");
        if (results != null && results.size() != 0) {
            for (NameSearchResult result : results)
                System.out.println(result);
        }
        System.out.println("###################################");
    }

    private boolean nameSearchResultEqual(NameSearchResult nsr1, NameSearchResult nsr2) {
        boolean equals = true;

        try {
            if (nsr1.getMatchType() == null && nsr2.getMatchType() == null) {
                equals = true;
            } else if (!nsr1.getMatchType().equals(nsr2.getMatchType())) {
                equals = false;
            }

            if (!nsr1.getId().equals(nsr2.getId())) {
                equals = false;
            }

            if (nsr1.getLsid() == null && nsr2.getLsid() == null) {
                equals = true;
            } else if (!nsr1.getLsid().equals(nsr2.getLsid())) {
                equals = false;
            }

        } catch (NullPointerException npe) {
            equals = false;
        }

        return equals;
    }

    @Test
    public void testIgnoredHomonyms() {
        //test that Macropus throws an exception in normal situations
        try {
            searcher.searchForLSID("Macropus");
        } catch (SearchResultException e) {
            assertTrue(e instanceof HomonymException);
            assertEquals(1, e.getResults().size());
        }
        //test that Macropus doesn't throw and exception when "ignoreHomonyms" is set
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification();
            cl.setScientificName("Macropus");
            cl.setGenus("Macropus");
            //NameSearchResult nsr =searcher.searchForRecord(cl.getScientificName(), cl, null, true,true);
            String lsid = searcher.searchForLSID("Macropus", false, true);
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:9e6a0bba-de5b-4465-8544-aa8fe3943fab", lsid);
        } catch (Exception e) {
            fail("ignored homonyms should not throw exception " + e.getMessage());
        }
        //test that Agathis still throws a homonym exception when "ignoreHomonyms" are set (because we have 2 different versions of this.
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification();
            cl.setScientificName("Agathis");
            cl.setGenus("Agathis");
            NameSearchResult nsr = searcher.searchForRecord(cl.getScientificName(), cl, null, true, true);
            fail("A Homonym should have been detected. Not result returned: " + nsr);
        } catch (Exception e) {
            assertTrue(e instanceof HomonymException);
        }
        //test that Agathis is resolvable with a kingdom
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification();
            cl.setScientificName("Agathis");
            cl.setGenus("Agathis");
            cl.setKingdom("Animalia");
            NameSearchResult nsr = searcher.searchForRecord(cl.getScientificName(), cl, null, true, true);
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:f604ded9-4c69-4da2-af64-90ded6d7325a", nsr.getLsid());
        } catch (Exception e) {
            fail("A kingdom was supplied and should be resolvable. " + e.getMessage());
        }
    }

    @Test
    public void testHomonym() {
        try {

            LinnaeanRankClassification cl = new LinnaeanRankClassification("Animalia", "Simsia");
            List<NameSearchResult> results = searcher.searchForRecords(
                    "Simsia", RankType.getForId(6000), cl, 10);
            printAllResults("hymonyms test 1", results);
            //test to ensure that kingdoms that almost match are being will not report homonym exceptions
            cl.setGenus("Silene");
            cl.setKingdom("Plantae");
            results = searcher.searchForRecords("Silene", RankType.getForId(6000), cl, 10);
            printAllResults("hymonyms test (Silene)", results);

            cl.setGenus("Serpula");
            cl.setKingdom("Animalia");
            cl.setPhylum("ANNELIDA");
            results = searcher.searchForRecords("Serpula", RankType.getForId(6000), cl, 10);
            printAllResults("hymonyms test (Serpula)", results);

            cl.setGenus("Gaillardia");
            cl.setKingdom("Plantae");
            results = searcher.searchForRecords("Gaillardia", RankType.getForId(6000), cl, 10);
            printAllResults("hymonyms test (Gaillardia)", results);


        } catch (SearchResultException e) {
            //			System.err.println(e.getMessage());
            e.printStackTrace();
            printAllResults("HOMONYM EXCEPTION", e.getResults());
            fail("testHomonym failed");
        }
    }

    @Test
    public void testIDLookup() {
        NameSearchResult result = searcher.searchForRecordByID("216346");
        System.out.println("testIDLookup: " + result);
    }

    @Test
    public void testSearchForRecord() {
        NameSearchResult result = null;
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification(null, "Rhinotia");
            result = searcher.searchForRecord("Rhinotia", cl, RankType.GENUS);
        } catch (SearchResultException e) {
            e.printStackTrace();
            fail("testSearchForRecord failed");
        }
        System.out.println("testSearchForRecord: " + result);
    }

    @Test
    public void testCommonNames() {
        //ANBG source
        String lsid = getCommonNameLSID("Red Kangaroo");
        String sciName = getCommonName("Red Kangaroo");
        System.out.println("Red Kangaroo LSID: " + lsid + ", Common Name: " + sciName);
        assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:31a9b8b8-4e8f-4343-a15f-2ed24e0bf1ae", lsid);
        //OLD LSID: assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:aa745ff0-c776-4d0e-851d-369ba0e6f537", lsid);
        //COL source
        lsid = getCommonNameLSID("Yellow-tailed Black-Cockatoo");
        sciName = getCommonName("Yellow-tailed Black-Cockatoo");
        System.out.println("Yellow-tailed Black-Cockatoo LSID: " + lsid + ", sciName: " + sciName);
        assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:e7873288-a90c-4f20-8be1-e8ec69a074a5", lsid);
        //not found
        lsid = getCommonNameLSID("Scarlet Robin");
        sciName = getCommonName("Scarlet Robin");
        System.out.println("Scarlet Robin LSID: " + lsid + ", sciName: " + sciName);
        assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:0f5fa076-fe30-4598-b90c-31f12121a4fc", lsid);
        //CoL source that maps to a ANBG lsid
        lsid = getCommonNameLSID("Australian tuna");
        sciName = getCommonName("Australian tuna");
        System.out.println("Australian tuna LSID: " + lsid + ", sciName: " + sciName);
        //		assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:0f5fa076-fe30-4598-b90c-31f12121a4fc", lsid);
        //ANBG and CoL have slightly different scientific name
        lsid = getCommonNameLSID("Pacific Black Duck");
        sciName = getCommonName("Pacific Black Duck");
        System.out.println("Pacific Black Duck LSID: " + lsid + ", sciName: " + sciName);
        assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:ce7507c4-eafc-411b-8b12-84b9e425018b", lsid);
        //assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:d09b3807-f8d8-4cfb-a951-70e614e2d546", lsid);
        //Maps to many different species thus should return no LSID
        lsid = getCommonNameLSID("Carp");
        sciName = getCommonName("Carp");
        System.out.println("Carp LSID: " + lsid + ", sciName: " + sciName);
    }


    private String getCommonNameLSID(String name) {
        return searcher.searchForLSIDCommonName(name);
    }

    private String getCommonName(String name) {
        NameSearchResult sciName = searcher.searchForCommonName(name);

        return (sciName == null ? null : sciName.toString());
    }

    @Test
    public void testIRMNGHomonymReconcile() {
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification("Animalia", "Chordata", null, null, "Macropodidae", "Macropus", null);
            RankType rank = searcher.resolveIRMNGHomonym(cl, RankType.GENUS);
            System.out.println("IRMNG Homonym resolved at " + rank + " rank");

            assertEquals("FAMILY", rank.toString());
            //now cause a homonym exception by removing the family
            cl.setFamily(null);
            searcher.resolveIRMNGHomonym(cl, RankType.GENUS);
        } catch (HomonymException e) {
            System.out.println("Expected HomonymException: " + e.getMessage());
            //			fail("testIRMNGHomonymReconcile failed");
        } catch (Exception e) {
            e.printStackTrace();
            fail("testIRMNGHomonymReconcile failed");
        }
    }

    @Test
    public void newHomonynmTest() {
        try {
            //Abelia grandiflora
        } catch (Exception e) {

        }
    }

    @Test
    public void testCultivars() {
        try {
            //species level concept
            System.out.println("Hypoestes phyllostachya: " + searcher.searchForLSID("Hypoestes phyllostachya"));
            //cultivar level concept
            System.out.println("Hypoestes phyllostachya 'Splash': " + searcher.searchForRecord("Hypoestes phyllostachya 'Splash'", null));

        } catch (Exception e) {
            e.printStackTrace();
            fail("testCultivars failed");
        }
    }

    @Test
    public void testMyrmecia() {
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification("Animalia", "Arthropoda", "Insecta", "Hymenoptera", "Formicidae", "Myrmecia", null);
            String output = null;
            NameSearchResult nsr = searcher.searchForRecord("Myrmecia", cl, RankType.GENUS);
            if (nsr != null) {
                output = nsr.toString();
            }
            System.out.println("testMyrmecia: " + output);
        } catch (Exception e) {
            e.printStackTrace();
            fail("testMyrmecia failed");
        }
    }

    @Test
    public void testSearchForLSID() {

        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification("Animalia", "Arthropoda", "Insecta", "Hymenoptera", "Formicidae", "Anochetus", null);
            String output = searcher.searchForLSID("Anochetus");
            System.out.println("LSID for Anochetus: " + output);
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:5323837c-9a09-462c-9b50-6d7b85ea8c4e", output);
            output = searcher.searchForLSID("Anochetus", true);
            System.out.println("LSID for Anochetus fuzzy: " + output);
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:5323837c-9a09-462c-9b50-6d7b85ea8c4e", output);
            output = searcher.searchForLSID("Anochetus", false);
            System.out.println("LSID for Anochetus NOT fuzzy: " + output);
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:5323837c-9a09-462c-9b50-6d7b85ea8c4e", output);
            output = searcher.searchForLSID("Anochetus", RankType.GENUS);
            System.out.println("LSID for Anochetus RankType Species: " + output);
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:5323837c-9a09-462c-9b50-6d7b85ea8c4e", output);
            output = searcher.searchForLSID("Anochetus", cl, RankType.GENUS);
            System.out.println("LSID for Anochetus with cl and rank: " + output);
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:5323837c-9a09-462c-9b50-6d7b85ea8c4e", output);
            output = searcher.searchForLSID(cl, true);
            System.out.println("LSID for cl and recursive matching: " + output);
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:5323837c-9a09-462c-9b50-6d7b85ea8c4e", output);
            output = searcher.searchForLSID(cl, false);
            System.out.println("LSID for cl and NOT recursive matching: " + output);
            assertEquals("urn:lsid:biodiversity.org.au:afd.taxon:5323837c-9a09-462c-9b50-6d7b85ea8c4e", output);

        } catch (Exception e) {
            e.printStackTrace();
            fail("testSearchForLSID failed");
        }
    }

    @Test
    public void testFuzzyMatches() {
        try {
            //Eolophus roseicapillus - non fuzzy match
            assertEquals(searcher.searchForLSID("Eolophus roseicapillus"), "urn:lsid:biodiversity.org.au:afd.taxon:8d061243-c39f-4b81-92a9-c81f4419e93c");

            //Eolophus roseicapilla - fuzzy match
            assertEquals(searcher.searchForLSID("Eolophus roseicapilla", true), "urn:lsid:biodiversity.org.au:afd.taxon:8d061243-c39f-4b81-92a9-c81f4419e93c");




        } catch (Exception e) {
            e.printStackTrace();
            fail("testFuzzyMatches failed");
        }
    }


    @Test
    public void testCrossRankHomonyms() {
        try {
            //Patellina is an order and genus
            searcher.searchForLSID("Patellina");
            fail("Cross Homonym Patellina test 1 failed");
        } catch (SearchResultException e) {
            System.out.println(e.getResults());
            assertEquals("Cross Homonysm Patellina test 1 failed to throw correct exception", e.getClass(), HomonymException.class);
        }
    }


    @Test
    /**
     * Test that the spp. does not match.
     */
    public void testGenusNotAllSpecies() {
        try {
            //System.out.println(searcher.searchForLSID("Stackhousia sp. (McIvor River J.R.Clarkson 5201)"));
            String lsid = searcher.searchForLSID("Opuntia spp.");
            fail("Genus spp. test failed to throw exception.");
        } catch (Exception e) {
            assertEquals("Genus spp. test failed", "Unable to perform search. Can not match to a subset of species within a genus.", e.getMessage());
        }
        try {
            LinnaeanRankClassification cl = new LinnaeanRankClassification(null, "Opuntia");
            cl.setScientificName("Opuntia spp.");
            searcher.searchForLSID(cl, true);
            fail("SPP2 failed to throw a homonym exception");
        } catch (Exception e) {
            assertEquals(e.getClass(), HomonymException.class);
        }
    }

    @Test
    public void testSinglePhraseName() {
        try {

            String name = "Astroloma sp. Cataby (EA Griffin 1022)";
            assertEquals(searcher.searchForLSID(name, null), "urn:lsid:biodiversity.org.au:apni.taxon:696312");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Test that the an infraspecific rank searches for the specified rank and
     * RankType.INFRASPECIFICNAME
     */
    @Test
    public void testInfraSpecificRank() {
        try {
            String name = "Acacia acanthoclada subsp. glaucescens";
            assertEquals(searcher.searchForLSID(name), "urn:lsid:biodiversity.org.au:apni.taxon:295870");
            assertEquals(searcher.searchForLSID("Macropus rufus", RankType.GENUS), null);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRankMarker() {
        try {
            String lsid = searcher.searchForLSID("Macropus sp. rufus");
            System.out.println("SP.:" + lsid);
            lsid = searcher.searchForLSID("Macropus ssp. rufus");
            System.out.println("ssp: " + lsid);
        } catch (Exception e) {
            fail("rank marker test failed");
        }
    }

}
