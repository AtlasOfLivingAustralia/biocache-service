package au.org.ala.names.search;

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.names.model.NameSearchResult;
import au.org.ala.names.model.LinnaeanRankClassification;
import au.org.ala.names.model.RankType;
import org.apache.commons.lang.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

//import org.gbif.file.CSVReader;

/**
 * Test cases for the Iconic species.  Ensures that that checklist bank classification
 * and the ALA name matching is working correctly.
 *
 * @author Natasha
 */
@Ignore
public class IconicSpeciesTest {

    static ALANameSearcher searcher;

    @org.junit.BeforeClass
    public static void init() {
        try {
            searcher = new ALANameSearcher("/data/lucene/namematchingv1_3");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //@Test
    public void testOtherProblemTaxa() {
        try {
            //tribolium  - there needs to be 2 entries one in Animalia and one in Plantae
            LinnaeanRankClassification cl = new LinnaeanRankClassification("Animalia", null);
            List<NameSearchResult> results = searcher.searchForRecords("Tribolium", null, cl, 10);
            System.out.println("After 1st");
            assertEquals("Tribolium failed", 1, results.size());
            cl.setKingdom("Plantae");
            results = searcher.searchForRecords("Tribolium", null, cl, 10);
            assertEquals("Tribolium failed", 1, results.size());
            //Genus Alseis need to belong to Animalia and Plantae
            //results = searcher.searchForRecords("Alseis", null);
            //assertEquals("Alseis failed",2, results.size());
            //Hydrophilus albipes

            //Isopoda - is a a genus that has a order homonym test include "Isopoda nigrigularis", Isopoda cerussata, Isopoda woodwardii
            //AFD marks these species as synonyms but CoL2010 does not.

            //Polychaeta - CoL2010 has this as a genus and class.

            //Agrotis turbulenta

            //Bacteria - kingdom and genus this really needs to be sorted out as an exception...

            //AFD has Red Back Spider : Latrodectus hasseltii
            //CoL has a taxon concept : Latrodectus hasselti
        } catch (Exception e) {
            // e.printStackTrace();
            fail(e.getMessage());
        }

    }

    //@Test
    public void testIconicSpeciesSRCCOL() {
        //Redback Spider - Latrodectus hasselti
        //AFD version is Latrodectus hasseltii

        //Australian Magpie - Gymnorhina tibicen CoL
        //AFD vesrion is - Cracticus tibicen

        //Willy Wagtail -

        //Red Wattlebird

        //Galah

        //Sulphur-crested Cockatoo

        //Barramundi

    }

    @Test
    public void testIconicSpeciesFile() {
        try {


            CSVReader reader = new CSVReader(new FileReader(new File(getClass().getResource("iconic_species_list.csv").toURI())), ',', '"');//CSVReader.build(new File(getClass().getResource("iconic_species_list.csv").toURI()), "UTF-8", ",", '"', 0);
            //cycle through all the test cases
            String[] values = reader.readNext();
            //fail("Testing");

            int passed = 0, failed = 0;
            while (values != null) {
                if (values.length >= 11 && !values[1].equals("Common name")) {
                    //System.out.println("Processing " + values.length + " : " + values[0]);
                    String commonName = values[1];
                    String kingdom = StringUtils.trimToNull(values[3]);
                    String phylum = StringUtils.trimToNull(values[4]);
                    String clazz = StringUtils.trimToNull(values[5]);
                    String order = StringUtils.trimToNull(values[6]);
                    String family = StringUtils.trimToNull(values[7]);
                    String genus = StringUtils.trimToNull(values[8]);
                    String species = StringUtils.trimToNull(values[9]);
                    String subspecies = StringUtils.trimToNull(values[10]);


                    //attempt to locat the genus species binomial or genus specues subspecies trinomial
                    String search = genus + " " + species;
                    //create the search classification
                    LinnaeanRankClassification classification = new LinnaeanRankClassification(kingdom, phylum, clazz, order, family, genus, search);
                    classification.setSpecies(search);

                    if (StringUtils.trimToNull(subspecies) != null) {
                        search += " " + subspecies;
                        classification.setScientificName(search);
                    }

//                   

                    try {
                        NameSearchResult result = searcher.searchForRecord(search, classification, null, true);

                        //assertNotNull(search + " could not be found" ,guid);
                        if (result == null) {
                            System.err.println(search + "(" + commonName + ") could not be found in index");
                            failed++;

                        } else {

                            if (result.getLsid().contains("catalogue")) {
                                System.err.println(search + " (" + commonName + ") has a CoL LSID");
                            }

                            if (result.isSynonym())
                                result = searcher.searchForRecordByLsid(result.getAcceptedLsid());
                            //test to see if the classification matches
                            if (!classification.hasIdenticalClassification(result.getRankClassification(), RankType.GENUS)) {

                                failed++;
                                //System.err.println(search + "("+commonName+") classification: "+ classification + " does not match " + result.getRankClassification());
                                System.err.println(search + "(" + commonName + ") classifications do not match");

                                printDiff(classification, result.getRankClassification(), result.getLsid(), true);
                            } else
                                passed++;
                        }

                        //                System.out.println(commonName + " GUID: " + guid);
                    } catch (SearchResultException e) {
                        failed++;
                        System.err.println("Searching for: " + search + "(" + commonName + ") caused an exception");

                        //fail("Searching for : "+search + " caused an exception");
                    }
                }
                values = reader.readNext();
            }
            System.out.println("Total names tested: " + (failed + passed) + " passed: " + passed);
            if (failed > 40)
                fail("Test failed.  See other error messaged for details.");
        } catch (Exception e) {
            e.printStackTrace();
            fail("Unable to open file. " + e.getMessage());
        }
    }

    private void printDiff(LinnaeanRankClassification c1, LinnaeanRankClassification c2, String lsid, boolean full) {
        String dif = "\tlsid: " + lsid + "\n";
        dif += "\t" + c1.getKingdom() + "|" + c1.getPhylum() + "|" + c1.getKlass() + "|" + c1.getOrder() + "|" + c1.getFamily() + "|" + c1.getGenus() + "\n";
        dif += "\t" + c2.getKingdom() + "|" + c2.getPhylum() + "|" + c2.getKlass() + "|" + c2.getOrder() + "|" + c2.getFamily() + "|" + c2.getGenus() + "\n";
        if (full) {
            if (c1.getKingdom() != null) {
                if (!c1.getKingdom().equalsIgnoreCase(c2.getKingdom()))
                    dif += "\tkingdoms don't match (" + c1.getKingdom() + " " + c2.getKingdom() + ")\n";
            }
            if (c1.getPhylum() != null) {
                if (!c1.getPhylum().equalsIgnoreCase(c2.getPhylum()))
                    dif += "\tphylums don't match (" + c1.getPhylum() + " " + c2.getPhylum() + ")\n";
            }
            if (c1.getKlass() != null) {
                if (!c1.getKlass().equalsIgnoreCase(c2.getKlass()))
                    dif += "\tclasses don't match (" + c1.getKlass() + " " + c2.getKlass() + ")\n";
            }
            if (c1.getOrder() != null) {
                if (!c1.getOrder().equalsIgnoreCase(c2.getOrder()))
                    dif += "\torders don't match (" + c1.getOrder() + " " + c2.getOrder() + ")\n";
            }
            if (c1.getFamily() != null) {
                if (!c1.getFamily().equalsIgnoreCase(c2.getFamily()))
                    dif += "\tfamilies don't match (" + c1.getFamily() + " " + c2.getFamily() + ")\n";
            }
            if (c1.getGenus() != null) {
                if (!c1.getGenus().equalsIgnoreCase(c2.getGenus()))
                    dif += "\tgenus don't match (" + c1.getGenus() + " " + c2.getGenus() + ")\n";
            }
            if (c1.getSpecies() != null) {
                if (!c1.getSpecies().equalsIgnoreCase(c2.getSpecies()))
                    dif += "\tspecies don't match (" + c1.getSpecies() + " " + c2.getSpecies() + ")\n";
            }
        }
        System.err.print(dif);

    }
}
