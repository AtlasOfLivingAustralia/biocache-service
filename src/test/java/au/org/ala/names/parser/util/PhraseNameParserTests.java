package au.org.ala.names.parser.util;

import au.org.ala.names.search.ALANameSearcher;
import au.org.ala.names.parser.PhraseNameParser;
import au.org.ala.names.model.ALAParsedName;
import org.gbif.ecat.voc.NameType;

import java.io.FileReader;

import org.gbif.ecat.model.ParsedName;
import org.gbif.ecat.parser.UnparsableException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the extra ala name parsing
 *
 * @author Natasha Carter
 */
public class PhraseNameParserTests {
    @Test
    public void testNameWithAuthor() {
        try {
            String name = "Trachymene incisa Rudge subsp. incisa";
            PhraseNameParser parser = new PhraseNameParser();
//            ParsedName pn = parser.parse(name);
//            System.out.println(pn);
//            System.out.println(pn.canonicalName());
//            System.out.println(pn.canonicalNameWithAuthorship());
            name = "Pterodroma arminjoniana s. str.";
            parser.debug = true;
            ParsedName pn = parser.parse(name);
            System.out.println(pn);
            name = "Serpula (hydroides) multispinosa";
            pn = parser.parse(name);
            assertEquals(pn.type, NameType.wellformed);
            //System.out.println(pn);

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void phraseNameTest() {
        try {
            String name = "Elaeocarpus sp. Rocky Creek";
            PhraseNameParser parser = new PhraseNameParser();
            ParsedName pn = parser.parse(name);
            System.out.println(pn);
            System.out.println(pn.canonicalName());
            System.out.println(pn.canonicalNameWithAuthorship());
            System.out.println(((ALAParsedName) pn).cleanPhrase);

            //this one should not be a phrase name but it is because GBIF does not recognise it as a well formed scientific name
            name = "Thelymitra sp. adorata";
            pn = parser.parse(name);
            assertTrue(!(pn instanceof ALAParsedName));
            System.out.println(pn);
            name = "Asterolasia sp. \"Dungowan Creek\"";
            pn = parser.parse(name);
            System.out.println(pn);

            name = "Pultenaea sp. 'Olinda' (Coveny 6616)";
            pn = parser.parse(name);
            assertEquals("Olinda", ((ALAParsedName) pn).cleanPhrase);

            name = "Thelymitra aff. pauciflora";
            pn = parser.parse(name);
            System.out.println(pn);

            name = "Corymbia ?hendersonii K.D.Hill & L.A.S.Johnson";
            pn = parser.parse(name);
            System.out.println(pn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testVoucherClean() {
        try {
            PhraseNameParser parser = new PhraseNameParser();
            ParsedName pn = parser.parse("Marsilea sp. Neutral Junction (D.E.Albrecht 9192)");
            pn = parser.parse("Dampiera sp. Central Wheatbelt (L.W.Sage, F.Hort, C.A.Hollister LWS2321)");
            assertEquals("SageHortHollisterLWS2321", ((ALAParsedName) pn).cleanVoucher);
            pn = parser.parse("Dampiera sp. Central Wheatbelt (L.W.Sage And F.Hort and C.A.Hollister LWS2321)");
            assertEquals("SageHortHollisterLWS2321", ((ALAParsedName) pn).cleanVoucher);
            pn = parser.parse("Baeckea sp. Bunney Road (S.Patrick 4059)");
            pn = parser.parse("Baeckea sp. Bunney Road (S Patrick 4059)");
            assertTrue(pn instanceof ALAParsedName);
            pn = parser.parse("Prostanthera sp. Bundjalung Nat. Pk. (B.J.Conn 3471)");
            assertEquals("Conn3471", ((ALAParsedName) pn).cleanVoucher);
            assertEquals("Bundjalung Nat. Pk.", ((ALAParsedName) pn).cleanPhrase);
            pn = parser.parse("Toechima sp. East Alligator (J.Russell-Smith 8418) NT Herbarium");
            assertEquals("RussellSmith8418", ((ALAParsedName) pn).cleanVoucher);
            System.out.println(pn);

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    //@Test
    public void testNormalNamesThroughParser() {
        try {
            PhraseNameParser parser = new PhraseNameParser();
            ParsedName pn = parser.parse("Atrichornis (rahcinta) clamosus");
            System.out.println(pn.canonicalName());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testSpeciesLevelPhraseNames() {
        PhraseNameParser parser = new PhraseNameParser();
        try {
            ParsedName pn = parser.parse("Goodenia sp. Bachsten Creek (M.D. Barrett 685) WA Herbarium");
            assertTrue(pn instanceof ALAParsedName);
            ALAParsedName alapn = (ALAParsedName) pn;
            assertEquals("Bachsten Creek", alapn.getLocationPhraseDesciption());
            assertEquals("(M.D. Barrett 685)", alapn.getPhraseVoucher());
            assertEquals("WA Herbarium", alapn.getPhraseNominatingParty());

            //test a date based one
            pn = parser.parse("Baeckea sp. Beringbooding (AR Main 11/9/1957)");
            System.out.println(ALANameSearcher.voucherRemovePattern.matcher("(BJ Lepschi & LA Craven 4586)").replaceAll(""));
            assertTrue(pn instanceof ALAParsedName);

            pn = parser.parse("Baeckea sp. Calingiri (F.Hort 1710)");
            assertTrue(pn instanceof ALAParsedName);

            pn = parser.parse("Baeckea sp. East Yuna (R Spjut & C Edson 7077)");
            assertTrue(pn instanceof ALAParsedName);

            pn = parser.parse("Acacia sp. Goodlands (BR Maslin 7761) [aff. resinosa]");
            assertTrue(pn instanceof ALAParsedName);

            pn = parser.parse("Acacia sp. Manmanning (BR Maslin 7711) [aff. multispicata]");
            assertTrue(pn instanceof ALAParsedName);
            alapn = (ALAParsedName) pn;
            assertEquals("[aff. multispicata]", alapn.getPhraseNominatingParty());


        } catch (Exception e) {
            fail("SpeciesLevel Phrase Name test failed due to exception. " + e.getMessage());
        }
        try {
            //Cucumovirus cucumber mosaic virus
            System.out.println(parser.parse("Cucumovirus"));
            System.out.println(parser.parse("Cucumovirus cucumber mosaic virus"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //@Test
    public void testAllNamesForType() {
        try {
            au.com.bytecode.opencsv.CSVReader reader = new au.com.bytecode.opencsv.CSVReader(new FileReader("/data/names/Version2011/ala_concepts_dump.txt"), '\t', '"', '\\', 1);
            PhraseNameParser parser = new PhraseNameParser();
            int i = 0;
            for (String[] values = reader.readNext(); values != null; values = reader.readNext()) {
                i++;
                if (values.length != 35)
                    System.out.println("Line " + i + " incorrect length");
                //scientific name only 6th
                String sciName = values[6];
                try {
                    ParsedName pn = parser.parse(sciName);
                    if (pn instanceof ALAParsedName) {
                        System.out.println(sciName);
                    }
                } catch (UnparsableException e) {

                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
