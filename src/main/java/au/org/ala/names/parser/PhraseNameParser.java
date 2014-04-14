/*
 * Copyright (C) 2014 Atlas of Living Australia
 * All Rights Reserved.
 *
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 */

package au.org.ala.names.parser;

import au.org.ala.names.model.ALAParsedName;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.gbif.ecat.model.ParsedName;
import org.gbif.ecat.parser.NameParser;
import org.gbif.ecat.parser.UnparsableException;
import org.gbif.ecat.voc.NameType;
import org.gbif.ecat.voc.Rank;

/**
 * A Parser that can be used to parse a "Phrase" name.  It is assumed
 * that any name being parsed has not been matched to a regular scientific name.
 * <p/>
 * It expects everything to the right of the rank marker.
 *
 * It extends the GBIF NameParser {@see org.gbif.ecat.parser.NameParser}, when the name is not wellformed this parser will then
 * attempt parse it into a phrase name. See https://code.google.com/p/ala-portal/wiki/ALANames#Glossary
 * for more information about phrase names.
 *
 * @author Natasha Carter
 */
public class PhraseNameParser extends NameParser {

    public static final HashMap<String, Rank> VALID_PHRASE_RANKS;

    static {
        HashMap<String, Rank> ranks = new HashMap<String, Rank>();
        ranks.put("subsp", Rank.SUBSPECIES);
        ranks.put("ssp", Rank.SUBSPECIES);
        ranks.put("var", Rank.VARIETY);
        ranks.put("sp", Rank.SPECIES);
        ranks.put("cv", Rank.SPECIES); // added for the situation where phrase names are mistaken for cultivars
        VALID_PHRASE_RANKS = ranks;
    }

    //protected static final String LOCATION_OR_DESCR = "["+NAME_LETTERS+"0-9'](?:["+all_letters_numbers+" -]+|\\.)";
    protected static final String LOCATION_OR_DESCR = "(?:[" + all_letters_numbers + " -'\"_\\.]+|\\.)";
    protected static final String VOUCHER = "(\\([" + all_letters_numbers + "- \\./&,']+\\))";
    protected static final String SOURCE_AUTHORITY = "([" + all_letters_numbers + "\\[\\]'\" -,\\.]+|\\.)";
    protected static final String PHRASE = "";
    protected static final String PHRASE_RANKS = "(?:" + StringUtils.join(VALID_PHRASE_RANKS.keySet(), "|") + ")\\.? ";
    private static final String RANK_MARKER_ALL = "(notho)? *(" + StringUtils.join(Rank.RANK_MARKER_MAP.keySet(), "|")
            + ")\\.?";
    public static final Pattern RANK_MARKER = Pattern.compile("^" + RANK_MARKER_ALL + "$");
    protected static final Pattern SPECIES_PATTERN = Pattern.compile("sp\\.?");

    protected static final Pattern POTENTIAL_SPECIES_PATTERN = Pattern.compile("^" + "([\\x00-\\x7F\\s]*)(" + SPECIES_PATTERN.pattern() + " )" + "([" + name_letters + "]{3,})(?: *)" + "([\\x00-\\x7F\\s]*)");

    protected static final Pattern PHRASE_PATTERN = Pattern.compile("^" +
            //GROUP 1 is normal scientific name - will either represent a Monomial or binomial
            "([\\x00-\\x7F\\s]*)(?: *)"
            // Group 2 is the Rank marker.  For a phrase it needs to be sp. subsp. or var.
            + "(" + PHRASE_RANKS + ")(?: *)"
            // Group 3 indicates the mandatory location/desc for the phrase name. But it may be possible to have homonyms if the VOUCHER is not supplied
            + "(" + LOCATION_OR_DESCR + ")"
            //Group 4 is the VOUCHER for the phrase it indicates the collector and a voucher id
            + VOUCHER + "?"
            //Group 5 is the party propsoing addition of the taxon
            + SOURCE_AUTHORITY + "?$"
    );

    protected static final Pattern WRONG_CASE_INFRAGENERIC = Pattern.compile("(?:" + "\\( ?([" + name_letters + "-]+) ?\\)"
            + "|" + "(" + StringUtils.join(Rank.RANK_MARKER_MAP_INFRAGENERIC.keySet(), "|") + ")\\.? ?([" + NAME_LETTERS
            + "][" + name_letters + "-]+)" + ")");

    protected static final Pattern IGNORE_MARKERS = Pattern.compile("s[\\.| ]+str[\\. ]+");


    @Override
    public <T> ParsedName<T> parse(String scientificName) throws UnparsableException {
        ParsedName pn = super.parse(scientificName);
        if (pn.getType() != NameType.wellformed && isPhraseRank(pn.rank) && (!pn.authorsParsed || pn.specificEpithet == null || SPECIES_PATTERN.matcher(pn.specificEpithet).matches())) {
            //if the rank marker is sp. and the word after the rank marker is lower case check to see if removing the marker will result is a wellformed name
            if (SPECIES_PATTERN.matcher(pn.rank).matches()) {
                Matcher m1 = POTENTIAL_SPECIES_PATTERN.matcher(scientificName);
                //System.out.println(POTENTIAL_SPECIES_PATTERN.pattern());
                if (m1.find()) {
                    //now reparse without the rankMarker
                    String newName = m1.group(1) + m1.group(3) + StringUtils.defaultString(m1.group(4), "");
                    pn = super.parse(newName);
                    if (pn.getType() == NameType.wellformed)
                        return pn;
                }
            }
            //check to see if the name represents a phrase name
            Matcher m = PHRASE_PATTERN.matcher(scientificName);
            if (m.find()) {
                ALAParsedName alapn = new ALAParsedName(pn);
                alapn.setLocationPhraseDescription(StringUtils.trimToNull(m.group(3)));
                alapn.setPhraseVoucher(StringUtils.trimToNull(m.group(4)));
                alapn.setPhraseNominatingParty(StringUtils.trimToNull(m.group(5)));
                return alapn;
            }

        } else {
            //check for the situation where the subgenus was supplied without Title case.
            Matcher m = WRONG_CASE_INFRAGENERIC.matcher(scientificName);
            if (m.find()) {
                scientificName = WordUtils.capitalize(scientificName, '(');
                pn = super.parse(scientificName);
            }
        }

        return pn;
    }

    private boolean isPhraseRank(String rank) {

        if (rank == null)
            return false;
        return VALID_PHRASE_RANKS.containsKey(rank.replaceAll("\\.", ""));
    }

}
