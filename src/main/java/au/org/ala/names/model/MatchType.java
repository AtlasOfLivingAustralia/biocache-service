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
package au.org.ala.names.model;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * An enumeration that represents the type of match that has been performed. Some of these match types will only happen
 * within certain search types.
 */
public enum MatchType {
    /**
     * @deprecated Used in the OLD search API
     */
    DIRECT("directMatch", "Deprecated"),
    /**
     * The supplied name matched the name exactly.  Very small chance of an incorrect match.
     */
    EXACT("exactMatch", "The supplied name matched the name exactly.  Very small chance of an incorrect match."),
    /**
     * The supplied name was parsed into canonical form before a match was obtained. There is a chance that the match is incorrect due to parse errors.
     */
    CANONICAL("canonicalMatch", "The supplied name was parsed into canonical form before a match was obtained. There is a chance that the match is incorrect due to parse errors."),
    /**
     * A match was determined by parsing the name into a phrase name.  Very small chance of an incorrect match.
     */
    PHRASE("phraseMatch", "A match was determined by parsing the name into a phrase name.  Very small chance of an incorrect match."),
    /**
     * A match was determined by using a sound expression of the supplied name.  There is a greater that average chance that the match is incorrect.
     */
    SOUNDEX("fuzzyMatch", "A match was determined by using a sound expression of the supplied name.  There is a greater that average chance that the match is incorrect."),
    /**
     * @deprecated Used in the OLD search API
     */
    ALTERNATE("alternateMatch", "Deprecated"),
    /**
     * @deprecated Usde in the OLD search API
     */
    SEARCHABLE("searchableMatch", "Deprecated"),
    /**
     * A match was determined by the vernacular name. Matches of this type may be unreliable due to the regional/duplicate nature of common names.
     *
     * Limited to searches by common name {@link au.org.ala.names.search.ALANameSearcher#searchForCommonName(String)}
     *
     */
    VERNACULAR("vernacularMatch", "A match was determined by the vernacular name. Matches of this type may be unreliable due to the regional/duplicate nature of common names."),
    /**
     * The match is based on the higher level classification
     *
     * Limited to recursive searches {@link au.org.ala.names.search.ALANameSearcher#searchForRecord(LinnaeanRankClassification, boolean)} etc
     *
     */
    RECURSIVE("higherMatch", "The match is based on the higher level classification"),
    /**
     * The match was based on the supplied taxon concept ID rather than the scientific name.
     *
     * Limited to searches that supplied an id as the search params {@link au.org.ala.names.search.ALANameSearcher#searchForRecordByLsid(String)} etc
     *
     */
    TAXON_ID("taxonIdMatch", "The match was based on the supplied taxon concept ID rather than the scientific name.");
    private String title;
    private String description;

    private static final Map<String, MatchType> titleLookup = new HashMap<String, MatchType>();

    static {
        for (MatchType mt : EnumSet.allOf(MatchType.class)) {
            titleLookup.put(mt.title, mt);
        }
    }

    MatchType(String title, String description) {

        this.title = title;
        this.description = description;
    }

    @Override
    public String toString() {
        return title;
    }

    public MatchType getMatchType(String match) {
        return titleLookup.get(match);
    }
}