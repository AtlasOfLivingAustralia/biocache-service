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

/**
 * Stores the Synonym type information
 * <p/>
 * TODO: Get a correct description for the synonym types.
 *
 * @author Natasha Carter
 */
public enum SynonymType {

    HOMONYM(2, "Concept is a homonym to another concept"),
    INVALID_PUBLICATION(3, "This name was invalid in a publication"),
    MISAPPLIED(5, "This name has been misapplied to the accepted concept in the past"),
    REPLACED(6, "This name has been replaced with the accepted concept"),
    TRADE_NAME(7, "This name is a trade name for the accepted concept"),
    VARIANT(8, "This name is a variant of the accepted concept"),
    EXCLUDES(9, "Name is excluded from the NSL"),
    GENERIC_COMBINATION(10, "A generic combination of the accepted concept."),
    GENERIC_COMB_UNPLACED(11, "An unplaced generic combination of the accepted concept"),
    LEGISLATIVE_NAME(12, "This name is a legislative name of the accepted concept"),
    MISC_LITERATURE(13, "This name is a miscellaneous literature name for the accepted concept"),
    SYNONYM(14, ""),
    SYNONYM_EMEDATION(15, ""),
    OBJECTIVE_SYNONYM(16, ""),
    ORIGINAL_SPELLING(17, ""),
    REPLACEMENT_NAME(18, ""),
    SYNONYM_SENS_LAT(19, ""),
    SUBJECTIVE_SYNONYM(20, ""),
    SUBSEQUENT_MISSPELLING(21, ""),
    SYNONYM_SYNONYM(22, ""),
    INCLUDES_INCERTAE_SEDIS(24, ""),
    INCLUDES_NOMENCLATURAL(25, ""),
    INCLUDES_SP_INQUIRENDA(26, ""),
    INCLUDES_TAXONOMIC(27, ""),
    INCLDUES_UNPLACED(28, ""),
    CONGRUENT(33, ""),
    CONGRUENT_EMENDATION(34, ""),
    CONGRUENT_ORIGINAL_SPELLING(35, ""),
    CONGRUENT_REPLACEMENT_NAME(36, ""),
    CONGRUENT_SUBJECTIVE(37, ""),
    CONGRUENT_SYNONYM(38, ""),
    COL_SYNONYM(52, "A synonym that has come from CoL");
    private static final java.util.Map<Integer, SynonymType> idLookup = new java.util.HashMap<Integer, SynonymType>();

    static {
        for (SynonymType st : java.util.EnumSet.allOf(SynonymType.class)) {
            idLookup.put(st.id, st);
        }
    }

    private Integer id;
    private String description;

    SynonymType(int id, String description) {
        this.id = id;
        this.description = description;
    }

    public Integer getId() {
        return id;
    }

    public static SynonymType getTypeFor(String value) {
        try {
            if (value != null) {
                return idLookup.get(Integer.parseInt(value));
            } else
                return null;
        } catch (Exception e) {
            return null;
        }
    }
}
