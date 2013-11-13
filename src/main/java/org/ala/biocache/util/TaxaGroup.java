/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
 *  All Rights Reserved.
 * 
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 * 
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/
package org.ala.biocache.util;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Enum class to represent the Taxa Groups on the Your Area page. E.g. Animals, Fish, Mammals, etc.
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
public enum TaxaGroup {
    ALL_LIFE ("All Species", null, "*", "*" ),
    ANIMALS ("Animals", ALL_LIFE, "kingdom", "Animalia"),
    MAMMALS ("Mammals", ANIMALS, "class", "Mammalia"),
    BIRDS ("Birds", ANIMALS, "class", "Aves"),
    REPTILES ("Reptiles", ANIMALS, "class", "Reptilia"),
    AMPHIBIANS ("Amphibians", ANIMALS, "class", "Amphibia"),
    FISH ("Fish", ANIMALS, "class", "Agnatha", "Chondrichthyes", "Osteichthyes", "Actinopterygii", "Sarcopterygii"),
    INSECTS ("Insects", ANIMALS, "class", "Insecta"),
    PLANTS ("Plants", ALL_LIFE, "kingdom", "Plantae"),
    FUNGI ("Fungi", ALL_LIFE, "kingdom", "Fungi"),
    CHROMISTA ("Chromista", ALL_LIFE, "kingdom", "Chromista"),
    PROTOZOA ("Protozoa", ALL_LIFE, "kingdom", "Protozoa"),
    BACTERIA ("Bacteria", ALL_LIFE, "kingdom", "Bacteria");

    private String label;
    private TaxaGroup parentGroup;
    private String rank;
    private String[] taxa;

    private TaxaGroup(String label, TaxaGroup parentGroup, String rank, String... taxa) {
        this.label = label;
        this.rank = rank;
        this.taxa = taxa;
        this.parentGroup = parentGroup;
    }
    
    /*
     * Allow reverse-lookup
     * (based on http://www.ajaxonomy.com/2007/java/making-the-most-of-java-50-enum-tricks)
     */
    private static final Map<String,TaxaGroup> labelLookup
          = new HashMap<String,TaxaGroup>();
    
    static {
        for (TaxaGroup tg : EnumSet.allOf(TaxaGroup.class)) {
            labelLookup.put(tg.getLabel().toLowerCase(), tg); // label (key) stored in Map as lower case
        }
    }

    public static TaxaGroup getForLabel(String label) {
        return labelLookup.get(label.toLowerCase()); // lookup uses lower case
    }

    // Getters & Setters
    
    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public TaxaGroup getParentGroup() {
        return parentGroup;
    }

    public void setParentGroup(TaxaGroup parentGroup) {
        this.parentGroup = parentGroup;
    }

    public String getRank() {
        return rank;
    }

    public void setRank(String rank) {
        this.rank = rank;
    }

    public String[] getTaxa() {
        return taxa;
    }

    public void setTaxa(String[] taxa) {
        this.taxa = taxa;
    }
}
