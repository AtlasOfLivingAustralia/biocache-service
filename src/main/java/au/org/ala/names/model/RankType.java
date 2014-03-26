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
import java.util.Set;
import java.util.TreeSet;

import org.gbif.ecat.voc.Rank;

/**
 * An enumeration of the rank types available in ALA.  The rank names come
 * from Checklist Bank and the identifiers come from ALA Portal.
 * <p/>
 * This is based on org.ala.web.util.RankFacet
 *
 * @author Natasha
 */
public enum RankType {


    KINGDOM(1000, "kingdom", Rank.KINGDOM, 2f, "http://rs.tdwg.org/ontology/voc/TaxonRank#Kingdom", "Kingdom"),
    SUBKINGDOM(1200, "subkingdom", Rank.Subkingdom, null),
    PHYLUM(2000, "phylum", Rank.PHYLUM, 2f, "http://rs.tdwg.org/ontology/voc/TaxonRank#Phylum", "Phylum", "div", "Division"),
    SUBPHYLUM(2200, "subphylum", Rank.Subphylum, null),
    SUPERCLASS(2800, "superclass", Rank.Superclass, null),
    CLASS(3000, "class", Rank.CLASS, 2f, "http://rs.tdwg.org/ontology/voc/TaxonRank#Class", "Class"),
    INFRACLASS(3350, "infraclass", null, null),
    SUBCLASS(3200, "subclass", Rank.Subclass, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Subclass"),
    SUPERORDER(3800, "superorder", Rank.Superorder, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Superorder"),
    ORDER(4000, "order", Rank.ORDER, 2f, "http://rs.tdwg.org/ontology/voc/TaxonRank#Order", "Order"),
    SUBORDER(4200, "suborder", Rank.Suborder, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Suborder"),
    INFRAORDER(4350, "infraorder", Rank.Infraorder, null),
    SUPERFAMILY(4500, "superfamily", Rank.Superfamily, null, "Superfamily"),
    FAMILY(5000, "family", Rank.FAMILY, 2f, "http://rs.tdwg.org/ontology/voc/TaxonRank#Family", "Family"),
    SUBFAMILY(5500, "subfamily", Rank.Subfamily, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Subfamily"),
    TRIBE(5600, "tribe", Rank.Tribe, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Tribe"),
    SUBTRIBE(5700, "subtribe", Rank.Subtribe, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Subtribe"),
    GENUS(6000, "genus", Rank.GENUS, 3f, "http://rs.tdwg.org/ontology/voc/TaxonRank#Genus", "Genus"),
    SUBGENUS(6500, "subgenus", Rank.SUBGENUS, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Subgenus"),
    SECTION(6600, "section", Rank.Section, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Section"),
    SUBSECTION(6700, "subsection", Rank.Subsection, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Subsection"),
    SERIES(6800, "series", Rank.Series, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Series"),
    SUBSERIES(6900, "subseries", Rank.Subseries, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Subseries"),
    INFRAGENERICNNAME(6925, "infragenericname", Rank.InfragenericName, null),
    SPECIES(7000, "species", Rank.SPECIES, 2f, "http://rs.tdwg.org/ontology/voc/TaxonRank#Species", "Species"),
    INFRASPECIFICNAME(8090, "infraspecificname", Rank.InfraspecificName, null, "Infraspecies"),
    SUBSPECIES(8000, "subspecies", Rank.SUBSPECIES, null, "subsp", "http://rs.tdwg.org/ontology/voc/TaxonRank#Subspecies", "subsp.", "ssp", "subtaxon", "staxon", "subsp..", "susp"),
    INFRASUBSPECIESNAME(-1, "infrasubspeciesname", Rank.InfrasubspecificName, null),
    VARIETY(8010, "variety", Rank.VARIETY, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Variety", "var.", "var,", "var"),
    SUBVARIETY(8015, "subvariety", Rank.Subvariety, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Sub-Variety", "subvar."),
    FORM(8020, "form", Rank.Form, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Form", "forma"),
    SUBFORM(-1, "subform", Rank.Subform, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Subform"),
    CULTIVARGROUP(-1, "cultivargroup", Rank.CultivarGroup, null),
    CULTIVAR(8050, "cultivar", Rank.Cultivar, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#Cultivar", "cv."),
    INFORMAL(-1, "informal", Rank.Informal, null),
    UNRANKED(0, "unranked", Rank.Unranked, null),
    SUPRAGENERICNAME(8200, "supragenericname", Rank.SupragenericName, null, "http://rs.tdwg.org/ontology/voc/TaxonRank#SupragenericTaxon"),
    HYBRID(8150, "hybrid", null, null);


    // Allow reverse-lookup (based on http://www.ajaxonomy.com/2007/java/making-the-most-of-java-50-enum-tricks)
    private static final Map<String, RankType> fieldLookup = new HashMap<String, RankType>();
    private static final Map<Integer, RankType> idLookup = new HashMap<Integer, RankType>();
    private static final Map<Rank, RankType> cbRankLookup = new HashMap<Rank, RankType>();

    private static final Map<String, RankType> strRankLookup = new HashMap<String, RankType>();

    static {
        for (RankType rt : EnumSet.allOf(RankType.class)) {
            fieldLookup.put(rt.getRank(), rt);
            idLookup.put(rt.getId(), rt);
            if (rt.cbRank != null)
                cbRankLookup.put(rt.cbRank, rt);
            if (rt.strRanks != null) {
                for (String rank : rt.strRanks)
                    strRankLookup.put(rank.toLowerCase(), rt);
            }
        }
    }

    private Integer id;
    private String field;
    private Rank cbRank;
    private Float boost;
    private String[] strRanks;

    private RankType(Integer id, String field, Rank rank, Float boost, String... strRanks) {


        this.id = id;
        this.field = field;
        this.cbRank = rank;
        this.strRanks = strRanks;
        this.boost = boost;

    }

    public static Set<RankType> getAllRanksBelow(Integer rank) {
        Set<RankType> ranks = new TreeSet<RankType>();
        for (RankType rt : EnumSet.allOf(RankType.class)) {
            if (rt.getId() >= rank)
                ranks.add(rt);
        }
        return ranks;
    }

    /**
     * @return id the id
     */
    public Integer getId() {
        return this.id;
    }

    /**
     * @return field the field
     */
    public String getRank() {
        return this.field;
    }

    /**
     * Gets the Rank Type for the supplied raw string...
     *
     * @param rank
     * @return
     */
    public static RankType getForStrRank(String rank) {
        return strRankLookup.get(rank.toLowerCase());
    }

    public Float getBoost() {
        return boost;
    }

    /**
     * @param field
     * @return The rankType for the field
     */
    public static RankType getForName(String field) {
        return fieldLookup.get(field);
    }

    public static RankType getForCBRank(Rank cbRank) {
        return cbRankLookup.get(cbRank);
    }

    /**
     * @param id
     * @return The RankType for the specified Portal id
     */
    public static RankType getForId(Integer id) {
        return idLookup.get(id);
    }

}


