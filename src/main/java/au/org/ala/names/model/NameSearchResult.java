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

import org.apache.lucene.document.Document;

import org.apache.commons.lang.StringUtils;

/**
 * A model to store the required information in a search result
 *
 * This includes the type of match that was used to get the result
 *
 * @author Natasha
 */
public class NameSearchResult {
    private String id;
    private String lsid;
    private String cleanName;
    private boolean isHomonym;
    private String acceptedLsid;
    //private long acceptedId = -1;
    private String kingdom;
    private String left, right;
    private LinnaeanRankClassification rankClass;
    private RankType rank;
    //The type of match that was performed
    private MatchType matchType;
    private SynonymType synonymType; //store that type of synonym that this name is    

    public NameSearchResult(String id, String lsid, MatchType type) {
        this.id = id;//Long.parseLong(id);
        this.lsid = StringUtils.trimToNull(lsid) == null ? id : StringUtils.trimToNull(lsid);
        matchType = type;
        isHomonym = false;
    }

    public NameSearchResult(Document doc, MatchType type) {
        this(doc.get(NameIndexField.ID.toString()), doc.get(NameIndexField.LSID.toString()), type);
        kingdom = doc.get(RankType.KINGDOM.getRank());
        //System.out.println("Rank to use : " +doc.get(IndexField.RANK.toString()));
        try {
            rank = RankType.getForId(Integer.parseInt(doc.get(NameIndexField.RANK_ID.toString())));
        } catch (Exception e) {
        }
        rankClass = new LinnaeanRankClassification(doc.get(RankType.KINGDOM.getRank()),
                doc.get(RankType.PHYLUM.getRank()),
                doc.get(RankType.CLASS.getRank()),
                doc.get(RankType.ORDER.getRank()),
                doc.get(RankType.FAMILY.getRank()),
                doc.get(RankType.GENUS.getRank()),
                doc.get(NameIndexField.NAME.toString()));
        rankClass.setSpecies(doc.get(RankType.SPECIES.getRank()));
        //add the ids
        rankClass.setKid(doc.get("kid"));
        rankClass.setPid(doc.get("pid"));
        rankClass.setCid(doc.get("cid"));
        rankClass.setOid(doc.get("oid"));
        rankClass.setFid(doc.get("fid"));
        rankClass.setGid(doc.get("gid"));
        rankClass.setSid(doc.get("sid"));
        rankClass.setAuthorship(doc.get(NameIndexField.AUTHOR.toString()));
        //left and right values for the taxon concept
        left = doc.get("left");
        right = doc.get("right");
        synonymType = SynonymType.getTypeFor(doc.get(NameIndexField.SYNONYM_TYPE.toString()));
        String syn = doc.get(NameIndexField.ACCEPTED.toString());
        if (syn != null) {
            acceptedLsid = syn;
        }
    }

    public SynonymType getSynonymType() {
        return synonymType;
    }

    /**
     *
     * @return The classification for the match
     */
    public LinnaeanRankClassification getRankClassification() {
        return rankClass;
    }

    /**
     *
     * @return
     * @deprecated Use the kingdom from the "getRankClassification"
     *
     */
    @Deprecated
    public String getKingdom() {
        return kingdom;
    }

    /**
     * Return the LSID for the result if it is not null otherwise return the id.
     *
     * @return
     */
    public String getLsid() {
        //if(lsid !=null || id <1)
        return lsid;
        //return Long.toString(id);
    }

    public String getId() {
        return id;
    }

    /**
     *
     * @return The match type used to get this result
     */
    public MatchType getMatchType() {
        return matchType;
    }

    /**
     * Set the match type that was used to get this result
     * @param type
     */
    public void setMatchType(MatchType type) {
        matchType = type;
    }

    @Deprecated
    public String getCleanName() {
        return cleanName;
    }
    @Deprecated
    public void setCleanName(String name) {
        cleanName = name;
    }
    @Deprecated
    public boolean hasBeenCleaned() {
        return cleanName != null;
    }

    public boolean isHomonym() {
        return isHomonym;
    }

    public boolean isSynonym() {
        return acceptedLsid != null;// || acceptedId >0;
    }

    /**
     * When the LSID for the synonym is null return the ID for the synonym
     *
     * @return
     * @deprecated Use {@link #getAcceptedLsid()} instead
     */
    @Deprecated
    public String getSynonymLsid() {
        return getAcceptedLsid();
    }

    /**
     * @return The accepted LSID for this name.  When the
     * name is not a synonym null is returned
     */
    public String getAcceptedLsid() {
        //if(acceptedLsid != null || acceptedId<1)
        return acceptedLsid;
//        else
//            return Long.toString(acceptedId);
    }

    @Override
    public String toString() {
        return "Match: " + matchType + " id: " + id + " lsid: " + lsid + " classification: " + rankClass + " synonym: " + acceptedLsid + " rank: " + rank;
    }

    public RankType getRank() {
        return rank;
    }

    public void setRank(RankType rank) {
        this.rank = rank;
    }

    public String getLeft() {
        return left;
    }

    public void setLeft(String left) {
        this.left = left;
    }

    public String getRight() {
        return right;
    }

    public void setRight(String right) {
        this.right = right;
    }
}
