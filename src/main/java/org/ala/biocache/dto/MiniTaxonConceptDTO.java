/**************************************************************************
 *  Copyright (C) 2013 Atlas of Living Australia
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
package org.ala.biocache.dto;

/**
 * Inner class/bean for transferring a few taxon concept properties into web page
 */
public class MiniTaxonConceptDTO {

    private String guid;
    private String scientificName;
    private String rankId;
    private String rank;
    private String commonName;
    private String imageThumbnailUrl;
    private String kingdom;
    private String family;
    private Long count = 0L;

    @Override
    public String toString() {
        return "MiniTaxonConceptDTO{" + "guid=" + guid + "; scientificName=" + scientificName + "; rankId=" + rankId + "; commonName=" + commonName + "; imageThumbnailUrl=" + imageThumbnailUrl + "; count=" + count + "}";
    }

    public String getCommonName() {
        return commonName;
    }

    public void setCommonName(String commonName) {
        this.commonName = commonName;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getImageThumbnailUrl() {
        return imageThumbnailUrl;
    }

    public void setImageThumbnailUrl(String imageThumbnailUrl) {
        this.imageThumbnailUrl = imageThumbnailUrl;
    }

    public String getScientificName() {
        return scientificName;
    }

    public void setScientificName(String scientificName) {
        this.scientificName = scientificName;
    }

    public String getRankId() {
        return rankId;
    }

    public void setRankId(String rankId) {
        this.rankId = rankId;
    }

    /**
     * @return the rank
     */
    public String getRank() {
        return rank;
    }

    /**
     * @param rank the rank to set
     */
    public void setRank(String rank) {
        this.rank = rank;
    }

    /**
     * @return the kingdom
     */
    public String getKingdom() {
        return kingdom;
    }

    /**
     * @param kingdom the kingdom to set
     */
    public void setKingdom(String kingdom) {
        this.kingdom = kingdom;
    }

    /**
     * @return the family
     */
    public String getFamily() {
        return family;
    }

    /**
     * @param family the family to set
     */
    public void setFamily(String family) {
        this.family = family;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
