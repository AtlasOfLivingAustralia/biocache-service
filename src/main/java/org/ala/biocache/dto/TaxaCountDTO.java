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
package org.ala.biocache.dto;

/**
 * DTO bean to store taxa name, guid and count from a search
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
public class TaxaCountDTO {

    protected String name;
    protected String commonName;
    protected String guid;
    protected String kingdom;
    protected String family;
    protected Long count;
    protected String rank;

    public TaxaCountDTO() {}

    public TaxaCountDTO(String name) {
        this.name = name;
    }

    public TaxaCountDTO(String name, Long count) {
        this.name = name;
        this.count = count;
    }

    @Override
    public String toString() {
        return "TaxaCountDTO{" + "name=" + name + "; commonName=" + commonName + "; guid=" + guid + "; kingdom=" + kingdom + "; family=" + family + "; count=" + count + "; rank=" + rank + '}';
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCommonName() {
        return commonName;
    }

    public void setCommonName(String commonName) {
        this.commonName = commonName;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getKingdom() {
        return kingdom;
    }

    public void setKingdom(String kingdom) {
        this.kingdom = kingdom;
    }

    public String getRank() {
        return rank;
    }

    public void setRank(String rank) {
        this.rank = rank;
    }
}
