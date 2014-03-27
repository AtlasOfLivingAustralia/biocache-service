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
package au.org.ala.biocache.dto;

import java.util.ArrayList;
import java.util.List;

/**
 * A DTO that stores the rank and list of corresponding taxa/counts
 * @author Natasha
 */
public class TaxaRankCountDTO {

    protected String rank;
    protected List<FieldResultDTO> taxa;

    public TaxaRankCountDTO(String rank) {
        this.rank = rank;
        taxa = new ArrayList<FieldResultDTO>();
    }

    public String getRank() {
        return rank;
    }

    public void setRank(String rank) {
        this.rank = rank;
    }

    public List<FieldResultDTO> getTaxa() {
        return taxa;
    }

    public void setTaxa(List<FieldResultDTO> taxa) {
        this.taxa = taxa;
    }
}
