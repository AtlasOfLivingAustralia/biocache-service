
package org.ala.biocache.dto;

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
