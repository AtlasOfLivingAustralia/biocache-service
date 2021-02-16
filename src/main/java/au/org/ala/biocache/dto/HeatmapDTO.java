package au.org.ala.biocache.dto;

import java.util.List;

public class HeatmapDTO {

    public final Integer gridLevel;
    public final List<List<Integer>> counts;

    public HeatmapDTO(Integer gridLevel, List<List<Integer>> counts) {
        this.gridLevel = gridLevel;
        this.counts = counts;
    }


}
