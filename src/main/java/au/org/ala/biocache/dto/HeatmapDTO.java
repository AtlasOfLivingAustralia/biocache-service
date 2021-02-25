package au.org.ala.biocache.dto;

import au.org.ala.biocache.util.LegendItem;

import java.util.List;

public class HeatmapDTO {

    public final Integer gridLevel;
    // each element of the list is a single grid layer
    public final List<List<List<Integer>>> layers;
    public final List<LegendItem> legend;
    public final boolean isGrid;

    public HeatmapDTO(Integer gridLevel, List<List<List<Integer>>> layers, List<LegendItem> legend, boolean isGrid) {
        this.gridLevel = gridLevel;
        this.layers = layers;
        this.legend = legend;
        this.isGrid = isGrid;
    }
}
