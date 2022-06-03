package au.org.ala.biocache.dto;

import au.org.ala.biocache.util.LegendItem;

import java.util.List;

public class HeatmapDTO {

    public final Integer gridLevel;
    // each element of the list is a single grid layer
    public final List<List<List<Integer>>> layers;
    public final List<LegendItem> legend;
    public final int gridSizeInPixels;
    public final Integer rows;
    public final Integer columns;
    public final Double minx;
    public final Double miny;
    public final Double maxx;
    public final Double maxy;
    public  Double tileMinx;
    public  Double tileMiny;
    public  Double tileMaxx;
    public  Double tileMaxy;

    public HeatmapDTO(Integer gridLevel, List<List<List<Integer>>> layers, List<LegendItem> legend, int gridSizeInPixels, Integer rows, Integer columns, Double minx, Double miny, Double maxx, Double maxy) {
        // adjust for dateline wrap
        while (minx >= 180)
            minx = minx - 360.0;
        while (maxx <= -180)
            maxx = maxx + 360.0;
        if (minx > maxx)
            maxx = maxx + 360;

        this.gridLevel = gridLevel;
        this.layers = layers;
        this.legend = legend;
        this.gridSizeInPixels = gridSizeInPixels;
        this.rows = rows;
        this.columns = columns;
        this.minx = minx;
        this.miny = miny;
        this.maxx = maxx;
        this.maxy = maxy;

        // default tile extents
        this.tileMinx = minx;
        this.tileMaxy = miny;
        this.tileMaxx = maxx;
        this.tileMaxy = maxy;
    }
    public Double columnWidth() {
        return (maxx - minx) / (double) columns;
    }

    public Double rowHeight() {
        return (maxy - miny) / (double) rows;
    }

    public void setTileExtents(double[] bbox) {
        tileMinx = bbox[0];
        tileMiny = bbox[1];
        tileMaxx = bbox[2];
        tileMaxy = bbox[3];
    }
}
