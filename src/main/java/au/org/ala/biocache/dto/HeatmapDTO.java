package au.org.ala.biocache.dto;

import au.org.ala.biocache.util.LegendItem;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.ArrayList;
import java.util.List;

@Schema(name = "HeatMap")
public class HeatmapDTO {

    public final Integer gridLevel;
    // each element of the list is a single grid layer
    public List<List<List<Integer>>> layers;
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
        this.tileMiny = miny;
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

    public HeatmapDTO shallowClone() {
        List<List<List<Integer>>> layersCopy = null;
        if (layers != null) {
            layersCopy = new ArrayList<>();
            for (List<List<Integer>> layer : layers) {
                layersCopy.add(layer);
            }
        }

        List<LegendItem> legendCopy = null;
        if (legend != null) {
            legendCopy = new ArrayList<>();
            for (LegendItem item : legend) {
                legendCopy.add(item);
            }
        }

        HeatmapDTO clone = new HeatmapDTO(
            gridLevel,
            layersCopy,
            legendCopy,
            gridSizeInPixels,
            rows,
            columns,
            minx,
            miny,
            maxx,
            maxy
        );
        clone.tileMinx = tileMinx;
        clone.tileMiny = tileMiny;
        clone.tileMaxx = tileMaxx;
        clone.tileMaxy = tileMaxy;
        return clone;
    }
}
