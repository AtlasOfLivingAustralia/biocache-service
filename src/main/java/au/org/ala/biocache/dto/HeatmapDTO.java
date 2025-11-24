package au.org.ala.biocache.dto;

import au.org.ala.biocache.util.LegendItem;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Schema(name = "HeatMap")
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


    private HeatmapDTO(HeatmapDTO other) {
        this.gridLevel = other.gridLevel;

        // Deep copy layers
        this.layers = (other.layers == null) ? null :
                other.layers.stream()
                        .map(layer -> (layer == null) ? null :
                                layer.stream()
                                        .map(row -> (row == null) ? null : new ArrayList<Integer>(row))
                                        .collect(Collectors.<List<Integer>>toList())
                        )
                        .collect(Collectors.<List<List<Integer>>>toList());

        // Create new list but no need to copy all legend items
        this.legend = (other .legend == null) ? null : new ArrayList<>(other.legend);

        this.gridSizeInPixels = other.gridSizeInPixels;
        this.rows = other.rows;
        this.columns = other.columns;
        this.minx = other.minx;
        this.miny = other.miny;
        this.maxx = other.maxx;
        this.maxy = other.maxy;

        // Mutable tile extents
        this.tileMinx = other.tileMinx;
        this.tileMiny = other.tileMiny;
        this.tileMaxx = other.tileMaxx;
        this.tileMaxy = other.tileMaxy;
    }

    public HeatmapDTO copy(){
        return new HeatmapDTO(this);
    }
}
