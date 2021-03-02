package au.org.ala.biocache.util;

/**
 * Case class representing a grid reference.
 * <p>
 * Merged from biocache-store.
 */
public class GridRef {
    String gridLetters;
    Integer easting;
    Integer northing;
    Integer coordinateUncertainty = -1;
    Integer minEasting;
    Integer minNorthing;
    Integer maxEasting;
    Integer maxNorthing;
    String datum;

    public GridRef(String gridletters, int easting, int northing, int coordinateUncertainty, int minEasting, int minNorthing, int maxEasting, int maxNorthing, String datum) {
        this.gridLetters = gridletters;
        this.easting = easting;
        this.northing = northing;
        this.coordinateUncertainty = coordinateUncertainty;
        this.minEasting = minEasting;
        this.minNorthing = minNorthing;
        this.maxEasting = maxEasting;
        this.maxNorthing = maxNorthing;
        this.datum = datum;
    }

    public String getGridLetters() {
        return gridLetters;
    }

    public void setGridLetters(String gridLetters) {
        this.gridLetters = gridLetters;
    }

    public Integer getEasting() {
        return easting;
    }

    public void setEasting(Integer easting) {
        this.easting = easting;
    }

    public Integer getNorthing() {
        return northing;
    }

    public void setNorthing(Integer northing) {
        this.northing = northing;
    }

    public Integer getCoordinateUncertainty() {
        return coordinateUncertainty;
    }

    public void setCoordinateUncertainty(Integer coordinateUncertainty) {
        this.coordinateUncertainty = coordinateUncertainty;
    }

    public Integer getMinEasting() {
        return minEasting;
    }

    public void setMinEasting(Integer minEasting) {
        this.minEasting = minEasting;
    }

    public Integer getMinNorthing() {
        return minNorthing;
    }

    public void setMinNorthing(Integer minNorthing) {
        this.minNorthing = minNorthing;
    }

    public Integer getMaxEasting() {
        return maxEasting;
    }

    public void setMaxEasting(Integer maxEasting) {
        this.maxEasting = maxEasting;
    }

    public Integer getMaxNorthing() {
        return maxNorthing;
    }

    public void setMaxNorthing(Integer maxNorthing) {
        this.maxNorthing = maxNorthing;
    }

    public String getDatum() {
        return datum;
    }

    public void setDatum(String datum) {
        this.datum = datum;
    }
}
