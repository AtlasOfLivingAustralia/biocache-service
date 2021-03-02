package au.org.ala.biocache.util;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Merged from biocache-store.
 */
public class GISPoint {
    String latitude;
    String longitude;
    String datum;
    String coordinateUncertaintyInMeters;
    String easting = null;
    String northing = null;
    String minLatitude = null;
    String maxLatitude = null;
    String minLongitude = null;
    String maxLongitude = null;

    public GISPoint() {
    }

    public GISPoint(double latitude, double longitude, String datum, String coordinateUncertaintyInMeters, String easting, String northing, double minLatitude, double minLongitude, double maxLatitude, double maxLongitude) {
        this.latitude = String.valueOf(latitude);
        this.longitude = String.valueOf(longitude);
        this.datum = datum;
        this.coordinateUncertaintyInMeters = coordinateUncertaintyInMeters;
        this.easting = easting;
        this.northing = northing;
        this.minLatitude = String.valueOf(minLatitude);
        this.minLongitude = String.valueOf(minLongitude);
        this.maxLatitude = String.valueOf(maxLatitude);
        this.maxLongitude = String.valueOf(maxLongitude);
    }

    public GISPoint(double latitude, double longitude, String datum, String coordinateUncertaintyInMeters) {
        this.latitude = String.valueOf(latitude);
        this.longitude = String.valueOf(longitude);
        this.datum = datum;
        this.coordinateUncertaintyInMeters = coordinateUncertaintyInMeters;
    }

    @JsonIgnore
    public String getBboxString() {
        if (minLatitude != null && minLongitude != null && maxLatitude != null && maxLongitude != null) {
            return minLatitude + "," + minLongitude + "," + maxLatitude + "," + maxLongitude;
        }
        return "";
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getDatum() {
        return datum;
    }

    public void setDatum(String datum) {
        this.datum = datum;
    }

    public String getCoordinateUncertaintyInMeters() {
        return coordinateUncertaintyInMeters;
    }

    public void setCoordinateUncertaintyInMeters(String coordinateUncertaintyInMeters) {
        this.coordinateUncertaintyInMeters = coordinateUncertaintyInMeters;
    }

    public String getEasting() {
        return easting;
    }

    public void setEasting(String easting) {
        this.easting = easting;
    }

    public String getNorthing() {
        return northing;
    }

    public void setNorthing(String northing) {
        this.northing = northing;
    }

    public String getMinLatitude() {
        return minLatitude;
    }

    public void setMinLatitude(String minLatitude) {
        this.minLatitude = minLatitude;
    }

    public String getMaxLatitude() {
        return maxLatitude;
    }

    public void setMaxLatitude(String maxLatitude) {
        this.maxLatitude = maxLatitude;
    }

    public String getMinLongitude() {
        return minLongitude;
    }

    public void setMinLongitude(String minLongitude) {
        this.minLongitude = minLongitude;
    }

    public String getMaxLongitude() {
        return maxLongitude;
    }

    public void setMaxLongitude(String maxLongitude) {
        this.maxLongitude = maxLongitude;
    }
}
