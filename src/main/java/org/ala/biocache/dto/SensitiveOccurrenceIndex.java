package org.ala.biocache.dto;

import org.apache.solr.client.solrj.beans.Field;

public class SensitiveOccurrenceIndex extends OccurrenceIndex {
    @Field("sensitive_latitude")  Double sensitiveDecimalLatitude;
    @Field("sensitive_longitude")  Double sensitiveDecimalLongitude;
    @Field("sensitive_coordinate_uncertainty") Double sensitiveCoordinateUncertaintyInMeters;
    /**
     * @return the sensitiveDecimalLatitude
     */
    public  Double getSensitiveDecimalLatitude() {
        return sensitiveDecimalLatitude;
    }
    /**
     * @param sensitiveDecimalLatitude the sensitiveDecimalLatitude to set
     */
    public void setSensitiveDecimalLatitude( Double sensitiveDecimalLatitude) {
        this.sensitiveDecimalLatitude = sensitiveDecimalLatitude;
    }
    /**
     * @return the sensitiveDecimalLongitude
     */
    public  Double getSensitiveDecimalLongitude() {
        return sensitiveDecimalLongitude;
    }
    /**
     * @param sensitiveDecimalLongitude the sensitiveDecimalLongitude to set
     */
    public void setSensitiveDecimalLongitude( Double sensitiveDecimalLongitude) {
        this.sensitiveDecimalLongitude = sensitiveDecimalLongitude;
    }
    /**
     * @return the sensitiveCoordinateUncertaintyInMeters
     */
    public Double getSensitiveCoordinateUncertaintyInMeters() {
      return sensitiveCoordinateUncertaintyInMeters;
    }
    /**
     * @param sensitiveCoordinateUncertaintyInMeters the sensitiveCoordinateUncertaintyInMeters to set
     */
    public void setSensitiveCoordinateUncertaintyInMeters(
        Double sensitiveCoordinateUncertaintyInMeters) {
      this.sensitiveCoordinateUncertaintyInMeters = sensitiveCoordinateUncertaintyInMeters;
    }
    
    
}
