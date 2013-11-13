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
package org.ala.biocache.dto;

import org.apache.solr.client.solrj.beans.Field;
/**
 * DTO for sensitive field values.
 * 
 * @author Natasha Carter
 */
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
