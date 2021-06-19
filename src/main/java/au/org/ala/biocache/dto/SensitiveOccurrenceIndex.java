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
package au.org.ala.biocache.dto;

import org.apache.solr.client.solrj.beans.Field;

/**
 * DTO for sensitive field values.
 *
 * @author Natasha Carter
 */
public class SensitiveOccurrenceIndex extends OccurrenceIndex {

    @Field("sensitive_decimalLatitude")
    Double sensitiveDecimalLatitude;
    @Field("sensitive_decimalLongitude")
    Double sensitiveDecimalLongitude;
    @Field("sensitive_coordinateUncertaintyInMeters")
    Double sensitiveCoordinateUncertaintyInMeters;
    @Field("sensitive_eventDate")
    String sensitiveEventDate;
    @Field("sensitive_eventDateEnd")
    String sensitiveEventDateEnd;
    @Field("sensitive_gridReference")
    String sensitiveGridReference;

    public Double getSensitiveDecimalLatitude() {
        return sensitiveDecimalLatitude;
    }

    public void setSensitiveDecimalLatitude(Double sensitiveDecimalLatitude) {
        this.sensitiveDecimalLatitude = sensitiveDecimalLatitude;
    }

    public Double getSensitiveDecimalLongitude() {
        return sensitiveDecimalLongitude;
    }

    public void setSensitiveDecimalLongitude(Double sensitiveDecimalLongitude) {
        this.sensitiveDecimalLongitude = sensitiveDecimalLongitude;
    }

    public Double getSensitiveCoordinateUncertaintyInMeters() {
        return sensitiveCoordinateUncertaintyInMeters;
    }

    public void setSensitiveCoordinateUncertaintyInMeters(
            Double sensitiveCoordinateUncertaintyInMeters) {
        this.sensitiveCoordinateUncertaintyInMeters = sensitiveCoordinateUncertaintyInMeters;
    }

    public String getSensitiveEventDate() {
        return sensitiveEventDate;
    }

    public void setSensitiveEventDate(String sensitiveEventDate) {
        this.sensitiveEventDate = sensitiveEventDate;
    }

    public String getSensitiveEventDateEnd() {
        return sensitiveEventDateEnd;
    }

    public void setSensitiveEventDateEnd(String sensitiveEventDateEnd) {
        this.sensitiveEventDateEnd = sensitiveEventDateEnd;
    }

    public String getSensitiveGridReference() {
        return sensitiveGridReference;
    }

    public void setSensitiveGridReference(String sensitiveGridReference) {
        this.sensitiveGridReference = sensitiveGridReference;
    }
}
