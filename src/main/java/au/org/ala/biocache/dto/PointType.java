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

import java.math.BigDecimal;

/**
 * Enum to store the groupings or lat/long accuracy level (for zoom levels)
 *
 * label correspond to SOLR fields in $SOLR_HOME/conf/schema.xml
 * value corresponds to the "accuracy" in decimal degrees
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
public enum PointType {
	
    POINT_1     ("point-1", 1.0f, 0),
    POINT_01    ("point-0.1", 0.1f, 1),
    POINT_001   ("point-0.01", 0.01f, 2),
    POINT_0001  ("point-0.001", 0.001f, 3),
    POINT_00001 ("point-0.0001", 0.0001f, 4),
    POINT_RAW   ("lat_long", 0f, 4);
    
    private String label;
    private Float value;
    private int rounding;

    private PointType(String label,Float value, int rounding) {
        this.label = label;
        this.value = value;
        this.rounding = rounding;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Float getValue() {
        return value;
    }

    public void setValue(Float value) {
        this.value = value;
    }

    public double roundDownToPointType(double value){
        BigDecimal bd = new BigDecimal(value);
        BigDecimal rounded = bd.setScale(rounding, BigDecimal.ROUND_FLOOR);
        return rounded.floatValue();
    }

    public double roundUpToPointType(double value){
        BigDecimal bd = new BigDecimal(value);
        BigDecimal rounded = bd.setScale(rounding, BigDecimal.ROUND_CEILING);
        return rounded.floatValue();
    }

    public double roundToPointType(double value){
        BigDecimal bd = new BigDecimal(value);
        BigDecimal rounded = bd.setScale(rounding, BigDecimal.ROUND_HALF_UP);
        return rounded.floatValue();
    }
}
