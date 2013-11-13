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

/**
 * Outlier details DTO.
 * 
 * @author Dave Martin
 */
public class OutlierInfo {
    
    String recordID;
    String layerId;
    Float recordLayerValue;
    Float[] outlierValues;
    Integer sampleSize;
    Float min;
    Float max;
    Float mean;
    Float stdDev;
    Float range;
    Float threshold;

    public String getRecordID() {
        return recordID;
    }

    public void setRecordID(String recordID) {
        this.recordID = recordID;
    }

    public String getLayerId() {
        return layerId;
    }

    public void setLayerId(String layerId) {
        this.layerId = layerId;
    }

    public Float getRecordLayerValue() {
        return recordLayerValue;
    }

    public void setRecordLayerValue(Float recordLayerValue) {
        this.recordLayerValue = recordLayerValue;
    }

    public Float[] getOutlierValues() {
        return outlierValues;
    }

    public void setOutlierValues(Float[] outlierValues) {
        this.outlierValues = outlierValues;
    }

    public Integer getSampleSize() {
        return sampleSize;
    }

    public void setSampleSize(Integer sampleSize) {
        this.sampleSize = sampleSize;
    }

    public Float getMin() {
        return min;
    }

    public void setMin(Float min) {
        this.min = min;
    }

    public Float getMax() {
        return max;
    }

    public void setMax(Float max) {
        this.max = max;
    }

    public Float getMean() {
        return mean;
    }

    public void setMean(Float mean) {
        this.mean = mean;
    }

    public Float getStdDev() {
        return stdDev;
    }

    public void setStdDev(Float stdDev) {
        this.stdDev = stdDev;
    }

    public Float getRange() {
        return range;
    }

    public void setRange(Float range) {
        this.range = range;
    }

    public Float getThreshold() {
        return threshold;
    }

    public void setThreshold(Float threshold) {
        this.threshold = threshold;
    }
}
