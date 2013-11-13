/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
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
package org.ala.biocache.util;

/**
 * An object representing a query. These objects are serialised to disk in JSON format.
 * 
 * @author Adam
 */
public class ParamsCacheObject {
    String q;
    String displayString;
    String wkt;
    double [] bbox;
    long lastUse;
    long size;
    String[] fqs;

    public ParamsCacheObject() {}

    public ParamsCacheObject(long lastUse, String q, String displayString, String wkt, double [] bbox, String[] fqs) {
        this.lastUse = lastUse;
        this.q = q;
        this.displayString = displayString;
        this.bbox = bbox;
        this.wkt = wkt;

        updateSize();
    }

    public String getQ() {
        return q;
    }

    public void setQ(String q) {
        this.q = q;
    }

    public String getDisplayString() {
        return displayString;
    }

    public void setDisplayString(String displayString) {
        this.displayString = displayString;
    }

    public String getWkt() {
        return wkt;
    }

    public void setWkt(String wkt) {
        this.wkt = wkt;
    }

    public double[] getBbox() {
        return bbox;
    }

    public void setBbox(double [] bbox) {
        this.bbox = bbox;
    }

    /**
     * get approximate size in bytes
     *
     * @return
     */
    public long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public long getLastUse() {
        return lastUse;
    }

    public void setLastUse(Long lastUse) {
        this.lastUse = lastUse;
    }

    public void updateSize() {
        size = 0;
        if(q != null) {
            size += q.getBytes().length;
        }
        if(displayString != null) {
            size += displayString.getBytes().length;
        }
        if(wkt != null) {
            size += wkt.getBytes().length;
        }
        if(bbox != null) {
            size += 4*4;
        }
        if(fqs != null){
            for(String fq:fqs){
                size += fq.getBytes().length;
            }
        }
        size += 8 + 8; //size, lastuse
    }

    public String[] getFqs() {
        return fqs;
    }

    public void setFqs(String[] fqs) {
        this.fqs = fqs;
    }
    
}
