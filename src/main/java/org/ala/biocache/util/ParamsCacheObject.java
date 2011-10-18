/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.ala.biocache.util;

/**
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

    public ParamsCacheObject() {}

    public ParamsCacheObject(long lastUse, String q, String displayString, String wkt, double [] bbox) {
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
        size += 8 + 8; //size, lastuse
    }
}
