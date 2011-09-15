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

    ParamsCacheObject(long lastUse, String q, String displayString, String wkt, double [] bbox) {
        this.lastUse = lastUse;
        this.q = q;
        this.displayString = displayString;
        this.bbox = bbox;
        this.wkt = wkt;
    }

    public String getQ() {
        return q;
    }

    public String getDisplayString() {
        return displayString;
    }

    public String getWkt() {
        return wkt;
    }

    public double[] getBbox() {
        return bbox;
    }
}
