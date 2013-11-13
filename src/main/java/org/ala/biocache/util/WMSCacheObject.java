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
package org.ala.biocache.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a WMS tile.
 *
 * @author Adam
 */
public class WMSCacheObject {

    String query;
    String colourmode;
    ArrayList<float[]> points;
    ArrayList<int[]> counts;
    List<Integer> colours;
    double[] bbox;
    long lastUse;
    long created;
    long size;
    boolean cached = false;

    WMSCacheObject(String query, String colourmode, ArrayList<float[]> points, ArrayList<int[]> counts, List<Integer> colours, double[] bbox) {
        this.query = query;
        this.colourmode = colourmode;
        this.points = points;
        this.counts = counts;
        this.colours = colours;
        this.bbox = bbox;
        this.created = this.lastUse = System.currentTimeMillis();

        updateSize();
    }

    WMSCacheObject() {
        this.created = System.currentTimeMillis();
        cached = false;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getColourmode() {
        return colourmode;
    }

    public void setColourmode(String colourmode) {
        this.colourmode = colourmode;
    }

    public ArrayList<float[]> getPoints() {
        return points;
    }

    public void setPoints(ArrayList<float[]> points) {
        this.points = points;
    }

    public List<Integer> getColours() {
        return colours;
    }

    public void setColours(List<Integer> colours) {
        this.colours = colours;
    }

    public double[] getBbox() {
        return bbox;
    }

    public void setBbox(double[] bbox) {
        this.bbox = bbox;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public boolean isCacheable() {
        return size < WMSCache.LARGEST_CACHEABLE_SIZE;
    }

    public void clearObjects() {
        points = null;
        colours = null;
    }

    public ArrayList<int[]> getCounts() {
        return counts;
    }

    public void setCounts(ArrayList<int[]> counts) {
        this.counts = counts;
    }

    public void setCached(boolean cached) {
        this.cached = cached;
    }

    public boolean getCached() {
        return cached;
    }

    public void updateSize() {
        int numPoints = 0;
        for (float[] d : points) {
            if (d != null) {
                numPoints += d.length;
            }
        }
        size = sizeOf(numPoints, counts != null);
    }

    /**
     * get approximate size in bytes for a WMSCacheObject.
     *
     * @param numberOfPoints
     * @param hasCounts true if points counts are to be stored.
     * @return
     */
    public static long sizeOf(int numberOfPoints, boolean hasCounts) {
        int s = numberOfPoints * (4 + (hasCounts ? 2 : 0));

        //bbox, lastuse, created, size, cached
        s += 4 * 4 + 8 + 8 + 8 + 4;

        //query, colourmode and colours
        s += 250 + 15 + 4 * 4;

        return s;
    }

    public void setLastUse(long lastUse) {
        this.lastUse = lastUse;
    }

    public long getLastUse() {
        return lastUse;
    }

    public void setCreated(long created) {
        this.created = created;
    }

    public long getCreated() {
        return created;
    }
}
