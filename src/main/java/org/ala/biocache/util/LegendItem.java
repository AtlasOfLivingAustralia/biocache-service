/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.ala.biocache.util;

/**
 *
 * @author Adam
 */
public class LegendItem implements Comparable<LegendItem> {
    String name;
    long count;
    int colour;
    String fq;

    public LegendItem(String name, long count, String fq) {
        this.name = name;
        this.count = count;
        this.fq = fq;
    }

    public String getName() {
        return name;
    }

    public long getCount() {
        return count;
    }

    public String getFq() {
        return fq;
    }
    
    public void setColour(int colour) {
        this.colour = colour;
    }
    
    public int getColour() {
        return colour;
    }

    /**
     * Sort 'count', descending, then 'name' ascending.
     * @param o
     * @return
     */
    @Override
    public int compareTo(LegendItem o) {
        long c = count - o.count;
        if(c == 0) {
           if(name == null && o.name == null) {
                return 0;
            } else if(name == null) {
                return 1;
            } else if(o.name == null) {
                return -1;
            }
            return name.compareTo(o.name);
        } else {
            return (c>0)?-1:1;
        }
    }
}
