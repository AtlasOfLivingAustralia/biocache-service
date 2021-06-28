package au.org.ala.biocache.dto;

import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * Merged from biocache-store.
 */
public class Qid {
    private String rowKey;
    private String q;
    private String displayString;
    private String wkt;
    private double[] bbox;
    private Long lastUse;
    private String[] fqs;
    private Long maxAge;
    private String source;

    public Qid() {
    }

    public Qid(String rowKey, String q, String displayString, String wkt, double[] bbox, Long lastUse, String[] fqs, Long maxAge, String source) {
        this.rowKey = rowKey;
        this.q = q;
        this.displayString = displayString;
        this.wkt = wkt;
        this.bbox = bbox;
        this.fqs = fqs;
        this.source = source;
    }

    @JsonIgnore
    public Long getSize() {
        long size = 0L;
        if (q != null) {
            size += q.getBytes().length;
        }
        if (displayString != null) {
            size += displayString.getBytes().length;
        }
        if (wkt != null) {
            size += wkt.getBytes().length;
        }
        if (bbox != null) {
            size += 4 * 4;
        }
        if (fqs != null) {
            for (String fq : fqs) {
                size += fq.getBytes().length;
            }
        }
        if (source != null) {
            size += source.getBytes().length;
        }
        size += 8 + 8 + 8;

        return size;
    }


    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
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

    public void setBbox(double[] bbox) {
        this.bbox = bbox;
    }

    public Long getLastUse() {
        return lastUse;
    }

    public void setLastUse(Long lastUse) {
        this.lastUse = lastUse;
    }

    public String[] getFqs() {
        return fqs;
    }

    public void setFqs(String[] fqs) {
        this.fqs = fqs;
    }

    public Long getMaxAge() {
        return maxAge;
    }

    public void setMaxAge(Long maxAge) {
        this.maxAge = maxAge;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

}