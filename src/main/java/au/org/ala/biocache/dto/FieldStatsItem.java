package au.org.ala.biocache.dto;

import org.apache.solr.client.solrj.response.FieldStatsInfo;

public class FieldStatsItem {
    private String fq;
    private Object min;
    private Object max;
    private Object sum;
    private Long count;
    private Long missing;
    private Object mean;
    private Double stddev;
    private String label;

    public FieldStatsItem(FieldStatsInfo info) {
        this.min = info.getMin();
        this.max = info.getMax();
        this.sum = info.getSum();
        this.count = info.getCount();
        this.missing = info.getMissing();
        this.mean = info.getMean();
        this.stddev = info.getStddev();
    }

    public String getFq() {
        return fq;
    }

    public void setFq(String fq) {
        this.fq = fq;
    }

    public Object getMin() {
        return min;
    }

    public void setMin(Object min) {
        this.min = min;
    }

    public Object getMax() {
        return max;
    }

    public void setMax(Object max) {
        this.max = max;
    }

    public Object getSum() {
        return sum;
    }

    public void setSum(Object sum) {
        this.sum = sum;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getMissing() {
        return missing;
    }

    public void setMissing(Long missing) {
        this.missing = missing;
    }

    public Object getMean() {
        return mean;
    }

    public void setMean(Object mean) {
        this.mean = mean;
    }

    public Double getStddev() {
        return stddev;
    }

    public void setStddev(Double stddev) {
        this.stddev = stddev;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }
}
