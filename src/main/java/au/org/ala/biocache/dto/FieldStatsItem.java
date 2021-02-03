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
    private Long countDistinct;

    public FieldStatsItem(FieldStatsInfo info) {
        this.min = info.getMin();       // TODO: PIPELINES: FieldStatsInfo::getMin entry point
        this.max = info.getMax();       // TODO: PIPELINES: FieldStatsInfo::getMax entry point
        this.sum = info.getSum();       // TODO: PIPELINES: FieldStatsInfo::getSum entry point
        this.count = info.getCount();   // TODO: PIPELINES: FieldStatsInfo::getCount entry point
        this.missing = info.getMissing();   // TODO: PIPELINES: FieldStatsInfo::getMissing entry point
        this.mean = info.getMean();     // TODO: PIPELINES: FieldStatsInfo::getMean entry point
        this.stddev = info.getStddev(); // TODO: PIPELINES: FieldStatsInfo::getStddev entry point
        this.label = info.getName();    // TODO: PIPELINES: FieldStatsInfo::getName entry point
        this.countDistinct = info.getCountDistinct();   // TODO: PIPELINES: FieldStatsInfo::getCountDistinct entry point
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

    public Long getCountDistinct() {
        return countDistinct;
    }

    public void setCountDistinct(Long countDistinct) {
        this.countDistinct = countDistinct;
    }
}
