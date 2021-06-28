package au.org.ala.biocache.dto;

import java.util.List;

public class RecordJackKnifeStats {

    String layerId;
    Float recordLayerValue;
    List<Float> outlierValues;

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

    public List<Float> getOutlierValues() {
        return outlierValues;
    }

    public void setOutlierValues(List<Float> outlierValues) {
        this.outlierValues = outlierValues;
    }

}
