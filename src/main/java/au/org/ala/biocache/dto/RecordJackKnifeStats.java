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

// this.layerId, this.outlierValues, this.recordLayerValue

/*
case class RecordJackKnifeStats(@BeanProperty uuid: String,
                                @BeanProperty layerId: String,
                                @BeanProperty recordLayerValue: Float,
                                @BeanProperty sampleSize: Int,
                                @BeanProperty min: Float,
                                @BeanProperty max: Float,
                                @BeanProperty mean: Float,
                                @BeanProperty stdDev: Float,
                                @BeanProperty range: Float,
                                @BeanProperty threshold: Float,
                                @BeanProperty outlierValues: Array[Float])
 */
}
