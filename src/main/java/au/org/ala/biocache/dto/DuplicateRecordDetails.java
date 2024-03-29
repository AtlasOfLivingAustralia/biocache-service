package au.org.ala.biocache.dto;

import org.apache.solr.common.SolrDocument;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Merged from biocache-store
 */
public class DuplicateRecordDetails {

    public static final String ASSOCIATED = "ASSOCIATED";
    public static final String REPRESENTATIVE = "REPRESENTATIVE";

    String id;
    String status;
    String duplicateOf;
    List<DuplicateRecordDetails> duplicates;
    List<String> dupTypes;
    List<String> justification;

    // fields that may be used to determine associated records
    String taxonConceptLsid;
    String point1;
    String point0_1;
    String point0_01;
    String point0_001;
    String point0_0001;
    String latLong;
    String rawScientificName;
    String collector;
    String recordNumber;
    String catalogueNumber;
    Double precision;
    Double coordinateUncertaintyInMeters;

    // deprecated fields for backward compatibility
    String rowKey;
    String uuid;
    String druid;


    public DuplicateRecordDetails() {}

    public DuplicateRecordDetails(SolrDocument d) {
        this.id = (String) d.getFieldValue(OccurrenceIndex.ID);

        Object duplicateStatusIndex = d.getFieldValue(OccurrenceIndex.DUPLICATE_STATUS);

        if (duplicateStatusIndex instanceof java.util.List){
            this.status = (String) ((java.util.List) duplicateStatusIndex).get(0);
        } else {
            this.status = (String) duplicateStatusIndex;
        }

        if (ASSOCIATED.equals(status)) {
            // is duplicate
            this.dupTypes = new ArrayList();

            Object isDuplicateOfIndex = d.getFieldValue(OccurrenceIndex.DUPLICATE_OF);
            if (isDuplicateOfIndex instanceof java.util.List){
                this.duplicateOf = (String) ((java.util.List) isDuplicateOfIndex).get(0);
            } else {
                this.duplicateOf = (String) isDuplicateOfIndex;
            }

            Collection justificationIndex = d.getFieldValues(OccurrenceIndex.DUPLICATE_JUSTIFICATION);
            this.justification = new ArrayList<>(justificationIndex);
        }

        taxonConceptLsid = (String) d.getFieldValue(OccurrenceIndex.TAXON_CONCEPT_ID);
        point1 = (String) d.getFieldValue(PointType.POINT_1.getLabel());
        point0_1 = (String) d.getFieldValue(PointType.POINT_01.getLabel());
        point0_001 = (String) d.getFieldValue(PointType.POINT_001.getLabel());
        point0_0001 = (String) d.getFieldValue(PointType.POINT_0001.getLabel());
        latLong = (String) d.getFieldValue(OccurrenceIndex.LAT_LNG);
        rawScientificName = (String) d.getFieldValue(OccurrenceIndex.RAW_TAXON_NAME);
        collector = (String) d.getFieldValue(OccurrenceIndex.RAW_RECORDED_BY);
        recordNumber = (String) d.getFieldValue(OccurrenceIndex.RECORD_NUMBER);
        catalogueNumber = (String) d.getFieldValue(OccurrenceIndex.CATALOGUE_NUMBER);
        precision =  (Double) d.getFieldValue(OccurrenceIndex.COORDINATE_PRECISION);
        coordinateUncertaintyInMeters = (Double) d.getFieldValue(OccurrenceIndex.COORDINATE_UNCERTAINTY);

        // deprecated fields for backward compatibility
        rowKey = (String) d.getFieldValue(OccurrenceIndex.ID);
        uuid = (String) d.getFieldValue(OccurrenceIndex.ID);
        druid = (String) d.getFieldValue(OccurrenceIndex.DATA_RESOURCE_UID);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getPrecision() {
        return precision;
    }

    public void setPrecision(Double precision) {
        this.precision = precision;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDuplicateOf() {
        return duplicateOf;
    }

    public void setDuplicateOf(String duplicateOf) {
        this.duplicateOf = duplicateOf;
    }

    public List<DuplicateRecordDetails> getDuplicates() {
        return duplicates;
    }

    public void setDuplicates(List<DuplicateRecordDetails> duplicates) {
        this.duplicates = duplicates;
    }

    public List<String> getDupTypes() {
        return dupTypes;
    }

    public void setDupTypes(List<String> dupTypes) {
        this.dupTypes = dupTypes;
    }

    public String getTaxonConceptLsid() {
        return taxonConceptLsid;
    }

    public void setTaxonConceptLsid(String taxonConceptLsid) {
        this.taxonConceptLsid = taxonConceptLsid;
    }

    public String getPoint1() {
        return point1;
    }

    public void setPoint1(String point1) {
        this.point1 = point1;
    }

    public String getPoint0_1() {
        return point0_1;
    }

    public void setPoint0_1(String point0_1) {
        this.point0_1 = point0_1;
    }

    public String getPoint0_01() {
        return point0_01;
    }

    public void setPoint0_01(String point0_01) {
        this.point0_01 = point0_01;
    }

    public String getPoint0_001() {
        return point0_001;
    }

    public void setPoint0_001(String point0_001) {
        this.point0_001 = point0_001;
    }

    public String getPoint0_0001() {
        return point0_0001;
    }

    public void setPoint0_0001(String point0_0001) {
        this.point0_0001 = point0_0001;
    }

    public String getLatLong() {
        return latLong;
    }

    public void setLatLong(String latLong) {
        this.latLong = latLong;
    }

    public String getRawScientificName() {
        return rawScientificName;
    }

    public void setRawScientificName(String rawScientificName) {
        this.rawScientificName = rawScientificName;
    }

    public String getCollector() {
        return collector;
    }

    public void setCollector(String collector) {
        this.collector = collector;
    }

    public String getRecordNumber() {
        return recordNumber;
    }

    public void setRecordNumber(String recordNumber) {
        this.recordNumber = recordNumber;
    }

    public String getCatalogueNumber() {
        return catalogueNumber;
    }

    public void setCatalogueNumber(String catalogueNumber) {
        this.catalogueNumber = catalogueNumber;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getDruid() {
        return druid;
    }

    public void setDruid(String druid) {
        this.druid = druid;
    }

    public List<String> getJustification() {
        return justification;
    }

    public void setJustification(List<String> justification) {
        this.justification = justification;
    }

    public Double getCoordinateUncertaintyInMeters() {
        return coordinateUncertaintyInMeters;
    }

    public void setCoordinateUncertaintyInMeters(Double coordinateUncertaintyInMeters) {
        this.coordinateUncertaintyInMeters = coordinateUncertaintyInMeters;
    }
}
