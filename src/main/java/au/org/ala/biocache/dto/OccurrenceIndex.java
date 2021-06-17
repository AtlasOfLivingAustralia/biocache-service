/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
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
package au.org.ala.biocache.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.beans.Field;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A DTO representing an result from the search indexes.
 */
public class OccurrenceIndex {

    public static final String MISC = "misc";
    protected static final Logger logger = Logger.getLogger(OccurrenceIndex.class);

    final static public String IMAGES = "imageIDs";
    final static public String SOUNDS = "soundIDs";
    //final static public String GEOSPATIAL_KOSHER = "geospatial_kosher"; // Not in pipelines
    final static public String COUNTRY = "country";
    final static public String STATE = "stateProvince";
    final static public String PROVENANCE = "provenance";
    public static final String OCCURRENCE_YEAR_INDEX_FIELD = "occurrenceYear";
    public static final String TAXON_NAME = "scientificName";
    public static final String RAW_TAXON_NAME = "raw_scientificName";
    public static final String RAW_RECORDED_BY = "raw_recordedBy";
    public static final String TAXON_RANK = "taxonRank";
    public static final String LATITUDE = "decimalLatitude";
    public static final String LONGITUDE = "decimalLongitude";
    public static final String DATA_RESOURCE_NAME = "dataResourceName";
    public static final String COORDINATE_UNCERTAINTY = "coordinateUncertaintyInMeters";
    public static final String COMMON_NAME = "vernacularName";
    public static final String STATE_CONSERVATION = "stateConservation";
    public static final String RAW_STATE_CONSERVATION = "raw_stateConservation";
    public static final String YEAR = "year";
    public static final String BASIS_OF_RECORD = "basisOfRecord";
    public static final String TYPE_STATUS = "typeStatus";
    public static final String MULTIMEDIA = "multimedia";
    public static final String COLLECTOR = "recordedBy";
    public static final String OCCURRENCE_STATUS = "occurrenceStatus";
    public static final String ALA_USER_ID = "userId";
    public static final String ASSERTION_USER_ID = "assertionUserId"; // TODO: add to pipeline
    public static final String OUTLIER_LAYER = "outlierLayer";
    public static final String OUTLIER_LAYER_COUNT = "outlierLayerCount";
    public static final String TAXONOMIC_ISSUE = "taxonomicIssues";
    public static final String OCCURRENCE_DATE = "eventDate";
    public static final String RECORD_NUMBER = "recordNumber";
    public static final String CATALOGUE_NUMBER = "catalogNumber";
    public static final String COORDINATE_PRECISION = "coordinatePrecision";
    public static final String TAXON_RANK_ID = "taxonRankID";
    public static final String TAXON_CONCEPT_ID = "taxonConceptID";
    public static final String SPECIESID = "speciesID";
    public static final String GENUSID = "genusID";
    public static final String DATA_PROVIDER_NAME = "dataProviderName";
    public static final String DATA_HUB_UID = "dataHubUid";
    final static public String COLLECTION_UID = "collectionUid";
    final static public String COLLECTION_NAME = "collectionName";
    final static public String INSTITUTION_UID = "institutionUid";
    final static public String DATA_PROVIDER_UID = "dataProviderUid";
    final static public String DATA_RESOURCE_UID = "dataResourceUid";
    final static public String SPECIES_HABITATS = "speciesHabitats";
    final public static String LOCALITY = "locality";

    @Field("id")
    String uuid;
    @Field("occurrenceID")
    String occurrenceID;
    //processed values
    @Field("dataHubUid")
    String[] dataHubUid;
    @Field("dataHubName")
    String[] dataHub;
    @Field("institutionUid")
    String institutionUid;
    @Field("institutionCode")
    String raw_institutionCode;
    @Field("institutionName")
    String institutionName;
    @Field("collectionUid")
    String collectionUid;
    @Field("collectionCode")
    String raw_collectionCode;
    @Field("collectionName")
    String collectionName;
    @Field("catalogNumber")
    String raw_catalogNumber;
    @Field("taxonConceptID")
    String taxonConceptID;
    @Field("eventDate")
    Date eventDate;
    @Field("eventDateEnd")
    Date eventDateEnd;
    // @Field for occurrenceYear is on Setter
    Date occurrenceYear;
    @Field("scientificName")
    String scientificName;
    @Field("vernacularName")
    String vernacularName;
    @Field("taxonRank")
    String taxonRank;
    @Field("taxonRankID")
    Integer taxonRankID;
    @Field("raw_countryCode")
    String raw_countryCode;
    @Field("country")
    String country;
    @Field("kingdom")
    String kingdom;
    @Field("phylum")
    String phylum;
    @Field("class")
    String classs;
    @Field("order")
    String order;
    @Field("family")
    String family;
    @Field("genus")
    String genus;
    @Field("genusID")
    String genusGuid;
    @Field("species")
    String species;
    @Field("speciesID")
    String speciesGuid;
    @Field("subspecies")
    String subspecies;
    @Field("subspeciesID")
    String subspeciesGuid;
    @Field("stateProvince")
    String stateProvince;
    @Field("decimalLatitude")
    Double decimalLatitude;
    @Field("decimalLongitude")
    Double decimalLongitude;
    @Field("coordinateUncertaintyInMeters")
    Double coordinateUncertaintyInMeters;
    @Field("year")
    Integer year;
    @Field("month")
    Integer month;
    @Field("basisOfRecord")
    String basisOfRecord;
    @Field("typeStaus")
    String typeStatus;
    @Field("locationRemarks")
    String raw_locationRemarks;
    @Field("occurrenceRemarks")
    String raw_occurrenceRemarks;
    @Field("lft")
    Integer left;
    @Field("rgt")
    Integer right;
    //@Field("ibra")
    String ibra;
    //@Field("imcra")
    String imcra;
    //@Field("places")
    String lga;
    @Field("dataProviderUid")
    String dataProviderUid;
    @Field("dataProviderName")
    String dataProviderName;
    @Field("dataResourceUid")
    String dataResourceUid;
    @Field("dataResourceName")
    String dataResourceName;
    @Field("assertions")
    String[] assertions;
    @Field("userAssertions")
    String hasUserAssertions;
    @Field("speciesGroup")
    String[] speciesGroups;
    @Field("imageID")
    String image;
    @Field("imageIDs")
    String[] images;
    @Field("spatiallyValid")
    Boolean geospatialKosher;
    //@Field("taxonomic_kosher")
    String taxonomicKosher;
    @Field("recordedBy")
    String[] collector;
    @Field("recordedBy")
    String[] collectors;
    //extra raw record fields
    @Field("raw_scientificName")
    String raw_scientificName;
    @Field("raw_basisOfRecord")
    String raw_basisOfRecord;
    @Field("raw_typeStatus")
    String raw_typeStatus;
    @Field("raw_vernacularName")
    String raw_vernacularName;
    //constructed fields
    @Field("lat_long")
    String latLong;
    @Field("point-1")
    String point1;
    @Field("point-0.1")
    String point01;
    @Field("point-0.01")
    String point001;
    @Field("point-0.001")
    String point0001;
    @Field("point-0.0001")
    String point00001;
    @Field("names_and_lsid")
    String namesLsid;
    @Field("multimedia")
    String[] multimedia;
    @Field("license")
    String license;
    @Field("identificationVerificationStatus")
    String identificationVerificationStatus;
    //conservation status field
    @Field("countryConservation")
    String austConservation;
    @Field("stateConservation")
    String stateConservation;
    @Field("sensitive")
    String sensitive;
    //AVH extra fields
    @Field("recordNumber")
    String recordNumber;
    //For harvesting of images into the BIE
    @Field("references")
    String occurrenceDetails;
    @Field("rights")
    String rights;
    //@Field("photographer_s")
    String photographer;
    //@Field("grid_ref")
    String gridReference;
    @Field("*_s")
    Map<String, Object> miscStringProperties;
    @Field("*_i")
    Map<String, Object> miscIntProperties;
    @Field("*_d")
    Map<String, Object> miscDoubleProperties;
    @Field("*_dt")
    Map<String, Object> miscDateProperties;
    List<Map<String, Object>> imageMetadata;

    String imageUrl;
    String largeImageUrl;
    String smallImageUrl;
    String thumbnailUrl;
    String[] imageUrls;

    //sensitive fields and their non-sensitive replacements
    public static final String EVENT_DATE = "eventDate";
    public static final String EVENT_DATE_END = "eventDateEnd";
    public static final String GRID_REFERENCE = "gridReference";
    public static final String DAY = "day";
    public static final String EVENT_ID = "eventID";
    public static final String FOOTPRINT_WKT = "footprintWKT";

    public static final String[] sensitiveSOLRHdr = {
            "sensitive_decimalLongitude",
            "sensitive_decimalLatitude",
            "sensitive_locality",
            "sensitive_eventDate",
            "sensitive_eventDateEnd",
            "sensitive_gridReference",
            "sensitive_coordinateUncertaintyInMeters",
            "sensitive_day",
            "sensitive_eventID",
            "sensitive_eventTime",
            "sensitive_footprintWKT"};
    public static final String[] notSensitiveSOLRHdr = {LONGITUDE, LATITUDE, LOCALITY, EVENT_DATE, EVENT_DATE_END, GRID_REFERENCE, COORDINATE_UNCERTAINTY, DAY, EVENT_ID, FOOTPRINT_WKT};
    public static final String CONTAINS_SENSITIVE_PATTERN = StringUtils.join(sensitiveSOLRHdr, "|");

    public static final String NAMES_AND_LSID = "names_and_lsid";
    public static final String COMMON_NAME_AND_LSID = "common_name_and_lsid";
    public static final String DECADE_FACET_NAME = "decade";

    public static final String spatialField = "geohash";
    public static final String spatialFieldWMS = "quad";

    //featureAssertions

    public static final String DUPLICATE_OF = "isDuplicateOf";
    public static final String DUPLICATE_STATUS = "duplicateStatus";
    public static final String DUPLICATE_REASONS = "duplicateType";
    public static final String DUPLICATE_JUSTIFICATION = "duplicateJustification";
    public static final String ID = "id";

    public static final String SPECIES_SUBGROUP = "speciesSubgroup";
    public static final String SPECIES_GROUP = "speciesGroup";
    public static final String IMAGE_URL = "imageID";
    public static final String LAT_LNG = "lat_long";
    public static final String ALL_IMAGE_URL = "imageIDs";
    public static final String RAW_NAME = "raw_scientificName"; // TODO: check mapping (this is a guess)
    public static final String LFT = "lft";
    public static final String MONTH = "month";
    public static final String ASSERTIONS = "assertions";
    public static final String SUBSPECIES_NAME = "subspecies";
    public static final String SPECIES = "species";
    public static final String GENUS = "genus";
    public static final String FAMILY = "family";
    public static final String ORDER = "order";
    public static final String CLASS = "class";
    public static final String PHYLUM = "phylum";
    public static final String KINGDOM = "kingdom";
    public static final String SENSITIVE = "sensitive";
    public static final String RGT = "rgt";

    private void addToMapIfNotNull(Map<String, String> map, String key, String value) {
        if (value != null && value != "") {
            map.put(key, value);
        }
    }

    private String safeDblToString(Double d) {
        if (d != null) return d.toString();
        return null;
    }

    private String safeIntToString(Integer d) {
        if (d != null) return d.toString();
        return null;
    }

    private String arrToString(String[] arr) {
        try {
            if (arr != null) {
                ObjectMapper o = new ObjectMapper();
                return o.writeValueAsString(arr);
            }
        } catch (Exception e) {
        }
        return null;
    }

    @JsonIgnore
    public static String defaultFields = org.apache.commons.lang3.StringUtils.join(new OccurrenceIndex().indexToJsonMap().keySet(), ",");

    @JsonIgnore
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<String, String>();
        for (java.lang.reflect.Field f : OccurrenceIndex.class.getDeclaredFields()) {
            Field annotation = f.getAnnotation(Field.class);
            if (annotation != null && !annotation.value().contains("*")) {
                try {
                    String value = null;
                    if (f.getType() == Integer.class) value = safeIntToString((Integer) f.get(this));
                    else if (f.getType() == Double.class) value = safeDblToString((Double) f.get(this));
                    else if (f.getType() == Date.class)
                        value = DateFormatUtils.format((Date) f.get(this), "yyyy-MM-dd");
                    else if (f.getType() == String.class) value = (String) f.get(this);
                    else if (f.getType() == (new String[0]).getClass()) value = arrToString((String[]) f.get(this));
                    else
                        logger.error(new Exception("Field type not yet implemented in OccurrenceIndex.toMap: " + f.getType().getName() + " " + f.getName()));

                    addToMapIfNotNull(map, annotation.value(), value);
                } catch (IllegalAccessException e) {
                    logger.error(e);
                }
            }
        }
        return map;
    }

    @JsonIgnore
    public Map<String, String> indexToJsonMap() {
        Map<String, String> map = new HashMap<String, String>();
        for (java.lang.reflect.Field f : OccurrenceIndex.class.getDeclaredFields()) {
            Field annotation = f.getAnnotation(Field.class);
            if (annotation != null && !annotation.value().contains("*")) {
                map.put(annotation.value(), f.getName());
            }
        }
        return map;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getOccurrenceID() {
        return occurrenceID;
    }

    public void setOccurrenceID(String occurrenceID) {
        this.occurrenceID = occurrenceID;
    }

    public String[] getDataHubUid() {
        return dataHubUid;
    }

    public void setDataHubUid(String[] dataHubUid) {
        this.dataHubUid = dataHubUid;
    }

    public String[] getDataHub() {
        return dataHub;
    }

    public void setDataHub(String[] dataHub) {
        this.dataHub = dataHub;
    }

    public String getInstitutionUid() {
        return institutionUid;
    }

    public void setInstitutionUid(String institutionUid) {
        this.institutionUid = institutionUid;
    }

    public String getRaw_institutionCode() {
        return raw_institutionCode;
    }

    public void setRaw_institutionCode(String institutionCode) {
        this.raw_institutionCode = institutionCode;
    }

    public String getInstitutionName() {
        return institutionName;
    }

    public void setInstitutionName(String institutionName) {
        this.institutionName = institutionName;
    }

    public String getCollectionUid() {
        return collectionUid;
    }

    public void setCollectionUid(String collectionUid) {
        this.collectionUid = collectionUid;
    }

    public String getRaw_collectionCode() {
        return raw_collectionCode;
    }

    public void setRaw_collectionCode(String collectionCode) {
        this.raw_collectionCode = collectionCode;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getRaw_catalogNumber() {
        return raw_catalogNumber;
    }

    public void setRaw_catalogNumber(String catalogNumber) {
        this.raw_catalogNumber = catalogNumber;
    }

    public String getTaxonConceptID() {
        return taxonConceptID;
    }

    public void setTaxonConceptID(String taxonConceptID) {
        this.taxonConceptID = taxonConceptID;
    }

    public Date getEventDate() {
        return eventDate;
    }

    public void setEventDate(Date eventDate) {
        this.eventDate = eventDate;
    }

    public Date getEventDateEnd() {
        return eventDateEnd;
    }

    public void setEventDateEnd(Date eventDateEnd) {
        this.eventDateEnd = eventDateEnd;
    }

    public Date getOccurrenceYear() {
        return occurrenceYear;
    }

    @Field("occurrenceYear")
    public void setOccurrenceYear(List<Date> occurrenceYears) {
        if (occurrenceYears != null && occurrenceYears.size() > 0) {
            this.occurrenceYear = occurrenceYears.get(0);
        } else {
            this.occurrenceYear = null;
        }
    }

    public void setOccurrenceYear(Date occurrenceYear) {
        this.occurrenceYear = occurrenceYear;
    }

    public String getScientificName() {
        return scientificName;
    }

    public void setScientificName(String scientificName) {
        this.scientificName = scientificName;
    }

    public String getVernacularName() {
        return vernacularName;
    }

    public void setVernacularName(String vernacularName) {
        this.vernacularName = vernacularName;
    }

    public String getTaxonRank() {
        return taxonRank;
    }

    public void setTaxonRank(String taxonRank) {
        this.taxonRank = taxonRank;
    }

    public Integer getTaxonRankID() {
        return taxonRankID;
    }

    public void setTaxonRankID(Integer taxonRankID) {
        this.taxonRankID = taxonRankID;
    }

    public String getRaw_countryCode() {
        return raw_countryCode;
    }

    public void setRaw_countryCode(String raw_countryCode) {
        this.raw_countryCode = raw_countryCode;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getKingdom() {
        return kingdom;
    }

    public void setKingdom(String kingdom) {
        this.kingdom = kingdom;
    }

    public String getPhylum() {
        return phylum;
    }

    public void setPhylum(String phylum) {
        this.phylum = phylum;
    }

    public String getClasss() {
        return classs;
    }

    public void setClasss(String classs) {
        this.classs = classs;
    }

    public String getOrder() {
        return order;
    }

    public void setOrder(String order) {
        this.order = order;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getGenus() {
        return genus;
    }

    public void setGenus(String genus) {
        this.genus = genus;
    }

    public String getGenusGuid() {
        return genusGuid;
    }

    public void setGenusGuid(String genusGuid) {
        this.genusGuid = genusGuid;
    }

    public String getSpecies() {
        return species;
    }

    public void setSpecies(String species) {
        this.species = species;
    }

    public String getSpeciesGuid() {
        return speciesGuid;
    }

    public void setSpeciesGuid(String speciesGuid) {
        this.speciesGuid = speciesGuid;
    }

    public String getSubspecies() {
        return subspecies;
    }

    public void setSubspecies(String subspecies) {
        this.subspecies = subspecies;
    }

    public String getSubspeciesGuid() {
        return subspeciesGuid;
    }

    public void setSubspeciesGuid(String subspeciesGuid) {
        this.subspeciesGuid = subspeciesGuid;
    }

    public String getStateProvince() {
        return stateProvince;
    }

    public void setStateProvince(String stateProvince) {
        this.stateProvince = stateProvince;
    }

    public Double getDecimalLatitude() {
        return decimalLatitude;
    }

    public void setDecimalLatitude(Double decimalLatitude) {
        this.decimalLatitude = decimalLatitude;
    }

    public Double getDecimalLongitude() {
        return decimalLongitude;
    }

    public void setDecimalLongitude(Double decimalLongitude) {
        this.decimalLongitude = decimalLongitude;
    }

    public Double getCoordinateUncertaintyInMeters() {
        return coordinateUncertaintyInMeters;
    }

    public void setCoordinateUncertaintyInMeters(Double coordinateUncertaintyInMeters) {
        this.coordinateUncertaintyInMeters = coordinateUncertaintyInMeters;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getMonth() {
        return month != null ? String.format("%02d", month) : null;
    }

    public void setMonth(String month) {
        this.month = Integer.decode(month);
    }

    public String getBasisOfRecord() {
        return basisOfRecord;
    }

    public void setBasisOfRecord(String basisOfRecord) {
        this.basisOfRecord = basisOfRecord;
    }

    public String getTypeStatus() {
        return typeStatus;
    }

    public void setTypeStatus(String typeStatus) {
        this.typeStatus = typeStatus;
    }

    public String getRaw_locationRemarks() {
        return raw_locationRemarks;
    }

    public void setRaw_locationRemarks(String locationRemarks) {
        this.raw_locationRemarks = locationRemarks;
    }

    public String getRaw_occurrenceRemarks() {
        return raw_occurrenceRemarks;
    }

    public void setRaw_occurrenceRemarks(String occurrenceRemarks) {
        this.raw_occurrenceRemarks = occurrenceRemarks;
    }

    public Integer getLeft() {
        return left;
    }

    public void setLeft(Integer left) {
        this.left = left;
    }

    public Integer getRight() {
        return right;
    }

    public void setRight(Integer right) {
        this.right = right;
    }

    public String getIbra() {
        return ibra;
    }

    public void setIbra(String ibra) {
        this.ibra = ibra;
    }

    public String getImcra() {
        return imcra;
    }

    public void setImcra(String imcra) {
        this.imcra = imcra;
    }

    public String getLga() {
        return lga;
    }

    public void setLga(String lga) {
        this.lga = lga;
    }

    public String getDataProviderUid() {
        return dataProviderUid;
    }

    public void setDataProviderUid(String dataProviderUid) {
        this.dataProviderUid = dataProviderUid;
    }

    public String getDataProviderName() {
        return dataProviderName;
    }

    public void setDataProviderName(String dataProviderName) {
        this.dataProviderName = dataProviderName;
    }

    public String getDataResourceUid() {
        return dataResourceUid;
    }

    public void setDataResourceUid(String dataResourceUid) {
        this.dataResourceUid = dataResourceUid;
    }

    public String getDataResourceName() {
        return dataResourceName;
    }

    public void setDataResourceName(String dataResourceName) {
        this.dataResourceName = dataResourceName;
    }

    public String[] getAssertions() {
        return assertions;
    }

    public void setAssertions(String[] assertions) {
        this.assertions = assertions;
    }

    public String getHasUserAssertions() {
        return hasUserAssertions;
    }

    public void setHasUserAssertions(String hasUserAssertions) {
        this.hasUserAssertions = hasUserAssertions;
    }

    public String[] getSpeciesGroups() {
        return speciesGroups;
    }

    public void setSpeciesGroups(String[] speciesGroups) {
        this.speciesGroups = speciesGroups;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String[] getImages() {
        return images;
    }

    public void setImages(String[] images) {
        this.images = images;
    }

    public String getGeospatialKosher() {
        return geospatialKosher != null ? geospatialKosher.toString() : null;
    }

    public void setGeospatialKosher(String geospatialKosher) {
        this.geospatialKosher = Boolean.getBoolean(geospatialKosher);
    }

    public String getTaxonomicKosher() {
        return taxonomicKosher;
    }

    public void setTaxonomicKosher(String taxonomicKosher) {
        this.taxonomicKosher = taxonomicKosher;
    }

    public String[] getCollector() {
        return collector;
    }

    public void setCollector(String[] collector) {
        this.collector = collector;
    }

    public String getRaw_scientificName() {
        return raw_scientificName;
    }

    public void setRaw_scientificName(String raw_scientificName) {
        this.raw_scientificName = raw_scientificName;
    }

    public String getRaw_basisOfRecord() {
        return raw_basisOfRecord;
    }

    public void setRaw_basisOfRecord(String raw_basisOfRecord) {
        this.raw_basisOfRecord = raw_basisOfRecord;
    }

    public String getRaw_typeStatus() {
        return raw_typeStatus;
    }

    public void setRaw_typeStatus(String raw_typeStatus) {
        this.raw_typeStatus = raw_typeStatus;
    }

    public String getRaw_vernacularName() {
        return raw_vernacularName;
    }

    public void setRaw_vernacularName(String raw_vernacularName) {
        this.raw_vernacularName = raw_vernacularName;
    }

    public String getLatLong() {
        return latLong;
    }

    public void setLatLong(String latLong) {
        this.latLong = latLong;
    }

    public String getPoint1() {
        return point1;
    }

    public void setPoint1(String point1) {
        this.point1 = point1;
    }

    public String getPoint01() {
        return point01;
    }

    public void setPoint01(String point01) {
        this.point01 = point01;
    }

    public String getPoint001() {
        return point001;
    }

    public void setPoint001(String point001) {
        this.point001 = point001;
    }

    public String getPoint0001() {
        return point0001;
    }

    public void setPoint0001(String point0001) {
        this.point0001 = point0001;
    }

    public String getPoint00001() {
        return point00001;
    }

    public void setPoint00001(String point00001) {
        this.point00001 = point00001;
    }

    public String getNamesLsid() {
        return namesLsid;
    }

    public void setNamesLsid(String namesLsid) {
        this.namesLsid = namesLsid;
    }

    public String[] getMultimedia() {
        return multimedia;
    }

    public void setMultimedia(String[] multimedia) {
        this.multimedia = multimedia;
    }

    public String getAustConservation() {
        return austConservation;
    }

    public void setAustConservation(String austConservation) {
        this.austConservation = austConservation;
    }

    public String getStateConservation() {
        return stateConservation;
    }

    public void setStateConservation(String stateConservation) {
        this.stateConservation = stateConservation;
    }

    /**
     * @return the sensitive
     */
    public String getSensitive() {
        return sensitive;
    }

    /**
     * @param sensitive the sensitive to set
     */
    public void setSensitive(String sensitive) {
        this.sensitive = sensitive;
    }

    /**
     * @return the collector
     */
    public String[] getCollectors() {
        return collectors;
    }

    /**
     * @param collectors the collector to set
     */
    public void setCollectors(String[] collectors) {
        this.collectors = collectors;
    }

    /**
     * @return the recordNumber
     */
    public String getRecordNumber() {
        return recordNumber;
    }

    /**
     * @param recordNumber the recordNumber to set
     */
    public void setRecordNumber(String recordNumber) {
        this.recordNumber = recordNumber;
    }

    /**
     * @return the occurrence details
     */
    public String getOccurrenceDetails() {
        return occurrenceDetails;
    }

    /**
     * @param occurrenceDetails the occurrence details
     */
    public void setOccurrenceDetails(String occurrenceDetails) {
        this.occurrenceDetails = occurrenceDetails;
    }

    /**
     * @return rights information
     */
    public String getRights() {
        return rights;
    }

    /**
     * @param rights rights information
     */
    public void setRights(String rights) {
        this.rights = rights;
    }

    /**
     * @return photographer of the occurrence
     */
    public String getPhotographer() {
        return photographer;
    }

    /**
     * @param photographer photographer of the occurrence
     */
    public void setPhotographer(String photographer) {
        this.photographer = photographer;
    }

    public List<Map<String, Object>> getImageMetadata() {
        return imageMetadata;
    }

    public void setImageMetadata(List<Map<String, Object>> imageMetadata) {
        this.imageMetadata = imageMetadata;
    }

    public Map<String, Object> getMiscStringProperties() {
        return miscStringProperties;
    }

    public void setMiscStringProperties(Map<String, Object> miscStringProperties) {
        this.miscStringProperties = miscStringProperties;
    }

    public Map<String, Object> getMiscIntProperties() {
        return miscIntProperties;
    }

    public void setMiscIntProperties(Map<String, Object> miscIntProperties) {
        this.miscIntProperties = miscIntProperties;
    }

    public Map<String, Object> getMiscDoubleProperties() {
        return miscDoubleProperties;
    }

    public void setMiscDoubleProperties(Map<String, Object> miscDoubleProperties) {
        this.miscDoubleProperties = miscDoubleProperties;
    }

    public String getGridReference() {
        return gridReference;
    }

    public void setGridReference(String gridReference) {
        this.gridReference = gridReference;
    }

    public Map<String, Object> getMiscDateProperties() {
        return miscDateProperties;
    }

    public void setMiscDateProperties(Map<String, Object> miscDateProperties) {
        this.miscDateProperties = miscDateProperties;
    }

    public String getLicense() {
        return license;
    }

    public void setLicense(String license) {
        this.license = license;
    }

    public String getIdentificationVerificationStatus() {
        return identificationVerificationStatus;
    }

    public void setIdentificationVerificationStatus(String identificationVerificationStatus) {
        this.identificationVerificationStatus = identificationVerificationStatus;
    }

    public String getImageUrl() {
        return imageUrl;
    }

    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }

    public String getLargeImageUrl() {
        return largeImageUrl;
    }

    public void setLargeImageUrl(String largeImageUrl) {
        this.largeImageUrl = largeImageUrl;
    }

    public String getSmallImageUrl() {
        return smallImageUrl;
    }

    public void setSmallImageUrl(String smallImageUrl) {
        this.smallImageUrl = smallImageUrl;
    }

    public String getThumbnailUrl() {
        return thumbnailUrl;
    }

    public void setThumbnailUrl(String thumbnailUrl) {
        this.thumbnailUrl = thumbnailUrl;
    }

    public String[] getImageUrls() {
        return imageUrls;
    }

    public void setImageUrls(String[] imageUrls) {
        this.imageUrls = imageUrls;
    }
}
