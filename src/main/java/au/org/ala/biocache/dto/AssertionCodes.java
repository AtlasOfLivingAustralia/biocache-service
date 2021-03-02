package au.org.ala.biocache.dto;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static au.org.ala.biocache.dto.ErrorCode.Category.Error;
import static au.org.ala.biocache.dto.ErrorCode.Category.*;

// TODO: Use pipeline assertion codes instead of biocache-store assertion codes

/**
 * Assertion codes for records. These codes are a reflection of http://bit.ly/evMJv5
 * <p>
 * Merged from biocache-store
 */
public class AssertionCodes {

    //geospatial issues
    static final public ErrorCode GEOSPATIAL_ISSUE = new ErrorCode("geospatialIssue", 0, true, "Geospatial issue", Warning);  // general purpose option
    static final public ErrorCode NEGATED_LATITUDE = new ErrorCode("negatedLatitude", 1, false, "Latitude is negated", Warning);
    static final public ErrorCode NEGATED_LONGITUDE = new ErrorCode("negatedLongitude", 2, false, "Longitude is negated", Warning);
    static final public ErrorCode INVERTED_COORDINATES = new ErrorCode("invertedCoordinates", 3, false, "Coordinates are transposed", Warning);
    static final public ErrorCode ZERO_COORDINATES = new ErrorCode("zeroCoordinates", 4, true, "Supplied coordinates are zero", Warning);
    static final public ErrorCode COORDINATES_OUT_OF_RANGE = new ErrorCode("coordinatesOutOfRange", 5, true, "Coordinates are out of range", Error);
    static final public ErrorCode UNKNOWN_COUNTRY_NAME = new ErrorCode("unknownCountry", 6, false, "Supplied country not recognised", Error);
    static final public ErrorCode ALTITUDE_OUT_OF_RANGE = new ErrorCode("altitudeOutOfRange", 7, false, "Altitude out of range", Error);
    static final public ErrorCode BADLY_FORMED_ALTITUDE = new ErrorCode("erroneousAltitude", 8, false, "Badly formed altitude", Error);
    static final public ErrorCode MIN_MAX_ALTITUDE_REVERSED = new ErrorCode("minMaxAltitudeReversed", 9, false, "Min and max altitude reversed", Warning);
    static final public ErrorCode DEPTH_IN_FEET = new ErrorCode("depthInFeet", 10, false, "Depth value supplied in feet", Warning);
    static final public ErrorCode DEPTH_OUT_OF_RANGE = new ErrorCode("depthOutOfRange", 11, false, "Depth out of range", Error);
    static final public ErrorCode MIN_MAX_DEPTH_REVERSED = new ErrorCode("minMaxDepthReversed", 12, false, "Min and max depth reversed", Warning);
    static final public ErrorCode ALTITUDE_IN_FEET = new ErrorCode("altitudeInFeet", 13, false, "Altitude value supplied in feet", Warning);
    static final public ErrorCode ALTITUDE_NON_NUMERIC = new ErrorCode("altitudeNonNumeric", 14, false, "Altitude value non-numeric", Error);
    static final public ErrorCode DEPTH_NON_NUMERIC = new ErrorCode("depthNonNumeric", 15, false, "Depth value non-numeric", Error);
    static final public ErrorCode COUNTRY_COORDINATE_MISMATCH = new ErrorCode("countryCoordinateMismatch", 16, false, "Coordinates dont match supplied country", Error);
    static final public ErrorCode STATE_COORDINATE_MISMATCH = new ErrorCode("stateCoordinateMismatch", 18, false, "Coordinates dont match supplied state", Error);
    static final public ErrorCode COORDINATE_HABITAT_MISMATCH = new ErrorCode("habitatMismatch", 19, true, "Habitat incorrect for species", Error);
    static final public ErrorCode DETECTED_OUTLIER = new ErrorCode("detectedOutlier", 20, true, "Suspected outlier", Error);
    static final public ErrorCode COUNTRY_INFERRED_FROM_COORDINATES = new ErrorCode("countryInferredByCoordinates", 21, false, "Country inferred from coordinates", Warning);
    static final public ErrorCode COORDINATES_CENTRE_OF_STATEPROVINCE = new ErrorCode("coordinatesCentreOfStateProvince", 22, true, "Supplied coordinates centre of state", Warning);
    static final public ErrorCode COORDINATE_PRECISION_MISMATCH = new ErrorCode("coordinatePrecisionMismatch", 23, false, "Coordinate precision not valid", Error);
    static final public ErrorCode PRECISION_RANGE_MISMATCH = new ErrorCode("precisionRangeMismatch", 17, false, "The precision value should be between 0 and 1.", Error);
    static final public ErrorCode UNCERTAINTY_RANGE_MISMATCH = new ErrorCode("uncertaintyRangeMismatch", 24, false, "Coordinate accuracy not valid", Error);
    static final public ErrorCode UNCERTAINTY_IN_PRECISION = new ErrorCode("uncertaintyInPrecision", 25, false, "Coordinate precision and accuracy transposed", Error);
    static final public ErrorCode SPECIES_OUTSIDE_EXPERT_RANGE = new ErrorCode("speciesOutsideExpertRange", 26, true, "Geographic coordinates are outside the range as defined by 'expert/s' for the taxa", Error);
    static final public ErrorCode UNCERTAINTY_NOT_SPECIFIED = new ErrorCode("uncertaintyNotSpecified", 27, false, "Coordinate uncertainty was not supplied", Missing);
    static final public ErrorCode COORDINATES_CENTRE_OF_COUNTRY = new ErrorCode("coordinatesCentreOfCountry", 28, true, "Supplied coordinates centre of country", Warning);
    static final public ErrorCode MISSING_COORDINATEPRECISION = new ErrorCode("missingCoordinatePrecision", 29, false, "coordinatePrecision not supplied with the record", Missing);
    static final public ErrorCode MISSING_GEODETICDATUM = new ErrorCode("missingGeodeticDatum", 30, false, "geodeticDatum not supplied for coordinates", Missing);
    static final public ErrorCode MISSING_GEOREFERNCEDBY = new ErrorCode("missingGeorefencedBy", 31, false, "GeoreferencedBy not supplied with the record", Missing);
    static final public ErrorCode MISSING_GEOREFERENCEPROTOCOL = new ErrorCode("missingGeoreferenceProtocol", 32, false, "GeoreferenceProtocol not supplied with the record", Missing);
    static final public ErrorCode MISSING_GEOREFERENCESOURCES = new ErrorCode("missingGeoreferenceSources", 33, false, "GeoreferenceSources not supplied with the record", Missing);
    static final public ErrorCode MISSING_GEOREFERENCEVERIFICATIONSTATUS = new ErrorCode("missingGeoreferenceVerificationStatus", 34, false, "GeoreferenceVerificationStatus not supplied with the record", Missing);
    static final public ErrorCode INVALID_GEODETICDATUM = new ErrorCode("invalidGeodeticDatum", 35, false, "The geodetic datum is not valid", Error);

    static final public ErrorCode MISSING_GEOREFERENCE_DATE = new ErrorCode("missingGeoreferenceDate", 42, false, "GeoreferenceDate not supplied with the record", Missing);
    static final public ErrorCode LOCATION_NOT_SUPPLIED = new ErrorCode("locationNotSupplied", 43, false, "No location information has been provided with the record", Missing);
    static final public ErrorCode DECIMAL_COORDINATES_NOT_SUPPLIED = new ErrorCode("decimalCoordinatesNotSupplied", 44, false, "No decimal longitude and latitude provided", Missing);
    static final public ErrorCode DECIMAL_LAT_LONG_CONVERTED = new ErrorCode("decimalLatLongConverted", 45, false, "Decimal latitude and longitude were converted to WGS84", Warning);
    static final public ErrorCode DECIMAL_LAT_LONG_CONVERSION_FAILED = new ErrorCode("decimalLatLongConversionFailed", 46, true, "Conversion of decimal latitude and longitude to WGS84 failed", Error);
    static final public ErrorCode DECIMAL_LAT_LONG_CALCULATED_FROM_VERBATIM = new ErrorCode("decimalLatLongCalculatedFromVerbatim", 47, false, "Decimal latitude and longitude were calculated using verbatimLatitude, verbatimLongitude and verbatimSRS", Warning);
    static final public ErrorCode DECIMAL_LAT_LONG_CALCULATION_FROM_VERBATIM_FAILED = new ErrorCode("decimalLatLongCalculationFromVerbatimFailed", 48, true, "Failed to calculate decimal latitude and longitude from verbatimLatitude, verbatimLongitude and verbatimSRS", Error);
    static final public ErrorCode DECIMAL_LAT_LONG_CALCULATED_FROM_EASTING_NORTHING = new ErrorCode("decimalLatLongCalculatedFromEastingNorthing", 49, false, "Decimal latitude and longitude were calculated using easting, nothing and zone", Warning);
    static final public ErrorCode DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED = new ErrorCode("decimalLatLongCalculationFromEastingNorthingFailed", 50, true, "Failed to calculate decimal latitude and longitude using easting, northing and zone", Error);
    ;
    static final public ErrorCode GEODETIC_DATUM_ASSUMED_WGS84 = new ErrorCode("geodeticDatumAssumedWgs84", 51, false, "Geodetic datum assumed to be WGS84 (EPSG:4326)", Warning);
    static final public ErrorCode UNRECOGNIZED_GEODETIC_DATUM = new ErrorCode("unrecognizedGeodeticDatum", 52, false, "Geodetic datum not recognized", Error);
    static final public ErrorCode ZERO_LATITUDE_COORDINATES = new ErrorCode("zeroLatitude", 53, true, "Supplied latitude is zero", Warning);
    static final public ErrorCode ZERO_LONGITUDE_COORDINATES = new ErrorCode("zeroLongitude", 54, true, "Supplied longitude are zero", Warning);

    static final public ErrorCode DECIMAL_LAT_LONG_CALCULATED_FROM_GRID_REF = new ErrorCode("decimalLatLongCalculatedFromGridReference", 55, false, "Decimal latitude and longitude were calculated using grid reference", Warning);

    //taxonomy issues
    static final public ErrorCode TAXONOMIC_ISSUE = new ErrorCode("taxonomicIssue", 10000, false, "Taxonomic issue", Error);  // general purpose option
    static final public ErrorCode INVALID_SCIENTIFIC_NAME = new ErrorCode("invalidScientificName", 10001, false, "Invalid scientific name", Error);
    static final public ErrorCode UNKNOWN_KINGDOM = new ErrorCode("unknownKingdom", 10002, false, "Kingdom not recognised", Error);
    static final public ErrorCode AMBIGUOUS_NAME = new ErrorCode("ambiguousName", 10003, false, "Higher taxonomy missing", Error);
    static final public ErrorCode NAME_NOTRECOGNISED = new ErrorCode("nameNotRecognised", 10004, false, "Name not recognised", Error);
    static final public ErrorCode NAME_NOT_IN_NATIONAL_CHECKLISTS = new ErrorCode("nameNotInNationalChecklists", 10005, false, "Name not in national checklists", Warning);
    static final public ErrorCode HOMONYM_ISSUE = new ErrorCode("homonymIssue", 10006, false, "Homonym issues with supplied name", Error);
    static final public ErrorCode IDENTIFICATION_INCORRECT = new ErrorCode("identificationIncorrect", 10007, false, "Taxon misidentified", Error);
    static final public ErrorCode MISSING_TAXONRANK = new ErrorCode("missingTaxonRank", 10008, false, "taxonRank not supplied with the record", Missing);
    static final public ErrorCode MISSING_IDENTIFICATIONQUALIFIER = new ErrorCode("missingIdentificationQualifier", 10009, false, "identificationQualifier not supplied with the record", Missing);
    static final public ErrorCode MISSING_IDENTIFIEDBY = new ErrorCode("missingIdentifiedBy", 10010, false, "identifiedBy not supplied with the record", Missing);
    static final public ErrorCode MISSING_IDENTIFICATIONREFERENCES = new ErrorCode("missingIdentificationReferences", 10011, false, "identificationReferences not supplied with the record", Missing);
    static final public ErrorCode MISSING_DATEIDENTIFIED = new ErrorCode("missingDateIdentified", 10012, false, "identificationDate not supplied with the record", Missing);
    ;
    static final public ErrorCode NAME_NOT_SUPPLIED = new ErrorCode("nameNotSupplied", 10015, false, "No scientific name or vernacular name was supplied", Missing);

    //miscellaneous issues
    static final public ErrorCode MISSING_BASIS_OF_RECORD = new ErrorCode("missingBasisOfRecord", 20001, true, "Basis of record not supplied", Missing);
    static final public ErrorCode BADLY_FORMED_BASIS_OF_RECORD = new ErrorCode("badlyFormedBasisOfRecord", 20002, true, "Basis of record badly formed", Error);
    static final public ErrorCode UNRECOGNISED_TYPESTATUS = new ErrorCode("unrecognisedTypeStatus", 20004, false, "Type status not recognised", Error);
    static final public ErrorCode UNRECOGNISED_COLLECTIONCODE = new ErrorCode("unrecognisedCollectionCode", 20005, false, "Collection code not recognised", Error);
    static final public ErrorCode UNRECOGNISED_INSTITUTIONCODE = new ErrorCode("unrecognisedInstitutionCode", 20006, false, "Institution code not recognised", Error);
    static final public ErrorCode INVALID_IMAGE_URL = new ErrorCode("invalidImageUrl", 20007, false, "Image URL invalid", Error);
    static final public ErrorCode RESOURCE_TAXONOMIC_SCOPE_MISMATCH = new ErrorCode("resourceTaxonomicScopeMismatch", 20008, false, "Taxonomic scope mismatch between record and resource", Error);
    static final public ErrorCode DATA_ARE_GENERALISED = new ErrorCode("dataAreGeneralised", 20009, false, "The data has been supplied generalised", Warning);
    static final public ErrorCode OCCURRENCE_IS_CULTIVATED_OR_ESCAPEE = new ErrorCode("occCultivatedEscapee", 20010, false, "The occurrence is cultivated or escaped.", Warning);
    static final public ErrorCode INFERRED_DUPLICATE_RECORD = new ErrorCode("inferredDuplicateRecord", 20014, false, "The occurrence appears to be a duplicate", Warning);
    static final public ErrorCode MISSING_CATALOGUENUMBER = new ErrorCode("missingCatalogueNumber", 20015, false, "No catalogue number has been supplied", Missing);
    static final public ErrorCode RECORDED_BY_UNPARSABLE = new ErrorCode("recordedByUnparsable", 20016, false, "RecordedBy value unparseable", Warning);
    static final public ErrorCode UNRECOGNISED_OCCURRENCE_STATUS = new ErrorCode("unrecognisedOccurrenceStatus", 20017, false, "Occurrence status not recognised", Error);
    static final public ErrorCode ASSUMED_PRESENT_OCCURRENCE_STATUS = new ErrorCode("assumedPresentOccurrenceStatus", 20018, false, "Occurrence status assumed to be present", Warning);
    static final public ErrorCode USER_ASSERTION_OTHER = new ErrorCode("userAssertionOther", 20019, false, "Other error", Error);

    //temporal issues
    static final public ErrorCode TEMPORAL_ISSUE = new ErrorCode("temporalIssue", 30000, false, "Temporal issue", Error);  // general purpose option
    static final public ErrorCode ID_PRE_OCCURRENCE = new ErrorCode("idPreOccurrence", 30001, false, "Identification date before occurrence date", Error);
    static final public ErrorCode GEOREFERENCE_POST_OCCURRENCE = new ErrorCode("georefPostDate", 30002, false, "Georeferenced after occurrence date", Error);
    static final public ErrorCode FIRST_OF_MONTH = new ErrorCode("firstOfMonth", 30003, false, "First of the month", Warning);
    static final public ErrorCode FIRST_OF_YEAR = new ErrorCode("firstOfYear", 30004, false, "First of the year", Warning);
    static final public ErrorCode FIRST_OF_CENTURY = new ErrorCode("firstOfCentury", 30005, false, "First of the century", Warning);
    static final public ErrorCode DATE_PRECISION_MISMATCH = new ErrorCode("datePrecisionMismatch", 30006, false, "Date precision invalid", Error);
    static final public ErrorCode INVALID_COLLECTION_DATE = new ErrorCode("invalidCollectionDate", 30007, false, "Invalid collection date", Error);
    static final public ErrorCode MISSING_COLLECTION_DATE = new ErrorCode("missingCollectionDate", 30008, false, "Missing collection date", Missing);
    static final public ErrorCode DAY_MONTH_TRANSPOSED = new ErrorCode("dayMonthTransposed", 30009, false, "Day and month transposed", Warning);
    static final public ErrorCode INCOMPLETE_COLLECTION_DATE = new ErrorCode("incompleteCollectionDate", 30010, false, "The date supplied was incomplete, missing the day and/or month component", Warning);

    //verified type - this is a special code indicating a verification of an existing assertion
    static final public ErrorCode VERIFIED = new ErrorCode("userVerified", 50000, true, "Record Verified by collection manager", Verified);

 /* static final public ErrorCode QA_OPEN_ISSUE = new ErrorCode("qaStatusOpen", 50001, true, "Open issue", Verified)
  static final public ErrorCode QA_VERIFIED = new ErrorCode("qaStatusVerified", 50002, true, "Verified", Verified)
  static final public ErrorCode QA_CORRECTED = new ErrorCode("qaStatusCorrected", 50003, true, "Corrected", Verified)*/


    //this is a code user can use to flag a issue with processing
    static final public ErrorCode PROCESSING_ERROR = new ErrorCode("processingError", 60000, true, "The system has incorrectly processed a record", Error);

    //media issues
    static final public ErrorCode MEDIA_REPRESENTATIVE = new ErrorCode("mediaRepresentative", 70000, false, "Media representative of taxon", Comment);
    static final public ErrorCode MEDIA_UNREPRESENTATIVE = new ErrorCode("mediaUnrepresentative", 70001, false, "Media not representative of taxon", Comment);

    //        /**
//         * Retrieve all the terms defined in this vocab.
//         * @return
//         */
//        static final public ErrorCode retrieveAll : Set[ErrorCode] = (for {
//            method <- this.getClass.getMethods
//            if(method.getReturnType.getName == "au.org.ala.biocache.vocab.ErrorCode")
//        } yield (method.invoke(this).asInstanceOf[ErrorCode])).toSet[ErrorCode]
//
//        //all the codes
//        static final public ErrorCode all = retrieveAll
//
//        //ranges for error codes
//        static final public ErrorCode geospatialBounds = (0, 10000)
//        static final public ErrorCode taxonomicBounds = (10000, 20000)
//        static final public ErrorCode miscellanousBounds = (20000, 30000)
//        static final public ErrorCode temporalBounds = (30000, 40000)
//        static final public ErrorCode importantCodes = Array(4,5,18,19,26)
//
//        static final public ErrorCode geospatialCodes = all.filter(errorCode => {errorCode.code >= geospatialBounds._1 && errorCode.code < geospatialBounds._2})
//        static final public ErrorCode taxonomicCodes = all.filter(errorCode => {errorCode.code>=10000 && errorCode.code<20000})
//        static final public ErrorCode miscellaneousCodes = all.filter(errorCode => {errorCode.code>=20000 && errorCode.code<30000})
//        static final public ErrorCode temporalCodes = all.filter(errorCode => {errorCode.code>=30000 && errorCode.code<40000})
//
    static final public ErrorCode[] userAssertionCodes = new ErrorCode[]{GEOSPATIAL_ISSUE, COORDINATE_HABITAT_MISMATCH, DETECTED_OUTLIER, TAXONOMIC_ISSUE, IDENTIFICATION_INCORRECT, TEMPORAL_ISSUE, USER_ASSERTION_OTHER};

    public static ErrorCode[] getAll() {
        List<ErrorCode> list = new ArrayList();
        for (Field field : AssertionCodes.class.getFields()) {
            if (field.getType() == ErrorCode.class) {
                try {
                    list.add((ErrorCode) field.get(null));
                } catch (Exception e) {
                    // Igore exceptions
                }

            }
        }
        return list.toArray(new ErrorCode[0]);

    }

    public static ErrorCode getByName(String name) {
        for (Field field : AssertionCodes.class.getFields()) {
            if (field.getType() == ErrorCode.class) {
                try {
                    ErrorCode ec = (ErrorCode) field.get(null);
                    if (ec.name.equals(name) || field.getName().equals(name)) {
                        return ec;
                    }
                } catch (Exception e) {
                    // Igore exceptions
                }

            }
        }
        return null;
    }
}
