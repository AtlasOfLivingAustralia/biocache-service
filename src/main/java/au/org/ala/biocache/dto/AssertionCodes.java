package au.org.ala.biocache.dto;

import org.gbif.api.vocabulary.InterpretationRemarkSeverity;
import org.gbif.api.vocabulary.OccurrenceIssue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static au.org.ala.biocache.dto.ErrorCode.Category.Error;
import static au.org.ala.biocache.dto.ErrorCode.Category.*;

/**
 * Assertion codes for records.
 *
 * See https://github.com/gbif/pipelines/issues/530
 */
public class AssertionCodes {

    //geospatial issues
    static final public ErrorCode GEOSPATIAL_ISSUE = new ErrorCode("geospatialIssue", 0, true, "Geospatial issue", Warning);  // general purpose option
    static final public ErrorCode COORDINATE_HABITAT_MISMATCH = new ErrorCode("habitatMismatch", 19, true, "Habitat incorrect for species", Error);
    static final public ErrorCode DETECTED_OUTLIER = new ErrorCode("detectedOutlier", 20, true, "Suspected outlier", Error);
    static final public ErrorCode DECIMAL_LAT_LONG_CALCULATED_FROM_EASTING_NORTHING = new ErrorCode("decimalLatLongCalculatedFromEastingNorthing", 49, false, "Decimal latitude and longitude were calculated using easting, nothing and zone", Warning);
    static final public ErrorCode DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED = new ErrorCode("decimalLatLongCalculationFromEastingNorthingFailed", 50, true, "Failed to calculate decimal latitude and longitude using easting, northing and zone", Error);

    //taxonomy issues
    static final public ErrorCode TAXONOMIC_ISSUE = new ErrorCode("taxonomicIssue", 10000, false, "Taxonomic issue", Error);  // general purpose option
    static final public ErrorCode IDENTIFICATION_INCORRECT = new ErrorCode("identificationIncorrect", 10007, false, "Taxon misidentified", Error);
    static final public ErrorCode USER_ASSERTION_OTHER = new ErrorCode("userAssertionOther", 20019, false, "Other error", Error);
    static final public ErrorCode USER_DUPLICATE_RECORD = new ErrorCode("userDuplicateRecord", 20020, false,"The occurrence appears to be a duplicate", Warning);

    //temporal issues
    static final public ErrorCode TEMPORAL_ISSUE = new ErrorCode("temporalIssue", 30000, false, "Temporal issue", Error);  // general purpose option

    //verified type - this is a special code indicating a verification of an existing assertion
    static final public ErrorCode VERIFIED = new ErrorCode("userVerified", 50000, true, "Record Verified by collection manager", Verified);

    static final public ErrorCode[] userAssertionCodes = new ErrorCode[]{ GEOSPATIAL_ISSUE, COORDINATE_HABITAT_MISMATCH, DETECTED_OUTLIER, TAXONOMIC_ISSUE, IDENTIFICATION_INCORRECT, TEMPORAL_ISSUE, USER_DUPLICATE_RECORD, USER_ASSERTION_OTHER };

    public static List<ErrorCode> getCodes(){

        List<ErrorCode> codes = new ArrayList<>();
        ALAOccurrenceIssue[] alaIssues = ALAOccurrenceIssue.values();
        OccurrenceIssue[] issues = OccurrenceIssue.values();

        // convert to "ErrorCode"
        for (ALAOccurrenceIssue issue : alaIssues){
            ErrorCode.Category c = null;
            if (issue.getSeverity().equals(InterpretationRemarkSeverity.ERROR)){
                c = ErrorCode.Category.Error;
            } else if (issue.getSeverity().equals(InterpretationRemarkSeverity.WARNING)){
                c = ErrorCode.Category.Warning;
            } else {
                c = ErrorCode.Category.Comment;
            }

            // String name, Integer code, Boolean isFatal, String description, ErrorCode.Category category
            codes.add(new ErrorCode(issue.name(),
                    issue.ordinal() + 1000,
                    issue.getSeverity().equals(InterpretationRemarkSeverity.ERROR),
                    issue.name().toLowerCase().replaceAll("_", " "),
                    c));
        }
        for (OccurrenceIssue issue : issues){
            ErrorCode.Category c = null;
            if (issue.getSeverity().equals(InterpretationRemarkSeverity.ERROR)){
                c = ErrorCode.Category.Error;
            } else if (issue.getSeverity().equals(InterpretationRemarkSeverity.WARNING)){
                c = ErrorCode.Category.Warning;
            } else {
                c = ErrorCode.Category.Comment;
            }

            // String name, Integer code, Boolean isFatal, String description, ErrorCode.Category category
            codes.add(new ErrorCode(issue.name(),
                    issue.ordinal() + 2000,
                    issue.getSeverity().equals(InterpretationRemarkSeverity.ERROR),
                    issue.name().toLowerCase().replaceAll("_", " "),
                    c));
        }

        return codes;
    }

    public static ErrorCode[] getAll() {
        return getCodes().toArray(new ErrorCode[0]);
    }


    // Retrieve an error code by the numeric code
    public static ErrorCode getByCode(int code) {
        return Arrays.stream(getAll())
                .filter(errorCode -> errorCode.code == code)
                .findFirst()
                .orElse(null);
    }

    public static ErrorCode getByName(String name) {
        return getCodes().stream().filter(errorCode -> errorCode.getName().equals(name)).findFirst().get();
    }
}
