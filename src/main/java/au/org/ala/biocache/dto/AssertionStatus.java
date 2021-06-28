package au.org.ala.biocache.dto;

/**
 * Simple enum of quality assertion code status.
 * <p>
 * Merged from biocache-store
 */
public class AssertionStatus {

    // For System Assertions
    static final public Integer FAILED = 0;
    static final public Integer PASSED = 1;
    static final public Integer UNCHECKED = 2;

    // For user assertions
    static final public Integer QA_OPEN_ISSUE = 50001;  //open and unresolved issue with the data - but confirmed as a problem
    static final public Integer QA_VERIFIED = 50002;   //record has been verified by collection manager as being correct to the best of their knowledge.
    static final public Integer QA_CORRECTED = 50003;  //the record has been corrected by data custodian - the update may or may not be visible yet
    static final public Integer QA_NONE = 50004;     //status of a record with no user assertions ??
    static final public Integer QA_UNCONFIRMED = 50005;//open issue
}
