package org.ala.biocache.dto;

/**
 * DTO for the Australian information about a species.
 * 
 *  
 * @author "Natasha Carter <Natasha.Carter@csiro.au>"
 */
public class AustralianDTO {
    /** Indicates that the supplied taxon guid is on the National Species List*/
    private boolean isNSL =false;
    /** Indicates that there are occurrence records for taxonGuid */
    private boolean hasOccurrences=false;
    /** Indicates that the only occurrence records found were source from CS */
    private boolean hasCSOnly=false;
    /** The taxonGuid that the information is about */
    private String taxonGuid;
    /**
     * @return the isNSL
     */
    public boolean getIsNSL() {
        return isNSL;
    }

    /**
     * @param isNSL the isNSL to set
     */
    public void setIsNSL(boolean isNSL) {
        this.isNSL = isNSL;
    }

    /**
     * @return the hasOccurrenceRecords
     */
    public boolean isHasOccurrences() {
        return hasOccurrences;
    }
    /**
     * @param hasOccurrences the hasOccurrenceRecords to set
     */
    public void setHasOccurrenceRecords(boolean hasOccurrences) {
        this.hasOccurrences = hasOccurrences;
    }
    
    /**
     * @return the taxonGuid
     */
    public String getTaxonGuid() {
        return taxonGuid;
    }

    /**
     * @param taxonGuid the taxonGuid to set
     */
    public void setTaxonGuid(String taxonGuid) {
        this.taxonGuid = taxonGuid;
    }

    /**
     * @return the hasCSOnly
     */
    public boolean isHasCSOnly() {
        return hasCSOnly;
    }

    /**
     * @param hasCSOnly the hasCSOnly to set
     */
    public void setHasCSOnly(boolean hasCSOnly) {
        this.hasCSOnly = hasCSOnly;
    }

    /**
     * 
     * @return True when the species is considered Australian.
     */
    public boolean getIsAustralian(){
        return isNSL || hasOccurrences;
    }    
}
