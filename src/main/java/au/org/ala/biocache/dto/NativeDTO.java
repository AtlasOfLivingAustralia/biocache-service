/**************************************************************************
 *  Copyright (C) 2013 Atlas of Living Australia
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

/**
 * DTO for the native information about a species.
 * This class has been renamed from AustralianDTO.
 *  
 * @author "Natasha Carter <Natasha.Carter@csiro.au>"
 */
public class NativeDTO {
	
    /** Indicates that the supplied taxon guid is on the National Species List*/
    private boolean isNSL = false;
    /** Indicates that there are occurrence records for taxonGuid */
    private boolean hasOccurrences = false;
    /** Indicates that the only occurrence records found were source from CS */
    private boolean hasCSOnly = false;
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
