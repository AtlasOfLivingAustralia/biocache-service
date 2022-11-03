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

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * DTO for the native information about a species.
 * This class has been renamed from AustralianDTO.
 *  
 * @author "Natasha Carter <Natasha.Carter@csiro.au>"
 */
@Schema(name = "Native")
public class NativeDTO {
	
    @Parameter(description = "Indicates that the supplied taxon guid is on the National Species List")
    boolean isNSL = false;

    @Parameter(description = " Indicates that there are occurrence records for taxonConceptID")
    boolean hasOccurrences = false;

    @Parameter(description = " Indicates that the only occurrence records found were source from Citizen science")
    boolean hasCSOnly = false;

    /** The taxonGuid that the information is about */
    String taxonGuid;
    
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
     * @return True when the species is considered native.
     */
    public boolean getIsNative(){
        return isNSL || hasOccurrences;
    }
}
