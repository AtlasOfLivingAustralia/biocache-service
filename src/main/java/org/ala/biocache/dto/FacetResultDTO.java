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
package org.ala.biocache.dto;

import java.util.ArrayList;
import java.util.List;

/**
 * Facet result for a SOLR search
 * 
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
public class FacetResultDTO {
    /** Name of the field being treated as a facet */
    private String fieldName;
    /** Set of facet field results */
    private List<FieldResultDTO> fieldResult; 
    /** The number of distinct values in the field - can only be populated from "groups" */
    private Integer count;


    /**
     * Constructor
     * 
     * @param fieldName Field used as a facet
     * @param fieldResult Terms and counts returned from a facet search on this field
     */
    public FacetResultDTO(String fieldName, List<FieldResultDTO> fieldResult) {
        this.fieldName = fieldName;
        this.fieldResult = fieldResult;
    }
    
    public FacetResultDTO(String fieldName, List<FieldResultDTO> fieldResult, Integer count) {
        this(fieldName,fieldResult);
        this.count = count;
    }

    /**
     * Default constructor
     */
    public FacetResultDTO() {}

    /*
     * Getters & Setters
     */
    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public List<FieldResultDTO> getFieldResult() {
        return fieldResult;
    }

    public void setFieldResult(ArrayList<FieldResultDTO> fieldResult) {
        this.fieldResult = fieldResult;
    }

    /**
     * @return the count
     */
    public Integer getCount() {
        return count;
    }

    /**
     * @param count the count to set
     */
    public void setCount(Integer count) {
        this.count = count;
    }
    
}
