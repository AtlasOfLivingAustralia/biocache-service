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

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Group result for a SOLR search
 */
@Schema(name="FacetPivotResult")
public class FacetPivotResultDTO {
    /**
     * Name of the value of this pivot
     */
    private String value;
    /**
     * Name of the field being treated as a facet for the pivotResult
     */
    private String pivotField;
    /**
     * Set of pivot results
     */
    private List<FacetPivotResultDTO> pivotResult;
    /**
     * The number of distinct values in the field - can only be populated from "groups"
     */
    private Integer count;


    /**
     * Constructor
     *
     * @param pivotField  Field used as a facet
     * @param pivotResult Terms and counts returned from a facet search on this field
     */
    public FacetPivotResultDTO(String pivotField, List<FacetPivotResultDTO> pivotResult) {
        this.pivotField = pivotField;
        this.pivotResult = pivotResult;
    }

    public FacetPivotResultDTO(String pivotField, List<FacetPivotResultDTO> pivotResult, String value, Integer count) {
        this(pivotField, pivotResult);
        this.value = value != null ? value : "";
        this.count = count;
    }

    /**
     * Default constructor
     */
    public FacetPivotResultDTO() {
    }

    public String getPivotField() {
        return pivotField;
    }

    public void setPivotField(String pivotField) {
        this.pivotField = pivotField;
    }

    public List<FacetPivotResultDTO> getPivotResult() {
        return pivotResult;
    }

    public void setPivotResult(ArrayList<FacetPivotResultDTO> pivotResult) {
        this.pivotResult = pivotResult;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
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
