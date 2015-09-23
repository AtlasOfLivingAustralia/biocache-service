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

import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.List;

/**
 * A DTO bean that represents a single (facet) field result (SOLR)
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
public class GroupFieldResultDTO {
    String label;
    Long count;
    String fq;//the value to use for the FQ if it is different to the label
    /**
     * List of results from search
     */
    private List<OccurrenceIndex> occurrences;

    /**
     * Constructor
     *
     * @param fieldValue
     * @param count
     * @param fq
     */
    public GroupFieldResultDTO(String fieldValue, long count, String fq, List<OccurrenceIndex> occurrences) {
        this(fieldValue, count);
        this.fq = fq;
        this.occurrences = occurrences;
    }

    /**
     * Constructor
     *
     * @param fieldValue
     * @param count
     */
    public GroupFieldResultDTO(String fieldValue, long count) {
        this.label = fieldValue;
        this.count = count;
    }

    /**
     * Default constructor
     */
    public GroupFieldResultDTO() {
    }

    @JsonIgnore
    public void setFieldValue(String fieldValue) {
        this.label = fieldValue;
    }

    @JsonIgnore
    public String getFieldValue() {
        return label;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }


    /**
     * @return the fq
     */
    public String getFq() {
        return fq;
    }

    /**
     * @param fq the fq to set
     */
    public void setFq(String fq) {
        this.fq = fq;
    }

    /**
     * @see Object#toString()
     */
    @Override
    public String toString() {
        return "FieldResultDTO [count=" + count + ", label=" + label + "]";
    }

    public List<OccurrenceIndex> getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(List<OccurrenceIndex> values) {
        this.occurrences = values;
    }
}
