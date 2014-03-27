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

/**
 * A DTO bean that represents a single (facet) field result (SOLR)
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
public class FieldResultDTO implements Comparable<FieldResultDTO>{
    String label;
    Long count;
    String fq;//the value to use for the FQ if it is different to the label

    /**
     * Constructor
     * 
     * @param fieldValue
     * @param count
     * @param fq 
     */
    public FieldResultDTO(String fieldValue, long count, String fq) {
        this(fieldValue,count);
        this.fq = fq;
    }

    /**
     * Constructor
     * 
     * @param fieldValue
     * @param count 
     */
    public FieldResultDTO(String fieldValue, long count) {
        this.label = fieldValue;
        this.count = count;
    }
    
    /**
     * Default constructor
     */
    public FieldResultDTO() {}
    
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
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "FieldResultDTO [count=" + count + ", label=" + label + "]";
	}

    @Override
    public int compareTo(FieldResultDTO t) {
        return this.getLabel().compareTo(t.getLabel());
    }
    @Override
    public boolean equals(Object o){
    	//2 field results are considered the same if they are for the same field.
    	// don't change this without revising the Endemism WS's
        if(o instanceof FieldResultDTO){
            if(label != null)
                return label.equals(((FieldResultDTO)o).getLabel());
        }
        return false;
    }
}
