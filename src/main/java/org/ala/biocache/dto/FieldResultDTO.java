

package org.ala.biocache.dto;

import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * A DTO bean that represents a single (facet) field result (SOLR)
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
public class FieldResultDTO implements Comparable<FieldResultDTO>{
    String label;
    Long count;

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
