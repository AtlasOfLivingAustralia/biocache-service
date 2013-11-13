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
package org.ala.biocache.dto;

/**
 * DTO for the fields that belong to the index.
 * 
 * A field is available for faceting if indexed=true 
 * 
 * @author "Natasha Carter <Natasha.Carter@csiro.au>"
 */
public class IndexFieldDTO implements Comparable<IndexFieldDTO> {
    /** The name of the field in the index */
    private String name;
    /** The SOLR data type for the field */
    private String dataType;
    /** True when the field is available in the index for searching purposes */
    private boolean indexed;
    /** True when the field is available for extraction in search results */
    private boolean stored;
    /** Stores the number of distinct values that are in the field */
    private Integer numberDistinctValues;
    /** the i18n string to used for the field. */
    private String description;
    
    @Override
    public boolean equals(Object obj){
        if(obj instanceof IndexFieldDTO && name != null){
            return name.equals(((IndexFieldDTO)obj).getName());
        }
        return false;
    }
    
    /**
     * @return the name
     */
    public String getName() {
        return name;
    }
    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }
    /**
     * @return the dataType
     */
    public String getDataType() {
        return dataType;
    }
    /**
     * @param dataType the dataType to set
     */
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
    /**
     * @return the indexed
     */
    public boolean isIndexed() {
        return indexed;
    }
    /**
     * @param indexed the indexed to set
     */
    public void setIndexed(boolean indexed) {
        this.indexed = indexed;
    }
    /**
     * @return the stored
     */
    public boolean isStored() {
        return stored;
    }
    /**
     * @param stored the stored to set
     */
    public void setStored(boolean stored) {
        this.stored = stored;
    }
    /**
     * @return the numberDistinctValues
     */
    public Integer getNumberDistinctValues() {
        return numberDistinctValues;
    }
    /**
     * @param numberDistinctValues the numberDistinctValues to set
     */
    public void setNumberDistinctValues(Integer numberDistinctValues) {
        this.numberDistinctValues = numberDistinctValues;
    }

    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public int compareTo(IndexFieldDTO other) {        
        return this.getName().compareTo(other.getName());
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return "IndexFieldDTO [name=" + name + ", dataType=" + dataType
          + ", indexed=" + indexed + ", stored=" + stored
          + ", numberDistinctValues=" + numberDistinctValues + "]";
    }
    
    
    
}
