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
    /** True when the field is a multivalue field */
    private boolean multivalue;
    /**
     * True when the field is a docvalue field
     */
    private boolean docvalue;
    /** Stores the number of distinct values that are in the field */
    private Integer numberDistinctValues;
    /** the i18n string to used for the field. */
    private String description;
    /** the i18n information used for this field */
    private String info;
    /** the occurrences/search json key for this field */
    private String jsonName;
    /** the DwC name for this field */
    private String dwcTerm;
    /** the download name for this field (biocache-store name) valid for DownloadRequestParams.fl */
    private String downloadName;
    /** the download description for this field when downloadName is used in DownloadRequestParams.fl */
    private String downloadDescription;
    /** the values in this field can be looked up within i18n using name.{value} */
    private Boolean i18nValues;
    /** class of this field */
    private String classs;

    @Override
    public boolean equals(Object obj){
        if(obj instanceof IndexFieldDTO && name != null){
            if (name.equals(((IndexFieldDTO)obj).getName())) {
                //test the Cassandra field name
                return (downloadName != null && downloadName.equals(((IndexFieldDTO)obj).getDownloadName())) ||
                        (downloadName == null && ((IndexFieldDTO)obj).getDownloadName() == null);
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        if (downloadName != null) {
            return (name + downloadName).hashCode();
        } else {
            return name.hashCode();
        }
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
    /**
     * @return the i18nValues
     */
    public Boolean isI18nValues() {
        return i18nValues;
    }
    /**
     * @param i18nValues the jsonName to set
     */
    public void setI18nValues(Boolean i18nValues) {
        this.i18nValues = i18nValues;
    }
    public String getJsonName() {
        return jsonName;
    }
    /**
     * @param jsonName the jsonName to set
     */
    public void setJsonName(String jsonName) {
        this.jsonName = jsonName;
    }
    /**
     * @return the info
     */
    public String getInfo() {
        return info;
    }
    /**
     * @param info the info to set
     */
    public void setInfo(String info) {
        this.info = info;
    }
    /**
     * @return the dwcTerm
     */
    public String getDwcTerm() {
        return dwcTerm;
    }
    /**
     * @param dwcTerm the dwcTerm to set
     */
    public void setDwcTerm(String dwcTerm) {
        this.dwcTerm = dwcTerm;
    }
    /**
     * @return the downloadName
     */
    public String getDownloadName() {
        return downloadName;
    }
    /**
     * @param downloadName the downloadName to set
     */
    public void setDownloadName(String downloadName) {
        this.downloadName = downloadName;
    }
    /**
     * @return the downloadDescription
     */
    public String getDownloadDescription() {
        return downloadDescription;
    }
    /**
     * @param downloadDescription the downloadName to set
     */
    public void setDownloadDescription(String downloadDescription) {
        this.downloadDescription = downloadDescription;
    }

    @Override
    public int compareTo(IndexFieldDTO other) {
        //Include the Cassandra field name
        return (this.getName() + " " + this.getDownloadName()).compareTo(other.getName() + " " + other.getDownloadName());
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

    public void setClasss(String classs) {
        this.classs = classs;
    }

    public String getClasss() {
        return classs;
    }

    public void setMultivalue(boolean multivalue) {
        this.multivalue = multivalue;
    }

    public boolean isMultivalue() {
        return multivalue;
    }

    public boolean isDocvalue() {
        return docvalue;
    }

    public void setDocvalue(boolean docvalue) {
        this.docvalue = docvalue;
    }
}
