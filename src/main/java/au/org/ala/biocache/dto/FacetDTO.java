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

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * A facet for FacetThemes and FacetTheme.
 */
@Schema(name = "Facet")
public class FacetDTO {

    private String field;
    private String sort;
    private String description;
    private String dwcTerm;
    private Boolean i18nValues;

    public FacetDTO() {}

    public FacetDTO(String title, String sort, String description, String dwcTerm, Boolean i18nValues) {
        this.field = title;
        this.sort = sort;
        this.description = description;
        this.dwcTerm = dwcTerm;
        this.i18nValues = i18nValues;
    }

    /**
     * @return the title
     */
    public String getField() {
        return field;
    }

    /**
     * @param field the field to set
     */
    public void setField(String field) {
        this.field = field;
    }

    /**
     * @return the defaultSort
     */
    public String getSort() {
        return sort;
    }

    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * @param dwcTerm the dwcTerm to set
     */
    public void setDwcTerm(String dwcTerm) {
        this.dwcTerm = dwcTerm;
    }

    /**
     * @return the dwcTerm
     */
    public String getDwcTerm() {
        return dwcTerm;
    }

    /**
     * @param i18nValues the i18nValues to set
     */
    public void setI18nValues(Boolean i18nValues) {
        this.i18nValues = i18nValues;
    }

    /**
     * @return the i18nValues
     */
    public Boolean isI18nValues() {
        return i18nValues;
    }
}