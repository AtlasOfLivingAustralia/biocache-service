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

import java.util.List;

public class FacetTheme {

    private String title;
    private FacetDTO[] facets;

    public FacetTheme() {}

    public FacetTheme(String title, FacetDTO... facets) {
        this.title = title;
        this.facets = facets;
    }

    public FacetTheme(String title, List<FacetDTO> facets) {
        this.title = title;
        this.facets = facets.toArray(new FacetDTO[0]);
    }

    /**
     * @return the title
     */
    public String getTitle() {
        return title;
    }

    /**
     * @param title the title to set
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * @return the facets
     */
    public FacetDTO[] getFacets() {
        return facets;
    }

    /**
     * @param facets the facets to set
     */
    public void setFacets(FacetDTO[] facets) {
        this.facets = facets;
    }
}