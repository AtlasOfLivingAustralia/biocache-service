/**************************************************************************
 *  Copyright (C) 2011 Atlas of Living Australia
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

import au.org.ala.biocache.util.converter.FqField;
import au.org.ala.biocache.validate.ValidSpatialParams;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.Pattern;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the request parameters required to perform
 * a spatial search on occurrence records against biocache-service.
 *
 * Strictly a bean with no inherent logic other than validation through annotations.
 */
@Schema(name = "Spatial search parameters")
@ValidSpatialParams
@Getter
@Setter
public class SpatialSearchRequestParams {

    @Parameter(name="q", description = "Main search query. Examples 'q=Kangaroo' or 'q=vernacularName:red'")
    protected String q = "*:*";

    @FqField()
    @Parameter(name="fq", description = "Filter queries. " +
            "Examples 'fq=state:Victoria&fq=state:Queensland")
    protected String[] fq = {}; // must not be null

    @Parameter(name="qId", description = "Query ID for persisted queries")
    protected Long qId = null;  // "qid:12312321"

    @Parameter(name="fl", description = "Fields to return in the search response. Optional")
    protected String fl = OccurrenceIndex.defaultFields;

    @Parameter(name="facets", description = "The facets to be included by the search")
    protected String[] facets = FacetThemes.getAllFacetsLimited();

    @Parameter(name="flimit", description = "The limit for the number of facet values to return")
    protected Integer flimit = 30;

    @Parameter(name="fsort", description = "The sort order in which to return the facets.  Either 'count' or 'index'")
    protected String fsort = "";

    @Parameter(name="foffset", description = "The offset of facets to return.  Used in conjunction to flimit")
    protected Integer foffset = 0;

    @Parameter(name="fprefix", description = "The prefix to limit facet values")
    protected String fprefix ="";

    @Parameter(name="start", description = "Paging start index")
    protected Integer start;

    @Parameter(name="startIndex", description = "Deprecated  - Paging start index", deprecated = true)
    protected Integer startIndex ;

    @Parameter(name="pageSize", description = "The prefix to limit facet values")
    protected Integer pageSize = 10;

    @Parameter(name="sort", description = "The sort field to use")
    protected String sort = "score";

    @Parameter(name="dir", description = "Direction of sort")
    @Pattern(regexp="(asc|desc)")
    protected String dir = "asc";

    @Parameter(name="includeMultivalues", description = "Include multi values")
    protected Boolean includeMultivalues = false;

    @Parameter(name="qc", description = "The query context to be used for the search. " +
            "This will be used to generate extra query filters.")
    protected String qc = "";

    @Parameter(name="facet", description = "Enable/disable facets")
    protected Boolean facet = FacetThemes.getFacetDefault();

    @Parameter(name="qualityProfile", description = "The quality profile to use, null for default")
    protected String qualityProfile;

    @Parameter(name="disableAllQualityFilters", description = "Disable all default filters")
    protected boolean disableAllQualityFilters = false;

    @Parameter(name="disableQualityFilter", description = "Default filters to disable (currently can only disable on " +
            "category, so it's a list of disabled category name)")
    protected List<String> disableQualityFilter = new ArrayList<>();

    @Parameter(name="radius", description = "Radius for a spatial search")
    protected Float radius = null;

    @Parameter(name="lat", description = "Decimal latitude for the spatial search")
    protected Float lat = null;

    @Parameter(name="lon", description = "Decimal longitude for the spatial search")
    protected Float lon = null;

    @Parameter(name="wkt", description = "Well Known Text for the spatial search")
    protected String wkt = "";
}
