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
package au.org.ala.biocache.web;

import au.org.ala.biocache.service.SpeciesSearchService;
import au.org.ala.biocache.util.converter.FqField;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.Parameter;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import java.util.Map;

/**
 * a basic autocomplete service using the SpeciesLookupService
 */
@Controller
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AutocompleteController extends AbstractSecureController {

    @Inject
    protected SpeciesSearchService speciesSearchService;

    @Operation(summary = "Autocomplete service which filters only lists taxa with occurrence data", tags = "Autocomplete")
    @Tag(name ="Autocomplete", description = "Services that support autocomplete text boxes")
    @RequestMapping(value = {
            "autocomplete/search" }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public
    @ResponseBody
    Map search(
            @RequestParam(value = "q") String query,
            @FqField @RequestParam(value = "fq", required = false) String[] filterQuery,

            @Parameter(description = "Maximum number of results to return")
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer max,

            @Parameter(description = "Whether to include all taxa, not just those with occurrence records")
            @RequestParam(value = "all", required = false, defaultValue = "false") Boolean includeAll,

            @Parameter(description = "Whether to include synonyms in the search results")
            @RequestParam(value = "synonyms", required = false, defaultValue = "true") Boolean searchSynonyms,

            @Parameter(description = "Whether to include occurrence counts in the results")
            @RequestParam(value = "counts", required = false, defaultValue = "true") Boolean counts,

            @Parameter(description = "Whether to return a response that omits some fields for a quicker response")
            @RequestParam(value = "simplified", required = false, defaultValue = "false") Boolean simplified) throws Exception {
        return speciesSearchService.search(query, filterQuery, max, searchSynonyms, includeAll, counts, simplified);
    }
}
