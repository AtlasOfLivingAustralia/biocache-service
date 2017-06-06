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

import au.org.ala.biocache.service.SpeciesLookupService;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.inject.Inject;
import java.util.Map;

/**
 * a basic autocomplete service using the SpeciesLookupService
 */
@Controller
public class AutocompleteController extends AbstractSecureController {

    @Inject
    protected SpeciesLookupService speciesLookupIndexService;

    @RequestMapping(value = "autocomplete/search", method = RequestMethod.GET)
    public
    @ResponseBody
    Map search(
            @RequestParam(value = "q", required = true) String query,
            @RequestParam(value = "fq", required = false) String[] filterQuery,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer max,
            @RequestParam(value = "all", required = false, defaultValue = "false") Boolean includeAll,
            @RequestParam(value = "synonyms", required = false, defaultValue = "true") Boolean searchSynonyms,
            @RequestParam(value = "counts", required = false, defaultValue = "true") Boolean counts) throws Exception {


        return speciesLookupIndexService.search(query, filterQuery, max, searchSynonyms, includeAll, counts);
    }
}
