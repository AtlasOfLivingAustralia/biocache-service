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
package au.org.ala.biocache.service;

import java.util.Map;

/**
 * Service layer interface for accessing species lookups.
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
public interface SpeciesSearchService {

    /**
     * Return autocomplete results.
     *
     * @param query search term
     * @param filterQuery fq applied to occurrence counts returned. e.g. geospatial_kosher:true
     * @param max limit returned matches
     * @param includeSynonyms can include matched synonyms (or resolve to their parents)
     * @param includeAll do not limit remove matches with 0 matches
     * @param counts include occurrence counts in the output
     * @return
     */
    Map search(String query, String [] filterQuery, int max, boolean includeSynonyms, boolean includeAll, boolean counts);


}
