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
import au.org.ala.biocache.Store;
import au.org.ala.biocache.model.DuplicateRecordDetails;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.inject.Inject;
import java.util.Map;

@Controller
public class DuplicationController {

    /** Logger initialisation */
    private final static Logger logger = Logger.getLogger(DuplicationController.class);
    /** Fulltext search DAO */
    @Inject
    protected SearchDAO searchDAO;

    /**
     * Retrieves the duplication information for the supplied guid.
     * <p/>
     * Returns empty details when the record is not the "representative" occurrence.
     *
     * @param guid
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {"/duplicates/{guid:.+}.json*", "/duplicates/{guid:.+}*"})
    public @ResponseBody DuplicateRecordDetails getDuplicateStats(@PathVariable("guid") String guid) throws Exception {
        try {
            return Store.getDuplicateDetails(guid);
        } catch (Exception e) {
            logger.error("Unable to get duplicate details for " + guid, e);
            return new DuplicateRecordDetails();
        }
    }

    @RequestMapping(value = {"/stats/{guid:.+}.json*", "/stats/{guid:.+}*"})
    public @ResponseBody Map<String, FieldStatsInfo> printStats(@PathVariable("guid") String guid) throws Exception {
        SpatialSearchRequestParams searchParams = new SpatialSearchRequestParams();
        searchParams.setQ("*:*");
        searchParams.setFacets(new String[]{guid});
        return searchDAO.getStatistics(searchParams);
    }
}
