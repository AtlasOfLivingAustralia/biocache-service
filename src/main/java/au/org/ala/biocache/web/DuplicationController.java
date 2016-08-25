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
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.model.DuplicateRecordDetails;
import au.org.ala.biocache.util.SearchUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import java.util.Map;

@Controller
public class DuplicationController {

    /** Logger initialisation */
    private final static Logger logger = Logger.getLogger(DuplicationController.class);
    /** Fulltext search DAO */
    @Inject
    protected SearchDAO searchDAO;
    @Inject
    protected SearchUtils searchUtils;

    /**
     * Retrieves the duplication information for the supplied guid.
     * <p/>
     * Returns empty details when the record is not the "representative" occurrence.
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {"/duplicates/**"})
    public @ResponseBody DuplicateRecordDetails getDuplicateStats(HttpServletRequest request) throws Exception {
        String guid = searchUtils.getGuidFromPath(request);
        try {
            return Store.getDuplicateDetails(guid);
        } catch (Exception e) {
            logger.error("Unable to get duplicate details for " + guid, e);
            return new DuplicateRecordDetails();
        }
    }

    @RequestMapping(value = {"/stats/**"})
    public @ResponseBody Map<String, FieldStatsInfo> printStats(HttpServletRequest request) throws Exception {
        String guid = searchUtils.getGuidFromPath(request);
        SpatialSearchRequestParams searchParams = new SpatialSearchRequestParams();
        searchParams.setQ("*:*");
        searchParams.setFacets(new String[]{guid});
        return searchDAO.getStatistics(searchParams);
    }
}
