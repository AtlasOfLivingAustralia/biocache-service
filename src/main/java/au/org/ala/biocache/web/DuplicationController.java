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

import au.org.ala.biocache.dao.IndexDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.DuplicateRecordDetails;
import au.org.ala.biocache.dto.PointType;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.util.SearchUtils;
import io.swagger.annotations.Api;
import io.swagger.v3.oas.annotations.Operation;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonInclude;

import static au.org.ala.biocache.dto.OccurrenceIndex.*;

@Controller
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DuplicationController {

    /**
     * Logger initialisation
     */
    private final static Logger logger = Logger.getLogger(DuplicationController.class);
    /**
     * Fulltext search DAO
     */
    @Inject
    protected SearchDAO searchDAO;
    @Inject
    protected IndexDAO indexDao;
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
    @Operation(summary = "Retrieves the duplication information for the supplied guid.", tags = "Duplicates")
    @RequestMapping(value = {"/duplicates/**"}, method = RequestMethod.GET)
    public @ResponseBody
    DuplicateRecordDetails getDuplicateStats(HttpServletRequest request) throws Exception {
        String guid = searchUtils.getGuidFromPath(request);

        return getDuplicateStatsForGuid(guid);
    }

    private DuplicateRecordDetails getDuplicateStatsForGuid(String guid) {
        try {

            SolrDocumentList sdl = searchDuplicates(ID, guid);
            if (sdl.isEmpty()) {
                return null;
            }

            SolrDocument sd = sdl.get(0);

            DuplicateRecordDetails drd = new DuplicateRecordDetails(sd);

            if (DuplicateRecordDetails.REPRESENTATIVE.equals(drd.getStatus())) {
                // is representative id
                List<DuplicateRecordDetails> dups = new ArrayList<>();
                SolrDocumentList list = searchDuplicates(DUPLICATE_OF, guid);
                for (SolrDocument d : list) {
                    dups.add(new DuplicateRecordDetails(d));
                }
                drd.setDuplicates(dups);
            } else if (DuplicateRecordDetails.ASSOCIATED.equals(drd.getStatus())) {
                // is duplicate id, return result for the representative id
                return getDuplicateStatsForGuid(drd.getDuplicateOf());
            } else {
                // not a duplicate
                return null;
            }

            return drd;
        } catch (Exception e) {
            logger.error("Unable to get duplicate details for " + guid, e);
            return new DuplicateRecordDetails();
        }
    }

    private SolrDocumentList searchDuplicates(String field, String value) throws Exception {
        SpatialSearchRequestParams query = new SpatialSearchRequestParams();
        query.setFacet(false);
        query.setQ(field + ":" + value);
        query.setFl(String.join(",",
                ID,
                DUPLICATE_OF,
                DUPLICATE_REASONS,
                DUPLICATE_STATUS,
                DUPLICATE_JUSTIFICATION,
                TAXON_CONCEPT_ID,
                PointType.POINT_1.getLabel(),
                PointType.POINT_01.getLabel(),
                PointType.POINT_001.getLabel(),
                PointType.POINT_0001.getLabel(),
                LAT_LNG,
                RAW_TAXON_NAME,
                COLLECTOR,
                RECORD_NUMBER,
                CATALOGUE_NUMBER,
                DATA_RESOURCE_UID
        ));
        return searchDAO.findByFulltext(query);
    }

    @Operation(summary = "Retrieves the duplication statistics", tags = "Duplicates")
    @RequestMapping(value = {"/stats/**"}, method = RequestMethod.GET)
    public @ResponseBody
    Map<String, FieldStatsInfo> printStats(HttpServletRequest request) throws Exception {
        String guid = searchUtils.getGuidFromPath(request);
        return indexDao.getStatistics(guid);
    }
}
