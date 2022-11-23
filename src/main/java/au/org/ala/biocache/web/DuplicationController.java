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
import au.org.ala.biocache.dto.SpatialSearchRequestDTO;
import au.org.ala.biocache.util.SearchUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
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
    @Tag(name = "Duplicates", description = "Services for retrieval of duplication information on occurrence records")
    @RequestMapping(value = {"/duplicates/{recordUuid}"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody
    DuplicateRecordDetails getDuplicateStats(@PathVariable("recordUuid") String recordUuid) throws Exception {
        return getDuplicateStatsForGuid(recordUuid);
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
        SpatialSearchRequestDTO query = new SpatialSearchRequestDTO();
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
    @RequestMapping(value = {"/stats/{recordUuid}"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody
    Map<String, FieldStatsInfo> printStats(@PathVariable("recordUuid") String recordUuid) throws Exception {
        return indexDao.getStatistics(recordUuid);
    }
}
