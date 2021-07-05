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
package au.org.ala.biocache.util;


import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

@Component("OccurrenceUtils")
public class OccurrenceUtils {

    private static final Logger logger = LoggerFactory.getLogger(OccurrenceUtils.class);

    @Inject
    protected SearchDAO searchDAO;

    private SolrDocument lookupRecordFromSolr(String uuid) {
        SpatialSearchRequestParams idRequest = new SpatialSearchRequestParams();
        idRequest.setQ("id:\"" + uuid + "\"");
        idRequest.setFacet(false);
        idRequest.setFl("*");
        idRequest.setPageSize(1);
        SolrDocumentList list = null;
        try {
            list = searchDAO.findByFulltext(idRequest);
        } catch (Exception ignored) {
            logger.debug("Failed to find occurrence with id " + uuid);
        }
        return (list != null && list.size() > 0) ? list.get(0) : null;
    }

    public SolrDocument getOcc(String uuid) {
        return lookupRecordFromSolr(uuid);
    }

    @Value("${media.store.url:}")
    private String remoteMediaStoreUrl;

    public Map<String, String> getImageFormats(String imageId) {
        Map<String, String> map = new HashMap();
        map.put("thumb", remoteMediaStoreUrl + "/image/proxyImageThumbnail?imageId=" + imageId);
        map.put("small", remoteMediaStoreUrl + "/image/proxyImageThumbnail?imageId=" + imageId);
        map.put("large", remoteMediaStoreUrl + "/image/proxyImageThumbnailLarge?imageId=" + imageId);
        map.put("raw", remoteMediaStoreUrl + "/image/proxyImage?imageId=" + imageId);

        return map;
    }
}
