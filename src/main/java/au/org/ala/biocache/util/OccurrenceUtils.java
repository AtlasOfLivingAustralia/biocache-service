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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Component("OccurrenceUtils")
public class OccurrenceUtils {

    @Inject
    protected SearchDAO searchDAO;

    String RAW_PREFIX = "raw_";

    static final Pattern EL_REGEX = Pattern.compile("el[0-9]{1,}");
    static final Pattern CL_REGEX = Pattern.compile("cl[0-9]{1,}");

    private SolrDocument lookupRecordFromSolr(String uuid) throws Exception {
        SpatialSearchRequestParams idRequest = new SpatialSearchRequestParams();
        idRequest.setQ("id:\"" + uuid + "\"");
        idRequest.setFacet(false);
        idRequest.setFl("*");
        SolrDocumentList list = searchDAO.findByFulltext(idRequest);
        if (list.size() > 0) {
            return list.get(0);
        }
        return null;
    }

//    private FullRecord[] getFullRecord(String uuid, SolrDocumentList result) {
//        FullRecord raw = new FullRecord();
//        FullRecord processed = new FullRecord();
//        FullRecord[] fullRecord = new FullRecord[]{raw, processed};
//
//        //initialise ID
//        raw.setRowKey(uuid);
//        processed.setRowKey(uuid);
//
//        Map<String, String> cl = new HashMap<String,String>();
//        Map<String, Object> el = new HashMap<String,Object>();
//
//        if (result.getNumFound() == 1){
//            SolrDocument doc = result.iterator().next();
//            for (String fieldName : doc.getFieldNames()){
//                Object value = doc.getFieldValue(fieldName);
//                if(EL_REGEX.matcher(fieldName).matches()){
//                    el.put(fieldName, Double.parseDouble(value.toString()));
//                } else if(CL_REGEX.matcher(fieldName).matches()){
//                    cl.put(fieldName, value.toString());
//                } else if (fieldName.startsWith(RAW_PREFIX)){
//                    //we have a processed field
//                    raw.setProperty(fieldName.substring(RAW_PREFIX.length()), value.toString());
//                }  else {
//                    processed.setProperty(fieldName, value.toString());
//                }
//            }
//        }
//
//        processed.setCl(cl);
//        processed.setEl(el);
//
//        return sd;
//    }


    public SolrDocument getOcc(String uuid) throws Exception {
        SolrDocument sd = lookupRecordFromSolr(uuid);

//        Map<String, List<QualityAssertion>> assertions = new HashMap<String, List<QualityAssertion>>();
//        SolrDocument doc = result.iterator().next();
//        Collection<Object> values = doc.getFieldValues("assertions");
//        if(values == null){
//            values = new ArrayList<Object>();
//            if (doc.getFieldValue("assertions") != null){
//                values.add(doc.getFieldValue("assertions"));
//            }
//        }
//
//        //"passed" vs "failed"
//
//        //failed
//        List<QualityAssertion> failed = new ArrayList<QualityAssertion>();
//        List<QualityAssertion> passed = new ArrayList<QualityAssertion>();
//
//        if(values != null) {
//            for (Object value : values) {
//                QualityAssertion qa = new QualityAssertion();
//                qa.setName((String) value);
//                failed.add(qa);
//            }
//        }
//
//        assertions.put("failed", failed);
//        assertions.put("passed", passed);
//        occ.setSystemAssertions(assertions);
        return sd;
    }

    @Value("${media.store.url:}")
    private String remoteMediaStoreUrl;


    // TODO: a different `getImageFormats` is required may be required if a different MediaStore class is in use.
    public Map<String, String> getImageFormats(String imageId) {
        Map<String, String> map = new HashMap();
        map.put("thumb", remoteMediaStoreUrl + "/image/proxyImageThumbnail?imageId=" + imageId);
        map.put("small", remoteMediaStoreUrl + "/image/proxyImageThumbnail?imageId=" + imageId);
        map.put("large", remoteMediaStoreUrl + "/image/proxyImageThumbnailLarge?imageId=" + imageId);
        map.put("raw", remoteMediaStoreUrl + "/image/proxyImage?imageId=" + imageId);

        return map;
    }
}
