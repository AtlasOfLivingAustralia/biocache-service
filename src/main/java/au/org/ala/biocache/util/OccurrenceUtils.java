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
import au.org.ala.biocache.dto.OccurrenceDTO;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.model.FullRecord;
import au.org.ala.biocache.model.QualityAssertion;
import au.org.ala.biocache.parser.ProcessedValue;
import au.org.ala.biocache.poso.POSO;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.springframework.stereotype.Component;
import scala.collection.Iterator;

import javax.inject.Inject;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Component("OccurrenceUtils")
public class OccurrenceUtils {

    @Inject
    protected SearchDAO searchDAO;

    String RAW_PREFIX = "raw_";

    static final Pattern EL_REGEX = Pattern.compile("el[0-9]{1,}");
    static final Pattern CL_REGEX = Pattern.compile("cl[0-9]{1,}");

    /**
     * Retrieve occurrence record from the search index.
     *
     * @param uuid
     * @param includeSensitive
     * @return quality assertions
     */
    public FullRecord[] getAllVersionsByUuid(String uuid, Boolean includeSensitive) throws Exception {
        SolrDocumentList result = lookupRecordFromSolr(uuid);
        return getFullRecord(uuid, result);
    }

    private SolrDocumentList lookupRecordFromSolr(String uuid) throws Exception {
        SpatialSearchRequestParams idRequest = new SpatialSearchRequestParams();
        idRequest.setQ("id:\"" + uuid + "\"");
        idRequest.setFacet(false);
        idRequest.setFl("*");
        return searchDAO.findByFulltext(idRequest);
    }

    private FullRecord[] getFullRecord(String uuid, SolrDocumentList result) {

        FullRecord raw = new FullRecord();
        FullRecord processed = new FullRecord();
        FullRecord[] fullRecord = new FullRecord[]{raw, processed};

        //initialise ID
        raw.setRowKey(uuid);
        processed.setRowKey(uuid);

        Map<String, String> cl = new HashMap<String,String>();
        Map<String, Object> el = new HashMap<String,Object>();

        if (result.getNumFound() == 1){
            SolrDocument doc = result.iterator().next();
            for (String fieldName : doc.getFieldNames()){
                Object value = doc.getFieldValue(fieldName);
                if(EL_REGEX.matcher(fieldName).matches()){
                    el.put(fieldName, Double.parseDouble(value.toString()));
                } else if(CL_REGEX.matcher(fieldName).matches()){
                    cl.put(fieldName, value.toString());
                } else if (fieldName.startsWith(RAW_PREFIX)){
                    //we have a processed field
                    raw.setProperty(fieldName.substring(RAW_PREFIX.length()), value.toString());
                }  else {
                    processed.setProperty(fieldName, value.toString());
                }
            }
        }

        processed.setCl(cl);
        processed.setEl(el);

        return fullRecord;
    }


    public OccurrenceDTO getOcc(String uuid) throws Exception {
        SolrDocumentList result = lookupRecordFromSolr(uuid);
        FullRecord[] fullRecord = getFullRecord(uuid, result);
        OccurrenceDTO occ = new OccurrenceDTO(fullRecord);

        Map<String, List<QualityAssertion>> assertions = new HashMap<String, List<QualityAssertion>>();
        SolrDocument doc = result.iterator().next();
        Collection<Object> values = doc.getFieldValues("assertions");
        final List<String> assertionList = new ArrayList<>();

        if (values == null){
            if (doc.getFieldValue("assertions") != null){
                assertionList.add((String) doc.getFieldValue("assertions"));
            }
        } else {
            values.stream().forEach(value -> assertionList.add((String) value));
        }

        //"passed" vs "failed"
        List<OccurrenceIssue> passedIssues = Arrays.stream(OccurrenceIssue.values()).filter( occurrenceIssue ->
                !assertionList.contains(occurrenceIssue.name())
        ).collect(Collectors.toList());

        //failed
        List<QualityAssertion> failed = new ArrayList<QualityAssertion>();
        List<QualityAssertion> passed = new ArrayList<QualityAssertion>();

        for (String value : assertionList) {
            QualityAssertion qa = new QualityAssertion();
            qa.setName(value);
            failed.add(qa);
        }
        for (OccurrenceIssue passedIssue :  passedIssues){
            QualityAssertion qa = new QualityAssertion();
            qa.setName(passedIssue.name());
            passed.add(qa);
        }

        assertions.put("failed", failed);
        assertions.put("passed", passed);
        occ.setSystemAssertions(assertions);
        return occ;
    }

    public Map getComparisonByUuid(String uuid) throws Exception {

        FullRecord[] fullRecord = getAllVersionsByUuid(uuid, false);
        FullRecord f1 = fullRecord[0];
        FullRecord f2 = fullRecord[1];

        POSO[] posos1 = f1.objectArray();
        POSO[] posos2 = f2.objectArray();

        Map<String, List<ProcessedValue>> comparison = new HashMap<String, List<ProcessedValue>>();

        for (int i = 0; i < posos1.length; i++) {

            POSO poso1 = posos1[i];
            POSO poso2 = posos2[i];

            String objectName = poso1.getClass().getName().substring(poso1.getClass().getName().lastIndexOf(".") + 1);
            List<ProcessedValue> values = new ArrayList<ProcessedValue>();
            scala.collection.immutable.List<String> properties = poso1.getPropertyNames();
            Iterator<String> propIter = properties.iterator();
            while (propIter.hasNext()) {
                String property = propIter.next();
                String p1 = poso1.getProperty(property).isEmpty() ? "" : poso1.getProperty(property).get();
                String p2 = poso2.getProperty(property).isEmpty() ? "" : poso2.getProperty(property).get();
                if(p1 != "" || p2 != "") {
                    values.add(new ProcessedValue(property, p1, p2));
                }
            }
            comparison.put(objectName, values);
        }
        return comparison;
    }
}
