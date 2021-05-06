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


import au.org.ala.biocache.dao.StoreDAO;
import au.org.ala.biocache.dto.ContactDTO;
import au.org.ala.biocache.dto.OccurrenceIndex;
import au.org.ala.biocache.dto.QualityAssertion;
import au.org.ala.biocache.dto.UserAssertions;
import au.org.ala.biocache.service.AuthService;
import org.apache.solr.common.SolrDocument;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Optional;

import static au.org.ala.biocache.dto.OccurrenceIndex.ID;

@Component("assertionUtils")
public class AssertionUtils {

    @Inject
    protected AuthService authService;
    @Inject
    protected ContactUtils contactUtils;
    @Inject
    protected OccurrenceUtils occurrenceUtils;
    @Inject
    protected StoreDAO storeDao;

    /**
     * Retrieve the user assertions adding additional metadata about users
     * and attribution.
     *
     * @param recordUuid
     * @return quality assertions
     */
    public UserAssertions getUserAssertions(String recordUuid, Double version) throws Exception {
        SolrDocument sd = occurrenceUtils.getOcc(recordUuid);
        return getUserAssertions(sd, version);
    }

    /**
     * Retrieve the user assertions adding additional metadata about users
     * and attribution.
     *
     * @param occ
     * @return quality assertions
     */
    public UserAssertions getUserAssertions(SolrDocument sd, Double version) throws IOException {
        if (sd.containsKey(ID)) {
            //set the user assertions
            UserAssertions userAssertions = storeDao.get(UserAssertions.class, (String) sd.getFieldValue(OccurrenceIndex.ID)).orElse(new UserAssertions());
            //Legacy integration - fix up the user assertions - legacy - to add replace with CAS IDs....
            for (QualityAssertion ua : userAssertions) {
                // remove snapshot
                ua.setSnapshot(null);

                if (ua.getUserId().contains("@")) {
                    String email = ua.getUserId();
                    String userId = authService.getMapOfEmailToId().get(email);
                    ua.setUserEmail(email);
                    ua.setUserId(userId);
                }

                //add user roles....
                enhanceQA(sd, ua, version);
            }
            return userAssertions;
        }

        return null;
    }

    public QualityAssertion enhanceQA(SolrDocument sd, QualityAssertion ua, Double version) {
        String collectionUid = OccurrenceIndex.COLLECTION_UID;
        String collectionName = OccurrenceIndex.COLLECTION_NAME;

        String email = ua.getUserEmail();
        ContactDTO contact = contactUtils.getContactForEmailAndUid(email, (String) sd.getFieldValue(collectionUid));
        if (contact != null) {
            ua.setUserRole(contact.getRole());
            ua.setUserEntityName((String) sd.getFieldValue(collectionName));
            ua.setUserEntityUid((String) sd.getFieldValue(collectionUid));
        }
        return ua;
    }

    public QualityAssertion enhanceQA(String recordUuid, QualityAssertion ua, Double version) throws Exception {
        SolrDocument sd = occurrenceUtils.getOcc(recordUuid);
        return enhanceQA(sd, ua, version);
    }
}
