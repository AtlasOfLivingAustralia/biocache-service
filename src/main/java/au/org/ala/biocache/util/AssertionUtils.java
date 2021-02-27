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

import au.org.ala.biocache.Store;
import au.org.ala.biocache.dto.ContactDTO;
import au.org.ala.biocache.dto.OccurrenceDTO;
import au.org.ala.biocache.model.FullRecord;
import au.org.ala.biocache.model.QualityAssertion;
import au.org.ala.biocache.model.ValidationRule;
import au.org.ala.biocache.service.AuthService;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

@Component("assertionUtils")
public class AssertionUtils {

    @Inject
    protected AuthService authService;
    @Inject
    protected ContactUtils contactUtils;
    @Inject
    protected OccurrenceUtils occurrenceUtils;

    /**
     * Retrieve the user assertions adding additional metadata about users
     * and attribution.
     *
     * @param recordUuid
     * @return quality assertions
     */
    public List<QualityAssertion> getUserAssertions(String recordUuid) throws Exception {
        FullRecord[] fr = occurrenceUtils.getAllVersionsByUuid(recordUuid, false);
        OccurrenceDTO occ = new OccurrenceDTO(fr);
        return getUserAssertions(occ);
    }

    public ValidationRule[] getQueryAssertions(String recordUuid) throws Exception {
        FullRecord[] fr = occurrenceUtils.getAllVersionsByUuid(recordUuid, false);

        if (fr == null) {
            return null;
        } else {
            final OccurrenceDTO occ = new OccurrenceDTO(fr);
            final FullRecord processed = occ.getProcessed();
            if(processed == null) {
                return new ValidationRule[0];
            } else {
                Map<String,String> queryAssertionMap = processed.getQueryAssertions();


                ValidationRule[] aqs = Store.getValidationRules(queryAssertionMap.keySet().toArray(new String[0]));
                //Legacy integration - fix up the user assertions - legacy - to add replace with CAS IDs....
                for(ValidationRule ua : aqs){
                    if(ua.getUserId() == null && ua.getUserName().contains("@")){
                        String email = ua.getUserName();
                        String userId = authService.getMapOfEmailToId().get(ua.getUserName());
                        ua.setUserEmail(email);
                        ua.setUserId(userId);
                    }

                    String userName = authService.getMapOfAllUserNamesByNumericId().get(ua.getUserId());
                    ua.setUserName(userName);
                }
                return aqs;
            }
        }
    }

    /**
     * Retrieve the user assertions adding additional metadata about users
     * and attribution.
     *
     * @param occ
     * @return quality assertions
     */
    public List<QualityAssertion> getUserAssertions(OccurrenceDTO occ) {
        if (occ.getRaw() != null){
            //set the user assertions
            List<QualityAssertion> userAssertions = Store.getUserAssertions(occ.getRaw().getRowKey());
            //Legacy integration - fix up the user assertions - legacy - to add replace with CAS IDs....
            for(QualityAssertion ua : userAssertions){
                if(ua.getUserId().contains("@")){
                    String email = ua.getUserId();
                    String userId = authService.getMapOfEmailToId().get(email);
                    ua.setUserEmail(email);
                    ua.setUserId(userId);
                }
            }
    
            //add user roles....
            for(QualityAssertion ua : userAssertions){
                enhanceQA(occ, ua);
            }
            return userAssertions;
        } else {
            return null;
        }
    }

    public QualityAssertion enhanceQA(OccurrenceDTO occ, QualityAssertion ua) {
        String email = ua.getUserEmail();
        ContactDTO contact = contactUtils.getContactForEmailAndUid(email, occ.getProcessed().getAttribution().getCollectionUid());
        if (contact != null){
            ua.setUserRole(contact.getRole());
            ua.setUserEntityName(occ.getProcessed().getAttribution().getCollectionName());
            ua.setUserEntityUid(occ.getProcessed().getAttribution().getCollectionUid());
        }
        return ua;
    }

    public QualityAssertion enhanceQA(String recordUuid, QualityAssertion ua) throws Exception {
        FullRecord[] fr = occurrenceUtils.getAllVersionsByUuid(recordUuid, false);
        OccurrenceDTO occ = new OccurrenceDTO(fr);
        String email = ua.getUserEmail();
        ContactDTO contact = contactUtils.getContactForEmailAndUid(email, occ.getProcessed().getAttribution().getCollectionUid());
        if(contact != null){
            ua.setUserRole(contact.getRole());
            ua.setUserEntityName(occ.getProcessed().getAttribution().getCollectionName());
            ua.setUserEntityUid(occ.getProcessed().getAttribution().getCollectionUid());
        }
        return ua;
    }

    public QualityAssertion getUserAssertion(String recordUuid, String assertionUuid) throws Exception {
        QualityAssertion qa = Store.getUserAssertion(recordUuid,assertionUuid);
        return enhanceQA(recordUuid, qa);
    }
}
