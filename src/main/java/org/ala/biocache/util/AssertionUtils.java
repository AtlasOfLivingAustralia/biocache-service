package org.ala.biocache.util;

import au.org.ala.biocache.AssertionQuery;
import au.org.ala.biocache.FullRecord;
import au.org.ala.biocache.QualityAssertion;
import au.org.ala.biocache.Store;
import org.ala.biocache.dto.ContactDTO;
import org.ala.biocache.dto.store.OccurrenceDTO;
import org.ala.biocache.service.AuthService;
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

    /**
     * Retrieve the user assertions adding additional metadata about users
     * and attribution.
     *
     * @param recordUuid
     * @return quality assertions
     */
    public List<QualityAssertion> getUserAssertions(String recordUuid) {
        FullRecord[] fr = Store.getAllVersionsByUuid(recordUuid, false);
        OccurrenceDTO occ = new OccurrenceDTO(fr);
        return getUserAssertions(occ);
    }

    public AssertionQuery[] getQueryAssertions(String recordUuid) {
        FullRecord[] fr = Store.getAllVersionsByUuid(recordUuid, false);
        OccurrenceDTO occ = new OccurrenceDTO(fr);
        Map<String,String> queryAssertionMap = occ.getProcessed().getQueryAssertions();
        AssertionQuery[] aqs = Store.getAssertionQueries(queryAssertionMap.keySet().toArray(new String[0]));
        //Legacy integration - fix up the user assertions - legacy - to add replace with CAS IDs....
        for(AssertionQuery ua : aqs){
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

    /**
     * Retrieve the user assertions adding additional metadata about users
     * and attribution.
     *
     * @param occ
     * @return quality assertions
     */
    public List<QualityAssertion> getUserAssertions(OccurrenceDTO occ) {
        if(occ.getRaw() != null){
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
        } else{
            return null;
        }
    }

    public QualityAssertion enhanceQA(OccurrenceDTO occ, QualityAssertion ua) {
        String email = ua.getUserEmail();
        ContactDTO contact = contactUtils.getContactForEmailAndUid(email, occ.getProcessed().getAttribution().getCollectionUid());
        if(contact != null){
            ua.setUserRole(contact.getRole());
            ua.setUserEntityName(occ.getProcessed().getAttribution().getCollectionName());
            ua.setUserEntityUid(occ.getProcessed().getAttribution().getCollectionUid());
        }
        return ua;
    }

    public QualityAssertion enhanceQA(String recordUuid, QualityAssertion ua) {
        FullRecord[] fr = Store.getAllVersionsByUuid(recordUuid, false);
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

    public QualityAssertion getUserAssertion(String recordUuid, String assertionUuid){
        QualityAssertion qa = Store.getUserAssertion(recordUuid,assertionUuid);
        return enhanceQA(recordUuid, qa);
    }

    public void setAuthService(AuthService authService) {
        this.authService = authService;
    }

    public void setContactUtils(ContactUtils contactUtils) {
        this.contactUtils = contactUtils;
    }
}
