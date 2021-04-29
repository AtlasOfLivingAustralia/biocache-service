package au.org.ala.biocache.service;

import au.org.ala.biocache.dao.StoreDAO;
import au.org.ala.biocache.dto.AssertionCodes;
import au.org.ala.biocache.dto.AssertionStatus;
import au.org.ala.biocache.dto.QualityAssertion;
import au.org.ala.biocache.dto.UserAssertions;
import au.org.ala.biocache.util.OccurrenceUtils;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
public class AssertionService {

    private static final Logger logger = LoggerFactory.getLogger(AssertionService.class);

    @Inject
    private OccurrenceUtils occurrenceUtils;
    @Inject
    private StoreDAO store;

    public Optional<QualityAssertion> addAssertion(
            String recordUuid,
            String code,
            String comment,
            String userId,
            String userDisplayName,
            String userAssertionStatus,
            String assertionUuid
    ) throws IOException {
        logger.debug("Adding assertion to: " + recordUuid + ", code: " + code + ", comment: " + comment
                + ", userId: " + userId + ", userDisplayName: " + userDisplayName +", relatedRecordId: "
                + ", userAssertionStatus: " + userAssertionStatus + ", assertionUuid: " + assertionUuid);

        SolrDocument sd = occurrenceUtils.getOcc(recordUuid);
        // only when record uuid is valid
        if (sd != null) {
            QualityAssertion qa = new QualityAssertion();
            qa.setCode(Integer.parseInt(code));
            qa.setComment(comment);
            qa.setUserId(userId);
            qa.setUserDisplayName(userDisplayName);
            qa.setReferenceRowKey(recordUuid);
            if (code.equals(Integer.toString(AssertionCodes.VERIFIED.getCode()))) {
                qa.setRelatedUuid(assertionUuid);
                qa.setQaStatus(Integer.parseInt(userAssertionStatus));
            } else {
                qa.setQaStatus(AssertionStatus.QA_UNCONFIRMED);
            }

            qa.setDataResourceUid((String) sd.getFieldValue("dataResourceUid"));

            UserAssertions existingAssertions = store.get(UserAssertions.class, recordUuid).orElse(new UserAssertions());
            UserAssertions newAssertions = new UserAssertions();
            newAssertions.add(qa);
            UserAssertions combinedAssertions = getCombinedAssertions(existingAssertions, newAssertions);
            store.put(recordUuid, combinedAssertions);
            return Optional.of(qa);
        }

        return Optional.empty();
    }


    public boolean deleteAssertion(String recordUuid, String assertionUuid) throws IOException {
        SolrDocument sd = occurrenceUtils.getOcc(recordUuid);
        // only when record uuid is valid
        if (sd != null) {
            UserAssertions userAssertions = store.get(UserAssertions.class, recordUuid).orElse(new UserAssertions());
            // if there's any change made
            if (userAssertions.deleteUuid(assertionUuid)) {
                // Update assertions
                if (!userAssertions.isEmpty()) {
                    store.put(recordUuid, userAssertions);
                } else { // no assertions, delete the entry
                    store.delete(UserAssertions.class, recordUuid);
                }
                return true;
            }
        }

        return false;
    }

    public QualityAssertion getAssertion(String recordUuid, String assertionUuid) throws IOException {
        UserAssertions userAssertions = store.get(UserAssertions.class, recordUuid).orElse(new UserAssertions());
        for (QualityAssertion qa : userAssertions) {
            if (qa.getUuid().equals(assertionUuid)) {
                // do not return the snapshot
                qa.setSnapshot(null);
                return qa;
            }
        }
        return null;
    }

    public UserAssertions getAssertions(String recordUuid) throws IOException {
        UserAssertions userAssertions = store.get(UserAssertions.class, recordUuid).orElse(new UserAssertions());
        for (QualityAssertion qa : userAssertions) {
            qa.setSnapshot(null);
        }
        return userAssertions;
    }

    public boolean bulkAddAssertions(String recordUuid, UserAssertions userAssertions) throws IOException {
        SolrDocument sd = occurrenceUtils.getOcc(recordUuid);
        // only when record uuid is valid
        if (sd != null) {
            UserAssertions existingAssertions = store.get(UserAssertions.class, recordUuid).orElse(new UserAssertions());
            UserAssertions combinedAssertions = getCombinedAssertions(existingAssertions, userAssertions);
            if (!combinedAssertions.isEmpty()) {
                store.put(recordUuid, combinedAssertions);
            }
            return true;
        }

        return false;
    }

    // recordId + userId + code should be unique for normal assertions
    // verification should be per assertion
    private UserAssertions getCombinedAssertions(UserAssertions existingAssertions, UserAssertions newAssertions) {
        // un-verified assertions grouped by recordId + code + userId
        Map<Integer, QualityAssertion> existingAssertionsMap =
                existingAssertions.stream().filter(qa -> !qa.getCode().equals(AssertionCodes.VERIFIED.getCode())).collect(Collectors.toMap(qa -> Objects.hash(qa.getReferenceRowKey(), qa.getCode(), qa.getUserId()), qa -> qa));

        // verifications grouped by related uuid
        Map<String, QualityAssertion> existingVerificationMap =
                existingAssertions.stream().filter(qa -> qa.getCode().equals(AssertionCodes.VERIFIED.getCode())).collect(Collectors.toMap(QualityAssertion::getRelatedUuid, qa -> qa));

        UserAssertions combined = new UserAssertions();

        // new un-verified assertions
        List<QualityAssertion> newAssertionsList =
                newAssertions.stream().filter(qa -> !qa.getCode().equals(AssertionCodes.VERIFIED.getCode())).collect(Collectors.toList());

        // new verifications
        List<QualityAssertion> newVerificationList =
                newAssertions.stream().filter(qa -> qa.getCode().equals(AssertionCodes.VERIFIED.getCode())).collect(Collectors.toList());

        for (QualityAssertion qa : newAssertionsList) {
            int hash = Objects.hash(qa.getReferenceRowKey(), qa.getCode(), qa.getUserId());
            if (!existingAssertionsMap.containsKey(hash)) {
                // for those new assertions with unique keys
                combined.add(qa);
            } else {
                existingAssertionsMap.put(hash, qa);
            }
        }

        // for those new assertions with duplicate keys
        combined.addAll(existingAssertionsMap.values());

        for (QualityAssertion verification : newVerificationList) {
            String relatedUuid = verification.getRelatedUuid();
            if (!existingVerificationMap.containsKey(relatedUuid)) {
                // for those verifications on new assertions
                combined.add(verification);
            } else {
                existingVerificationMap.put(relatedUuid, verification);
            }
        }

        // for those verifications overwriting previous ones
        combined.addAll(existingVerificationMap.values());

        return combined;
    }
}
