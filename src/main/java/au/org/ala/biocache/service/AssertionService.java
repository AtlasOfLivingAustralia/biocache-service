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
import java.util.Optional;

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
            existingAssertions.add(qa);
            store.put(recordUuid, existingAssertions);
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
                // TODO: if userAssertions.size() == 0, we actually need to remove the key/value pair instead of having an empty value in Cassandra
                store.put(recordUuid, userAssertions);
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
            UserAssertions existing = store.get(UserAssertions.class, recordUuid).orElse(new UserAssertions());
            existing.addAll(userAssertions);
            if (!existing.isEmpty()) {
                store.put(recordUuid, existing);
            }
            return true;
        }

        return false;
    }
}
