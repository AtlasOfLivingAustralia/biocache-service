package au.org.ala.biocache.service;

import au.org.ala.biocache.Store;
import au.org.ala.biocache.model.QualityAssertion;
import au.org.ala.biocache.vocab.AssertionCodes;
import au.org.ala.biocache.vocab.AssertionStatus;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class AssertionService {

    private static final Logger logger = LoggerFactory.getLogger(AssertionService.class);

    public QualityAssertion addAssertion(
            String recordUuid,
            String code,
            String comment,
            String userId,
            String userDisplayName,
            String userAssertionStatus,
            String assertionUuid,
            String relatedRecordId
    ) {
        logger.debug("Adding assertion to:" + recordUuid + ", code:" + code + ", comment:" + comment
                + ",userAssertionStatus: " + userAssertionStatus + ", assertionUuid: " + assertionUuid
                + ", userId:" +userId + ", userDisplayName:" + userDisplayName);

        QualityAssertion qa = au.org.ala.biocache.model.QualityAssertion.apply(Integer.parseInt(code));
        qa.setComment(comment);
        if (qa.code() == AssertionCodes.USER_DUPLICATE_RECORD().code()) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(relatedRecordId), "Related Record ID must be set for User Duplicate Record Assertion");
            Preconditions.checkArgument(!Objects.equals(recordUuid, relatedRecordId), "User Duplicate Record Assertion can not be related to itself");

            qa.setRelatedRecordId(relatedRecordId);
        }
        qa.setUserId(userId);
        qa.setUserDisplayName(userDisplayName);
        if (code.equals(Integer.toString(AssertionCodes.VERIFIED().getCode()))) {
            qa.setRelatedUuid(assertionUuid);
            qa.setQaStatus(Integer.parseInt(userAssertionStatus));
        } else {
            qa.setQaStatus(AssertionStatus.QA_UNCONFIRMED());
        }

        Store.addUserAssertion(recordUuid, qa);

        return qa;
    }

}
