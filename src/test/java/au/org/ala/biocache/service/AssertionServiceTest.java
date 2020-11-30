package au.org.ala.biocache.service;

import au.org.ala.biocache.Store;
import au.org.ala.biocache.model.QualityAssertion;
import au.org.ala.biocache.vocab.AssertionCodes;
import au.org.ala.biocache.vocab.AssertionStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Store.class)
public class AssertionServiceTest {

    AssertionService service;

    @Before
    public void setup() {
        service = new AssertionService();
        mockStatic(Store.class);
    }

    @Test
    public void testAddAssertion() {
        String recordUuid = UUID.randomUUID().toString();
        String assertionUuid = UUID.randomUUID().toString();
        int code = AssertionCodes.USER_ASSERTION_OTHER().code();

        QualityAssertion qualityAssertion = service.addAssertion(
                recordUuid,
                Integer.toString(code),
                "Comment",
                "userId",
                "user display name",
                "",
                assertionUuid,
                null
        );

        assertThat("Quality Assertion is returned", qualityAssertion, notNullValue());
        assertThat("Quality Assertion has code", qualityAssertion.code(), equalTo(code));
        assertThat("Assertion UUID is not applied because user assertion is not verified", qualityAssertion.relatedUuid(), nullValue());
    }

    @Test
    public void testAddVerifiedAssertion() {
        String recordUuid = UUID.randomUUID().toString();
        String assertionUuid = UUID.randomUUID().toString();
        int userAssertionStatus = AssertionStatus.PASSED();
        int code = AssertionCodes.VERIFIED().code();

        QualityAssertion qualityAssertion = service.addAssertion(
                recordUuid,
                Integer.toString(code),
                "Comment",
                "userId",
                "user display name",
                Integer.toString(userAssertionStatus),
                assertionUuid,
                null
        );

        assertThat("Quality Assertion is returned", qualityAssertion, notNullValue());
        assertThat("Quality Assertion has code", qualityAssertion.code(), equalTo(code));
        assertThat("Assertion UUID is applied because user assertion is verified", qualityAssertion.relatedUuid(), equalTo(assertionUuid));
        assertThat("Assertion QA status is applied because user assertion is verified", qualityAssertion.qaStatus(), equalTo(userAssertionStatus));
    }

    @Test
    public void testAddDuplicateRecordAssertion() {
        String recordUuid = UUID.randomUUID().toString();
        String assertionUuid = UUID.randomUUID().toString();
        String relatedRecordUuid = UUID.randomUUID().toString();
        int code = AssertionCodes.USER_DUPLICATE_RECORD().code();

        QualityAssertion qualityAssertion = service.addAssertion(
                recordUuid,
                Integer.toString(code),
                "Comment",
                "userId",
                "user display name",
                "",
                assertionUuid,
                relatedRecordUuid
        );

        assertThat("Quality Assertion is returned", qualityAssertion, notNullValue());
        assertThat("Quality Assertion has code", qualityAssertion.code(), equalTo(code));
        assertThat("Quality Assertion has related record id", qualityAssertion.relatedRecordId(), equalTo(relatedRecordUuid));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddDuplicateRecordAssertionFailsWithNoRelatedRecordUuid() {
        String recordUuid = UUID.randomUUID().toString();
        String assertionUuid = UUID.randomUUID().toString();
        int code = AssertionCodes.USER_DUPLICATE_RECORD().code();

        QualityAssertion qualityAssertion = service.addAssertion(
                recordUuid,
                Integer.toString(code),
                "Comment",
                "userId",
                "user display name",
                "",
                assertionUuid,
                null
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddDuplicateRecordAssertionFailsWithSameRecordAsAssertion() {
        String recordUuid = UUID.randomUUID().toString();
        String assertionUuid = UUID.randomUUID().toString();
        int code = AssertionCodes.USER_DUPLICATE_RECORD().code();

        QualityAssertion qualityAssertion = service.addAssertion(
                recordUuid,
                Integer.toString(code),
                "Comment",
                "userId",
                "user display name",
                "",
                assertionUuid,
                recordUuid
        );
    }
}
