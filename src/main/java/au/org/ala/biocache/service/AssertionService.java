package au.org.ala.biocache.service;

import au.org.ala.biocache.dao.IndexDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dao.StoreDAO;
import au.org.ala.biocache.dto.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Service
public class AssertionService {

    private static final Logger logger = LoggerFactory.getLogger(AssertionService.class);

    @Inject
    private StoreDAO store;
    @Inject
    private SearchDAO searchDAO;
    @Inject
    private IndexDAO indexDao;

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private final Pattern uuidPattern = Pattern.compile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$");

    // max 1 indexAll thread can run at same time
    ThreadPoolExecutor executorService = new ThreadPoolExecutor(1, 1, 0, MILLISECONDS,
            new SynchronousQueue<>(),
            new ThreadPoolExecutor.AbortPolicy());
    @PostConstruct
    public void init() {
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    Runnable indexAll = () -> {
        try {
            // get all user assertions from database
            Map<String, UserAssertions> allAssertions = store.getAll(UserAssertions.class);
            logger.debug("total " + allAssertions.size() + " records found have assertions");

            indexDao.indexFromMap(allAssertions.entrySet().stream().filter(e -> ifRecordExist(e.getKey()) && !e.getValue().isEmpty()).map(e -> getIndexMap(e.getKey(), e.getValue())).collect(Collectors.toList()));
            logger.debug("index job finished");
        } catch (Exception e) {
            logger.error("Failed to read all assertions, e = " + e.getMessage());
        }
    };

    public Optional<QualityAssertion> addAssertion(
            String recordUuid,
            String code,
            String comment,
            String userId,
            String userDisplayName,
            String userAssertionStatus,
            String assertionUuid,
            String relatedRecordId,
            String relatedRecordReason
    ) throws IOException {
        logger.debug("Adding assertion to: " + recordUuid + ", code: " + code + ", comment: " + comment
                + ", userId: " + userId + ", userDisplayName: " + userDisplayName + ", userAssertionStatus: "
                + userAssertionStatus + ", assertionUuid: " + assertionUuid + ", relatedRecordId: " + relatedRecordId
                + ", relatedRecordReason: " + relatedRecordReason);

        SolrDocument sd = searchDAO.getOcc(recordUuid);
        // only when record uuid is valid
        if (sd != null) {
            QualityAssertion qa = new QualityAssertion();
            if (code.equals(Integer.toString(AssertionCodes.USER_DUPLICATE_RECORD.getCode()))) {
                Preconditions.checkArgument(!Strings.isNullOrEmpty(relatedRecordId), "Related Record ID must be set for User Duplicate Record Assertion");
                Preconditions.checkArgument(!Objects.equals(recordUuid, relatedRecordId), "User Duplicate Record Assertion can not be related to itself");
                Preconditions.checkArgument(!Strings.isNullOrEmpty(relatedRecordReason), "Duplicate record must have a reason recorded");
                qa.setRelatedRecordId(relatedRecordId);
                qa.setRelatedRecordReason(relatedRecordReason);
            }
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
            updateUserAssertions(recordUuid, combinedAssertions);
            return Optional.of(qa);
        }

        return Optional.empty();
    }

    public boolean editAssertion(
            String recordUuid,
            String assertionId,
            String comment
    ) throws IOException {
        logger.debug("Editing assertion to: " + recordUuid + ", assertionId: " + assertionId + ", comment: " + comment);

        SolrDocument sd = searchDAO.getOcc(recordUuid);
        // only when record uuid is valid
        if (sd != null) {
            UserAssertions userAssertions = store.get(UserAssertions.class, recordUuid).orElse(new UserAssertions());
            // if there's any change made
            if (userAssertions.updateComment(assertionId, comment)) {
                // Update assertions
                updateUserAssertions(recordUuid, userAssertions);
                return true;
            }
        }

        return false;
    }


    public boolean deleteAssertion(String recordUuid, String assertionUuid) throws IOException {
        SolrDocument sd = searchDAO.getOcc(recordUuid);
        // only when record uuid is valid
        if (sd != null) {
            UserAssertions userAssertions = store.get(UserAssertions.class, recordUuid).orElse(new UserAssertions());
            // if there's any change made
            if (userAssertions.deleteUuid(assertionUuid)) {
                // Update assertions
                updateUserAssertions(recordUuid, userAssertions);
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
                qa.setCode(qa.getCode());
                return qa;
            }
        }
        return null;
    }

    public UserAssertions getAssertions(String recordUuid) throws IOException {
        UserAssertions userAssertions = store.get(UserAssertions.class, recordUuid).orElse(new UserAssertions());
        for (QualityAssertion qa : userAssertions) {
            qa.setSnapshot(null);
            qa.setCode(qa.getCode());
        }
        return userAssertions;
    }

    public boolean bulkAddAssertions(String recordUuid, UserAssertions userAssertions) throws IOException {
        SolrDocument sd = searchDAO.getOcc(recordUuid);
        // only when record uuid is valid
        if (sd != null) {
            UserAssertions existingAssertions = store.get(UserAssertions.class, recordUuid).orElse(new UserAssertions());
            UserAssertions combinedAssertions = getCombinedAssertions(existingAssertions, userAssertions);
            if (!combinedAssertions.isEmpty()) {
                updateUserAssertions(recordUuid, combinedAssertions);
            }
            return true;
        }

        return false;
    }

    // this method merge new assertions with existing assertions
    // 1. for each type of normal assertion, 1 user can only have 1 on a record.
    //    so if user already created a 'Geospatial issue' assertion and he now creates a new one, new one will overwrite old one
    //    normal assertion is unique by recordId + userId + code
    // 2. for verified assertion (whose code == 50000), it's uniquely related to an assertion.
    //    so if there's already a verification for an assertion, a newly created verification on same assertion will overwrite existing one
    //    verification is unique per assertion
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

    // * There are 5 states for User Assertion Status:
    // * NONE: If no user assertion record exist
    // * UNCONFIRMED: If there is a user assertion but has not been verified by Collection Admin
    // * OPEN ISSUE: Collection Admin verifies the record and flags the user assertion as Open issue
    // * VERIFIED: Collection Admin verifies the record and flags the user assertion as Verified
    // * CORRECTED: Collection Admin verifies the record and flags the user assertion as Corrected
    private void updateUserAssertions(String recordUuid, UserAssertions userAssertions) throws IOException {
        // update database
        if (!userAssertions.isEmpty()) {
            store.put(recordUuid, userAssertions);
        } else { // no assertions, delete the entry
            store.delete(UserAssertions.class, recordUuid);
        }
        try {
            indexDao.indexFromMap(Collections.singletonList(getIndexMap(recordUuid, userAssertions)));
        } catch (Exception e) {
            logger.error("Failed to update Solr index, e = " + e.getMessage());
        }
    }

    @NotNull
    private Map<String, Object> getIndexMap(String recordUuid, UserAssertions userAssertions) {
        Map<String, Object> indexMap = new HashMap<>();
        indexMap.put("record_uuid", recordUuid);

        // set default user assertion status QA_NONE, means there's no assertion
        Integer assertionStatus = AssertionStatus.QA_NONE;

        // original user assertions
        List<QualityAssertion> assertions =
                userAssertions.stream().filter(qa -> !qa.getCode().equals(AssertionCodes.VERIFIED.getCode())).collect(Collectors.toList());

        // admin verifications
        List<QualityAssertion> verifications =
                userAssertions.stream().filter(qa -> qa.getCode().equals(AssertionCodes.VERIFIED.getCode())).collect(Collectors.toList());

        List<String> assertionIds = assertions.stream().map(QualityAssertion::getUuid).collect(Collectors.toList());
        List<String> verifiedIds = verifications.stream().map(QualityAssertion::getRelatedUuid).collect(Collectors.toList());

        // only those not yet verified left
        assertionIds.removeAll(verifiedIds);

        // if there's user assertion not yet verified
        if (!assertionIds.isEmpty()) {
            assertionStatus = AssertionStatus.QA_UNCONFIRMED;
        } else if (!verifications.isEmpty()) { // all verified
            if (verifications.stream().anyMatch(qa -> qa.getQaStatus().equals(AssertionStatus.QA_OPEN_ISSUE))) {
                assertionStatus = AssertionStatus.QA_OPEN_ISSUE;
            } else {
                List<QualityAssertion> verificationsWithValidDate =
                        verifications.stream().filter(qa -> validDate(qa.getCreated())).collect(Collectors.toList());
                if (!verificationsWithValidDate.isEmpty()) {
                    // sort by datetime DESC
                    verificationsWithValidDate.sort((qa1, qa2) -> {
                        try {
                            Date qa2Date = simpleDateFormat.parse(qa2.getCreated());
                            Date qa1Date = simpleDateFormat.parse(qa1.getCreated());
                            return qa2Date.compareTo(qa1Date);
                        } catch (ParseException e) {
                            e.printStackTrace();
                            throw new IllegalArgumentException(e);
                        }
                    });

                    assertionStatus = verificationsWithValidDate.get(0).getQaStatus();
                } else {
                    // assertions have no date, just pick one
                    assertionStatus = verifications.get(0).getQaStatus();
                }
            }
        }

        // put assertion status into index map
        indexMap.put("userAssertions", String.valueOf(assertionStatus));
        // set hasUserAssertions
        indexMap.put("hasUserAssertions", !assertions.isEmpty());
        // set userVerified
        indexMap.put("userVerified", !verifications.isEmpty());

        List<QualityAssertion> assertionsWithValidDate =
                userAssertions.stream().filter(qa -> validDate(qa.getCreated())).collect(Collectors.toList());

        if (!assertionsWithValidDate.isEmpty()) {
            assertionsWithValidDate.sort((qa1, qa2) -> {
                try {
                    Date date1 = simpleDateFormat.parse(qa1.getCreated());
                    Date date2 = simpleDateFormat.parse(qa2.getCreated());
                    return date2.compareTo(date1);
                } catch (ParseException e) {
                    e.printStackTrace();
                    throw new IllegalArgumentException(e);
                }
            });
            Date lastDate = null;
            try {
                lastDate = simpleDateFormat.parse(assertionsWithValidDate.get(0).getCreated());
            } catch (ParseException e) {
                e.printStackTrace();
            }
            indexMap.put("lastAssertionDate", lastDate);
        }

        List<String> assertionUserIds = userAssertions.stream().map(QualityAssertion::getUserId).filter(StringUtils::isNotBlank).distinct().collect(Collectors.toList());
        if (!assertionUserIds.isEmpty()) {
            indexMap.put("assertionUserId", assertionUserIds);
        }
        return indexMap;
    }

    public boolean indexAll() {
        try {
            executorService.execute(indexAll);
        } catch (RejectedExecutionException e){
            logger.debug("An index threading is already running at background");
            return false;
        }
        return true;
    }

    public boolean isIndexAllRunning() {
        return executorService.getActiveCount() == 1;
    }

    // if solr doc with specified id exists
    private boolean ifRecordExist(String recordUuid) {
        logger.debug("Try to retrieve occurrence record with guid: '" + recordUuid + "'");

        if (!isValidUUID(recordUuid)) {
            logger.debug(recordUuid + " is not a valid uuid");
            return false;
        }

        SpatialSearchRequestDTO idRequest = new SpatialSearchRequestDTO();
        idRequest.setQ(OccurrenceIndex.ID + ":" + recordUuid);
        idRequest.setFacet(false);
        idRequest.setFl(OccurrenceIndex.ID);

        try {
            SolrDocumentList sdl = searchDAO.findByFulltext(idRequest);
            boolean resultNotEmpty = !sdl.isEmpty();
            logger.debug(resultNotEmpty ? "Found " : "Can't find " + "record with uid: " + recordUuid);
            return resultNotEmpty;
        } catch (Exception e) {
            logger.error("Error happened when searching for record with uid: " + recordUuid);
        }

        return false;
    }

    private boolean validDate(String date) {
        try {
            simpleDateFormat.parse(date);
            return true;
        } catch (ParseException e) {
           return false;
        }
    }

    private boolean isValidUUID(String uuid) {
        return uuidPattern.matcher(uuid).find();
    }
}
