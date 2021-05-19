package au.org.ala.biocache.service;

import au.org.ala.biocache.dao.IndexDAO;
import au.org.ala.biocache.dao.StoreDAO;
import au.org.ala.biocache.dto.AssertionStatus;
import au.org.ala.biocache.dto.FacetThemes;
import au.org.ala.biocache.dto.QualityAssertion;
import au.org.ala.biocache.dto.UserAssertions;
import au.org.ala.biocache.util.OccurrenceUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

public class AssertionServiceTest {

    @Mock
    OccurrenceUtils occurrenceUtils;
    @Mock
    StoreDAO store;
    @Mock
    IndexDAO indexDAO;

    @InjectMocks
    AssertionService assertionService;

    AutoCloseable mocks;

    @Before
    public void setup() {
        // Every application needs to explicitly initialise static fields in
        // FacetThemes by calling its constructor 🤮
        new FacetThemes();
        mocks = MockitoAnnotations.openMocks(this);
    }

    @After
    public void tearDown() throws Exception {
        mocks.close();
    }

    public UserAssertions getMockAssertions(int numberOfAssertions, int numberOfVerifications) {
        UserAssertions userAssertions = new UserAssertions();
        for (int i = 0; i < numberOfAssertions; i++) {
            QualityAssertion qa = new QualityAssertion();
            qa.setReferenceRowKey("recordUuid");
            qa.setCode(i);
            qa.setUserId("userId");
            qa.setComment("comment_" + i);
            userAssertions.add(qa);
            if (i < numberOfVerifications) {
                QualityAssertion verification = new QualityAssertion();
                verification.setReferenceRowKey("recordUuid");
                verification.setRelatedUuid(qa.getUuid());
                verification.setCode(50000);
                verification.setQaStatus(50001);
                verification.setUserId("userId");
                userAssertions.add(verification);
            }
        }
        return userAssertions;
    }

    @Test
    public void testAddAssertion_record_not_found() throws Exception {
        // test when record is not found
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(null);
        Optional<QualityAssertion> qualityAssertion = assertionService.addAssertion("", "", "", "", "", "", "");
        assert(!qualityAssertion.isPresent());
    }

    @Test
    public void testAddAssertion_existing_empty_add_1_assertion() throws Exception {
        // test when record is found
        SolrDocument sd = new SolrDocument();
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(sd);
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(getMockAssertions(0, 0)));

        ArgumentCaptor<String> myUuid = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<UserAssertions> myUserAssertions = ArgumentCaptor.forClass(UserAssertions.class);
        ArgumentCaptor<List<Map<String, Object>>> myIndexMaps = ArgumentCaptor.forClass(List.class);

        // test when add succeed -- code = 50000
        Optional<QualityAssertion> qualityAssertion1 = assertionService.addAssertion("recordUuid", "0",
                "comment", "userId", "userDisplayName", "", "");
        assert(qualityAssertion1.isPresent());

        // verify combined assertions
        Mockito.verify(store).put(myUuid.capture(), myUserAssertions.capture());
        assert(myUserAssertions.getValue().size() == 1);
        assert(myUserAssertions.getValue().get(0).getComment().equals("comment"));

        // verify indexmap
        Mockito.verify(indexDAO).indexFromMap(myIndexMaps.capture());
        assert(myIndexMaps.getValue().size() == 1);
        Map<String, Object> indexMap = myIndexMaps.getValue().get(0);
        assert(indexMap.get("userAssertions").equals("50005"));
        assert((boolean)indexMap.get("hasUserAssertions"));
        assert(((List<String>)indexMap.get("assertionUserId")).size() == 1);
        assert(((List<String>)indexMap.get("assertionUserId")).get(0).equals("userId"));
    }

    @Test
    public void testAddAssertion_existing_1_add_1_assertion_different_type_coexist() throws Exception {
        // test when record is found
        SolrDocument sd = new SolrDocument();
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(sd);
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(getMockAssertions(1, 0)));

        ArgumentCaptor<String> myUuid = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<UserAssertions> myUserAssertions = ArgumentCaptor.forClass(UserAssertions.class);
        ArgumentCaptor<List<Map<String, Object>>> myIndexMaps = ArgumentCaptor.forClass(List.class);

        // test when add succeed -- code = 50000
        Optional<QualityAssertion> qualityAssertion1 = assertionService.addAssertion("recordUuid", "1",
                "comment", "userId1", "userDisplayName", "", "");
        assert(qualityAssertion1.isPresent());

        // verify combined assertions
        Mockito.verify(store).put(myUuid.capture(), myUserAssertions.capture());
        assert(myUserAssertions.getValue().size() == 2);
        Set<Integer> codes = myUserAssertions.getValue().stream().map(QualityAssertion::getCode).collect(Collectors.toSet());
        assert(codes.size() == 2);
        assert(codes.contains(0));
        assert(codes.contains(1));

        // verify indexmap
        Mockito.verify(indexDAO).indexFromMap(myIndexMaps.capture());
        Map<String, Object> indexMap = myIndexMaps.getValue().get(0);
        assert(indexMap.get("userAssertions").equals("50005"));
        assert((boolean)indexMap.get("hasUserAssertions"));
        Set<String> userIds = new HashSet<>((List<String>)indexMap.get("assertionUserId"));
        assert(userIds.size() == 2);
        assert(userIds.contains("userId"));
        assert(userIds.contains("userId1"));
    }

    @Test
    public void testAddAssertion_existing_1_add_1_assertion_same_type_overwrite() throws Exception {
        // test when record is found
        SolrDocument sd = new SolrDocument();
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(sd);
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(getMockAssertions(1, 0)));

        ArgumentCaptor<String> myUuid = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<UserAssertions> myUserAssertions = ArgumentCaptor.forClass(UserAssertions.class);
        ArgumentCaptor<List<Map<String, Object>>> myIndexMaps = ArgumentCaptor.forClass(List.class);

        // test when add succeed -- code = 50000
        Optional<QualityAssertion> qualityAssertion1 = assertionService.addAssertion("recordUuid", "0",
                "comment_new", "userId", "userDisplayName", "", "");
        assert(qualityAssertion1.isPresent());

        // verify combined assertions
        Mockito.verify(store).put(myUuid.capture(), myUserAssertions.capture());
        assert(myUserAssertions.getValue().size() == 1);
        assert(myUserAssertions.getValue().get(0).getCode() == 0);
        assert(myUserAssertions.getValue().get(0).getComment().equals("comment_new"));

        // verify indexmap
        Mockito.verify(indexDAO).indexFromMap(myIndexMaps.capture());
        Map<String, Object> indexMap = myIndexMaps.getValue().get(0);
        assert(indexMap.get("userAssertions").equals("50005"));
        assert((boolean)indexMap.get("hasUserAssertions"));
        Set<String> userIds = new HashSet<>((List<String>)indexMap.get("assertionUserId"));
        assert(userIds.size() == 1);
        assert(userIds.contains("userId"));
    }

    @Test
    public void testAddAssertion_existing_1_add_1_verification_50001() throws Exception {
        // test when record is found
        SolrDocument sd = new SolrDocument();
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(sd);

        UserAssertions existingAssertions = new UserAssertions();
        QualityAssertion qa = new QualityAssertion();
        qa.setReferenceRowKey("recordUuid");
        qa.setCode(0);
        qa.setUserId("userId");
        qa.setComment("comment_old");
        existingAssertions.add(qa);
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(existingAssertions));

        ArgumentCaptor<String> myUuid = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<UserAssertions> myUserAssertions = ArgumentCaptor.forClass(UserAssertions.class);
        ArgumentCaptor<List<Map<String, Object>>> myIndexMaps = ArgumentCaptor.forClass(List.class);

        // test when add succeed -- code = 50000
        Optional<QualityAssertion> qualityAssertion1 = assertionService.addAssertion("recordUuid", "50000",
                "comment_verification", "userId2", "userDisplayName", "50001", existingAssertions.get(0).getUuid());
        assert(qualityAssertion1.isPresent());

        // verify combined assertions
        Mockito.verify(store).put(myUuid.capture(), myUserAssertions.capture());
        UserAssertions assertions = myUserAssertions.getValue();
        assert(assertions.size() == 2);

        assertions.sort(Comparator.comparing(QualityAssertion::getCode));
        assert(assertions.get(0).getCode() == 0);
        assert(assertions.get(1).getCode() == 50000);

        // verify indexmap
        Mockito.verify(indexDAO).indexFromMap(myIndexMaps.capture());
        Map<String, Object> indexMap = myIndexMaps.getValue().get(0);
        assert(indexMap.get("userAssertions").equals("50001"));
        assert((boolean)indexMap.get("hasUserAssertions"));
        Set<String> userIds = new HashSet<>((List<String>)indexMap.get("assertionUserId"));
        assert(userIds.size() == 1);
        assert(userIds.contains("userId"));
    }

    @Test
    public void testAddAssertion_existing_1_add_1_verification_50002() throws Exception {
        // test when record is found
        SolrDocument sd = new SolrDocument();
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(sd);

        UserAssertions existingAssertions = new UserAssertions();
        QualityAssertion qa = new QualityAssertion();
        qa.setReferenceRowKey("recordUuid");
        qa.setCode(0);
        qa.setUserId("userId");
        qa.setComment("comment_old");
        existingAssertions.add(qa);
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(existingAssertions));

        ArgumentCaptor<String> myUuid = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<UserAssertions> myUserAssertions = ArgumentCaptor.forClass(UserAssertions.class);
        ArgumentCaptor<List<Map<String, Object>>> myIndexMaps = ArgumentCaptor.forClass(List.class);

        // test when add succeed -- code = 50000
        Optional<QualityAssertion> qualityAssertion1 = assertionService.addAssertion("recordUuid", "50000",
                "comment_verification", "userId2", "userDisplayName", "50002", existingAssertions.get(0).getUuid());
        assert(qualityAssertion1.isPresent());

        // verify combined assertions
        Mockito.verify(store).put(myUuid.capture(), myUserAssertions.capture());
        UserAssertions assertions = myUserAssertions.getValue();
        assert(assertions.size() == 2);

        assertions.sort(Comparator.comparing(QualityAssertion::getCode));
        assert(assertions.get(0).getCode() == 0);
        assert(assertions.get(1).getCode() == 50000);
        assert(assertions.get(1).getQaStatus() == 50002);

        // verify indexmap
        Mockito.verify(indexDAO).indexFromMap(myIndexMaps.capture());
        Map<String, Object> indexMap = myIndexMaps.getValue().get(0);
        assert(indexMap.get("userAssertions").equals("50002"));
        assert((boolean)indexMap.get("hasUserAssertions"));
        Set<String> userIds = new HashSet<>((List<String>)indexMap.get("assertionUserId"));
        assert(userIds.size() == 1);
        assert(userIds.contains("userId"));
    }

    @Test
    public void testAddAssertion_existing_1_assertion_1_verification_add_1_verification_overwrite() throws Exception {
        // test when record is found
        SolrDocument sd = new SolrDocument();
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(sd);
        UserAssertions existingAssertions = getMockAssertions(1, 1);
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(existingAssertions));

        ArgumentCaptor<String> myUuid = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<UserAssertions> myUserAssertions = ArgumentCaptor.forClass(UserAssertions.class);
        ArgumentCaptor<List<Map<String, Object>>> myIndexMaps = ArgumentCaptor.forClass(List.class);

        // test when add succeed -- code = 50000
        Optional<QualityAssertion> qualityAssertion1 = assertionService.addAssertion("recordUuid", "50000",
                "comment_verification", "userId", "userDisplayName", "50002", existingAssertions.get(0).getUuid());
        assert(qualityAssertion1.isPresent());

        // verify combined assertions
        Mockito.verify(store).put(myUuid.capture(), myUserAssertions.capture());
        UserAssertions assertions = myUserAssertions.getValue();
        assert(assertions.size() == 2);

        assertions.sort(Comparator.comparing(QualityAssertion::getCode));
        assert(assertions.get(0).getCode() == 0);
        assert(assertions.get(1).getCode() == 50000);
        assert(assertions.get(1).getQaStatus() == 50002);

        // verify indexmap
        Mockito.verify(indexDAO).indexFromMap(myIndexMaps.capture());
        Map<String, Object> indexMap = myIndexMaps.getValue().get(0);
        assert(indexMap.get("userAssertions").equals("50002"));
        assert((boolean)indexMap.get("hasUserAssertions"));
        Set<String> userIds = new HashSet<>((List<String>)indexMap.get("assertionUserId"));
        assert(userIds.size() == 1);
        assert(userIds.contains("userId"));
    }

    @Test
    public void testDeleteAssertion_record_not_found() throws IOException {
        // test when record is not found
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(null);
        assert(!assertionService.deleteAssertion("recordUuid", "assertionUuid"));
    }

    @Test
    public void testDeleteAssertion_no_existing_assertion_delete_not_found() throws IOException, SolrServerException {
        // test when record is found
        SolrDocument sd = new SolrDocument();
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(sd);
        // existing assertions empty so no deletion done
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(getMockAssertions(0, 0)));
        assert(!assertionService.deleteAssertion("recordUuid", "assertionUuid"));
        Mockito.verify(store, never()).delete(Mockito.any(), Mockito.any());
        Mockito.verify(indexDAO, never()).indexFromMap(Mockito.any());

    }

    @Test
    public void testDeleteAssertion_existing_1_and_delete_not_found() throws IOException, SolrServerException {
        SolrDocument sd = new SolrDocument();
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(sd);
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(getMockAssertions(1, 0)));

        assert(!assertionService.deleteAssertion("recordUuid", "invalid_assertionUuid"));
        Mockito.verify(store, never()).put(Mockito.any(), Mockito.any());
        Mockito.verify(indexDAO, never()).indexFromMap(Mockito.any());
    }

    @Test
    public void testDeleteAssertion_existing_1_and_delete_found() throws IOException, SolrServerException {
        SolrDocument sd = new SolrDocument();
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(sd);
        UserAssertions userAssertions = getMockAssertions(1, 0);

        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(userAssertions));
        assert(assertionService.deleteAssertion("recordUuid", userAssertions.get(0).getUuid()));

        // verify delete cassandra called
        Mockito.verify(store).delete(Mockito.any(), Mockito.any());

        ArgumentCaptor<List<Map<String, Object>>> myIndexMaps = ArgumentCaptor.forClass(List.class);

        // verify index
        Mockito.verify(indexDAO).indexFromMap(myIndexMaps.capture());
        Map<String, Object> indexMap = myIndexMaps.getValue().get(0);

        assert(indexMap.get("userAssertions").equals(String.valueOf(AssertionStatus.QA_NONE)));
        assert(!(boolean)indexMap.get("hasUserAssertions"));
        assert(!indexMap.containsKey("assertionUserId"));
    }

    @Test
    public void testGetAllAssertions() throws IOException {
        // test get empty
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(new UserAssertions()));
        assert(assertionService.getAssertions("recordUuid").size() == 0);

        UserAssertions qualityAssertions = new UserAssertions();
        qualityAssertions.add(new QualityAssertion());
        qualityAssertions.add(new QualityAssertion());
        // test get non-empty
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(qualityAssertions));
        assert(assertionService.getAssertions("recordUuid").size() == 2);
        assert(assertionService.getAssertions("recordUuid").get(0).getUuid().equals(qualityAssertions.get(0).getUuid()));
        assert(assertionService.getAssertions("recordUuid").get(1).getUuid().equals(qualityAssertions.get(1).getUuid()));
    }

    @Test
    public void testGetOneAssertion() throws Exception {
        // store.get(UserAssertions.class, recordUuid).orElse(new UserAssertions());
        // test no assertions at all
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(new UserAssertions()));
        assert(assertionService.getAssertion("recordUuid", "assertionUuid") == null);

        // test no matching assertion found
        UserAssertions qualityAssertions = new UserAssertions();
        qualityAssertions.add(new QualityAssertion());
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(qualityAssertions));
        assert(assertionService.getAssertion("recordUuid", "assertionUuid") == null);

        // test matching assertion found
        assert(assertionService.getAssertion("recordUuid", qualityAssertions.get(0).getUuid()) != null);
    }

    @Test
    public void testBulkAdd() throws Exception {
        // test when record is not found
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(null);
        assert(!assertionService.bulkAddAssertions("recordUuid", new UserAssertions()));

        // test add succeed
        SolrDocument sd = new SolrDocument();
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(sd);

        UserAssertions existingAssertions = new UserAssertions();
        QualityAssertion qa1 = new QualityAssertion();
        qa1.setReferenceRowKey("record_uuid_1");
        qa1.setCode(0);
        qa1.setUserId("userId");
        QualityAssertion qa2 = new QualityAssertion();
        qa1.setReferenceRowKey("record_uuid_1");
        qa2.setCode(2000);
        qa2.setUserId("userId");
        existingAssertions.add(qa1);
        existingAssertions.add(qa2);

        UserAssertions newAssertions = new UserAssertions();
        QualityAssertion qa3 = new QualityAssertion();
        qa3.setReferenceRowKey("record_uuid_1");
        qa3.setCode(1000);
        qa3.setUserId("userId");
        QualityAssertion qa4 = new QualityAssertion();
        qa4.setReferenceRowKey("record_uuid_1");
        qa4.setCode(3000);
        qa4.setUserId("userId");
        newAssertions.add(qa3);
        newAssertions.add(qa4);

        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(existingAssertions));
        assert(assertionService.bulkAddAssertions("recordUuid", newAssertions));
        ArgumentCaptor<String> myUuids = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<UserAssertions> myUserAssertions = ArgumentCaptor.forClass(UserAssertions.class);

        Mockito.verify(store).put(myUuids.capture(), myUserAssertions.capture());
        assert(myUserAssertions.getValue().size() == 4);
        UserAssertions assertions = myUserAssertions.getValue();
        assertions.sort(Comparator.comparingInt(QualityAssertion::getCode));
        assert(assertions.get(0).getUuid().equals(qa1.getUuid()));
        assert(assertions.get(1).getUuid().equals(qa3.getUuid()));
        assert(assertions.get(2).getUuid().equals(qa2.getUuid()));
        assert(assertions.get(3).getUuid().equals(qa4.getUuid()));
    }
}
