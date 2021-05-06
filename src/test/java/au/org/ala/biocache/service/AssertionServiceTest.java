package au.org.ala.biocache.service;

import au.org.ala.biocache.dao.StoreDAO;
import au.org.ala.biocache.dto.FacetThemes;
import au.org.ala.biocache.dto.QualityAssertion;
import au.org.ala.biocache.dto.UserAssertions;
import au.org.ala.biocache.util.OccurrenceUtils;
import org.apache.solr.common.SolrDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class AssertionServiceTest {

    @Mock
    OccurrenceUtils occurrenceUtils;
    @Mock
    StoreDAO store;

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

    @Test
    public void testAddAssertion() throws IOException {
        // test when record is not found
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(null);
        Optional<QualityAssertion> qualityAssertion = assertionService.addAssertion("", "", "", "", "", "", "");

        assert(!qualityAssertion.isPresent());

        // test when record is found
        SolrDocument sd = new SolrDocument();

        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(sd);
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(new UserAssertions()));

        // test when add succeed -- code = 50000
        Optional<QualityAssertion> qualityAssertion1 = assertionService.addAssertion("recordUuid1", "50000",
                "comment1", "userId1", "userDisplayName1", "2000", "assertionUuid1");
        assert(qualityAssertion1.isPresent());
        // test when add succeed -- code = 50005
        Optional<QualityAssertion> qualityAssertion2 = assertionService.addAssertion("recordUuid2", "20000",
                "comment2", "userId2", "userDisplayName2", "2000", "assertionUuid");
        assert(qualityAssertion2.isPresent());

        ArgumentCaptor<String> myUuids = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<UserAssertions> myUserAssertions = ArgumentCaptor.forClass(UserAssertions.class);
        Mockito.verify(store, times(2)).put(myUuids.capture(), myUserAssertions.capture());

        List<String> uuids = myUuids.getAllValues();
        String uuid1 = uuids.get(0);
        assert(uuid1.equals("recordUuid1"));
        String uuid2 = uuids.get(1);
        assert(uuid2.equals("recordUuid2"));

        List<UserAssertions> userAssertions = myUserAssertions.getAllValues();
        assert(userAssertions.size() == 2);
        assert(userAssertions.get(0).size() == 1);

        QualityAssertion qa = userAssertions.get(0).get(0);

        assert(qa.getUserId().equals("userId1"));
        assert(qa.getReferenceRowKey().equals("recordUuid1"));
        assert(qa.getComment().equals("comment1"));
        assert(qa.getUserDisplayName().equals("userDisplayName1"));
        assert(qa.getCode() == 50000);
        assert(qa.getRelatedUuid().equals("assertionUuid1"));
        assert(qa.getQaStatus() == 2000);

        qa = userAssertions.get(1).get(0);

        assert(qa.getUserId().equals("userId2"));
        assert(qa.getReferenceRowKey().equals("recordUuid2"));
        assert(qa.getComment().equals("comment2"));
        assert(qa.getUserDisplayName().equals("userDisplayName2"));
        assert(qa.getCode() == 20000);
        assert(qa.getQaStatus() == 50005);
    }

    @Test
    public void testDeleteAssertion() throws IOException {
        // test when record is not found
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(null);
        assert(!assertionService.deleteAssertion("recordUuid", "assertionUuid"));

        // test when record is found
        SolrDocument sd = new SolrDocument();
        when(occurrenceUtils.getOcc(Mockito.any())).thenReturn(sd);
        // existing assertions empty so no deletion done
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(new UserAssertions()));
        assert(!assertionService.deleteAssertion("recordUuid", "assertionUuid"));

        // test assertion deleted
        UserAssertions qualityAssertions = new UserAssertions();
        QualityAssertion qa = new QualityAssertion();
        qa.setUuid("test_uuid");
        qualityAssertions.add(qa);
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(qualityAssertions));
        assert(assertionService.deleteAssertion("recordUuid", "test_uuid"));

        ArgumentCaptor<String> uuidCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(store).delete(Mockito.any(), uuidCaptor.capture());
        assert(uuidCaptor.getValue().equals("recordUuid"));

        // delete non-exist assertion
        when(store.get(Mockito.any(), Mockito.any())).thenReturn(Optional.of(qualityAssertions));
        assert(!assertionService.deleteAssertion("recordUuid", "test_uuid_not_exist"));
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
