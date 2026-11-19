package au.org.ala.biocache.dao;

import au.org.ala.biocache.dto.Qid;
import au.org.ala.biocache.util.QidMissingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for QID TTL handling: the maxage parameter must be passed to the
 * store as a cassandra row TTL and expired qids must throw QidMissingException.
 */
class QidCacheDAOImplTest {

    @Mock
    StoreDAO storeDao;

    @InjectMocks
    QidCacheDAOImpl qidCacheDao;

    AutoCloseable mocks;

    @BeforeEach
    void setup() {
        mocks = MockitoAnnotations.openMocks(this);
        // @Value defaults are not applied outside of Spring
        qidCacheDao.setMaxCacheSize(104857600);
        qidCacheDao.setMinCacheSize(52428800);
        qidCacheDao.setLargestCacheableSize(5242880);
        qidCacheDao.clear();
    }

    @AfterEach
    void tearDown() throws Exception {
        mocks.close();
    }

    @Test
    void putWithMaxAgePassesTtlToStore() throws Exception {
        // 1500 ms rounds up to a TTL of 2 seconds
        qidCacheDao.put("q1", "displayQ", null, null, null, 1500L, null);

        ArgumentCaptor<Integer> ttl = ArgumentCaptor.forClass(Integer.class);
        verify(storeDao).put(anyString(), any(Qid.class), ttl.capture());
        assertEquals(Integer.valueOf(2), ttl.getValue());
    }

    @Test
    void putWithoutMaxAgeStoresWithoutTtl() throws Exception {
        qidCacheDao.put("q1", "displayQ", null, null, null, -1L, null);

        verify(storeDao).put(anyString(), any(Qid.class), isNull());
    }

    @Test
    void expiredQidThrowsQidMissingException() throws Exception {
        String key = qidCacheDao.put("q1", "displayQ", null, null, null, 1L, null);
        Thread.sleep(50);

        assertThrows(QidMissingException.class, () -> qidCacheDao.get(key));
    }

    @Test
    void expiredQidLoadedFromStoreAlsoThrows() throws Exception {
        // a row that is past its maxAge but still physically present in the store
        Qid stale = new Qid("1000", "q1", "displayQ", null, null, 0L, null, 1L, null);
        when(storeDao.get(Qid.class, "1000")).thenReturn(Optional.of(stale));

        assertThrows(QidMissingException.class, () -> qidCacheDao.get("1000"));
        verify(storeDao).get(Qid.class, "1000");
    }

    @Test
    void nonExpiredQidIsRetrievable() throws Exception {
        String key = qidCacheDao.put("q1", "displayQ", null, null, null, 365L * 24 * 3600 * 1000, null);

        Qid qid = qidCacheDao.get(key);
        assertNotNull(qid);
        assertEquals("q1", qid.getQ());
    }

    @Test
    void ttlSecondsConversion() {
        assertNull(QidCacheDAOImpl.ttlSeconds(null));
        assertNull(QidCacheDAOImpl.ttlSeconds(-1L));
        assertNull(QidCacheDAOImpl.ttlSeconds(0L));
        assertEquals(Integer.valueOf(1), QidCacheDAOImpl.ttlSeconds(1L));
        assertEquals(Integer.valueOf(1), QidCacheDAOImpl.ttlSeconds(1000L));
        assertEquals(Integer.valueOf(2), QidCacheDAOImpl.ttlSeconds(1001L));
        assertEquals(Integer.valueOf(Integer.MAX_VALUE), QidCacheDAOImpl.ttlSeconds(Long.MAX_VALUE));
    }

    @Test
    void isExpiredLogic() {
        long now = 10_000L;
        // a qid expires when creation time (the numeric key) + maxAge is before now
        assertTrue(QidCacheDAOImpl.isExpired("5000", 4999L, now));
        assertFalse(QidCacheDAOImpl.isExpired("5000", 5000L, now));
        assertFalse(QidCacheDAOImpl.isExpired("5000", 5001L, now));
        assertFalse(QidCacheDAOImpl.isExpired("5000", -1L, now));
        assertFalse(QidCacheDAOImpl.isExpired("5000", null, now));
        assertFalse(QidCacheDAOImpl.isExpired("not-a-number", 1L, now));
    }
}
