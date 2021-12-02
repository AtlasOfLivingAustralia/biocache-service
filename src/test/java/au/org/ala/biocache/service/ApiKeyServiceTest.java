package au.org.ala.biocache.service;

import au.org.ala.biocache.dao.IndexDAO;
import au.org.ala.biocache.dao.StoreDAO;
import au.org.ala.biocache.dto.AuthenticatedUser;
import au.org.ala.biocache.dto.FacetThemes;
import au.org.ala.biocache.dto.QualityAssertion;
import au.org.ala.biocache.dto.UserAssertions;
import au.org.ala.biocache.util.OccurrenceUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.web.client.RestTemplate;

import javax.inject.Inject;

import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApiKeyServiceTest {

    @Spy
    CacheManager cacheManager = new ConcurrentMapCacheManager("apiKeys");

    AutoCloseable mocks;

    @InjectMocks
    LegacyApiKeyService legacyApiKeyService = new LegacyApiKeyService();

    RestTemplate restTemplate;

    @Before
    public void setup() {
        restTemplate = mock(RestTemplate.class);
        mocks = MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testValidKey() throws Exception {

        Map<String, Object> response  = new HashMap<String, Object>() {{
            put("valid", Boolean.TRUE);
            put("email", "value1");
            put("userId", "value2");
        }};

        when(restTemplate.getForObject(any(String.class), any())).thenReturn(response);
        Optional<AuthenticatedUser> authOptional = legacyApiKeyService.isValidKey("valid-key");
        assertTrue(authOptional.isPresent());
    }

    @Test
    public void testInvalidKey() throws Exception {
        Map<String, Object> response  = new HashMap<String, Object>() {{
            put("valid", Boolean.FALSE);
        }};

        when(restTemplate.getForObject(any(String.class), any())).thenReturn(response);
        Optional<AuthenticatedUser> authOptional = legacyApiKeyService.isValidKey("invalid-key");
        assertTrue(!authOptional.isPresent());
    }
}

