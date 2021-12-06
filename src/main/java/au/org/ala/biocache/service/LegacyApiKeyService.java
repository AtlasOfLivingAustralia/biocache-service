package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.AuthenticatedUser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestOperations;

import javax.inject.Inject;
import java.util.*;

/**
 * Service for validating legacy api keys provided by apikey app.
 */
@Service
@Slf4j
public class LegacyApiKeyService {

    public static final String ROLE_LEGACY_APIKEY = "ROLE_LEGACY_APIKEY";
    public static final String API_KEYS_CACHE_NAME = "apiKeys";

    @Value("${spring.security.legacy.apikey.serviceUrl}")
    String legacyApiKeyServiceUrl;

    String[] legacyApiKeysRoles = new String[]{ROLE_LEGACY_APIKEY};

    @Inject
    protected RestOperations restTemplate;

    @Inject
    protected CacheManager cacheManager;

    @Inject
    protected AuthService authService;

    /**
     * Use a webservice to validate a key
     *
     * @param keyToTest
     * @return True if API key checking is disabled, or the API key is valid, and false otherwise.
     */
    public Optional<AuthenticatedUser> isValidKey(String keyToTest) {

        if (StringUtils.isBlank(keyToTest)) {
            return Optional.empty();
        }

        // caching manually managed via the cacheManager not using the @Cacheable annotation
        // the @Cacheable annotation only works when an external call is made to a method, for
        // an explanation see: https://stackoverflow.com/a/32999744
        Cache cache = cacheManager.getCache(API_KEYS_CACHE_NAME);
        Cache.ValueWrapper valueWrapper = cache.get(keyToTest);

        if (valueWrapper != null && (AuthenticatedUser) valueWrapper.get() != null) {
            return Optional.of((AuthenticatedUser) valueWrapper.get());
        }

        //check via a web service
        try {
            if (log.isDebugEnabled()) {
                log.debug("Checking api key: " + keyToTest);
            }
            String url = legacyApiKeyServiceUrl + keyToTest;
            Map<String, Object> response = restTemplate.getForObject(url, Map.class);
            boolean isValid = (Boolean) response.get("valid");
            String userId = (String) response.get("userId");
            String email = (String) response.get("email");
            if (log.isDebugEnabled()) {
                log.debug("Checking api key: " + keyToTest + ", valid: " + isValid);
            }
            if (isValid) {
                AuthenticatedUser auth = new AuthenticatedUser(email, userId, Arrays.asList(legacyApiKeysRoles), Collections.emptyMap(), null, null);
                cache.put(keyToTest, auth);
                return Optional.of(auth);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return Optional.empty();
    }

}
