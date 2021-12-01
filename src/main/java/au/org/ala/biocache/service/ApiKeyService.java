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
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

@Service
@Slf4j
public class ApiKeyService {

    public static final String ROLE_LEGACY_APIKEY = "ROLE_LEGACY_APIKEY";

    @Value("${spring.security.legacy.apikey.serviceUrl}")
    String legacyApiKeyServiceUrl;

    String[] legacyApiKeysRoles = new String[]{ROLE_LEGACY_APIKEY};

    @Inject
    protected RestOperations restTemplate;

    @Inject
    protected CacheManager cacheManager;

    /**
     * Use a webservice to validate a key
     *
     * @param keyToTest
     * @return True if API key checking is disabled, or the API key is valid, and false otherwise.
     */
    public AuthenticatedUser isValidKey(String keyToTest){

        if (StringUtils.isBlank(keyToTest)){
            return null;
        }

        // caching manually managed via the cacheManager not using the @Cacheable annotation
        // the @Cacheable annotation only works when an external call is made to a method, for
        // an explanation see: https://stackoverflow.com/a/32999744
        Cache cache = cacheManager.getCache("apiKeys");
        Cache.ValueWrapper valueWrapper = cache.get(keyToTest);

        if (valueWrapper != null && (AuthenticatedUser) valueWrapper.get() != null) {
            return (AuthenticatedUser) valueWrapper.get();
        }

        //check via a web service
        try {
            log.debug("Checking api key: + keyToTest");
            String url = legacyApiKeyServiceUrl + keyToTest;
            Map<String,Object> response = restTemplate.getForObject(url, Map.class);
            boolean isValid = (Boolean) response.get("valid");
            String userId = (String) response.get("userId");
            String email = (String) response.get("email");
            log.debug("Checking api key: " + keyToTest + ", valid: " + isValid);
            AuthenticatedUser auth = null;
            if (isValid) {
                auth = new AuthenticatedUser(email, userId, Arrays.asList(legacyApiKeysRoles), Collections.emptyMap(), null, null);
                cache.put(keyToTest, auth);
            }
            return auth;
        } catch (Exception e){
            log.error(e.getMessage(), e);
        }

        return null;
    }
}
