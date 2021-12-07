package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.AuthenticatedUser;
import au.org.ala.biocache.dto.DownloadRequestDTO;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.web.client.RestTemplate;

import java.util.*;

import static au.org.ala.biocache.service.LegacyApiKeyService.ROLE_LEGACY_APIKEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class AuthServiceTest {

    @InjectMocks
    AuthService authService = new AuthService();

    AutoCloseable mocks;

    JwtService jwtService;
    LegacyApiKeyService legacyApiKeyService;
    RestTemplate restTemplate;

    final static AuthenticatedUser API_KEY_TEST_USER =
            new AuthenticatedUser("test@test.com","Tester",
                    Arrays.asList(new String[]{ROLE_LEGACY_APIKEY}), Collections.EMPTY_MAP, null, null);

    @Before
    public void setup() {
        authService.userDetailsUrl = "http://mocked";
        jwtService = mock(JwtService.class);
        legacyApiKeyService = mock(LegacyApiKeyService.class);
        restTemplate = mock(RestTemplate.class);
        mocks = MockitoAnnotations.openMocks(this);
    }

        @Test
    public void authValidEmailTestAllAttributes() {
        // mock the user details lookup
        when(restTemplate.postForObject(any(String.class), any(), any()))
                .thenReturn(new HashMap<String, Object>() {{
                    put("userid", "1234");
                    put("email", "test@test.com");
                    put("activated", true);
                    put("locked", false);
                    put("first_name", "Test");
                    put("last_name", "User");
                }});

        Optional<AuthenticatedUser> authenticatedUser =
                authService.lookupAuthUser("1234", true);
        assertTrue(authenticatedUser.isPresent());
        assertEquals("1234", authenticatedUser.get().getUserId());
        assertEquals("test@test.com", authenticatedUser.get().getEmail());
    }

    @Test
    public void offlineDownloadValidEmailTestNoActivated() throws Exception {
        // mock the user details lookup
        when(restTemplate.postForObject(any(String.class), any(), any()))
                .thenReturn(new HashMap<String, Object>() {{
                    put("userid", "1234");
                    put("email", "test@test.com");
                    put("activated", false);
                    put("locked", false);
                    put("first_name", "Test");
                    put("last_name", "User");
                }});

        Optional<AuthenticatedUser> authenticatedUser =
                authService.lookupAuthUser("1234", true);

        assertFalse(authenticatedUser.isPresent());
    }

    private void assertFalse(boolean present) {
    }

    @Test
    public void offlineDownloadValidEmailTestLocked() throws Exception {
        // mock the user details lookup
        when(restTemplate.postForObject(any(String.class), any(), any()))
                .thenReturn(new HashMap<String, Object>() {{
                    put("userid", "1234");
                    put("email", "test@test.com");
                    put("activated", true);
                    put("locked", true);
                    put("first_name", "Test");
                    put("last_name", "User");
                }});

        Optional<AuthenticatedUser> authenticatedUser =
                authService.lookupAuthUser("1234", true);

        assertFalse(authenticatedUser.isPresent());
    }

    @Test
    public void offlineDownloadInValidUserid() throws Exception {
        // mock the user details lookup
        when(restTemplate.postForObject(any(String.class), any(), any()))
                .thenReturn(new HashMap<String, Object>() {{ }});

        Optional<AuthenticatedUser> authenticatedUser =
                authService.lookupAuthUser("1234", true);

        assertFalse(authenticatedUser.isPresent());
    }
}
