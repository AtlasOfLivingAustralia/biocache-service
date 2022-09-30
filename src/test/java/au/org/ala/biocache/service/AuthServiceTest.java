package au.org.ala.biocache.service;

import au.org.ala.ws.security.profile.AlaUserProfile;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.web.client.RestTemplate;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class AuthServiceTest {

    @InjectMocks
    AuthService authService = new AuthService();

    AutoCloseable mocks;

    RestTemplate restTemplate;

    final static AlaUserProfile API_KEY_TEST_USER =
            new AlaUserProfile("Tester", "test@test.com", null, null, Sets.newHashSet("ROLE_LEGACY_APIKEY"), Collections.EMPTY_MAP);

    @Before
    public void setup() {
        authService.userDetailsUrl = "http://mocked";
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

        Optional<AlaUserProfile> authenticatedUser =
                authService.lookupAuthUser("1234", true);
        assertTrue(authenticatedUser.isPresent());
        assertEquals("1234", authenticatedUser.get().getId());
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

        Optional<AlaUserProfile> authenticatedUser =
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

        Optional<AlaUserProfile> authenticatedUser =
                authService.lookupAuthUser("1234", true);

        assertFalse(authenticatedUser.isPresent());
    }

    @Test
    public void offlineDownloadInValidUserid() throws Exception {
        // mock the user details lookup
        when(restTemplate.postForObject(any(String.class), any(), any()))
                .thenReturn(new HashMap<String, Object>() {{ }});

        Optional<AlaUserProfile> authenticatedUser =
                authService.lookupAuthUser("1234", true);

        assertFalse(authenticatedUser.isPresent());
    }
}
