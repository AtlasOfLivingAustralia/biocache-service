package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.AuthenticatedUser;
import au.org.ala.biocache.dto.DownloadRequestDTO;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.web.client.RestTemplate;

import java.util.*;

import static au.org.ala.biocache.service.LegacyApiKeyService.ROLE_LEGACY_APIKEY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
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

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(AuthService.LEGACY_X_ALA_USER_ID_HEADER, "1234");
        request.setUserPrincipal(new PreAuthenticatedAuthenticationToken(
                API_KEY_TEST_USER, null,
                Lists.newArrayList(new SimpleGrantedAuthority(ROLE_LEGACY_APIKEY))
            )
        );
        request.addUserRole(ROLE_LEGACY_APIKEY);

        DownloadRequestDTO dto = new DownloadRequestDTO();
        Optional<AuthenticatedUser> authenticatedUser = authService.getDownloadUser(dto, request);

        assertTrue(authenticatedUser.isPresent());
        assertEquals("1234", authenticatedUser.get().userId);
        assertEquals("test@test.com", authenticatedUser.get().email);
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

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(AuthService.LEGACY_X_ALA_USER_ID_HEADER, "1234");
        request.setUserPrincipal(new PreAuthenticatedAuthenticationToken(
                        API_KEY_TEST_USER, null,
                        Lists.newArrayList(new SimpleGrantedAuthority(ROLE_LEGACY_APIKEY))
                )
        );
        request.addUserRole(ROLE_LEGACY_APIKEY);

        DownloadRequestDTO dto = new DownloadRequestDTO();
        Optional<AuthenticatedUser> authenticatedUser = authService.getDownloadUser(dto, request);

        assertFalse(authenticatedUser.isPresent());
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

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(AuthService.LEGACY_X_ALA_USER_ID_HEADER, "1234");
        request.setUserPrincipal(new PreAuthenticatedAuthenticationToken(
                        API_KEY_TEST_USER, null,
                        Lists.newArrayList(new SimpleGrantedAuthority(ROLE_LEGACY_APIKEY))
                )
        );
        request.addUserRole(ROLE_LEGACY_APIKEY);

        DownloadRequestDTO dto = new DownloadRequestDTO();
        Optional<AuthenticatedUser> authenticatedUser = authService.getDownloadUser(dto, request);

        assertFalse(authenticatedUser.isPresent());
    }

    @Test
    public void offlineDownloadInValidUserid() throws Exception {
        // mock the user details lookup
        when(restTemplate.postForObject(any(String.class), any(), any()))
                .thenReturn(new HashMap<String, Object>() {{ }});

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(AuthService.LEGACY_X_ALA_USER_ID_HEADER, "invalid-id");
        request.setUserPrincipal(new PreAuthenticatedAuthenticationToken(
                        API_KEY_TEST_USER, null,
                        Lists.newArrayList(new SimpleGrantedAuthority(ROLE_LEGACY_APIKEY))
                )
        );
        request.addUserRole(ROLE_LEGACY_APIKEY);

        DownloadRequestDTO dto = new DownloadRequestDTO();
        Optional<AuthenticatedUser> authenticatedUser = authService.getDownloadUser(dto, request);

        assertFalse(authenticatedUser.isPresent());
    }
}
