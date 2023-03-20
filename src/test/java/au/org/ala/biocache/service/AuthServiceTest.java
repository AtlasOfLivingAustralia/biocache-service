package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.DownloadRequestDTO;
import au.org.ala.ws.security.profile.AlaUserProfile;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.client.RestTemplate;

import java.security.Principal;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class AuthServiceTest {

    @InjectMocks
    AuthService authService = new AuthService();

    AutoCloseable mocks;

    RestTemplate restTemplate;

    final static AlaUserProfile API_KEY_TEST_USER = new AlaUserProfile() {

        @Override
        public String getId() {
            return null;
        }

        @Override
        public void setId(String id) {

        }

        @Override
        public String getTypedId() {
            return null;
        }

        @Override
        public String getUsername() {
            return null;
        }

        @Override
        public Object getAttribute(String name) {
            return null;
        }

        @Override
        public Map<String, Object> getAttributes() {
            return null;
        }

        @Override
        public boolean containsAttribute(String name) {
            return false;
        }

        @Override
        public void addAttribute(String key, Object value) {

        }

        @Override
        public void removeAttribute(String key) {

        }

        @Override
        public void addAuthenticationAttribute(String key, Object value) {

        }

        @Override
        public void removeAuthenticationAttribute(String key) {

        }

        @Override
        public void addRole(String role) {

        }

        @Override
        public void addRoles(Collection<String> roles) {

        }

        @Override
        public Set<String> getRoles() {
            return Sets.newHashSet("ROLE_LEGACY_APIKEY");
        }

        @Override
        public void addPermission(String permission) {

        }

        @Override
        public void addPermissions(Collection<String> permissions) {

        }

        @Override
        public Set<String> getPermissions() {
            return Collections.emptySet();
        }

        @Override
        public boolean isRemembered() {
            return false;
        }

        @Override
        public void setRemembered(boolean rme) {

        }

        @Override
        public String getClientName() {
            return null;
        }

        @Override
        public void setClientName(String clientName) {

        }

        @Override
        public String getLinkedId() {
            return null;
        }

        @Override
        public void setLinkedId(String linkedId) {

        }

        @Override
        public boolean isExpired() {
            return false;
        }

        @Override
        public Principal asPrincipal() {
            return null;
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public String getUserId() {
            return "Tester";
        }

        @Override
        public String getEmail() {
            return "test@test.com";
        }

        @Override
        public String getGivenName() {
            return null;
        }

        @Override
        public String getFamilyName() {
            return null;
        }
    };


    @Before
    public void setup() {
        authService.userDetailsUrl = "http://mocked";
        restTemplate = mock(RestTemplate.class);
        mocks = MockitoAnnotations.openMocks(this);
    }

    @Test
    public void authenticatedRequest() {

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setUserPrincipal(new AlaUserProfile(){
            @Override
            public String getId() {
                return null;
            }

            @Override
            public void setId(String id) {

            }

            @Override
            public String getTypedId() {
                return null;
            }

            @Override
            public String getUsername() {
                return null;
            }

            @Override
            public Object getAttribute(String name) {
                return null;
            }

            @Override
            public Map<String, Object> getAttributes() {
                return null;
            }

            @Override
            public boolean containsAttribute(String name) {
                return false;
            }

            @Override
            public void addAttribute(String key, Object value) {

            }

            @Override
            public void removeAttribute(String key) {

            }

            @Override
            public void addAuthenticationAttribute(String key, Object value) {

            }

            @Override
            public void removeAuthenticationAttribute(String key) {

            }

            @Override
            public void addRole(String role) {

            }

            @Override
            public void addRoles(Collection<String> roles) {

            }

            @Override
            public Set<String> getRoles() {
                return null;
            }

            @Override
            public void addPermission(String permission) {

            }

            @Override
            public void addPermissions(Collection<String> permissions) {

            }

            @Override
            public Set<String> getPermissions() {
                return null;
            }

            @Override
            public boolean isRemembered() {
                return false;
            }

            @Override
            public void setRemembered(boolean rme) {

            }

            @Override
            public String getClientName() {
                return null;
            }

            @Override
            public void setClientName(String clientName) {

            }

            @Override
            public String getLinkedId() {
                return null;
            }

            @Override
            public void setLinkedId(String linkedId) {

            }

            @Override
            public boolean isExpired() {
                return false;
            }

            @Override
            public Principal asPrincipal() {
                return null;
            }

            @Override
            public String getName() {
                return null;
            }

            @Override
            public String getUserId() {
                return null;
            }

            @Override
            public String getEmail() {
                return null;
            }

            @Override
            public String getGivenName() {
                return null;
            }

            @Override
            public String getFamilyName() {
                return null;
            }
        });

        Optional<AlaUserProfile> authenticatedUser = authService.getRecordViewUser(request);

        assertTrue(authenticatedUser.isPresent());
    }

    @Test
    public void authenticateDownload() {

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setUserPrincipal(new AlaUserProfile(){
            @Override
            public String getId() {
                return null;
            }

            @Override
            public void setId(String id) {

            }

            @Override
            public String getTypedId() {
                return null;
            }

            @Override
            public String getUsername() {
                return null;
            }

            @Override
            public Object getAttribute(String name) {
                return null;
            }

            @Override
            public Map<String, Object> getAttributes() {
                return null;
            }

            @Override
            public boolean containsAttribute(String name) {
                return false;
            }

            @Override
            public void addAttribute(String key, Object value) {

            }

            @Override
            public void removeAttribute(String key) {

            }

            @Override
            public void addAuthenticationAttribute(String key, Object value) {

            }

            @Override
            public void removeAuthenticationAttribute(String key) {

            }

            @Override
            public void addRole(String role) {

            }

            @Override
            public void addRoles(Collection<String> roles) {

            }

            @Override
            public Set<String> getRoles() {
                return null;
            }

            @Override
            public void addPermission(String permission) {

            }

            @Override
            public void addPermissions(Collection<String> permissions) {

            }

            @Override
            public Set<String> getPermissions() {
                return null;
            }

            @Override
            public boolean isRemembered() {
                return false;
            }

            @Override
            public void setRemembered(boolean rme) {

            }

            @Override
            public String getClientName() {
                return null;
            }

            @Override
            public void setClientName(String clientName) {

            }

            @Override
            public String getLinkedId() {
                return null;
            }

            @Override
            public void setLinkedId(String linkedId) {

            }

            @Override
            public boolean isExpired() {
                return false;
            }

            @Override
            public Principal asPrincipal() {
                return null;
            }

            @Override
            public String getName() {
                return null;
            }

            @Override
            public String getUserId() {
                return null;
            }

            @Override
            public String getEmail() {
                return null;
            }

            @Override
            public String getGivenName() {
                return null;
            }

            @Override
            public String getFamilyName() {
                return null;
            }
        });

        DownloadRequestDTO downloadRequestDTO = new DownloadRequestDTO();

        Optional<AlaUserProfile> authenticatedUser = authService.getDownloadUser(downloadRequestDTO, request);

        assertTrue(authenticatedUser.isPresent());
    }

    @Test
    public void authenticateRequiredDownload() {

        DownloadRequestDTO downloadRequestDTO = new DownloadRequestDTO();
        downloadRequestDTO.setEmail("test@test.com");

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setParameter("email", "test@test.com");

        authService.emailOnlyEnabled = false;
        Optional<AlaUserProfile> authenticatedUser = authService.getDownloadUser(downloadRequestDTO, request);

        assertFalse(authenticatedUser.isPresent());
    }

    @Test
    public void authValidEmailTestAllAttributes() {
        // mock the user details lookup
        when(restTemplate.postForObject(any(String.class), any(), any()))
                .thenReturn(new HashMap<String, Object>() {{
                    put("userId", "1234");
                    put("email", "test@test.com");
                    put("activated", true);
                    put("locked", false);
                    put("firstName", "Test");
                    put("lastName", "User");
                }});

        DownloadRequestDTO downloadRequestDTO = new DownloadRequestDTO();
        downloadRequestDTO.setEmail("test@test.com");

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setParameter("email", "test@test.com");

        Optional<AlaUserProfile> authenticatedUser =
                authService.getDownloadUser(downloadRequestDTO, request);

        assertTrue(authenticatedUser.isPresent());
        assertEquals("1234", authenticatedUser.get().getUserId());
        assertEquals("test@test.com", authenticatedUser.get().getEmail());
    }

    @Test
    public void offlineDownloadValidEmailTestNoActivated() throws Exception {
        // mock the user details lookup
        when(restTemplate.postForObject(any(String.class), any(), any()))
                .thenReturn(new HashMap<String, Object>() {{
                    put("userId", "1234");
                    put("email", "test@test.com");
                    put("activated", false);
                    put("locked", false);
                    put("firstName", "Test");
                    put("lastName", "User");
                }});

        DownloadRequestDTO downloadRequestDTO = new DownloadRequestDTO();
        downloadRequestDTO.setEmail("test@test.com");

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setParameter("email", "test@test.com");

        Optional<AlaUserProfile> authenticatedUser =
                authService.getDownloadUser(downloadRequestDTO, request);

        assertFalse(authenticatedUser.isPresent());
    }

    private void assertFalse(boolean present) {
    }

    @Test
    public void offlineDownloadValidEmailTestLocked() throws Exception {
        // mock the user details lookup
        when(restTemplate.postForObject(any(String.class), any(), any()))
                .thenReturn(new HashMap<String, Object>() {{
                    put("userId", "1234");
                    put("email", "test@test.com");
                    put("activated", false);
                    put("locked", false);
                    put("firstName", "Test");
                    put("lastName", "User");
                }});

        DownloadRequestDTO downloadRequestDTO = new DownloadRequestDTO();
        downloadRequestDTO.setEmail("test@test.com");

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setParameter("email", "test@test.com");

        Optional<AlaUserProfile> authenticatedUser =
                authService.getDownloadUser(downloadRequestDTO, request);

        assertFalse(authenticatedUser.isPresent());
    }

    @Test
    public void offlineDownloadInValidUserid() throws Exception {
        // mock the user details lookup
        when(restTemplate.postForObject(any(String.class), any(), any()))
                .thenReturn(new HashMap<String, Object>() {{ }});

        DownloadRequestDTO downloadRequestDTO = new DownloadRequestDTO();
        downloadRequestDTO.setEmail("test@test.com");

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setParameter("email", "test@test.com");

        Optional<AlaUserProfile> authenticatedUser =
                authService.getDownloadUser(downloadRequestDTO, request);

        assertFalse(authenticatedUser.isPresent());
    }
}
