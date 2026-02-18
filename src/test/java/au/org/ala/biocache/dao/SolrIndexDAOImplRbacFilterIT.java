package au.org.ala.biocache.dao;

import au.org.ala.biocache.util.SolrUtils;
import junit.framework.TestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Integration tests for RBAC (Role-Based Access Control) logic in SolrIndexDAOImpl.
 * <p>
 * Tests scenarios:
 * - rbacEnabled = false: all records are returned as before
 * - rbacEnabled = true:
 * - Records without RBAC fields are shown to all users
 * - Records with dynamicProperties_rbac=true are shown only to users with matching roles
 * - Records with dynamicProperties_rbac=false are hidden from users with matching roles
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:springTest.xml"})
@WebAppConfiguration
@TestPropertySource(locations = "classpath:biocache-test-config.properties")
public class SolrIndexDAOImplRbacFilterIT extends TestCase {

    static {
        System.setProperty("biocache.config", System.getProperty("user.dir") + "/src/test/resources/biocache-test-config.properties");
    }

    private static final String ROLE_PREFIX = "DATA_ROLE_";

    @Autowired
    SolrIndexDAOImpl solrIndexDAO;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        SolrUtils.setupIndex();
        loadRbacTestData();
    }

    @Before
    public void setup() throws Exception {
        // Clear any existing security context before each test
        SecurityContextHolder.clearContext();
    }

    @After
    public void tearDown() {
        // Clear security context after each test
        SecurityContextHolder.clearContext();
    }

    @Test
    public void givenRbacDisabledThenAllRecordsReturnedForAnonymousUser() throws Exception {
        setRbacEnabled(false);
        setSecurityContext(Collections.emptyList());

        SolrQuery query = new SolrQuery();
        query.setQuery("id:rbac-*");
        query.setRows(100);

        QueryResponse response = solrIndexDAO.query(query);

        // All 8 RBAC test records should be returned when RBAC is disabled
        assertEquals(8, response.getResults().size());
    }

    @Test
    public void givenRbacDisabledThenAllRecordsReturnedForAuthenticatedUser() throws Exception {
        setRbacEnabled(false);
        setRolePrefix(ROLE_PREFIX);
        setSecurityContext(List.of(ROLE_PREFIX + "roleA"));

        SolrQuery query = new SolrQuery();
        query.setQuery("id:rbac-*");
        query.setRows(100);

        QueryResponse response = solrIndexDAO.query(query);

        // All 8 RBAC test records should be returned when RBAC is disabled
        assertEquals(8, response.getResults().size());
    }

    @Test
    public void givenRbacEnabledThenAnonymousUserSeesOnlyUnrestrictedAndPublicRecords() throws Exception {
        setRbacEnabled(true);
        setRolePrefix(ROLE_PREFIX);
        setSecurityContext(Collections.emptyList());

        SolrQuery query = new SolrQuery();
        query.setQuery("id:rbac-*");
        query.setRows(100);

        QueryResponse response = solrIndexDAO.query(query);

        // Anonymous user should see:
        // - 2 records without RBAC fields (rbac-no-restrictions-*)
        // - 3 records with dynamicProperties_rbac=false (rbac-denied-roleA-1, rbac-denied-roleB-1, rbac-public-1)
        //   Anonymous users have no roles, so role-specific denials don't apply to them
        // NOT visible: records with dynamicProperties_rbac=true (require specific role to access)
        // Total: 5 records
        assertEquals(5, response.getResults().size());

        // Verify which records are returned
        List<String> returnedIds = response.getResults().stream()
                .map(doc -> (String) doc.getFieldValue("id"))
                .sorted()
                .toList();

        assertTrue(returnedIds.contains("rbac-no-restrictions-1"));
        assertTrue(returnedIds.contains("rbac-no-restrictions-2"));
        assertTrue(returnedIds.contains("rbac-denied-roleA-1"));
        assertTrue(returnedIds.contains("rbac-denied-roleB-1"));
        assertTrue(returnedIds.contains("rbac-public-1"));

        // Should NOT see records with dynamicProperties_rbac=true (require specific roles)
        assertFalse(returnedIds.contains("rbac-allowed-roleA-1"));
        assertFalse(returnedIds.contains("rbac-allowed-roleA-2"));
        assertFalse(returnedIds.contains("rbac-allowed-roleB-1"));
    }

    @Test
    public void givenRbacEnabledThenDataWithoutRbacFieldsShownToAllUsers() throws Exception {
        setRbacEnabled(true);
        setRolePrefix(ROLE_PREFIX);
        setSecurityContext(Collections.emptyList());

        SolrQuery query = new SolrQuery();
        query.setQuery("id:rbac-no-restrictions-*");
        query.setRows(100);

        QueryResponse response = solrIndexDAO.query(query);

        // Records without RBAC fields should always be visible
        assertEquals(2, response.getResults().size());
    }

    @Test
    public void givenRbacEnabledThenUserWithRoleASeesAllowedRecords() throws Exception {
        setRbacEnabled(true);
        setRolePrefix(ROLE_PREFIX);
        setSecurityContext(List.of(ROLE_PREFIX + "roleA"));

        SolrQuery query = new SolrQuery();
        query.setQuery("id:rbac-*");
        query.setRows(100);

        QueryResponse response = solrIndexDAO.query(query);

        // User with roleA should see:
        // - 2 records without RBAC fields (rbac-no-restrictions-*)
        // - 2 records with dynamicProperties_rbac=true AND allowed=roleA (rbac-allowed-roleA-*)
        // - 1 record with dynamicProperties_rbac=false AND allowed=roleB (rbac-denied-roleB-1) - not denied to roleA
        // - 1 record with dynamicProperties_rbac=false and no roles (rbac-public-1)
        // NOT: rbac-denied-roleA-1 (dynamicProperties_rbac=false AND allowed=roleA)
        // NOT: rbac-allowed-roleB-1 (dynamicProperties_rbac=true AND allowed=roleB, not roleA)
        // Total: 6 records

        List<String> returnedIds = response.getResults().stream()
                .map(doc -> (String) doc.getFieldValue("id"))
                .sorted()
                .toList();

        assertEquals(6, response.getResults().size());

        assertTrue(returnedIds.contains("rbac-no-restrictions-1"));
        assertTrue(returnedIds.contains("rbac-no-restrictions-2"));
        assertTrue(returnedIds.contains("rbac-allowed-roleA-1"));
        assertTrue(returnedIds.contains("rbac-allowed-roleA-2"));
        assertTrue(returnedIds.contains("rbac-denied-roleB-1"));
        assertTrue(returnedIds.contains("rbac-public-1"));

        // Should NOT contain records denied to roleA
        assertFalse(returnedIds.contains("rbac-denied-roleA-1"));
        // Should NOT contain records allowed only to roleB
        assertFalse(returnedIds.contains("rbac-allowed-roleB-1"));
    }

    @Test
    public void givenRbacEnabledThenUserWithRoleADoesNotSeeDeniedRecords() throws Exception {
        setRbacEnabled(true);
        setRolePrefix(ROLE_PREFIX);
        setSecurityContext(List.of(ROLE_PREFIX + "roleA"));

        SolrQuery query = new SolrQuery();
        query.setQuery("id:rbac-denied-roleA-*");
        query.setRows(100);

        QueryResponse response = solrIndexDAO.query(query);

        // User with roleA should NOT see records specifically denied to roleA
        assertEquals(0, response.getResults().size());
    }

    @Test
    public void givenRbacEnabledThenUserWithRoleBSeesAllowedRecords() throws Exception {
        setRbacEnabled(true);
        setRolePrefix(ROLE_PREFIX);
        setSecurityContext(List.of(ROLE_PREFIX + "roleB"));

        SolrQuery query = new SolrQuery();
        query.setQuery("id:rbac-*");
        query.setRows(100);

        QueryResponse response = solrIndexDAO.query(query);

        // User with roleB should see:
        // - 2 records without RBAC fields (rbac-no-restrictions-*)
        // - 1 record with dynamicProperties_rbac=true AND allowed=roleB (rbac-allowed-roleB-1)
        // - 1 record with dynamicProperties_rbac=false AND allowed=roleA (rbac-denied-roleA-1) - not denied to roleB
        // - 1 record with dynamicProperties_rbac=false and no roles (rbac-public-1)
        // NOT: rbac-denied-roleB-1 (dynamicProperties_rbac=false AND allowed=roleB)
        // NOT: rbac-allowed-roleA-* (dynamicProperties_rbac=true AND allowed=roleA, not roleB)
        // Total: 5 records

        List<String> returnedIds = response.getResults().stream()
                .map(doc -> (String) doc.getFieldValue("id"))
                .sorted()
                .toList();

        assertEquals(5, response.getResults().size());

        assertTrue(returnedIds.contains("rbac-no-restrictions-1"));
        assertTrue(returnedIds.contains("rbac-no-restrictions-2"));
        assertTrue(returnedIds.contains("rbac-allowed-roleB-1"));
        assertTrue(returnedIds.contains("rbac-denied-roleA-1"));
        assertTrue(returnedIds.contains("rbac-public-1"));

        // Should NOT contain records denied to roleB
        assertFalse(returnedIds.contains("rbac-denied-roleB-1"));
        // Should NOT contain records allowed only to roleA
        assertFalse(returnedIds.contains("rbac-allowed-roleA-1"));
        assertFalse(returnedIds.contains("rbac-allowed-roleA-2"));
    }

    @Test
    public void givenRbacEnabledThenUserWithMultipleRolesSeesAllAllowedRecords() throws Exception {
        setRbacEnabled(true);
        setRolePrefix(ROLE_PREFIX);
        setSecurityContext(List.of(ROLE_PREFIX + "roleA", ROLE_PREFIX + "roleB"));

        SolrQuery query = new SolrQuery();
        query.setQuery("id:rbac-*");
        query.setRows(100);

        QueryResponse response = solrIndexDAO.query(query);

        // User with both roleA and roleB should see:
        // - 2 records without RBAC fields (rbac-no-restrictions-*)
        // - 2 records allowed to roleA (rbac-allowed-roleA-*)
        // - 1 record allowed to roleB (rbac-allowed-roleB-1)
        // - 1 record with dynamicProperties_rbac=false and no roles (rbac-public-1)
        // NOT: rbac-denied-roleA-1 (denied to roleA)
        // NOT: rbac-denied-roleB-1 (denied to roleB)
        // Total: 6 records

        List<String> returnedIds = response.getResults().stream()
                .map(doc -> (String) doc.getFieldValue("id"))
                .sorted()
                .toList();

        assertEquals(6, response.getResults().size());

        assertTrue(returnedIds.contains("rbac-no-restrictions-1"));
        assertTrue(returnedIds.contains("rbac-no-restrictions-2"));
        assertTrue(returnedIds.contains("rbac-allowed-roleA-1"));
        assertTrue(returnedIds.contains("rbac-allowed-roleA-2"));
        assertTrue(returnedIds.contains("rbac-allowed-roleB-1"));
        assertTrue(returnedIds.contains("rbac-public-1"));

        // Should NOT contain denied records
        assertFalse(returnedIds.contains("rbac-denied-roleA-1"));
        assertFalse(returnedIds.contains("rbac-denied-roleB-1"));
    }

    @Test
    public void givenRbacEnabledThenUserWithNonMatchingRolePrefixTreatedAsAnonymous() throws Exception {
        setRbacEnabled(true);
        setRolePrefix(ROLE_PREFIX);
        // User has roles but not with the expected prefix
        setSecurityContext(List.of("OTHER_PREFIX_roleA"));

        SolrQuery query = new SolrQuery();
        query.setQuery("id:rbac-*");
        query.setRows(100);

        QueryResponse response = solrIndexDAO.query(query);

        // User with non-matching role prefix should be treated as anonymous
        // Should see same records as anonymous user: 5 records
        assertEquals(5, response.getResults().size());
    }

    private static final String RBAC_TEST_DATA = """
            id,scientificName,vernacularName,dataResourceUid,year,decimalLatitude,decimalLongitude,dynamicProperties_rbac,dynamicProperties_rbac_allowed
            rbac-no-restrictions-1,Circus assimilis,Spotted Harrier,dr0,2020,-32.75,151.08333,,
            rbac-no-restrictions-2,Circus assimilis,Spotted Harrier,dr0,2020,-33.75,152.08333,,
            rbac-allowed-roleA-1,Circus assimilis,Spotted Harrier,dr0,2021,-34.75,153.08333,true,roleA
            rbac-allowed-roleA-2,Circus assimilis,Spotted Harrier,dr0,2021,-35.75,154.08333,true,roleA
            rbac-allowed-roleB-1,Circus assimilis,Spotted Harrier,dr0,2021,-36.75,155.08333,true,roleB
            rbac-denied-roleA-1,Circus assimilis,Spotted Harrier,dr0,2022,-37.75,156.08333,false,roleA
            rbac-denied-roleB-1,Circus assimilis,Spotted Harrier,dr0,2022,-38.75,157.08333,false,roleB
            rbac-public-1,Circus assimilis,Spotted Harrier,dr0,2023,-39.75,158.08333,false,
            """;

    private static void loadRbacTestData() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        body.add("file", new InMemoryCsvResource(RBAC_TEST_DATA, "rbac-test-data.csv"));

        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);

        String serverUrl = "http://localhost:8983/solr/biocache/update?commit=true";

        RestTemplate restTemplate = new RestTemplate();
        var response = restTemplate.postForEntity(serverUrl, requestEntity, String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

    /**
     * In-memory resource that mimics a file for multipart uploads.
     */
    private static class InMemoryCsvResource extends org.springframework.core.io.ByteArrayResource {
        private final String filename;

        public InMemoryCsvResource(String content, String filename) {
            super(content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            this.filename = filename;
        }

        @Override
        public String getFilename() {
            return filename;
        }
    }

    private void setRbacEnabled(boolean enabled) {
        ReflectionTestUtils.setField(solrIndexDAO, "rbacEnabled", enabled);
    }

    private void setRolePrefix(String prefix) {
        ReflectionTestUtils.setField(solrIndexDAO, "rolePrefix", prefix);
    }

    private void setSecurityContext(List<String> roles) {
        if (roles == null || roles.isEmpty()) {
            SecurityContextHolder.clearContext();
            return;
        }
        List<SimpleGrantedAuthority> authorities = roles.stream()
                .map(SimpleGrantedAuthority::new)
                .collect(Collectors.toList());
        UsernamePasswordAuthenticationToken auth =
                new UsernamePasswordAuthenticationToken("testuser", "password", authorities);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }
}





