package au.org.ala.biocache.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class DataRoleCacheKeyGeneratorTest {

    private static final String ROLE_PREFIX = "DATA_ROLE_";

    private DataRoleCacheKeyGenerator keyGenerator;

    @Before
    public void setUp() {
        keyGenerator = new DataRoleCacheKeyGenerator();
        SecurityContextHolder.clearContext();
    }

    @After
    public void tearDown() {
        SecurityContextHolder.clearContext();
    }

    @Test
    public void givenRbacDisabledThenOnlyParams() {
        setRbacEnabled(false);
        setRolePrefix(ROLE_PREFIX);

        Object cacheKey = keyGenerator.generate(this, null, "param1", "param2");

        assertEquals("param1_param2", cacheKey);
    }

    @Test
    public void givenRbacDisabledAndAuthenticationThenOnlyParams() {
        setRbacEnabled(false);
        setRolePrefix(ROLE_PREFIX);
        setSecurityContext(List.of(ROLE_PREFIX + "roleA"));

        Object cacheKey = keyGenerator.generate(this, null, "param1", "param2");

        assertEquals("param1_param2", cacheKey);
    }

    @Test
    public void givenRbacEnabledAndNoAuthenticationThenOnlyParams() {
        setRbacEnabled(true);
        setRolePrefix(ROLE_PREFIX);
        // No security context set

        Object cacheKey = keyGenerator.generate(this, null, "param1", "param2");

        assertEquals("param1_param2", cacheKey);
    }

    @Test
    public void givenRbacEnabledAndSingleRoleThenIncludesRoleInKey() {
        setRbacEnabled(true);
        setRolePrefix(ROLE_PREFIX);
        setSecurityContext(List.of(ROLE_PREFIX + "roleA"));

        Object cacheKey = keyGenerator.generate(this, null, "param1", "param2");

        assertEquals("param1_param2roleA", cacheKey);
    }

    @Test
    public void givenRbacEnabledAndMultipleRolesThenIncludesSortedRolesInKey() {
        setRbacEnabled(true);
        setRolePrefix(ROLE_PREFIX);
        // Roles added in non-sorted order
        setSecurityContext(List.of(ROLE_PREFIX + "roleC", ROLE_PREFIX + "roleA", ROLE_PREFIX + "roleB"));

        Object cacheKey = keyGenerator.generate(this, null, "param1", "param2");

        // Roles should be sorted alphabetically
        assertEquals("param1_param2roleA_roleB_roleC", cacheKey);
    }

    @Test
    public void givenRbacEnabledAndMixedRolesThenOnlyIncludesMatchingPrefixRoles() {
        setRbacEnabled(true);
        setRolePrefix(ROLE_PREFIX);
        // Mix of matching and non-matching role prefixes
        setSecurityContext(List.of(
                ROLE_PREFIX + "roleA",
                "OTHER_PREFIX_roleX",
                ROLE_PREFIX + "roleB",
                "ROLE_ADMIN"
        ));

        Object cacheKey = keyGenerator.generate(this, null, "param1", "param2");

        // Only roles with DATA_ROLE_ prefix should be included
        assertEquals("param1_param2roleA_roleB", cacheKey);
    }

    @Test
    public void givenRbacEnabledAndNoMatchingRolesThenOnlyParams() {
        setRbacEnabled(true);
        setRolePrefix(ROLE_PREFIX);
        // User has roles but none with the expected prefix
        setSecurityContext(List.of("ROLE_USER", "ROLE_ADMIN"));

        Object cacheKey = keyGenerator.generate(this, null, "param1", "param2");

        assertEquals("param1_param2", cacheKey);
    }

    @Test
    public void givenRbacEnabledAndEmptyParamsThenOnlyRoles() {
        setRbacEnabled(true);
        setRolePrefix(ROLE_PREFIX);
        setSecurityContext(List.of(ROLE_PREFIX + "roleA"));

        Object cacheKey = keyGenerator.generate(this, null);

        assertEquals("roleA", cacheKey);
    }

    @Test
    public void givenRbacDisabledAndEmptyParamsThenEmptyString() {
        setRbacEnabled(false);
        setRolePrefix(ROLE_PREFIX);

        Object cacheKey = keyGenerator.generate(this, null);

        assertEquals("", cacheKey);
    }

    @Test
    public void givenRbacEnabledAndEmptyRolePrefixThenIncludesAllRoles() {
        setRbacEnabled(true);
        setRolePrefix("");
        setSecurityContext(List.of("roleA", "ROLE_ADMIN", "roleB"));

        Object cacheKey = keyGenerator.generate(this, null, "param1");

        // All roles should be included and sorted
        assertEquals("param1ROLE_ADMIN_roleA_roleB", cacheKey);
    }

    @Test
    public void givenRbacEnabledAndCustomRolePrefixThenFiltersCorrectly() {
        setRbacEnabled(true);
        setRolePrefix("CUSTOM_");
        setSecurityContext(List.of("CUSTOM_role1", "DATA_ROLE_roleA", "CUSTOM_role2"));

        Object cacheKey = keyGenerator.generate(this, null, "param1");

        assertEquals("param1role1_role2", cacheKey);
    }

    private void setRbacEnabled(boolean enabled) {
        ReflectionTestUtils.setField(keyGenerator, "rbacEnabled", enabled);
    }

    private void setRolePrefix(String prefix) {
        ReflectionTestUtils.setField(keyGenerator, "rolePrefix", prefix);
    }

    private void setSecurityContext(List<String> roles) {
        if (roles == null || roles.isEmpty()) {
            SecurityContextHolder.clearContext();
            return;
        }
        List<SimpleGrantedAuthority> authorities = roles.stream()
                .map(SimpleGrantedAuthority::new)
                .toList();
        UsernamePasswordAuthenticationToken auth =
                new UsernamePasswordAuthenticationToken("testuser", "password", authorities);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }
}