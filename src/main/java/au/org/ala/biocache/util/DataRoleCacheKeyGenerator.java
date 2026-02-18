package au.org.ala.biocache.util;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.stream.Collectors;

/*
 Generate a dynamic cache-key based on the users data roles,
 as result queries can differ base on user permissions when rbac is enabled.
 */

@Component("dataRoleCacheKeyGenerator")
public class DataRoleCacheKeyGenerator implements KeyGenerator {

    @Value("${rbac.enabled}")
    private boolean rbacEnabled;

    @Value("${rbac.rolePrefix:}")
    private String rolePrefix;

    @Override
    public Object generate(Object target, Method method, Object... params) {
        var cacheKey = StringUtils.arrayToDelimitedString(params, "_");

        if (rbacEnabled) {
            // Add concatenated data roles to cache-key if auth context provided
            var auth = SecurityContextHolder.getContext().getAuthentication();
            if (auth != null) {
                cacheKey += auth.getAuthorities().stream()
                        .map(GrantedAuthority::getAuthority)
                        .filter(role -> role.startsWith(rolePrefix))
                        .map(role -> role.substring(rolePrefix.length()))
                        .sorted()
                        .collect(Collectors.joining("_"));
            }
        }

        return cacheKey;
    }
}
