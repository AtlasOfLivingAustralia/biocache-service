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
 Generate a dynamic cache-key bases on the users data roles, as result queries can differ base on user permissions when rbac is enabled.
 */

@Component("dataRoleCacheKeyGenerator")
public class DataRoleCacheKeyGenerator implements KeyGenerator {

    @Value("${rbac.rolePrefix:}")
    private String rolePrefix;

    @Override
    public Object generate(Object target, Method method, Object... params) {
        var auth = SecurityContextHolder.getContext().getAuthentication();
        var roles = "";
        if (auth != null) {
            roles = auth.getAuthorities().stream()
                    .map(GrantedAuthority::getAuthority)
                    .filter(role -> role.startsWith(rolePrefix))
                    .map(role -> role.substring(rolePrefix.length()))
                    .sorted()
                    .collect(Collectors.joining("_"));
        }

        return StringUtils.arrayToDelimitedString(params, "_") + "_"
                + roles;
    }
}
