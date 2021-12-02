package au.org.ala.biocache.web;

import au.org.ala.biocache.dto.AuthenticatedUser;
import au.org.ala.biocache.service.LegacyApiKeyService;
import au.org.ala.biocache.service.JwtService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpHeaders;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.FilterChainProxy;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

/**
 * Spring based Webservice Authentication Filter. This filter supports 3 modes of authentication:
 * 1) JSON Web tokens
 * 2) Legacy API keys using ALA's apikey app
 * 3) Whitelist IP
 */
@Component
@DependsOn("springSecurityFilterChain")
@Slf4j
public class AlaWebServiceAuthFilter extends OncePerRequestFilter {

    public static final String BEARER = "Bearer";
    public static final String API_KEY = "apiKey";

    @Autowired
    @Qualifier("springSecurityFilterChain")
    Filter springSecurityFilterChain;

    @Value("${spring.security.jwt.enabled}")
    Boolean jwtApiKeysEnabled;

    @Value("${spring.security.legacy.apikey.enabled}")
    Boolean legacyApiKeysEnabled;

    @Inject
    JwtService jwtService;

    @Inject
    LegacyApiKeyService apiKeyService;

    /** The name of the filter which this filter should be placed after in the spring security filter array. */
    String addAfterFilterName = "LogoutFilter";

    /** The idx at which this filter should be placed after in the spring security filter array. */
    int addAfterFilterIdx = 4;

    public AlaWebServiceAuthFilter(){}

    @PostConstruct
    void init(){
        List<Filter> filterChain = ((FilterChainProxy) springSecurityFilterChain).getFilterChains().get(0).getFilters();
        // get index of configured "addAfterFilterName"
        int filterIdx = -1;
        for (int i = 0; i < filterChain.size(); i++){
            Filter filter = filterChain.get(i);
            if (filter.getClass().getSimpleName().equalsIgnoreCase(addAfterFilterName) || filter.getClass().getCanonicalName().equalsIgnoreCase(addAfterFilterName)){
                filterIdx = i;
            }
        }
        if (filterIdx > 0){
            filterChain.add(filterIdx + 1, this);
        } else {
            filterChain.add(addAfterFilterIdx, this);
        }
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {

        // prevent stateful requests
        request.getSession().invalidate();

        if (jwtApiKeysEnabled) {
            String authorizationHeader = request.getHeader(HttpHeaders.AUTHORIZATION);
            if (authorizationHeader != null) {
                // parse JWT or check whitelist or Check API Key
                if (authorizationHeader.startsWith(BEARER)) {
                    Optional<AuthenticatedUser> authenticatedUser = jwtService.checkJWT(authorizationHeader);
                    if (authenticatedUser.isPresent()) {
                        setAuthenticatedUserAsPrincipal(authenticatedUser.get());
                    }
                }
            }
        }

        // look for annotations ????
        if (legacyApiKeysEnabled){
            // check header
            // check for requestParam - for backwards compatibilty
            String apiKeyHeader = request.getHeader(API_KEY);
            String apiKeyParam = request.getParameter(API_KEY);
            Optional<AuthenticatedUser> user = null;
            if (apiKeyHeader != null){
                user = apiKeyService.isValidKey(apiKeyHeader);
            }
            if (user == null && apiKeyParam != null){
                user = apiKeyService.isValidKey(apiKeyParam);
            }

            if (user.isPresent()) {
                setAuthenticatedUserAsPrincipal(user.get());
            }
        }
        
        chain.doFilter(request, response);
    }

    private void setAuthenticatedUserAsPrincipal(AuthenticatedUser authenticatedUser) {
        SecurityContext securityContext = SecurityContextHolder.getContext();
        List<String> credentials = new ArrayList<>();
        List<GrantedAuthority> authorities = new ArrayList<>();
        authenticatedUser.roles.forEach( s -> authorities.add(new SimpleGrantedAuthority(s)));
        PreAuthenticatedAuthenticationToken token = new PreAuthenticatedAuthenticationToken(
                authenticatedUser, credentials, authorities);
        token.setAuthenticated(true);
        securityContext.setAuthentication(token);
    }
}