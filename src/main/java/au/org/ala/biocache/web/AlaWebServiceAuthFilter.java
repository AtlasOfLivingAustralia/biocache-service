package au.org.ala.biocache.web;


import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.UrlJwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.exceptions.SignatureVerificationException;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import lombok.AllArgsConstructor;
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
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.annotation.PostConstruct;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URL;
import java.security.Principal;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Spring based Webservice Authentication Filter. This filter supports 3 modes of authentication:
 * 1) JSON Web tokens
 * 2) Legacy API keys using ALA's apikey app
 * 3) Whitelist IP
 */
@Component
@DependsOn("springSecurityFilterChain")
@Slf4j
class AlaWebServiceAuthFilter extends OncePerRequestFilter {

    public static final String BEARER = "Bearer";

    @Autowired
    @Qualifier("springSecurityFilterChain")
    Filter springSecurityFilterChain;

    @Value("${spring.security.jwt.enabled:true}")
    Boolean jwtApiKeysEnabled;

    @Value("${spring.security.jwt.jwk.url}")
    String jwkUrl;

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
        for (int i=0; i < filterChain.size(); i++){
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

        if (jwtApiKeysEnabled) {
            String authorizationHeader = ((HttpServletRequest) request).getHeader(HttpHeaders.AUTHORIZATION);
            if (authorizationHeader != null) {
                // parse JWT or check whitelist or Check API Key
                if (authorizationHeader.startsWith(BEARER)) {
                    AuthenticatedUser authenticatedUser = checkJWT(authorizationHeader);
                    if (authenticatedUser != null) {
                        setAuthenticatedUserAsPrincipal(authenticatedUser);
                    }
                }
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

    /**
     * Verifies the signature of a JWT and retrieves the user information.
     *
     * @param authorizationHeader
     * @return
     */
    AuthenticatedUser checkJWT(String authorizationHeader) {

        try {
            // https://auth0.com/docs/security/tokens/json-web-tokens/validate-json-web-tokens
            String token = authorizationHeader.substring(BEARER.length() + 1);

            // decode and verify
            DecodedJWT jwt = JWT.decode(token);
            JwkProvider provider = new UrlJwkProvider(new URL(jwkUrl));
            String keyId = jwt.getKeyId();
            Jwk jwk = provider.get(keyId);
            Algorithm algorithm = Algorithm.RSA256((RSAPublicKey) jwk.getPublicKey(), null);

            try {
                algorithm.verify(jwt);
                List<String> roles = jwt.getClaims().get("role").asList(String.class);
                String email = jwt.getClaims().get("email").asString();
                String userId = jwt.getClaims().get("userid").asString();
                return new AuthenticatedUser(email, userId, roles, jwt.getClaims());
            } catch (SignatureVerificationException e) {
                log.error("Verify of JWT failed");
                return null;
            }
        } catch (JWTDecodeException  e){
            // this will happen for some legacy API keys which are past in the Authorization header
            log.debug("Decode of JWT failed, supplied authorizationHeader is not a recognised JWT");
            log.debug(e.getMessage(), e);
        }  catch (Exception  e){
            // this will happen for some legacy API keys which are past in the Authorization header
            log.debug("Check of JWT failed, supplied authorizationHeader is not a recognised JWT");
            log.debug(e.getMessage(), e);
        }
        return null;
    }

    @AllArgsConstructor
    class AuthenticatedUser implements Principal {

        String email;
        String userId;
        List<String> roles;
        Map<String, Claim> attributes;

        @Override
        public String getName() {
            return email;
        }
    }
}