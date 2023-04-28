package au.org.ala.ws.config;

import au.org.ala.ws.tokens.TokenClient;
import au.org.ala.ws.tokens.TokenInterceptor;
import au.org.ala.ws.tokens.TokenService;
import org.pac4j.core.config.Config;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.oidc.config.OidcConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
class AlaWsConfig {

    @Value("${webservice.client-id}")
    String clientId;

    @Value("${webservice.client-secret}")
    String clientSecret;

    @Value("${webservice.jwt-scopes}")
    String jwtScopes;

    @Value("${webservices.cache-tokens:true}")
    boolean cacheTokens;

    @Bean
    TokenClient tokenClient(
            @Autowired(required = false) OidcConfiguration oidcConfiguration
    ) {
        return new TokenClient(oidcConfiguration);
    }

    @Bean
    TokenService tokenService(
            @Autowired(required = false) OidcConfiguration oidcConfiguration,
            @Autowired(required = false) ProfileManager profileManager,
            @Autowired TokenClient tokenClient
    ) {

        return new TokenService(
                oidcConfiguration, profileManager, tokenClient,
                clientId, clientSecret, jwtScopes, cacheTokens);
    }

    /**
     * OK HTTP Interceptor that injects a client credentials Bearer token into a request
     * @return
     */
    @ConditionalOnProperty(prefix="webservice", name="jwt")
    @ConditionalOnMissingBean(name = "jwtInterceptor")
    @Bean
    TokenInterceptor jwtInterceptor(@Autowired TokenService tokenService) {

        return new TokenInterceptor(tokenService);
    }
}
