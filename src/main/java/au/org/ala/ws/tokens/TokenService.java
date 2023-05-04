package au.org.ala.ws.tokens;

import com.google.common.annotations.VisibleForTesting;
import com.nimbusds.oauth2.sdk.*;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.credentials.OidcCredentials;
import org.pac4j.oidc.profile.OidcProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Component for getting access tokens for using on web service requests.
 */
public class TokenService {

    static final Logger log = LoggerFactory.getLogger(TokenService.class);

    final boolean cacheTokens;

    final String clientId;
    final String clientSecret;

    final String jwtScopes;
    final List<String> finalScopes;


    private final OidcConfiguration oidcConfiguration;

    //    private final Pac4jContextProvider pac4jContextProvider;
    private final ProfileManager profileManager;

    private final TokenClient tokenClient;

    public TokenService(OidcConfiguration oidcConfiguration,
                        ProfileManager profileManager,
                        TokenClient tokenClient, String clientId, String clientSecret, String jwtScopes,
                        boolean cacheTokens) {

        this.cacheTokens = cacheTokens;
        this.oidcConfiguration = oidcConfiguration;
        this.profileManager = profileManager;
        this.tokenClient = tokenClient;

        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.jwtScopes = jwtScopes;

        if (jwtScopes != null && !jwtScopes.isEmpty()) {

            this.finalScopes = Arrays.stream(jwtScopes.split(" "))
                    .filter(Predicate.not(String::isEmpty))
                    .collect(Collectors.toList());

        } else {

            this.finalScopes = null;
        }
    }

    /**
     * Get an access token.  Will return the current user's access token or if there is no
     * current user, will request a client credentials grant based access token for this app.
     * @param requireUser Whether the auth token must belong to an individual user (setting this to true will disable requesting a client credentials based app JWT)
     * @return The access token
     */
    public AccessToken getAuthToken(boolean requireUser) {

        if (profileManager == null) {

            if (requireUser) {
                log.debug("Unable to retrieve user access token, no profile manager configured");
                return null;
            } else {
                return getClientCredentialsAccessToken();
            }
        }

        return profileManager
                .getProfile(OidcProfile.class)
                .map(OidcProfile::getAccessToken)
                .orElseGet(() -> {

                    if (requireUser) {
                        return null;
                    }

                    return getClientCredentialsAccessToken();
                });
    }

    private long expiryWindow = 1; // 1 second
    private volatile transient OidcCredentials cachedCredentials;
    private volatile transient long cachedCredentialsLifetime = 0;
    @VisibleForTesting
    final Object lock = new Object();

    AccessToken getClientCredentialsAccessToken() {

        if (oidcConfiguration != null) {

            try {

                OidcCredentials credentials;

                if (cacheTokens) {
                    credentials = getOrRefreshToken();
                } else {
                    credentials = clientCredentials();
                }

                return credentials.getAccessToken();

            } catch (Exception e) {
                log.debug("Error generating token", e);
            }

        } else {
            log.debug("Not generating token because OIDC is not configured");
        }

        return null;
    }

    private OidcCredentials getOrRefreshToken() throws IOException, ParseException {

        long now = (System.currentTimeMillis() / 1000) - expiryWindow;

        long lifetime = cachedCredentialsLifetime;
        if (lifetime == 0 || now >= lifetime) {
            synchronized (lock) {
                lifetime = cachedCredentialsLifetime;
                if (lifetime == 0 || now >= lifetime) {
                    OidcCredentials credentials = tokenSupplier(cachedCredentials);
                    cachedCredentials = credentials;
                    cachedCredentialsLifetime = (System.currentTimeMillis() / 1000) + credentials.getAccessToken().getLifetime();
                    return credentials;
                }
            }
        }
        return cachedCredentials;
    }

    private OidcCredentials tokenSupplier(OidcCredentials existingCredentials) throws IOException, ParseException {

        OidcCredentials credentials = null;

        if (existingCredentials != null && existingCredentials.getRefreshToken() != null) {

            try {
                log.debug("Refreshing existing token");
                credentials = refreshToken(existingCredentials.getRefreshToken());
            } catch (Exception e) {
                log.warn("Couldn't get refresh token from {}", existingCredentials.getRefreshToken(), e);
            }
        }
        if (credentials == null) { // no refresh token or refresh token grant failed
            log.debug("Requesting new client credentials token");
            credentials = clientCredentials();
        }
        return credentials;
    }

    private OidcCredentials clientCredentials() throws IOException, ParseException {

        TokenRequest tokenRequest = new TokenRequest(
                oidcConfiguration.findProviderMetadata().getTokenEndpointURI(),
                new ClientSecretBasic(new ClientID(clientId), new Secret(clientSecret)),
                new ClientCredentialsGrant(),
                finalScopes == null ? new Scope() : new Scope(finalScopes.toArray(new String[0]))
        );

        return tokenClient.executeTokenRequest(tokenRequest);
    }


    private OidcCredentials refreshToken(RefreshToken refreshToken) throws IOException, ParseException {

        TokenRequest tokenRequest = new TokenRequest(
                oidcConfiguration.findProviderMetadata().getTokenEndpointURI(),
                new ClientSecretBasic(new ClientID(clientId), new Secret(clientSecret)),
                new RefreshTokenGrant(refreshToken),
                finalScopes == null ? new Scope() : new Scope(finalScopes.toArray(new String[0]))
        );

        return tokenClient.executeTokenRequest(tokenRequest);
    }

}
