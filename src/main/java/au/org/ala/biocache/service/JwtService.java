package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.AuthenticatedUser;
import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.UrlJwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.exceptions.SignatureVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URL;
import java.security.interfaces.RSAPublicKey;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static au.org.ala.biocache.web.AlaWebServiceAuthFilter.BEARER;

@Service
@Slf4j
public class JwtService {

    @Value("${spring.security.jwt.jwk.url}")
    String jwkUrl;

    /**
     * Verifies the signature of a JWT and retrieves the user information.
     *
     * @param authorizationHeader
     * @return
     */
    public Optional<AuthenticatedUser> checkJWT(String authorizationHeader) {

        try {
            if (!authorizationHeader.startsWith(BEARER)){
                return Optional.empty();
            }

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
                // check the expiry....
                if (jwt.getExpiresAt().before(new Date())){
                    log.error("JWT expired");
                    return Optional.empty();
                }
                List<String> roles = jwt.getClaims().get("role").asList(String.class);
                String email = jwt.getClaims().get("email").asString();
                String userId = jwt.getClaims().get("userid").asString();
                String firstName = jwt.getClaims().get("given_name").asString();
                String lastName = jwt.getClaims().get("family_name").asString();
                return Optional.of(new AuthenticatedUser(email, userId, roles, jwt.getClaims(), firstName, lastName));
            } catch (SignatureVerificationException e) {
                log.error("Verify of JWT failed");
                return Optional.empty();
            }
        } catch (JWTDecodeException e){
            // this will happen for some legacy API keys which are past in the Authorization header
            log.debug("Decode of JWT failed, supplied authorizationHeader is not a recognised JWT");
            log.debug(e.getMessage(), e);
        }  catch (Exception  e){
            // this will happen for some legacy API keys which are past in the Authorization header
            log.debug("Check of JWT failed, supplied authorizationHeader is not a recognised JWT");
            log.debug(e.getMessage(), e);
        }
        return Optional.empty();
    }
}
