package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.AuthenticatedUser;
import au.org.ala.biocache.web.AlaWebServiceAuthFilter;
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
import java.util.List;

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
    public AuthenticatedUser checkJWT(String authorizationHeader) {

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
        } catch (JWTDecodeException e){
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
}
