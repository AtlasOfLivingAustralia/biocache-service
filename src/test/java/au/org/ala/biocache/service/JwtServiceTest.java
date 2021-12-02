package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.AuthenticatedUser;
import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.UrlJwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTCreationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import java.io.File;
import java.math.BigInteger;
import java.net.URL;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.RSAMultiPrimePrivateCrtKeySpec;
import java.security.spec.RSAPrivateKeySpec;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JwtServiceTest {

    @InjectMocks
    JwtService jwtService = new JwtService();

    @Before
    public void setup() throws Exception {
        jwtService.jwkUrl = getJwkUrl().toString();
    }

    @Test
    public void testValidJWT() throws Exception {
        String generatedJWT = generateTestJwt(false);
        Optional<AuthenticatedUser> result = jwtService.checkJWT("Bearer " + generatedJWT);
        assertTrue(result.isPresent());
    }

    @Test
    public void testInvalidJWT() throws Exception {
        Optional<AuthenticatedUser> result = jwtService.checkJWT("invalid auth header");
        assertFalse(result.isPresent());

        Optional<AuthenticatedUser> result2 = jwtService.checkJWT("Basic sadasdsada");
        assertFalse(result2.isPresent());

        Optional<AuthenticatedUser> result3 = jwtService.checkJWT("Bearer sadasdsada");
        assertFalse(result3.isPresent());
    }

    @Test
    public void testExpiredJWT() throws Exception {
        String generatedJWT = generateTestJwt(true);
        Optional<AuthenticatedUser> result = jwtService.checkJWT("Bearer " + generatedJWT);
        assertFalse(result.isPresent());
    }

    static URL getJwkUrl() throws Exception {
        File tempFile = File.createTempFile("test-jwk-", ".jwks");
        FileUtils.writeStringToFile(tempFile, TEST_JWK, "UTF-8");
        URL jwkUrl = tempFile.toURI().toURL();
        return jwkUrl;
    }

    String generateTestJwt(boolean expired) throws Exception {
        try {

            String keyId = "x5NlU73k1xGmmchGMKfQBEqaKUfWhlMTS8OROillD40";

            File tempFile = File.createTempFile("test-jwk-", ".jwks");
            FileUtils.writeStringToFile(tempFile, TEST_JWK, "UTF-8");

            URL jwkUrl = tempFile.toURI().toURL();

            JwkProvider provider = new UrlJwkProvider(jwkUrl);
            Jwk jwk = provider.get("x5NlU73k1xGmmchGMKfQBEqaKUfWhlMTS8OROillD40");

            Instant issuedAt = Instant.now().truncatedTo(ChronoUnit.SECONDS);
            Instant expiration = null;
            if (expired){
                expiration = issuedAt.minus(10, ChronoUnit.MINUTES);
            } else {
                expiration = issuedAt.plus(10, ChronoUnit.MINUTES);
            }

            Instant nbf = issuedAt.plus(10, ChronoUnit.MINUTES);

            Algorithm algorithm = Algorithm.RSA256((RSAPublicKey) jwk.getPublicKey(), getPrivateKey());
            String token = JWT.create()
                    .withIssuer(keyId)
                    .withKeyId(keyId)
                    .withClaim("iat", Date.from(issuedAt))
                    .withClaim("exp", Date.from(expiration))
                    .withClaim("nbf", Date.from(nbf))
                    .withClaim("email", "test@test.com")
                    .withClaim("sub", "test@test.com")
                    .withClaim("authority", "ROLE_TEST")
                    .withClaim("aud", "AUD") // this corresponds to AppClient in Cognito or Service in CAS
                    .withClaim("family_name", "test_lastname")
                    .withClaim("given_name", "test_firstname")
                    .withClaim("userid", "99999999")
                    .withClaim("role", Arrays.asList(new String[]{"ROLE_TEST"}))
                    .sign(algorithm);

            return token;
        } catch (JWTCreationException exception){
            //Invalid Signing configuration / Couldn't convert Claims.
            throw new Exception(exception);
        }
    }

    /**
     * For details of magic strings below see: https://tools.ietf.org/html/rfc7518#section-6.3
     * @return
     */
    RSAPrivateKey getPrivateKey() throws Exception {

        try {
            Map<String, List<Map<String, Object>>> keys = new ObjectMapper().readValue(TEST_JWK, Map.class);
            Map<String, Object> additionalAttributes = keys.get("keys").get(0);
            KeyFactory kf = KeyFactory.getInstance("RSA");

            BigInteger modules = getValues(additionalAttributes, "n");
            BigInteger publicExponent = getValues(additionalAttributes, "e");
            BigInteger privateExponent = getValues(additionalAttributes, "d");
            BigInteger primeP = getValues(additionalAttributes, "p");
            BigInteger primeQ = getValues(additionalAttributes, "q");
            BigInteger primeExponentP = getValues(additionalAttributes, "dp");
            BigInteger primeExponentQ = getValues(additionalAttributes, "dq");
            BigInteger crtCoefficient = getValues(additionalAttributes, "qi");

            RSAPrivateKeySpec privateKeySpec = new RSAMultiPrimePrivateCrtKeySpec(
                    modules,
                    publicExponent,
                    privateExponent,
                    primeP,
                    primeQ,
                    primeExponentP,
                    primeExponentQ,
                    crtCoefficient,
                    null
            );

            return (RSAPrivateKey) kf.generatePrivate(privateKeySpec);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private BigInteger getValues(Map<String, Object> additionalAttributes, String key) {
        return new BigInteger(1, Base64.getUrlDecoder().decode((String) additionalAttributes.get(key)));
    }

    public static String TEST_JWK = "{\n" +
            "    \"keys\": [\n" +
            "        {\n" +
            "            \"p\": \"8GyZCV3iQYINm8JdWNN3eJS4F-lq4VBhV7WPz0M8H4St1-CWCxxXDYCXkRJqlzaL6wYkfvrLrQVGZsmCLNlDhR171UGg-19Vx09uNjR_S57SOirvnafnkifiKXlvrEvm3i76Fq7voZjPjK8yTwFVowKoCSSvGND_u6flyWGISLc\",\n" +
            "            \"kty\": \"RSA\",\n" +
            "            \"q\": \"ifSiLC2po2CqU4bs2LKrx5uVpfq38czDXMunId98Vd0DPbz0uCxMXHqFoPVtLAVEVsnJxzSZoyuichVNJYFx0nD7RTf2Lku-dGRL-DMcjFFExUeadZJ0XUOeC0QPaW0lCNHuEByvrMjTOReBHXLROQAp8y4rQ5uY36h7zftliPE\",\n" +
            "            \"d\": \"QWkftzF3Bhpyx93WFlveQyMo44-Qy9aDD9DI8sLM1vDmoOjiWrgflezDWX4taARpMigI_CZlfGB1xNIu1fSOGoslYALoGpIi7fcAE3ORK8p73bphCSX3ZOMnvEnNWL2o1nYU8UhdLML6O7vOH4B6fMZT5mFgVXm8q3cR4vIsuc6AqggXePXDRBJxrJ2rNMrBfyr7sU-m7Mhcez_yjmCR4PmjP7fvkfciAPLGc2NqHQBObDsgIsovmSGd_QLaeCL3f3uoWcRKxkCIx4m2KoUCZ4VQu1idxrYVpzVlhQdF04Xwf6aMRDj70B1H8RC0zv19Upvq6sD4VydQxQmZR01tYQ\",\n" +
            "            \"e\": \"AQAB\",\n" +
            "            \"use\": \"sig\",\n" +
            "            \"kid\": \"x5NlU73k1xGmmchGMKfQBEqaKUfWhlMTS8OROillD40\",\n" +
            "            \"qi\": \"4ZrbqOU7Kt8-jxco_ZU4T6dwI7DvrlzA7VpbYQiipGfYfLgPHuQImYmCsH68uTdYVpmr1Eg83ntWRTW99YZ-gyzxL1yVACbfRXHh6LqMR8lqxI6Ezi2DX-Pz0JWLq5bbhbPc8Lo4ZSwzUbAbFWhGPOKt0GiFFH-9e8V32EeiRM8\",\n" +
            "            \"dp\": \"hVu3h3qMBcodwkgNfzfNDRVxB9JxdokfdYdAPbcNom31_8iTcZZGszag29dbSIT5F2RQ2h5C27YRVvJvQnFBApVYGvJMWSKIcgWdHHQCJ-_wrFMklk6MJIX67QZu6yMu7A8iqXJfeUNJ3L9RKIGU_tZ6Xpf6h8lmELEQxKNU9Q\",\n" +
            "            \"alg\": \"RS256\",\n" +
            "            \"dq\": \"VfmCk_vFK8-TdsiwlIANNDHPOoic2HGfExbMSvznlO9PFMvMy3a4BC5LVzt81SFXLKtHOAGc-ia4b1a9JHGeiOLmhYXRw5pq0EitW7omwX_oVvY_2aPzJyh7t1OsMyzVFsEWFy55gToLARqX2c1zfI2Ql0AfsFupE0ICHiXdalE\",\n" +
            "            \"n\": \"gY_dthaQM8H0ZYwomzEkxGFtPy11bCsKiGe--SaRFeblsyqClcO22mRsRqrvyshDVVPPaXXvWwcIwrqaQ9HDWVnR_PwUARm824i9TSEQ1sAciU5MZKsen2PeWrzwZffjX5Wf6jD24varPqXyJSSirRZD3EuJLdPuHvZQ1pFhCCWlL55dcDpyUehAXVjkiO428TH6zO1a18A3eAykKy-c8DL2YhdLoKV8wH7XedlePFnB-5SWbzrevkb2jhYut4DzZPzI-4ibB6YmHasjTbyr-LjsZ1K5atbV28p1JdSmMJl8MkU4rpYrOXMcqtgksGVQ-4Cz8pIWuomsYJAAVSCsRw\"\n" +
            "        }\n" +
            "    ]\n" +
            "}";
}
