package au.org.ala.ws.tokens;

import okhttp3.Interceptor;
import okhttp3.Response;

import java.io.IOException;

/**
 * okhttp interceptor that inserts a bearer token into the request
 */
public class TokenInterceptor implements Interceptor {

    private final TokenService tokenService;

    public TokenInterceptor(TokenService tokenService) {
        this.tokenService = tokenService;
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        return chain.proceed(
                chain.request().newBuilder()
                        .addHeader("Authorization", tokenService.getAuthToken(false).toAuthorizationHeader())
                        .build()
        );
    }

}
