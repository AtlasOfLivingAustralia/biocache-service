package au.org.ala.biocache.dto;

import com.auth0.jwt.interfaces.Claim;
import lombok.AllArgsConstructor;

import java.security.Principal;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class AuthenticatedUser implements Principal {

    public final String email;
    public final String userId;
    public final List<String> roles;
    public final Map<String, Claim> attributes;

    @Override
    public String getName() {
        return email;
    }
}