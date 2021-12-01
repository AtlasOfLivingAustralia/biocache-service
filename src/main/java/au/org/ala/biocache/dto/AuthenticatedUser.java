package au.org.ala.biocache.dto;

import com.auth0.jwt.interfaces.Claim;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.security.core.AuthenticatedPrincipal;

import java.security.Principal;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Getter
public class AuthenticatedUser implements Principal, AuthenticatedPrincipal {

    public final String email;
    public final String userId;
    public final List<String> roles;
    public final Map<String, Claim> attributes;
    public final String firstName;
    public final String lastName;

    @Override
    public String getName() {
        return email;
    }
}