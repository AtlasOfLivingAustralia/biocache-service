package au.org.ala.biocache.dto;

import com.auth0.jwt.interfaces.Claim;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.security.core.AuthenticatedPrincipal;

import java.security.Principal;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class AuthenticatedUser implements Principal, AuthenticatedPrincipal {

    String email;
    String userId;
    List<String> roles;
    Map<String, Claim> attributes;
    String firstName;
    String lastName;

    @Override
    public String getName() {
        return email;
    }
}