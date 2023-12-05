package au.org.ala.biocache.util;

import au.org.ala.ws.security.profile.AlaUserProfile;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.security.Principal;
import java.util.*;

public class AlaUserProfileDeserializer extends StdDeserializer<AlaUserProfile> {
    public AlaUserProfileDeserializer() {
        this(null);
    }

    public AlaUserProfileDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public AlaUserProfile deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        JsonNode node = jp.getCodec().readTree(jp);

        final String familyName = node.get("familyName").asText();
        final String name = node.get("name").asText();
        final String username = node.get("username").asText();
        final String givenName = node.get("givenName").asText();
        final String email = node.get("email").asText();
        final String userId = node.get("userId").asText();
        final Set<String> roles = new HashSet<>();

        if (node.get("roles") != null) {
            Iterator<JsonNode> iterator = ((ArrayNode) node.get("roles")).iterator();
            while (iterator.hasNext()) {
                roles.add(iterator.next().textValue());
            }
        }

        return new AlaUserProfile() {

            @Override
            public String getUserId() { return userId; }

            @Override
            public String getEmail() {
                return email;
            }

            @Override
            public String getGivenName() {
                return givenName;
            }

            @Override
            public String getFamilyName() {
                return familyName;
            }

            @Override
            public String getName() {
                return name;
            }

            @Override
            public String getId() {
                return null;
            }

            @Override
            public void setId(String id) {}

            @Override
            public String getTypedId() {
                return null;
            }

            @Override
            public String getUsername() {
                return username;
            }

            @Override
            public Object getAttribute(String name) {
                return null;
            }

            @Override
            public Map<String, Object> getAttributes() {
                return null;
            }

            @Override
            public boolean containsAttribute(String name) {
                return false;
            }

            @Override
            public void addAttribute(String key, Object value) {

            }

            @Override
            public void removeAttribute(String key) {

            }

            @Override
            public void addAuthenticationAttribute(String key, Object value) {

            }

            @Override
            public void removeAuthenticationAttribute(String key) {

            }

            @Override
            public void addRole(String role) {

            }

            @Override
            public void addRoles(Collection<String> roles) {

            }

            @Override
            public Set<String> getRoles() {
                return null;
            }

            @Override
            public void addPermission(String permission) {

            }

            @Override
            public void addPermissions(Collection<String> permissions) {

            }

            @Override
            public Set<String> getPermissions() {
                return null;
            }

            @Override
            public boolean isRemembered() {
                return false;
            }

            @Override
            public void setRemembered(boolean rme) {

            }

            @Override
            public String getClientName() {
                return null;
            }

            @Override
            public void setClientName(String clientName) {

            }

            @Override
            public String getLinkedId() {
                return null;
            }

            @Override
            public void setLinkedId(String linkedId) {

            }

            @Override
            public boolean isExpired() {
                return false;
            }

            @Override
            public Principal asPrincipal() {
                return null;
            }
        };
    }
}
