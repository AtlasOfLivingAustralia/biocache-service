package au.org.ala.biocache.util;

import au.org.ala.ws.security.profile.AlaUserProfile;

import java.security.Principal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AlaUnvalidatedProfile implements AlaUserProfile {

    String email;

    public AlaUnvalidatedProfile(String email) {
        this.email = email;
    }

    @Override
    public String getUserId() {
        return email;
    }

    @Override
    public String getEmail() {
        return email;
    }

    @Override
    public String getGivenName() {
        return null;
    }

    @Override
    public String getFamilyName() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public void setId(String id) {

    }

    @Override
    public String getTypedId() {
        return null;
    }

    @Override
    public String getUsername() {
        return null;
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
        return new HashSet();
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
}
