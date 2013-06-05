package org.ala.biocache.dto;

/**
 * Represents a contact for the system that may or may not have a role within
 * an entity such as a collection.
 */
public class ContactDTO {

    String userId; //might be null
    String displayName;
    String email;
    String role;
    String phone;
    String associatedEntityUid; //e.g. the collection UID

    public String getAssociatedEntityUid() {
        return associatedEntityUid;
    }

    public void setAssociatedEntityUid(String associatedEntityUid) {
        this.associatedEntityUid = associatedEntityUid;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }
}
