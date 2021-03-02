package au.org.ala.biocache.dto;

import java.util.Date;
import java.util.UUID;

/**
 * A companion object for the QualityAssertion class that provides factory
 * type functionality.
 * <p>
 * Merged from biocache-store
 */
public class QualityAssertion {
    String uuid = UUID.randomUUID().toString();
    String dataResourceUid;
    String referenceRowKey;
    String name;
    Integer code;
    String relatedUuid;
    Integer qaStatus = 0;
    String comment;
    String value;
    String userId;
    String userEmail;
    String userDisplayName;
    String userRole;
    String userEntityUid;
    String userEntityName;
    String created = new Date().toString();
    String snapshot;
    Boolean problemAsserted = false;

    public QualityAssertion() {
    }

    public QualityAssertion(ErrorCode errorCode, String comment) {
        this.name = errorCode.name;
        this.code = errorCode.code;
        this.comment = comment;
    }

    public QualityAssertion(ErrorCode errorCode, Integer qaStatus) {
        this.name = errorCode.name;
        this.code = errorCode.code;
        this.qaStatus = qaStatus;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getReferenceRowKey() {
        return referenceRowKey;
    }

    public void setReferenceRowKey(String referenceRowKey) {
        this.referenceRowKey = referenceRowKey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getRelatedUuid() {
        return relatedUuid;
    }

    public void setRelatedUuid(String relatedUuid) {
        this.relatedUuid = relatedUuid;
    }

    public Integer getQaStatus() {
        return qaStatus;
    }

    public void setQaStatus(Integer qaStatus) {
        this.qaStatus = qaStatus;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public String getUserDisplayName() {
        return userDisplayName;
    }

    public void setUserDisplayName(String userDisplayName) {
        this.userDisplayName = userDisplayName;
    }

    public String getUserRole() {
        return userRole;
    }

    public void setUserRole(String userRole) {
        this.userRole = userRole;
    }

    public String getUserEntityUid() {
        return userEntityUid;
    }

    public void setUserEntityUid(String userEntityUid) {
        this.userEntityUid = userEntityUid;
    }

    public String getUserEntityName() {
        return userEntityName;
    }

    public void setUserEntityName(String userEntityName) {
        this.userEntityName = userEntityName;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public String getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(String snapshot) {
        this.snapshot = snapshot;
    }

    public Boolean getProblemAsserted() {
        return problemAsserted;
    }

    public void setProblemAsserted(Boolean problemAsserted) {
        this.problemAsserted = problemAsserted;
    }

    public String getDataResourceUid() {
        return dataResourceUid;
    }

    public void setDataResourceUid(String dataResourceUid) {
        this.dataResourceUid = dataResourceUid;
    }
}
