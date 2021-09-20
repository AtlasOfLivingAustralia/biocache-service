package au.org.ala.biocache.dto;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

/**
 * A companion object for the QualityAssertion class that provides factory
 * type functionality.
 * <p>
 * Merged from biocache-store
 */
public class QualityAssertion {
    String uuid;
    String dataResourceUid;
    String referenceRowKey;
    String name;
    Integer code;
    String relatedUuid;
    Integer qaStatus = 0;
    String relatedRecordId;
    String relatedRecordReason;
    String comment;
    String value;
    String userId;
    String userEmail;
    String userDisplayName;
    String userRole;
    String userEntityUid;
    String userEntityName;
    String created;
    String snapshot;
    Boolean problemAsserted = false;

    public QualityAssertion() {
        uuid = UUID.randomUUID().toString();
        // to ISO date format to compatible with existing database records
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        created = simpleDateFormat.format(new Date());
    }

    public QualityAssertion(ErrorCode errorCode, String comment) {
        this();
        this.name = errorCode.name;
        this.code = errorCode.code;
        this.comment = comment;
    }

    public QualityAssertion(ErrorCode errorCode, Integer qaStatus) {
        this();
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
        if (name == null && code != null) {
            ErrorCode errorCode = AssertionCodes.getByCode(code);
            if (errorCode != null) {
                name = errorCode.getName();
            }
        }
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

    public String getRelatedRecordId() {
        return relatedRecordId;
    }

    public void setRelatedRecordId(String relatedRecordId) {
        this.relatedRecordId = relatedRecordId;
    }

    public String getRelatedRecordReason() {
        return relatedRecordReason;
    }

    public void setRelatedRecordReason(String relatedRecordReason) {
        this.relatedRecordReason = relatedRecordReason;
    }
}
