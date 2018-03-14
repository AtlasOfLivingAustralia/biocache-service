package au.org.ala.doi;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class Doi {


    private Long id  = 0L;
    private String uuid;
    private String doi;
    private String title;
    private String authors;
    private List<String> licence;
    private String userId;
    private String description;
    private Date dateMinted;
    private Provider provider;
    private String filename;
    private String contentType;
    private Map<String, ?> providerMetadata;
    private Map<String, ?>  applicationMetadata;
    private String customLandingPageUrl;
    private String applicationUrl;
    private Long version;
    private Date dateCreated;
    private Date lastUpdated;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getDoi() {
        return doi;
    }

    public void setDoi(String doi) {
        this.doi = doi;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getAuthors() {
        return authors;
    }

    public void setAuthors(String authors) {
        this.authors = authors;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getDateMinted() {
        return dateMinted;
    }

    public void setDateMinted(Date dateMinted) {
        this.dateMinted = dateMinted;
    }

    public Provider getProvider() {
        return provider;
    }

    public void setProvider(Provider provider) {
        this.provider = provider;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public Map<String, ?> getProviderMetadata() {
        return providerMetadata;
    }

    public void setProviderMetadata(Map<String, ?> providerMetadata) {
        this.providerMetadata = providerMetadata;
    }

    public Map<String, ?> getApplicationMetadata() {
        return applicationMetadata;
    }

    public void setApplicationMetadata(Map<String, ?> applicationMetadata) {
        this.applicationMetadata = applicationMetadata;
    }

    public String getCustomLandingPageUrl() {
        return customLandingPageUrl;
    }

    public void setCustomLandingPageUrl(String customLandingPageUrl) {
        this.customLandingPageUrl = customLandingPageUrl;
    }

    public String getApplicationUrl() {
        return applicationUrl;
    }

    public void setApplicationUrl(String applicationUrl) {
        this.applicationUrl = applicationUrl;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public Date getDateCreated() {
        return dateCreated;
    }

    public void setDateCreated(Date dateCreated) {
        this.dateCreated = dateCreated;
    }

    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public List<String> getLicence() {
        return licence;
    }

    public void setLicence(List<String> licence) {
        this.licence = licence;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
