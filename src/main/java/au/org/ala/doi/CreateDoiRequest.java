package au.org.ala.doi;

import java.util.List;
import java.util.Map;

public class CreateDoiRequest {
    protected String provider;
    protected Map<String, ?> providerMetadata;
    protected String title;
    protected String authors;
    protected String description;
    protected String applicationUrl;
    protected String fileUrl;
    protected List<String> licence;
    protected String userId;
    protected List<String> authorisedRoles;
    protected Map<String, ?> applicationMetadata;
    protected String customLandingPageUrl;
    protected Boolean active;

    protected String displayTemplate;

    public String getDisplayTemplate() {
        return displayTemplate;
    }

    public void setDisplayTemplate(String displayTemplate) {
        this.displayTemplate = displayTemplate;
    }

    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public Map<String, ?> getProviderMetadata() {
        return providerMetadata;
    }

    public void setProviderMetadata(Map<String, ?> providerMetadata) {
        this.providerMetadata = providerMetadata;
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

    public String getApplicationUrl() {
        return applicationUrl;
    }

    public void setApplicationUrl(String applicationUrl) {
        this.applicationUrl = applicationUrl;
    }

    public String getFileUrl() {
        return fileUrl;
    }

    public void setFileUrl(String fileUrl) {
        this.fileUrl = fileUrl;
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

    public List<String> getAuthorisedRoles() {
        return authorisedRoles;
    }

    public void setAuthorisedRoles(List<String> authorisedRoles) {
        this.authorisedRoles = authorisedRoles;
    }

}
