package au.org.ala.doi;

import java.util.Map;

public class CreateDoiRequest {
    String provider;
    Map<String, ?> providerMetadata;
    String title;
    String authors;
    String description;
    String applicationUrl;
    String fileUrl;
    Map<String, ?> applicationMetadata;
    String customLandingPageUrl;

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
}
