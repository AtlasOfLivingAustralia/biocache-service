package au.org.ala.doi;

public class CreateDoiResponse {
    String uuid;
    String doi;
    String error;
    String doiServiceLandingPage;
    String landingPage;
    String status;

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

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getDoiServiceLandingPage() {
        return doiServiceLandingPage;
    }

    public void setDoiServiceLandingPage(String doiServiceLandingPage) {
        this.doiServiceLandingPage = doiServiceLandingPage;
    }

    public String getLandingPage() {
        return landingPage;
    }

    public void setLandingPage(String landingPage) {
        this.landingPage = landingPage;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

}
