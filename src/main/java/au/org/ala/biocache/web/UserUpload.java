package au.org.ala.biocache.web;

public class UserUpload {
    String name = "my test dataset";
    String api_key = "";  //API key taken from /apikey
    String user= "Sandbox upload services";
    String email = "";
    String firstName = "";
    String lastName = "";
    Integer numberOfRecords = 0;
    String alaId = "";
    String webserviceUrl = "";
    String uiUrl = "";

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public Integer getNumberOfRecords() {
        return numberOfRecords;
    }

    public void setNumberOfRecords(Integer numberOfRecords) {
        this.numberOfRecords = numberOfRecords;
    }

    public String getApi_key() {
        return api_key;
    }

    public void setApi_key(String api_key) {
        this.api_key = api_key;
    }

    public String getAlaId() {
        return alaId;
    }

    public void setAlaId(String alaId) {
        this.alaId = alaId;
    }

    public String getWebserviceUrl() {
        return webserviceUrl;
    }

    public void setWebserviceUrl(String webserviceUrl) {
        this.webserviceUrl = webserviceUrl;
    }

    public String getUiUrl() {
        return uiUrl;
    }

    public void setUiUrl(String uiUrl) {
        this.uiUrl = uiUrl;
    }
}
