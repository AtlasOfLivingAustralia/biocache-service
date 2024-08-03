package au.org.ala.biocache.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Map;

/**
 * User Property bean. Represents a user property.
 */
@Schema(name = "UserProperty", description = "Represents a single user property")
public class UserProperty {

    private String alaId;
    private Map<String, String> properties;

    public UserProperty() {
    }

    public UserProperty(String alaId, Map<String, String> properties) {
        this.alaId = alaId;
        this.properties = properties;
    }

    public String getAlaId() {
        return alaId;
    }

    public void setAlaId(String alaId) {
        this.alaId = alaId;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
