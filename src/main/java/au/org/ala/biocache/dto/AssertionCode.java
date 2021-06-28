package au.org.ala.biocache.dto;

import au.org.ala.biocache.util.AssertionCodeSerializer;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;

@JsonSerialize(using = AssertionCodeSerializer.class)
public class AssertionCode extends ErrorCode {

    boolean deprecated = false;
    String newName;

    public AssertionCode() {
        super();
    }

    public AssertionCode(String name, Integer code, Boolean isFatal, String description, Category category, List<String> termsRequiredToTest) {
        super(name, code, isFatal, description, category, termsRequiredToTest);
    }

    public boolean isDeprecated() {
        return deprecated;
    }

    public void setDeprecated(boolean deprecated) {
        this.deprecated = deprecated;
    }

    public String getNewName() {
        return newName;
    }

    public void setNewName(String newName) {
        this.newName = newName;
    }
}
