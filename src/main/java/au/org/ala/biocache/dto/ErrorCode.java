package au.org.ala.biocache.dto;

import java.util.Collections;
import java.util.List;

/**
 * Case class that represents an error code for a occurrence record.
 * <p>
 * Merged from biocache-store.
 */
public class ErrorCode {
    String name;
    Integer code;
    Boolean isFatal;
    String description;
    String category = Category.Error.toString();
    List<String> termsRequiredToTest;

    public ErrorCode(String name, Integer code, Boolean isFatal, String description, Category category, List<String> termsRequiredToTest) {
        this.name = name;
        this.code = code;
        this.isFatal = isFatal;
        this.description = description;
        this.category = category.toString();
        this.termsRequiredToTest = termsRequiredToTest;
    }

    public ErrorCode(String name, Integer code, Boolean isFatal, String description, Category category) {
        this.name = name;
        this.code = code;
        this.isFatal = isFatal;
        this.description = description;
        this.category = category.toString();
        this.termsRequiredToTest = Collections.emptyList();
    }

    // Compatible with biocache-store
    public enum Category {
        Error, Missing, Warning, Verified, Comment, Geospatial, Taxonomic
    }

    public ErrorCode() {}

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

    public Boolean getFatal() {
        return isFatal;
    }

    public void setFatal(Boolean fatal) {
        isFatal = fatal;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public List<String> getTermsRequiredToTest() {
        return termsRequiredToTest;
    }

    public void setTermsRequiredToTest(List<String> termsRequiredToTest) {
        this.termsRequiredToTest = termsRequiredToTest;
    }
}
