package au.org.ala.biocache.dto;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(name = "FieldComparison", description = "Bean capturing a property name and its raw and processed values")
public class Comparison {

    private String name;
    private String raw;
    private String processed;

    public Comparison() {}

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRaw() {
        return raw;
    }

    public void setRaw(String raw) {
        this.raw = raw;
    }

    public String getProcessed() {
        return processed;
    }

    public void setProcessed(String processed) {
        this.processed = processed;
    }
}
