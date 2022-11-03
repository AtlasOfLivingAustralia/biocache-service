package au.org.ala.biocache.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Objects;

@Schema(name = "QualityFilter", description = "Data Quality Filter")
public class QualityFilterDTO {

    private String name;
    private String filter;

    public QualityFilterDTO(String name, String filter) {
        this.name = name;
        this.filter = filter;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QualityFilterDTO that = (QualityFilterDTO) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, filter);
    }
}
