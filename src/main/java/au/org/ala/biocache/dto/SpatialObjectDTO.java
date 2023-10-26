package au.org.ala.biocache.dto;

public class SpatialObjectDTO {
    Double area_km;
    String bbox;
    String name;
    String description;
    String fieldname;
    String pid;
    String id;
    String fid;
    String wmsurl;

    public Double getArea_km() {
        return area_km;
    }

    public void setArea_km(Double area_km) {
        this.area_km = area_km;
    }

    public String getBbox() {
        return bbox;
    }

    public void setBbox(String bbox) {
        this.bbox = bbox;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getFieldname() {
        return fieldname;
    }

    public void setFieldname(String fieldname) {
        this.fieldname = fieldname;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getFid() {
        return fid;
    }

    public void setFid(String fid) {
        this.fid = fid;
    }

    public String getWmsurl() {
        return wmsurl;
    }

    public void setWmsurl(String wmsurl) {
        this.wmsurl = wmsurl;
    }
}
