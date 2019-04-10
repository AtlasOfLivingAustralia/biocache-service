package au.org.ala.biocache.dto;

public class ImageParams {
    String format = "jpg";
    Double widthMm = 60.0;
    Double pointRadiusMm = 2.0;
    Integer pradiusPx;
    String pointColour = "FF0000";
    Double pointOpacity = 0.8;
    String baseLayer = "world";
    String scale = "off";
    Integer dpi = 300;
    String baselayerStyle = "";
    String fileName;
    String baseMap = "ALA";
    String extents;

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public Double getWidthMm() {
        return widthMm;
    }

    public void setWidthMm(Double widthMm) {
        this.widthMm = widthMm;
    }

    public Double getPointRadiusMm() {
        return pointRadiusMm;
    }

    public void setPointRadiusMm(Double pointRadiusMm) {
        this.pointRadiusMm = pointRadiusMm;
    }

    public Integer getPradiusPx() {
        return pradiusPx;
    }

    public void setPradiusPx(Integer pradiusPx) {
        this.pradiusPx = pradiusPx;
    }

    public String getPointColour() {
        return pointColour;
    }

    public void setPointColour(String pointColour) {
        this.pointColour = pointColour;
    }

    public Double getPointOpacity() {
        return pointOpacity;
    }

    public void setPointOpacity(Double pointOpacity) {
        this.pointOpacity = pointOpacity;
    }

    public String getBaseLayer() {
        return baseLayer;
    }

    public void setBaseLayer(String baseLayer) {
        this.baseLayer = baseLayer;
    }

    public String getScale() {
        return scale;
    }

    public void setScale(String scale) {
        this.scale = scale;
    }

    public Integer getDpi() {
        return dpi;
    }

    public void setDpi(Integer dpi) {
        this.dpi = dpi;
    }

    public String getBaselayerStyle() {
        return baselayerStyle;
    }

    public void setBaselayerStyle(String baselayerStyle) {
        this.baselayerStyle = baselayerStyle;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getBaseMap() {
        return baseMap;
    }

    public void setBaseMap(String baseMap) {
        this.baseMap = baseMap;
    }

    public String getExtents() {
        return extents;
    }

    public void setExtents(String extents) {
        this.extents = extents;
    }
}

