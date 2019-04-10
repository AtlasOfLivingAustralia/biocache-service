package au.org.ala.biocache.dto;

public class WmsDimensions {
    Integer width;
    Integer height;

    // services have different defaults
    Integer defaultWidth = 30;
    Integer defaultHeight = 20;

    public WmsDimensions() {
    }

    public Integer getWidth() {
        if (width == null) {
            return defaultWidth;
        } else {
            return width;
        }
    }

    public void setWidth(Integer width) {
        this.width = width;
    }

    public Integer getHeight() {
        if (height == null) {
            return defaultHeight;
        } else {
            return height;
        }
    }

    public void setHeight(Integer height) {
        this.height = height;
    }

    public void setDefaults(Integer width, Integer height) {
        this.defaultWidth = width;
        this.defaultHeight = height;
    }
}
