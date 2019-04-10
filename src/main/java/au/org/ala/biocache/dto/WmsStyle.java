package au.org.ala.biocache.dto;


import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;


/**
 * WMS style.
 * <p>
 * Set values directly.
 * <p>
 * env and style are parsed and used to set other values.
 */
public class WmsStyle {
    private final static Logger logger = Logger.getLogger(WmsStyle.class);

    // values that can be parsed from env and style
    Integer red = 0;
    Integer green = 0;
    Integer blue = 0;
    Integer alpha = 255;
    Integer size = 10;
    Integer colour = 0x00cd3844;
    Boolean uncertainty = false;
    Boolean gridlabels = false;
    String colourMode = "-1";
    String highlight = null;
    String gridres = null;

    // env is parsed into other fields
    String env = null;

    // style and style have the same value and are parsed into other fields
    String styles = null;
    String style = null;

    // additional styling that must be set directly (cannot be parsed from env or style)
    Boolean outline = true;
    String outlineColour = "0xFF000000";
    Integer gridDetail = 16;

    /**
     * Set style defaults to be consistent before the addition of this class.
     */
    void setStyleDefaults() {
        colour = 0x00000000;
        size = 4;
        alpha = 255;
    }

    public void setEnv(String env) {
        setStyleDefaults();

        try {
            env = URLDecoder.decode(env, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage(), e);
        }

        if (StringUtils.trimToNull(env) != null) {
            for (String s : env.split(";")) {
                String[] pair = s.split(":");
                pair[1] = s.substring(s.indexOf(":") + 1);
                if (pair[0].equals("color")) {
                    while (pair[1].length() < 6) {
                        pair[1] = "0" + pair[1];
                    }
                    red = Integer.parseInt(pair[1].substring(0, 2), 16);
                    green = Integer.parseInt(pair[1].substring(2, 4), 16);
                    blue = Integer.parseInt(pair[1].substring(4), 16);

                    setRGB();
                } else if (pair[0].equals("size")) {
                    size = Integer.parseInt(pair[1]);
                } else if (pair[0].equals("opacity")) {
                    alpha = (int) (255 * Double.parseDouble(pair[1]));
                } else if (pair[0].equals("uncertainty")) {
                    uncertainty = true;
                } else if (pair[0].equals("sel")) {
                    highlight = s.replace("sel:", "").replace("%3B", ";");
                } else if (pair[0].equals("colormode")) {
                    colourMode = pair[1];
                } else if (pair[0].equals("gridres")) {
                    gridres = pair[1];
                } else if (pair[0].equals("gridlabels")) {
                    gridlabels = BooleanUtils.toBoolean(pair[1]);
                }
            }
        }
    }

    public void setStyle(String style) {
        this.style = style;
        this.styles = style;

        setStyles(style);
    }

    public void setStyles(String styles) {
        this.style = styles;
        this.styles = styles;

        setStyleDefaults();
        if (StringUtils.trimToNull(styles) != null) {
            //named styles
            //blue;opacity=1;size=1
            String firstStyle = styles.split(",")[0];
            String[] styleParts = firstStyle.split(";");

            red = Integer.parseInt(styleParts[0].substring(0, 2), 16);
            green = Integer.parseInt(styleParts[0].substring(2, 4), 16);
            blue = Integer.parseInt(styleParts[0].substring(4), 16);
            alpha = (int) (255 * Double.parseDouble(styleParts[1].substring(8)));
            size = Integer.parseInt(styleParts[2].substring(5));

            setRGB();
        }
    }

    void setRGB() {
        colour = (red << 16) | (green << 8) | blue;
        colour = colour | (alpha << 24);
    }

    public Integer getRed() {
        return red;
    }

    public void setRed(Integer red) {
        this.red = red;
    }

    public Integer getGreen() {
        return green;
    }

    public void setGreen(Integer green) {
        this.green = green;
    }

    public Integer getBlue() {
        return blue;
    }

    public void setBlue(Integer blue) {
        this.blue = blue;
    }

    public Integer getAlpha() {
        return alpha;
    }

    public void setAlpha(Integer alpha) {
        this.alpha = alpha;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public Integer getColour() {
        return colour;
    }

    public void setColour(Integer colour) {
        this.colour = colour;
    }

    public Boolean getUncertainty() {
        return uncertainty;
    }

    public void setUncertainty(Boolean uncertainty) {
        this.uncertainty = uncertainty;
    }

    public Boolean getGridlabels() {
        return gridlabels;
    }

    public void setGridlabels(Boolean gridlabels) {
        this.gridlabels = gridlabels;
    }

    public String getColourMode() {
        return colourMode;
    }

    public void setColourMode(String colourMode) {
        this.colourMode = colourMode;
    }

    public String getHighlight() {
        return highlight;
    }

    public void setHighlight(String highlight) {
        this.highlight = highlight;
    }

    public String getGridres() {
        return gridres;
    }

    public void setGridres(String gridres) {
        this.gridres = gridres;
    }

    public String getEnv() {
        return env;
    }

    public String getStyles() {
        return styles;
    }

    public String getStyle() {
        return style;
    }

    public Boolean getOutline() {
        return outline;
    }

    public void setOutline(Boolean outline) {
        this.outline = outline;
    }

    public String getOutlineColour() {
        return outlineColour;
    }

    public void setOutlineColour(String outlineColour) {
        this.outlineColour = outlineColour;
    }

    public Integer getGridDetail() {
        return gridDetail;
    }

    public void setGridDetail(Integer gridDetail) {
        this.gridDetail = gridDetail;
    }
}
