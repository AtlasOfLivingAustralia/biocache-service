package au.org.ala.biocache.web;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * Created by mar759 on 23/09/2016.
 */
public class WMSEnv {

    private final static Logger logger = Logger.getLogger(WMSEnv.class);
    public int red, green, blue, alpha, size, colour;
    public boolean uncertainty, gridlabels;
    public String colourMode, highlight, gridres;

    /**
     * Get WMS ENV values from String, or use defaults.
     *
     * @param env
     */
    public WMSEnv(String env, String styles) {
        try {
            env = URLDecoder.decode(env, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage(), e);
        }

        red = green = blue = alpha = 0;
        size = 4;
        uncertainty = false;
        highlight = null;
        colourMode = "-1";
        colour = 0x00000000; //rgba

        if (StringUtils.trimToNull(env) == null && StringUtils.trimToNull(styles) == null) {
            env = "color:cd3844;size:10;opacity:1.0";
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
                }  else if (pair[0].equals("gridlabels")) {
                    gridlabels = BooleanUtils.toBoolean(pair[1]);
                }
            }
        } else if (StringUtils.trimToNull(styles) != null) {
            //named styles
            //blue;opacity=1;size=1
            String firstStyle = styles.split(",")[0];
            String[] styleParts = firstStyle.split(";");

            red = Integer.parseInt(styleParts[0].substring(0, 2), 16);
            green = Integer.parseInt(styleParts[0].substring(2, 4), 16);
            blue = Integer.parseInt(styleParts[0].substring(4), 16);
            alpha = (int) (255 * Double.parseDouble(styleParts[1].substring(8)));
            size = Integer.parseInt(styleParts[2].substring(5));
        }

        colour = (red << 16) | (green << 8) | blue;
        colour = colour | (alpha << 24);
    }
}
