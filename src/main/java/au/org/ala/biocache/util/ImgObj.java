package au.org.ala.biocache.util;

import java.awt.*;
import java.awt.image.BufferedImage;

public class ImgObj {

    final public Graphics2D g;
    final public BufferedImage img;

    public static ImgObj create(int width, int height) {
        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = (Graphics2D) img.getGraphics();
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        return new ImgObj(g, img);
    }

    public ImgObj(Graphics2D g, BufferedImage img) {
        this.g = g;
        this.img = img;
    }
}
