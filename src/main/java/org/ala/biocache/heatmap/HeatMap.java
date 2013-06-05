package org.ala.biocache.heatmap;

import java.awt.*;
import java.awt.geom.Ellipse2D;
import java.awt.image.BufferedImage;
import java.awt.image.ByteLookupTable;
import java.awt.image.FilteredImageSource;
import java.awt.image.ImageFilter;
import java.awt.image.ImageProducer;
import java.awt.image.LookupOp;
import java.awt.image.LookupTable;
import java.awt.image.RGBImageFilter;
import java.awt.image.Raster;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;
import javax.imageio.ImageIO;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

/**
 * HeatMap generator for species.
 * 
 * Code based on http://www.itstud.chalmers.se/~liesen/heatmap/
 *
 * @author ajay
 */
public class HeatMap {

    private final static Logger logger = Logger.getLogger(HeatMap.class);
    private BufferedImage backgroundImage;
    private BufferedImage legendImage;
    private int radius = 8;
    private int numColours = 10;
    private BufferedImage dotImage; /// createFadedCircleImage(radius);
    private BufferedImage monochromeImage;
    private BufferedImage heatmapImage;
    private BufferedImage colorImage;
    private LookupTable colorTable;
    private LookupOp colorOp;
    private double minX = 0;
    private double minY = 0;
    private double maxX = 0;
    private double maxY = 0;
    private double bMinX = 110.911; //112.911; //112.911;
    private double bMinY = -44.778; //-50.778; //-54.778;
    private double bMaxX = 156.113; //158.113; //159.113;
    private double bMaxY = -9.221; //-9.221; //-9.221;
    private File baseDir;
    private String baseFile;

    public HeatMap() {
        initImages();
    }

    public HeatMap(File baseDir, String baseFile) {
        this.baseDir = baseDir;
        this.baseFile = baseFile;

        initImages();
    }

    private void initImages() {
        try {
            backgroundImage = ImageIO.read(new File(baseDir.getAbsolutePath() + "/base/mapaus1_white.png"));
            legendImage = ImageIO.read(new File(baseDir.getAbsolutePath() + "/base/heatmap_key.png"));
            dotImage = getDotImageFile();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        int width = backgroundImage.getWidth();
        int height = backgroundImage.getHeight();

        colorImage = createEvenlyDistributedGradientImage(new Dimension(
                512, 20), new Color(255, 0, 0), new Color(255, 30, 0),
                new Color(255, 60, 0), new Color(255, 90, 0),
                new Color(255, 120, 0), new Color(255, 150, 0),
                new Color(255, 180, 0), new Color(255, 210, 0),
                new Color(255, 230, 0), new Color(255, 255, 0),
                Color.WHITE);

        colorTable = createColorLookupTable(colorImage, .5f);
        colorOp = new LookupOp(colorTable, null);

        monochromeImage = createCompatibleTranslucentImage(width, height);

        Graphics g = monochromeImage.getGraphics();
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, width, height);
    }

    /**
     * Creates the color lookup table from an image
     *
     * @param im
     * @return
     */
    public static LookupTable createColorLookupTable(BufferedImage im,
            float alpha) {
        int tableSize = 256;
        Raster imageRaster = im.getData();
        double sampleStep = 1D * im.getWidth() / tableSize; // Sample pixels
        // evenly
        byte[][] colorTable = new byte[4][tableSize];
        int[] pixel = new int[1]; // Sample pixel
        Color c;

        for (int i = 0; i < tableSize; ++i) {
            imageRaster.getDataElements((int) (i * sampleStep), 0, pixel);

            c = new Color(pixel[0]);

            colorTable[0][i] = (byte) c.getRed();
            colorTable[1][i] = (byte) c.getGreen();
            colorTable[2][i] = (byte) c.getBlue();
            colorTable[3][i] = (byte) (alpha * 0xff);
        }

        LookupTable lookupTable = new ByteLookupTable(0, colorTable);

        return lookupTable;
    }

    public static BufferedImage createEvenlyDistributedGradientImage(
            Dimension size, Color... colors) {
        BufferedImage im = createCompatibleTranslucentImage(
                size.width, size.height);
        Graphics2D g = im.createGraphics();

        float[] fractions = new float[colors.length];
        float step = 1f / colors.length;

        for (int i = 0; i < colors.length; ++i) {
            fractions[i] = i * step;
        }

        LinearGradientPaint gradient = new LinearGradientPaint(
                0, 0, size.width, 1, fractions, colors,
                MultipleGradientPaint.CycleMethod.REPEAT);

        g.setPaint(gradient);
        g.fillRect(0, 0, size.width, size.height);

        g.dispose();

        return im;
    }

    public static BufferedImage createCompatibleTranslucentImage(int width,
            int height) {
        /*
        GraphicsEnvironment ge = GraphicsEnvironment.getLocalGraphicsEnvironment();
        GraphicsDevice gd = ge.getDefaultScreenDevice();
        GraphicsConfiguration gc = gd.getDefaultConfiguration();
        
        return gc.createCompatibleImage(
        width, height, Transparency.TRANSLUCENT);
         *
         */

        BufferedImage image = new BufferedImage(width, height,
                BufferedImage.TYPE_INT_ARGB);
        int[] image_bytes;

        image_bytes = image.getRGB(0, 0, image.getWidth(), image.getHeight(),
                null, 0, image.getWidth());

        /* try transparency as missing value */
        for (int i = 0; i < image_bytes.length; i++) {
            image_bytes[i] = 0x00000000;
        }

        /* write bytes to image */
        image.setRGB(0, 0, image.getWidth(), image.getHeight(),
                image_bytes, 0, image.getWidth());

        return image;

    }

    public BufferedImage doColorize() {

        //System.out.println("doColorize()");

        int[] image_bytes, image_bytes2;

        image_bytes = colorImage.getRGB(0, 0, colorImage.getWidth(), colorImage.getHeight(),
                null, 0, colorImage.getWidth());
        image_bytes2 = monochromeImage.getRGB(0, 0, monochromeImage.getWidth(), monochromeImage.getHeight(),
                null, 0, monochromeImage.getWidth());

        //System.out.println("imagebytes.1: " + image_bytes.length);
        //System.out.println("imagebytes.2: " + image_bytes2.length);

        for (int i = 0; i < image_bytes2.length; i++) {
            //System.out.print(image_bytes2[i] + " --> ");
            int pos = image_bytes2[i] & 0x000000ff;
            //System.out.print("at " + pos * 2 + " ==> ");
            image_bytes2[i] = image_bytes[pos * 2] & 0x99ffffff;
            //System.out.println(image_bytes2[i]);
        }

        /* write bytes to image */
        BufferedImage biColorized = new BufferedImage(monochromeImage.getWidth(), monochromeImage.getHeight(), BufferedImage.TYPE_INT_ARGB);
        biColorized.setRGB(0, 0, biColorized.getWidth(), biColorized.getHeight(),
                image_bytes2, 0, biColorized.getWidth());

        return biColorized;
    }

    public static Image makeColorTransparent(BufferedImage im, final Color color) {
        ImageFilter filter = new RGBImageFilter() {

            // the color we are looking for... Alpha bits are set to opaque
            public int markerRGB = color.getRGB() | 0xFF000000;

            public final int filterRGB(int x, int y, int rgb) {
                if ((rgb | 0xFF000000) == markerRGB) {
                    // Mark the alpha bits as zero - transparent
                    return 0x00FFFFFF & rgb;
                    //return (rgb) | (rgb << 8) | (rgb << 16) | 0xff000000;
                } else {
                    // nothing to do
                    return rgb;
                }
            }
        };

        ImageProducer ip = new FilteredImageSource(im.getSource(), filter);
        return Toolkit.getDefaultToolkit().createImage(ip);
    }

    public static BufferedImage createFadedCircleImage(int size) {
        BufferedImage im = createCompatibleTranslucentImage(size, size);
        float radius = size / 2f;

        RadialGradientPaint gradient = new RadialGradientPaint(
                radius, radius, radius, new float[]{0f, 1f}, new Color[]{
                    Color.BLACK, new Color(0xffffffff, true)});

        Graphics2D g = (Graphics2D) im.getGraphics();

        g.setPaint(gradient);
        g.fillRect(0, 0, size, size);

        g.dispose();

        return im;
    }

    private void addDotImage(Point p, Color pointColor) {
        //int circleRadius = dotImage.getWidth() / 2;
        Graphics2D g = (Graphics2D) monochromeImage.getGraphics();
        float radius = 10f;

        Shape circle = new Ellipse2D.Float(p.x - (radius / 2), p.y - (radius / 2), radius, radius);
        //g.drawImage(dotImage, null, p.x - circleRadius, p.y - circleRadius);
        g.draw(circle);
        g.setPaint(pointColor);
        g.fill(circle);
    }    

    private BufferedImage getDotImageFile() {
        try {
            if (baseDir != null) {
                return ImageIO.read(new File(baseDir.getAbsolutePath() + "/base/bullet_blue.png"));
            }
        } catch (Exception ex) {
            logger.error("Unable to load Dot Image File: " + baseDir.getAbsolutePath() + "/base/bullet_blue.png", ex);
        }
        return createFadedCircleImage(radius);
    }

    private void setMinMax() {
        minX = bMinX;
        minY = bMinY;
        maxX = bMaxX;
        maxY = bMaxY;
    }

    private Point translate(double x, double y) {
        try {
            //System.out.println("translating: " + x + ", " + y);
            // normalize points into range (0 - 1)...
            x = (x - minX) / (maxX - minX);
            y = (y - minY) / (maxY - minY);

            //System.out.println("normalised: " + x + ", " + y);
            // ...and the map into our image size...
            x = (x * backgroundImage.getWidth());
            y = ((1 - y) * backgroundImage.getHeight());

            //System.out.println("pixeled: " + x + ", " + y);
            return new Point(new Double(x).intValue(), new Double(y).intValue());
        } catch (Exception e) {
            logger.error("Exception with translating " + e.getMessage(), e);
        }
        return null;
    }

    private void generateLogScaleCircle(int dPoints[][]) {
        try {

            int maxValue = 0;
            int width = monochromeImage.getWidth();
            int height = monochromeImage.getHeight();

            for (int mi = 0; mi < width; mi++) {
                for (int mj = 0; mj < height; mj++) {
                    if (maxValue < dPoints[mi][mj]) {
                        maxValue = dPoints[mi][mj];
                    }
                }
            }

            //System.out.println("maxW: " + maxValue);

            // we check if the maxValue = 0
            // 0 tells us that there are no records in the 
            // current "bounding box"            
            if (maxValue > 0) {
                // we are doing "1" here to make sure nothing is 0
                int roundFactor = 1;

                for (int mi = 0; mi < width; mi++) {
                    for (int mj = 0; mj < height; mj++) {
                        int rgba = (int) (255 - Math.log(dPoints[mi][mj]) * 255 / Math.log((double) maxValue));
                        if (rgba < 255 && rgba > 255 - (255 / numColours) - roundFactor) {
                            rgba = 255 - (255 / numColours) - roundFactor;
                        }
                        //rgba <<= 8;
                        //
                        //rgba = (((short)rgba) & 0x000000ff) << 24;
                        rgba = (rgba) | (rgba << 8) | (rgba << 16) | 0xff000000;
                        //System.out.println("rgba: " + Integer.toHexString(rgba));

                        monochromeImage.setRGB(mi, mj, rgba);
                    }
                }

                generateLegend(maxValue);
            }
        } catch (Exception e) {
            logger.error("Error generating log scale circle: " + e.getMessage(), e);
        }
    }

    public void generateClasses(double[] v) {
        setMinMax();

        int width = backgroundImage.getWidth();
        int height = backgroundImage.getHeight();
        //System.out.println("bounding box: " + minX + "," + minY + "," + maxX + "," + maxY);
        //System.out.println("Adding " + (v.length / 2) + " points to base image...");
        //System.out.println("Adding to base image...");
        int dPoints[][] = new int[width][height];
        for (int i = 0; i < v.length; i += 2) {
            double cx = v[i];
            double cy = v[i + 1];

            Point p = translate(cx, cy);

            int pradius = radius * radius;

            for (int ci = (int) (p.x - radius); ci <= (p.x + radius); ci++) {
                for (int cj = (int) (p.y - radius); cj <= (p.y + radius); cj++) {
                    if (ci >= 0 && ci < width && cj >= 0 && cj < height) {
                        double d = Math.pow((p.x - ci), 2) + Math.pow((p.y - cj), 2);
                        if ((int) d <= pradius) {
                            // applying gradient to this circle so outter influence is low
                            // and at the peak it's maximum
                            dPoints[ci][cj] += numColours - ((d * numColours) / pradius);
                        }
                    }
                }
            }

            //addDotImage(p, .75f);
        }

        generateLogScaleCircle(dPoints);
    }

    public void generatePoints(double[] points, Color pointColour) {
        setMinMax();
        for (int i = 0; i < points.length; i += 2) {
            double cx = points[i];
            double cy = points[i + 1];
            Point p = translate(cx, cy);
            addDotImage(p, pointColour);
        }
    }

    private void generateLegend(int maxValue) {

        int scale[] = new int[numColours - 1];
        scale[0] = maxValue;
        for (int i = 1; i < scale.length - 1; i++) {
            scale[i] = (int) Math.pow(Math.E, ((numColours - i) * (Math.log((double) maxValue) / numColours)));
        }
        scale[scale.length - 1] = 0;

        Graphics2D cg = (Graphics2D) legendImage.getGraphics();
        cg.setColor(Color.BLACK);
        //1.2em/1.6em Arial, Helvetica, sans-serif
        Font font = new Font("SanSerif", Font.PLAIN, 11);
        cg.setFont(font);

        RenderingHints rh = new RenderingHints(
                RenderingHints.KEY_TEXT_ANTIALIASING,
                RenderingHints.VALUE_TEXT_ANTIALIAS_GASP
                );
        cg.setRenderingHints(rh);

        int padding = 10; // 10px padding around the image
        int keyHeight = 30; // 30px key height
        int keyWidth = 25; // 30px key width

        int scaleLength = scale.length;
        String value = (scale[scaleLength - 1] + 1) + "-" + (scale[scaleLength - 3]);
        int left = padding * 2 + keyWidth; // padding + width/2;
        int top = padding + (keyHeight / 2);
        cg.drawString(value, left, top);

        value = (scale[scaleLength - 3] + 1) + "-" + (scale[scaleLength - 5]);
        top = padding + (keyHeight / 2) + keyHeight;
        cg.drawString(value, left, top);

        value = (scale[scaleLength - 5] + 1) + "-" + (scale[scaleLength - 6]);
        top = padding + (keyHeight / 2) + (keyHeight * 2);
        cg.drawString(value, left, top);

        value = (scale[scaleLength - 6] + 1) + "-" + (scale[scaleLength - 7]);
        top = padding + (keyHeight / 2) + (keyHeight * 3);
        cg.drawString(value, left, top);

        value = (scale[scaleLength - 7] + 1) + "-" + (scale[scaleLength - 8]);
        top = padding + (keyHeight / 2) + (keyHeight * 4);
        cg.drawString(value, left, top);

        value = (scale[scaleLength - 8] + 1) + "+";
        top = padding + (keyHeight / 2) + (keyHeight * 5);
        cg.drawString(value, left, top);
    }

    public void drawLegend(String outfile) {
        File legOut = new File(outfile);
        try {
            ImageIO.write(legendImage, "png", legOut);
        } catch (Exception e) {
            logger.error("Unable to write legendImage: " + e.getMessage(), e);
        }
    }

    public void drawOutput(String outfile, boolean colorize) {
        try {
            if (colorize) {
                heatmapImage = doColorize();
            } else {
                heatmapImage = monochromeImage;
            }

            Graphics2D g = (Graphics2D) backgroundImage.getGraphics();
            g.drawImage(makeColorTransparent(heatmapImage, Color.WHITE), 0, 0, null);

            File hmOut = new File(outfile);
            ImageIO.write(backgroundImage, "png", hmOut);

        } catch (Exception ex) {
            logger.error("An error occurred drawing output to outfile: '"  + outfile
                    + "' Error message: " + ex.getMessage(), ex);
        }
    }
}
