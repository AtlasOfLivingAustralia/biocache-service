/**************************************************************************
 *  Copyright (C) 2013 Atlas of Living Australia
 *  All Rights Reserved.
 *
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.biocache.heatmap;

import org.apache.log4j.Logger;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.geom.Ellipse2D;
import java.awt.image.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import static java.awt.image.BufferedImage.TYPE_INT_ARGB;

/**
 * HeatMap generator for species.
 *
 * Code based on http://www.itstud.chalmers.se/~liesen/heatmap/
 *
 * @author ajay
 */
public class HeatMap {

    private final static Logger logger = Logger.getLogger(HeatMap.class);

    private int radius = 8;
    private int numColours = 10;

    private BufferedImage backgroundImage;
    private BufferedImage legendImage;
    private BufferedImage monochromeImage;
    private BufferedImage heatmapImage;
    private BufferedImage colorImage;

    private LookupTable colorTable;
    private LookupOp colorOp;

    /* bounding box coordinates for the image in decimal degrees. Default to BBOX for Australia */
    private double minX = 110.911;
    private double minY = -44.778;
    private double maxX = 156.113;
    private double maxY = -9.221;

    public HeatMap() throws Exception {
        InputStream bkImageInput = HeatMap.class.getResourceAsStream("/images/heatmap_background.png");
        InputStream legendImageInput = HeatMap.class.getResourceAsStream( "/images/heatmap_legend.png");

        backgroundImage = ImageIO.read(bkImageInput);
        legendImage = ImageIO.read(legendImageInput);

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
    private static LookupTable createColorLookupTable(BufferedImage im,
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

        return new ByteLookupTable(0, colorTable);
    }

    private static BufferedImage createEvenlyDistributedGradientImage(
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

    private static BufferedImage createCompatibleTranslucentImage(int width,
            int height) {

        BufferedImage image = new BufferedImage(width, height, TYPE_INT_ARGB);
        int[] image_bytes;

        image_bytes = image.getRGB(0, 0, image.getWidth(), image.getHeight(), null, 0, image.getWidth());

        /* try transparency as missing value */
        for (int i = 0; i < image_bytes.length; i++) {
            image_bytes[i] = 0x00000000;
        }

        /* write bytes to image */
        image.setRGB(0, 0, image.getWidth(), image.getHeight(),
                image_bytes, 0, image.getWidth());

        return image;

    }

    /**
     * Merges the monochrome and color image.
     *
     * @return
     */
    private BufferedImage doColorize() {

        int[] image_bytes = colorImage.getRGB(0, 0, colorImage.getWidth(), colorImage.getHeight(), null, 0, colorImage.getWidth());
        int[] image_bytes2 = monochromeImage.getRGB(0, 0, monochromeImage.getWidth(), monochromeImage.getHeight(), null, 0, monochromeImage.getWidth());

        for (int i = 0; i < image_bytes2.length; i++) {
            int pos = image_bytes2[i] & 0x000000ff;
            image_bytes2[i] = image_bytes[pos * 2] & 0x99ffffff;
        }

        /* write bytes to image */
        BufferedImage biColorized = new BufferedImage(monochromeImage.getWidth(), monochromeImage.getHeight(), TYPE_INT_ARGB);
        biColorized.setRGB(0, 0, biColorized.getWidth(), biColorized.getHeight(), image_bytes2, 0, biColorized.getWidth());

        return biColorized;
    }

    private static Image makeColorTransparent(BufferedImage im, final Color color) {
        ImageFilter filter = new RGBImageFilter() {

            // the color we are looking for... Alpha bits are set to opaque
            public int markerRGB = color.getRGB() | 0xFF000000;

            public final int filterRGB(int x, int y, int rgb) {
                if ((rgb | 0xFF000000) == markerRGB) {
                    // Mark the alpha bits as zero - transparent
                    return 0x00FFFFFF & rgb;
                } else {
                    // nothing to do
                    return rgb;
                }
            }
        };

        ImageProducer ip = new FilteredImageSource(im.getSource(), filter);
        return Toolkit.getDefaultToolkit().createImage(ip);
    }

    private void addDotImage(Point p, Color pointColor) {
        Graphics2D g = (Graphics2D) monochromeImage.getGraphics();
        float radius = 10f;

        Shape circle = new Ellipse2D.Float(p.x - (radius / 2), p.y - (radius / 2), radius, radius);
        g.draw(circle);
        g.setPaint(pointColor);
        g.fill(circle);
    }

    /**
     * Convert lat/long to coordinate on the generated image.
     *
     * @param x
     * @param y
     * @return
     */
    private Point translate(double x, double y) {
        try {
            // normalize points into range (0 - 1)...
            x = (x - minX) / (maxX - minX);
            y = (y - minY) / (maxY - minY);

            // ...and the map into our image size...
            x = (x * backgroundImage.getWidth());
            y = ((1 - y) * backgroundImage.getHeight());

            return new Point(Double.valueOf(x).intValue(), Double.valueOf(y).intValue());
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

    /**
     * Generate classes
     *
     * @param points
     */
    public void generateClasses(double[] points) {

        int width = backgroundImage.getWidth();
        int height = backgroundImage.getHeight();

        int dPoints[][] = new int[width][height];
        for (int i = 0; i < points.length; i += 2) {
            double cx = points[i];
            double cy = points[i + 1];

            Point p = translate(cx, cy);

            int pradius = radius * radius;

            for (int ci = (int) (p.x - radius); ci <= (p.x + radius); ci++) {
                for (int cj = (int) (p.y - radius); cj <= (p.y + radius); cj++) {
                    if (ci >= 0 && ci < width && cj >= 0 && cj < height) {
                        double d = Math.pow((p.x - ci), 2) + Math.pow((p.y - cj), 2);
                        if ((int) d <= pradius) {
                            // applying gradient to this circle so outer influence is low
                            // and at the peak it's maximum
                            dPoints[ci][cj] += numColours - ((d * numColours) / pradius);
                        }
                    }
                }
            }
        }

        generateLogScaleCircle(dPoints);
    }

    public void generatePoints(double[] points, Color pointColour, String label) {
        for (int i = 0; i < points.length; i += 2) {
            double cx = points[i];
            double cy = points[i + 1];
            Point p = translate(cx, cy);
            addDotImage(p, pointColour);
        }
        addToLegend(pointColour, label);
    }

    /**
     * add an item to the legend
     *
     * @param pointColour
     * @param label
     */
    private void addToLegend(Color pointColour, String label) {
        int padding = 10; // 10px padding around the image
        int keyHeight = 30; // 30px key height
        int keyWidth = 25; // 30px key width

        int left = padding * 2 + keyWidth; // padding + width/2;
        int top = padding + (keyHeight / 2);
        int height;

        BufferedImage newLegend;
        if (legendImage == null) {
            newLegend = new BufferedImage(150, 10 + keyHeight, TYPE_INT_ARGB);
            height = 0;
        } else {
            //add one item
            newLegend = new BufferedImage(legendImage.getWidth(), legendImage.getHeight() + keyHeight,
                    legendImage.getType());
            height = legendImage.getHeight();
        }

        Graphics2D cg = (Graphics2D) newLegend.getGraphics();

        //copy current legend
        cg.drawImage(newLegend, 0, 0, null);

        cg.setColor(Color.BLACK);
        //1.2em/1.6em Arial, Helvetica, sans-serif
        Font font = new Font("SanSerif", Font.PLAIN, 11);
        cg.setFont(font);

        RenderingHints rh = new RenderingHints(
                RenderingHints.KEY_TEXT_ANTIALIASING,
                RenderingHints.VALUE_TEXT_ANTIALIAS_GASP
        );
        cg.setRenderingHints(rh);

        cg.drawString(label, left, height + padding + (keyHeight / 2));

        cg.setColor(pointColour);
        cg.fillOval(padding + 5, padding + height + 5,keyWidth - 10, keyWidth - 10);

        legendImage = newLegend;
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

    /**
     * Outputs the image to the supplied file path.
     *
     * @param colorize
     */
    public void drawOutput(String outputFilePath, boolean colorize) throws IOException {
        try {
            if (colorize) {
                heatmapImage = doColorize();
            } else {
                heatmapImage = monochromeImage;
            }

            Graphics2D g = (Graphics2D) backgroundImage.getGraphics();
            g.drawImage(makeColorTransparent(heatmapImage, Color.WHITE), 0, 0, null);

            File hmOut = new File(outputFilePath);
            ImageIO.write(backgroundImage, "png", hmOut);

        } catch (IOException ex) {
            logger.error("An error occurred drawing output to outfile: '"  + outputFilePath
                    + "' Error message: " + ex.getMessage(), ex);
            throw ex;
        }
    }

    public void setLegendImage(BufferedImage legendImage) {
        this.legendImage = legendImage;
    }
}
