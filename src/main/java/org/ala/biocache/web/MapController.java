/* *************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
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
package org.ala.biocache.web;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Shape;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;

import javax.imageio.ImageIO;
import javax.inject.Inject;
import javax.servlet.ServletConfig;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.ala.biocache.dao.SearchDAO;
import org.ala.biocache.dto.OccurrencePoint;
import org.ala.biocache.dto.PointType;
import org.ala.biocache.dto.SpatialSearchRequestParams;
import org.ala.biocache.heatmap.HeatMap;
import org.ala.biocache.util.ColorUtil;
import org.ala.biocache.util.SearchUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.context.ServletConfigAware;

/**
 * WMS and static map controller
 * 
 * @author "Ajay Ranipeta <Ajay.Ranipeta@csiro.au>"
 *
 * TODO this should be factored out as its been superceded by functionality
 * in WebportalController.
 */
@Controller("mapController")
public class MapController implements ServletConfigAware {

    /** Logger initialisation */
    private final static Logger logger = Logger.getLogger(MapController.class);
    private String baseMapPath = "/images/mapaus1_white.png";
    
    protected String heatmapBase = "/data/output/heatmap";
    
    /** Fulltext search DAO */
    @Inject
    protected SearchDAO searchDAO;
    /** Search Utils helper class */
    @Inject
    protected SearchUtils searchUtils;
    private ServletConfig cfg;

    @RequestMapping(value = "/occurrences/wms", method = RequestMethod.GET)
    public void pointsWmsImage(SpatialSearchRequestParams requestParams,
            @RequestParam(value = "colourby", required = false, defaultValue = "0") Integer colourby,
            @RequestParam(value = "width", required = false, defaultValue = "256") Integer widthObj,
            @RequestParam(value = "height", required = false, defaultValue = "256") Integer heightObj,
            @RequestParam(value = "zoom", required = false, defaultValue = "0") Integer zoomLevel,
            @RequestParam(value = "symsize", required = false, defaultValue = "4") Integer symsize,
            @RequestParam(value = "symbol", required = false, defaultValue = "circle") String symbol,
            @RequestParam(value = "bbox", required = false, defaultValue = "110,-45,157,-9") String bboxString,
            @RequestParam(value = "type", required = false, defaultValue = "normal") String type,
            @RequestParam(value = "outline", required = true, defaultValue = "false") boolean outlinePoints,
            @RequestParam(value = "outlineColour", required = true, defaultValue = "0x000000") String outlineColour,

            HttpServletRequest request,
            HttpServletResponse response)
            throws Exception {

        // size of the circles
        int size = symsize.intValue();
        int width = widthObj.intValue();
        int height = heightObj.intValue();

        requestParams.setStart(0);
        requestParams.setPageSize(Integer.MAX_VALUE);
        String query = requestParams.getQ();
        String[] filterQuery = requestParams.getFq();

        if (StringUtils.isBlank(query) && StringUtils.isBlank(requestParams.getFormattedQuery())) {
            displayBlankImage(width, height, false, request, response);
            return;
        }

        // let's force it to PNG's for now 
        response.setContentType("image/png");

        // Convert array to list so we append more values onto it
        ArrayList<String> fqList = null;
        if (filterQuery != null) {
            fqList = new ArrayList<String>(Arrays.asList(filterQuery));
        } else {
            fqList = new ArrayList<String>();
        }

        // the bounding box
        double[] bbox = new double[4];
        int i;
        i = 0;
        for (String s : bboxString.split(",")) {
            try {
                bbox[i] = Double.parseDouble(s);
                i++;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        double pixelWidth = (bbox[2] - bbox[0]) / width;
        double pixelHeight = (bbox[3] - bbox[1]) / height;
        bbox[0] += pixelWidth / 2;
        bbox[2] -= pixelWidth / 2;
        bbox[1] += pixelHeight / 2;
        bbox[3] -= pixelHeight / 2;

        //offset for points bounding box by size
        double xoffset = (bbox[2] - bbox[0]) / (double) width * (size * 2);
        double yoffset = (bbox[3] - bbox[1]) / (double) height * (size * 2);

        //adjust offset for pixel height/width
        xoffset += pixelWidth;
        yoffset += pixelHeight;

        double[] bbox2 = new double[4];
        bbox2[0] = convertMetersToLng(bbox[0] - xoffset);
        bbox2[1] = convertMetersToLat(bbox[1] - yoffset);
        bbox2[2] = convertMetersToLng(bbox[2] + xoffset);
        bbox2[3] = convertMetersToLat(bbox[3] + yoffset);

        bbox[0] = convertMetersToLng(bbox[0]);
        bbox[1] = convertMetersToLat(bbox[1]);
        bbox[2] = convertMetersToLng(bbox[2]);
        bbox[3] = convertMetersToLat(bbox[3]);

        double[] pbbox = new double[4]; //pixel bounding box
        pbbox[0] = convertLngToPixel(bbox[0]);
        pbbox[1] = convertLatToPixel(bbox[1]);
        pbbox[2] = convertLngToPixel(bbox[2]);
        pbbox[3] = convertLatToPixel(bbox[3]);


        String bboxString2 = bbox2[0] + "," + bbox2[1] + "," + bbox2[2] + "," + bbox2[3];
        bboxToQuery(bboxString2, fqList);

        PointType pointType = getPointTypeForZoomLevel(zoomLevel);

        String[] newFilterQuery = (String[]) fqList.toArray(new String[fqList.size()]); // convert back to array

        requestParams.setFq(newFilterQuery);

        List<OccurrencePoint> points = searchDAO.getFacetPoints(requestParams, pointType);
        logger.debug("Points search for " + pointType.getLabel() + " - found: " + points.size());

        if (points.size() == 0) {
            displayBlankImage(width, height, false, request, response);
            return;
        }

        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = (Graphics2D) img.getGraphics();
        g.setColor(Color.RED);

        int x, y;
        int pointWidth = size * 2;
        double width_mult = (width / (pbbox[2] - pbbox[0]));
        double height_mult = (height / (pbbox[1] - pbbox[3]));

        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        float radius = 10f;

        Color oColour = Color.decode(outlineColour);

        for (i = 0; i < points.size(); i++) {
            OccurrencePoint pt = points.get(i);
            float lng = pt.getCoordinates().get(0).floatValue();
            float lat = pt.getCoordinates().get(1).floatValue();

            x = (int) ((convertLngToPixel(lng) - pbbox[0]) * width_mult);
            y = (int) ((convertLatToPixel(lat) - pbbox[3]) * height_mult);

            if (colourby != null) {
                int colour = 0xFF000000 | colourby.intValue();
                Color c = new Color(colour);
                g.setPaint(c);
            } else {
                g.setPaint(Color.blue);
            }

            // g.fillOval(x - (size / 2), y - (size / 2), pointWidth, pointWidth);
            Shape shp = getShape(symbol, x - (size / 2), y - (size / 2), pointWidth, pointWidth);
            g.draw(shp);
            g.fill(shp);
            if(outlinePoints){
                g.setPaint(oColour);
                g.drawOval(x - (size / 2), y - (size / 2), pointWidth, pointWidth);
            }
        }

        g.dispose();

        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ImageIO.write(img, "png", outputStream);
            ServletOutputStream outStream = response.getOutputStream();
            outStream.write(outputStream.toByteArray());
            outStream.flush();
            outStream.close();

        } catch (Exception e) {
            logger.error("Unable to write image", e);
        }
    }

    @RequestMapping(value = "/occurrences/static", method = RequestMethod.GET)
    public void pointsStaticImage(SpatialSearchRequestParams requestParams,
            @RequestParam(value = "colourby", required = false, defaultValue = "") String colourby,
            @RequestParam(value = "width", required = false, defaultValue = "256") Integer widthObj,
            @RequestParam(value = "height", required = false, defaultValue = "256") Integer heightObj,
            @RequestParam(value = "type", required = false, defaultValue = "normal") String type,
            HttpServletRequest request,
            HttpServletResponse response)
            throws Exception {

        int width = widthObj.intValue();
        int height = heightObj.intValue();

        // the bounding box
        String bboxString = "110,-45,157,-9";
        double[] bbox = new double[4];
        int i;
        i = 0;
        for (String s : bboxString.split(",")) {
            try {
                bbox[i] = Double.parseDouble(s);
                i++;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        requestParams.setPageSize(1000);
        requestParams.setStart(0);
        requestParams.setPageSize(Integer.MAX_VALUE);
        String query = requestParams.getQ();
        String[] filterQuery = requestParams.getFq();

        if (StringUtils.isBlank(query) && StringUtils.isBlank(requestParams.getFormattedQuery())) {
            displayBlankImage(width, height, true, request, response);
            return;
        }

        // let's force it to PNG's for now
        response.setContentType("image/png");

        // Convert array to list so we append more values onto it
        ArrayList<String> fqList = null;
        if (filterQuery != null) {
            fqList = new ArrayList<String>(Arrays.asList(filterQuery));
        } else {
            fqList = new ArrayList<String>();
        }

        // add the default bounding box 
        bboxToQuery(bboxString, fqList);

        PointType pointType = PointType.POINT_RAW;

        String[] newFilterQuery = (String[]) fqList.toArray(new String[fqList.size()]); // convert back to array
        //get the new query details
        //SearchQuery searchQuery = new SearchQuery(query, type, newFilterQuery);
        //searchUtils.updateQueryDetails(searchQuery);

        // add the details back into the requestParam object
        //requestParams.setQ(searchQuery.getQuery());
        requestParams.setFq(newFilterQuery);


        //List<OccurrencePoint> points = searchDAO.getFacetPoints(requestParams.getQ(), requestParams.getFq(), pointType);
        List<OccurrencePoint> points = searchDAO.getOccurrences(requestParams, pointType, colourby, 0);
        logger.debug("Points search for " + pointType.getLabel() + " - found: " + points.size());

        if (points.size() == 0) {
            displayBlankImage(width, height, true, request, response);
            return;
        }

        BufferedImage baseImage = createBaseMapImage();
        width = baseImage.getWidth();
        height = baseImage.getHeight();

        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = (Graphics2D) img.getGraphics();
        g.drawImage(baseImage, 0, 0, null);
        g.setPaint(Color.red);

        // size of the circles
        int size = 4;

        double pixelWidth = (bbox[2] - bbox[0]) / width;
        double pixelHeight = (bbox[3] - bbox[1]) / height;
        bbox[0] += pixelWidth / 2;
        bbox[2] -= pixelWidth / 2;
        bbox[1] += pixelHeight / 2;
        bbox[3] -= pixelHeight / 2;

        double[] pbbox = new double[4]; //pixel bounding box
        pbbox[0] = convertLngToPixel(bbox[0]);
        pbbox[1] = convertLatToPixel(bbox[1]);
        pbbox[2] = convertLngToPixel(bbox[2]);
        pbbox[3] = convertLatToPixel(bbox[3]);

        int x, y;
        int pointWidth = size * 2;
        double width_mult = (width / (pbbox[2] - pbbox[0]));
        double height_mult = (height / (pbbox[1] - pbbox[3]));

        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        //float radius = 10f;

        for (i = 0; i < points.size(); i++) {
            OccurrencePoint pt = points.get(i);
            float lng = pt.getCoordinates().get(0).floatValue();
            float lat = pt.getCoordinates().get(1).floatValue();

            x = (int) ((convertLngToPixel(lng) - pbbox[0]) * width_mult);
            y = (int) ((convertLatToPixel(lat) - pbbox[3]) * height_mult);

            //logger.debug("generating colour for: " + pt.getOccurrenceUid() + " <-" + colourby);
            if (StringUtils.isNotBlank(pt.getOccurrenceUid())) {
                //logger.debug("\twith hashcode: " + pt.getOccurrenceUid().hashCode());
                int colour = 0xFF000000 | pt.getOccurrenceUid().hashCode();
                //logger.debug("name: " + pt.getOccurrenceUid() + " => colour: " + colour);

                Color c = new Color(colour);
                //logger.debug("setting filter colour");
                g.setPaint(c);

            } else {
                g.setPaint(Color.red);
            }

            //Shape circle = new Ellipse2D.Float(x - (radius / 2), y - (radius / 2), radius, radius);
            //g.draw(circle);
            //g.fill(circle);
            //g.setPaint(Color.RED);
            g.fillOval(x - (size / 2), y - (size / 2), pointWidth, pointWidth);
            g.setPaint(Color.BLACK);
            //System.out.println("############ Drawing an outline on the points....");

            g.drawOval(x - (size / 2), y - (size / 2), pointWidth, pointWidth);
        }

        g.dispose();

        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ImageIO.write(img, "png", outputStream);
            ServletOutputStream outStream = response.getOutputStream();
            outStream.write(outputStream.toByteArray());
            outStream.flush();
            outStream.close();

        } catch (Exception e) {
            logger.error("Unable to write image.", e);
        }
    }

    @RequestMapping(value = "/occurrences/info", method = RequestMethod.GET)
    public String getOccurrencesInformation(SpatialSearchRequestParams requestParams,
            @RequestParam(value = "zoom", required = false, defaultValue = "0") Integer zoomLevel,
            @RequestParam(value = "callback", required = false) String callback,
            Model model,
            HttpServletRequest request,
            HttpServletResponse response)
            throws Exception {


        if (callback != null && !callback.isEmpty()) {
            response.setContentType("text/javascript");
        } else {
            response.setContentType("application/json");
        }

        PointType pointType = PointType.POINT_RAW; // default value for when zoom is null
        pointType = getPointTypeForZoomLevel(zoomLevel);

        List<OccurrencePoint> points = searchDAO.getOccurrences(requestParams, pointType, "", 1);
        logger.info("Points search for " + pointType.getLabel() + " - found: " + points.size());
        model.addAttribute("points", points);
        model.addAttribute("count", points.size());

        return "json/infoPointGeojson"; //POINTS_GEOJSON;
    }

    private void displayBlankImage(int width, int height, boolean useBase, HttpServletRequest request, HttpServletResponse response) {
        try {
            response.setContentType("image/png");

            BufferedImage baseImage = null;

            //BufferedImage baseImage = ImageIO.read(new File("/data/tmp/mapaus1_white.png"));
            //BufferedImage baseImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
            if (useBase) {
                baseImage = createBaseMapImage();
            } else {
                baseImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
                //baseImage.getGraphics().fillRect(0, 0, width, height);
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ImageIO.write(baseImage, "png", outputStream);
            ServletOutputStream outStream = response.getOutputStream();
            outStream.write(outputStream.toByteArray());
            outStream.flush();
            outStream.close();

        } catch (Exception e) {
            logger.error("Unable to write image", e);
        }
    }

    @RequestMapping(value = "/occurrences/legend", method = RequestMethod.GET)
    public void pointLegendImage(@RequestParam(value = "colourby", required = false, defaultValue = "0") Integer colourby,
            @RequestParam(value = "width", required = false, defaultValue = "50") Integer widthObj,
            @RequestParam(value = "height", required = false, defaultValue = "50") Integer heightObj,
            HttpServletResponse response) {
        try {

            response.setContentType("image/png");

            int width = widthObj.intValue();
            int height = heightObj.intValue();

            BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
            Graphics2D g = (Graphics2D) img.getGraphics();

            if (colourby != null) {

                int colour = 0xFF000000 | colourby.intValue();
                Color c = new Color(colour);
                g.setPaint(c);

            } else {
                g.setPaint(Color.blue);
            }

            g.fillOval(0, 0, width, width);

            g.dispose();

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ImageIO.write(img, "png", outputStream);
            ServletOutputStream outStream = response.getOutputStream();
            outStream.write(outputStream.toByteArray());
            outStream.flush();
            outStream.close();

        } catch (Exception e) {
            logger.error("Unable to write image", e);
        }
    }

    private Shape getShape(String symbol, int x, int y, int width, int height) {
        Shape shape = null;

        symbol = symbol.toLowerCase();

        if (symbol.equals("square")) {
            shape = new Rectangle2D.Float(x, y, width, height);
        } else {
            shape = new Ellipse2D.Float(x, y, width, height);
        }

        return shape;
    }
    private int map_offset = 268435456; // half the Earth's circumference at zoom level 21
    private double map_radius = map_offset / Math.PI;

    public int convertLatToPixel(double lat) {
        return (int) Math.round(map_offset - map_radius
                * Math.log((1 + Math.sin(lat * Math.PI / 180))
                / (1 - Math.sin(lat * Math.PI / 180))) / 2);
    }

    public int convertLngToPixel(double lng) {
        return (int) Math.round(map_offset + map_radius * lng * Math.PI / 180);
    }

    private double convertMetersToLng(double meters) {
        return meters / 20037508.342789244 * 180;
    }

    private double convertMetersToLat(double meters) {
        return 180.0 / Math.PI * (2 * Math.atan(Math.exp(meters / 20037508.342789244 * Math.PI)) - Math.PI / 2.0);
    }

    /**
     * Reformat bbox param to SOLR spatial query and add to fq list
     *
     * @param bbox
     * @param fqList
     */
    protected void bboxToQuery(String bbox, ArrayList<String> fqList) {
        // e.g. bbox=122.013671875,-53.015625,172.990234375,-10.828125
        if (bbox != null && !bbox.isEmpty()) {
            String[] bounds = StringUtils.split(bbox, ",");
            if (bounds.length == 4) {
                String fq1 = "longitude:[" + bounds[0] + " TO " + bounds[2] + "]";
                fqList.add(fq1);
                String fq2 = "latitude:[" + bounds[1] + " TO " + bounds[3] + "]";
                fqList.add(fq2);
            } else {
                logger.warn("BBOX does not contain the expected number of coords (4). Found: " + bounds.length);
            }
        }
    }

    /**
     * Map a zoom level to a coordinate accuracy level
     *
     * @param zoomLevel
     * @return
     */
    protected PointType getPointTypeForZoomLevel(Integer zoomLevel) {
        PointType pointType = null;
        // Map zoom levels to lat/long accuracy levels
        if (zoomLevel != null) {
            if (zoomLevel >= 0 && zoomLevel <= 6) {
                // 0-6 levels
                pointType = PointType.POINT_01;
            } else if (zoomLevel > 6 && zoomLevel <= 8) {
                // 6-8 levels
                pointType = PointType.POINT_00001;
            } else {
                // raw levels
                pointType = PointType.POINT_RAW;
            }
        }
        return pointType;
    }

    /**
     * Create a buffered image for the static map.
     * 
     * @return
     * @throws IOException
     */
    private BufferedImage createBaseMapImage() throws IOException {
        InputStream in = this.cfg.getServletContext().getResourceAsStream(baseMapPath);
        return ImageIO.read(in);
    }

    /**
     * This method creates and renders a density map for a species.
     * 
     * @param model
     * @throws Exception
     */
    @RequestMapping(value = "/density/map", method = RequestMethod.GET)
    public @ResponseBody
    void speciesDensityMap(SpatialSearchRequestParams requestParams, Model model,
            @RequestParam(value = "forceRefresh", required = false, defaultValue = "false") boolean forceRefresh,
            @RequestParam(value = "forcePointsDisplay", required = false, defaultValue = "false") boolean forcePointsDisplay,            
            @RequestParam(value = "pointColour", required = false, defaultValue = "0000ff") String pointColour,
            @RequestParam(value = "colourByFq", required = false, defaultValue = "") String colourByFqCSV,
            @RequestParam(value = "colours", required = false, defaultValue = "") String coloursCSV,
            @RequestParam(value = "pointHeatMapThreshold", required = false, defaultValue = "500") Integer pointHeatMapThreshold,
            @RequestParam(value = "opacity", required = false, defaultValue = "1.0") Float opacity,
            HttpServletRequest request, HttpServletResponse response) throws Exception {
        
    	response.setContentType("image/png");
        File baseDir = new File(heatmapBase);
        String outputHMFile = requestParams.getQ().replace(":", "_") + "_hm.png";
        
        
        String[] facetValues = null;
        String[] facetColours = null;
        if( StringUtils.trimToNull(colourByFqCSV) != null && StringUtils.trimToNull(coloursCSV) != null){
        	facetValues = colourByFqCSV.split(",");
        	facetColours = coloursCSV.split(",");
        	if(facetValues.length == 0 || facetValues.length != facetColours.length){
        		throw new IllegalArgumentException(String.format("Mismatch in facet values and colours. Values: %d, Colours: %d", facetValues.length, facetColours.length));
        	}
        }
               
        //Does file exist on disk?
        File f = new File(baseDir + "/" + outputHMFile);

        if (!f.isFile() || !f.exists() || forceRefresh) {
            logger.info("regenerating heatmap image");
            //If not, generate
            generateStaticHeatmapImages(requestParams, model, request, response, false, forcePointsDisplay, pointHeatMapThreshold, pointColour, facetValues, facetColours, opacity);
        } else {
            logger.info("heatmap file already exists on disk, sending file back to user");
        }

        try {
            //read file off disk and send back to user
            File file = new File(baseDir + "/" + outputHMFile);
            BufferedImage img = ImageIO.read(file);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ImageIO.write(img, "png", outputStream);
            ServletOutputStream outStream = response.getOutputStream();
            outStream.write(outputStream.toByteArray());
            outStream.flush();
            outStream.close();

        } catch (Exception e) {
            logger.error("Unable to write image.", e);
        }
    }

    /**
     * This method creates and renders a density map legend for a species.
     * 
     * @param model
     * @return
     * @throws Exception 
     */
    @RequestMapping(value = "/density/legend", method = RequestMethod.GET)
    public @ResponseBody
    void speciesDensityLegend(SpatialSearchRequestParams requestParams,
            @RequestParam(value = "forceRefresh", required = false, defaultValue = "false") boolean forceRefresh,
            Model model, HttpServletRequest request, HttpServletResponse response) throws Exception {

        response.setContentType("image/png");
        File baseDir = new File(heatmapBase);
        String outputHMFile = requestParams.getQ().replace(":", "_") + "_hm.png";

        //Does file exist on disk?
        File f = new File(baseDir + "/" + "legend_" + outputHMFile);
        
        if (!f.isFile() || !f.exists() || forceRefresh) {
            //If not, generate
            logger.debug("regenerating heatmap legend");
            generateStaticHeatmapImages(requestParams, model, request, response, true, false,  0, "0000ff", null, null, 1.0f);
        } else {
            logger.debug("legend file already exists on disk, sending file back to user");
        }

        //read file off disk and send back to user
        try {
            File file = new File(baseDir + "/" + "legend_" + outputHMFile);
            //only send the image back if it actually exists - a legend won't exist if we create the map based on points
            if(file.exists()){
                BufferedImage img = ImageIO.read(file);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                ImageIO.write(img, "png", outputStream);
                ServletOutputStream outStream = response.getOutputStream();
                outStream.write(outputStream.toByteArray());
                outStream.flush();
                outStream.close();
            }

        } catch (Exception e) {
            logger.error("Unable to write image.", e);
        }
    }

    /**
     * Generate heatmap image (and associated legend if applicable)
     * @param requestParams
     * @param model
     * @param request
     * @param response 
     */
    public void generateStaticHeatmapImages(
    		SpatialSearchRequestParams requestParams, 
    		Model model, 
    		HttpServletRequest request, 
    		HttpServletResponse response, 
    		boolean generateLegend,
    		boolean forcePointsDisplay,
    		Integer pointHeatMapThreshold,
    		String defaultPointColour,
    		String[] colourByFq,
    		String[] colours,
    		Float opacity
    		) {
        
        File baseDir = new File(heatmapBase);
        logger.info("heatmap base is " + heatmapBase);
        String outputHMFile = requestParams.getQ().replace(":", "_") + "_hm.png";

        //PointType pointType = PointType.POINT_RAW;
        PointType pointType = PointType.POINT_001;

        double[] points = retrievePoints(requestParams, pointType);
        
        if (points != null && points.length > 0) {
            HeatMap hm = new HeatMap(baseDir, outputHMFile);
            
            //heatmap versus points
            if (forcePointsDisplay || (points.length / 2) < pointHeatMapThreshold) {
                if (!generateLegend){
                	
                	if(colourByFq != null){
                		
                		String[] originalFq = requestParams.getFq();
                		
                		for(int k=0; k<colourByFq.length; k++){
                			if(originalFq != null){
                				requestParams.setFq(ArrayUtils.add(originalFq, colourByFq[k]));
                			} else {
                				requestParams.setFq(new String[]{colourByFq[k]});
                			}
                			double[] pointsForFacet = retrievePoints(requestParams, pointType);
                			Color pointColor = ColorUtil.getColor(colours[k], opacity);
                			hm.generatePoints(pointsForFacet, pointColor);
                		}
                	} else {
                		Color pointColor = ColorUtil.getColor(defaultPointColour, opacity);
                		hm.generatePoints(points, pointColor);
                	}
                    hm.drawOutput(baseDir + "/" + outputHMFile, false);
                }
            } else {
                hm.generateClasses(points); //this will create legend
                if (generateLegend){
                    hm.drawLegend(baseDir + "/legend_" + outputHMFile);
                }
                else {
                    hm.drawOutput(baseDir + "/" + outputHMFile, true);
                }
            }
        } else {
            logger.debug("No points provided, creating a blank map");
            
            File inMapFile = new File(baseDir + "/base/mapaus1_white.png");
            File outMapFile = new File(baseDir + "/" + outputHMFile);
            File inLegFile = new File(baseDir + "/base/blank.png");
            File outLegFile = new File(baseDir + "/" + "legend_" + outputHMFile);

            try {
                FileUtils.copyFile(inMapFile, outMapFile);
                FileUtils.copyFile(inLegFile, outLegFile);
            } catch (Exception e) {
                logger.error("Unable to create blank map/legend",e);
            }
        }
    }

	private double[] retrievePoints(SpatialSearchRequestParams requestParams,
			PointType pointType) {
		double[] points = null;
		try {
            requestParams.setQ(requestParams.getQ());
            List<OccurrencePoint> occ_points = searchDAO.getFacetPoints(requestParams, pointType);
            logger.debug("Points search for " + pointType.getLabel() + " - found: " + occ_points.size());

            int totalItems = 0;
            for (int i = 0; i < occ_points.size(); i++) {
                OccurrencePoint pt = occ_points.get(i);
                totalItems = (int) (totalItems + pt.getCount());
            }
            logger.info("total number of occurrence points is " + totalItems);

            points = new double[totalItems * 2];

            int j = 0;
            for (int i = 0; i < occ_points.size(); i++) {
                OccurrencePoint pt = occ_points.get(i);
                pt.getCount();
                double lng = pt.getCoordinates().get(0).doubleValue();
                double lat = pt.getCoordinates().get(1).doubleValue();
                points[j] = lng;
                points[j + 1] = lat;
                j = j + 2;
            }
        } catch (Exception e) {
            logger.error("An error occurred getting heatmap points");
        }
		return points;
	}

    public void setSearchDAO(SearchDAO searchDAO) {
        this.searchDAO = searchDAO;
    }

    public void setSearchUtils(SearchUtils searchUtils) {
        this.searchUtils = searchUtils;
    }

    @Override
    public void setServletConfig(ServletConfig cfg) {
        this.cfg = cfg;
    }

    public void setHeatmapBase(String heatmapBase) {
        this.heatmapBase = heatmapBase;
    }
}
