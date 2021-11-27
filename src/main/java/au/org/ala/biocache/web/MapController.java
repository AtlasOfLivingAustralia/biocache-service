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
package au.org.ala.biocache.web;

import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.heatmap.HeatMap;
import au.org.ala.biocache.util.ColorUtil;
import au.org.ala.biocache.util.QueryFormatUtils;
import com.google.common.base.Strings;
import io.swagger.v3.oas.annotations.Operation;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.springdoc.api.annotations.ParameterObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.annotation.JsonInclude;
import javax.imageio.ImageIO;
import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * WMS and static map controller. This controller generates static PNG image files
 * that provide a heatmap of occurrences. 
 * 
 * TODO: This functionality is currently only supporting
 * overview maps for Australia but could be extended to support other regions.
 *
 * @author "Ajay Ranipeta <Ajay.Ranipeta@csiro.au>"
 *
 * @Deprecated this should be factored out as its been superceded by functionality in WebportalController.
 */
@Controller("mapController")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MapController {

    /** Logger initialisation */
    private final static Logger logger = Logger.getLogger(MapController.class);

    @Value("${heatmap.output.dir:/data/output/heatmap}")
    protected String heatmapOutputDir;

    @Inject
    protected SearchDAO searchDAO;
    /** Search Utils helper class */
    @Inject
    protected QueryFormatUtils queryFormatUtils;

    @Value("${heatmap.legend.occurrence.label:occurrence}")
    protected String heatmapLegendOccurrenceLabel;

    /**
     * The public or private value to use in the Cache-Control HTTP header for WMS tiles. Defaults to public
     */
    @Value("${heatmap.legend.cache.cachecontrol.publicorprivate:public}")
    private String mapCacheControlHeaderPublicOrPrivate;

    /**
     * The max-age value to use in the Cache-Control HTTP header for WMS tiles. Defaults to 86400, equivalent to 1 day
     */
    @Value("${heatmap.legend.cache.cachecontrol.maxage:86400}")
    private String mapCacheControlHeaderMaxAge;

    private final AtomicReference<String> mapETag = new AtomicReference<String>(UUID.randomUUID().toString());

    @Inject
    protected WMSController wmsController;

    /**
     * Deprecated and moved from MapController in case it is still in use somewhere.
     */
    @Deprecated
    @Operation(summary = "Deprecated path.", tags = {"Deprecated"})
    @RequestMapping(value = "/occurrences/wms", method = RequestMethod.GET)
    public void pointsWmsImage(@ParameterObject SpatialSearchRequestParams requestParams,
                               @RequestParam(value = "colourby", required = false, defaultValue = "0") Integer colourby,
                               @RequestParam(value = "width", required = false, defaultValue = "256") Integer width,
                               @RequestParam(value = "height", required = false, defaultValue = "256") Integer height,
                               @RequestParam(value = "zoom", required = false, defaultValue = "0") Integer zoomLevel,
                               @RequestParam(value = "symsize", required = false, defaultValue = "4") Integer size,
                               @RequestParam(value = "symbol", required = false, defaultValue = "circle") String symbol,
                               @RequestParam(value = "bbox", required = false, defaultValue = "110,-45,157,-9") String bboxString,
                               @RequestParam(value = "type", required = false, defaultValue = "normal") String type,
                               @RequestParam(value = "outline", required = true, defaultValue = "false") boolean outlinePoints,
                               @RequestParam(value = "outlineColour", required = true, defaultValue = "0x000000") String outlineColour,
                               HttpServletRequest request,
                               HttpServletResponse response)
            throws Exception {

        String color = Integer.toHexString(colourby);
        color = "000000".substring(color.length()) + color;

        String env = "color:" + color + ";size:" + size + ";opacity:1.0";

        wmsController.generateWmsTileViaHeatmap(requestParams, "", env, "EPSG:3857", "", bboxString, width, height, "default", "", outlinePoints, outlineColour, "", null, 16, request, response);
    }

    /**
     * Used by phylolink, profiles, hubs
     *
     * @param requestParams
     * @param zoomLevel
     * @param callback
     * @param model
     * @param response
     * @return
     * @throws Exception
     */
    @Operation(summary = "Occurrence info summary service for JS map popups.", tags = {"Mapping"})
    @RequestMapping(value = {"/occurrences/info" }, method = RequestMethod.GET, produces = "application/json")
    public String getOccurrencesInformation(SpatialSearchRequestDTO requestParams,
                                            @RequestParam(value = "callback", required = false) String callback,
                                            Model model,
                                            HttpServletResponse response)
            throws Exception {

        if (requestParams.getLon() == null) {
            response.sendError(400, "Required Double parameter 'lon' is not present");
        }
        if (requestParams.getLat() == null) {
            response.sendError(400, "Required Double parameter 'lat' is not present");
        }
        if (requestParams.getRadius() == null) {
            response.sendError(400, "Required Double parameter 'radius' is not present");
        }

        // required field id
        requestParams.setFl(OccurrenceIndex.ID);

        // limit result
        requestParams.setPageSize(100);

        response.setHeader("Cache-Control", mapCacheControlHeaderPublicOrPrivate + ", max-age=" + mapCacheControlHeaderMaxAge);
        response.setHeader("ETag", mapETag.get());
        if (callback != null && !callback.isEmpty()) {
            response.setContentType("text/javascript");
        } else {
            response.setContentType("application/json");
        }

        SolrDocumentList sdl = searchDAO.findByFulltext(requestParams);
        List<OccurrencePoint> points = new ArrayList<OccurrencePoint>();

        if (sdl != null) {
            sdl.stream().forEach((SolrDocument sd) -> {
                OccurrencePoint point = new OccurrencePoint();
                point.setOccurrenceUid((String) sd.getFirstValue(OccurrenceIndex.ID));
                points.add(point);
            });
        }

        model.addAttribute("points", points);
        model.addAttribute("count", points.size());

        return "json/infoPointGeojson";
    }

    /**
     * May be used by hubs.
     *
     * @param colourby
     * @param width
     * @param height
     * @param response
     */
    @Deprecated
    @Operation(summary = "Point legend service", tags = {"Deprecated"})
    @RequestMapping(value = "/occurrences/legend", method = RequestMethod.GET)
    public void pointLegendImage(@RequestParam(value = "colourby", required = false, defaultValue = "0") Integer colourby,
                                 @RequestParam(value = "width", required = false, defaultValue = "50") Integer width,
                                 @RequestParam(value = "height", required = false, defaultValue = "50") Integer height,
                                 HttpServletResponse response) {
        try {

            BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
            Graphics2D g = (Graphics2D) img.getGraphics();

            int colour = 0xFF000000 | colourby.intValue();
            Color c = new Color(colour);
            g.setPaint(c);

            g.fillOval(0, 0, width, width);

            g.dispose();

            streamImage(img, response);

        } catch (Exception e) {
            logger.error("Unable to write image", e);
        }
    }

    private void streamImage(BufferedImage img, HttpServletResponse response) throws Exception {
        response.setHeader("Cache-Control", mapCacheControlHeaderPublicOrPrivate + ", max-age=" + mapCacheControlHeaderMaxAge);
        response.setHeader("ETag", mapETag.get());
        response.setContentType("image/png");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ImageIO.write(img, "png", outputStream);
        ServletOutputStream outStream = response.getOutputStream();
        outStream.write(outputStream.toByteArray());
        outStream.flush();
        outStream.close();
    }

    /**
     * This method creates and renders a density map for a species.
     * <p>
     * Used by fieldguide.
     *
     * @throws Exception
     */
    @Deprecated
    @Operation(summary = "Renders a density map for a species.", tags = {"Deprecated"})
    @RequestMapping(value = {"/density/map", "/occurrences/static"}, method = RequestMethod.GET)
    public @ResponseBody
    void speciesDensityMap(SpatialSearchRequestDTO requestParams,
                           @RequestParam(value = "forceRefresh", required = false, defaultValue = "false") boolean forceRefresh,
                           @RequestParam(value = "forcePointsDisplay", required = false, defaultValue = "false") boolean forcePointsDisplay,
                           @RequestParam(value = "pointColour", required = false, defaultValue = "0000ff") String pointColour,
                           @RequestParam(value = "colourByFq", required = false, defaultValue = "") String colourByFqCSV,
                           @RequestParam(value = "colours", required = false, defaultValue = "") String coloursCSV,
                           @RequestParam(value = "pointHeatMapThreshold", required = false, defaultValue = "500") Integer pointHeatMapThreshold,
                           @RequestParam(value = "opacity", required = false, defaultValue = "1.0") Float opacity,
                           HttpServletRequest request,
                           HttpServletResponse response) throws Exception {

        File outputDir = new File(heatmapOutputDir);
        if (!outputDir.exists()) {
            FileUtils.forceMkdir(outputDir);
        }

        if (Strings.isNullOrEmpty(request.getQueryString())) {
            response.sendError(400, "   No query parameters provided");
            return;
        }

        //output heatmap path
        String outputHMFile = getOutputFile(request);

        String[] facetValues = null;
        String[] facetColours = null;
        if (StringUtils.trimToNull(colourByFqCSV) != null && StringUtils.trimToNull(coloursCSV) != null) {
            facetValues = colourByFqCSV.split(",");
            facetColours = coloursCSV.split(",");
            if (facetValues.length == 0 || facetValues.length != facetColours.length) {
                throw new IllegalArgumentException(String.format("Mismatch in facet values and colours. Values: %d, Colours: %d", facetValues.length, facetColours.length));
            }
        }

        //Does file exist on disk?
        File f = new File(outputDir + "/" + outputHMFile);

        if (!f.isFile() || !f.exists() || forceRefresh) {
            logger.debug("Regenerating heatmap image");
            //If not, generate
            generateStaticHeatmapImages(requestParams, false, forcePointsDisplay, pointHeatMapThreshold, pointColour, facetValues, facetColours, opacity, request);
        } else {
            logger.debug("Heatmap file already exists on disk, sending file back to user");
        }

        try {
            //read file off disk and send back to user
            File file = new File(outputDir + "/" + outputHMFile);
            BufferedImage img = ImageIO.read(file);
            streamImage(img, response);

        } catch (Exception e) {
            logger.error("Unable to write image.", e);
        }
    }

    private String getQueryHash(HttpServletRequest request) throws NoSuchAlgorithmException, UnsupportedEncodingException {

        MessageDigest md = MessageDigest.getInstance("MD5");
        //replace forceRefresh if it is first or not
        String qs = request.getQueryString().replaceAll("&(?i)forceRefresh=true", "").replaceAll("(?i)forceRefresh=true&", "");
        md.update(qs.getBytes(StandardCharsets.UTF_8));
        byte[] digest = md.digest();
        StringBuffer sb = new StringBuffer();
        for (byte b : digest) {
            sb.append(Integer.toHexString((int) (b & 0xff)));
        }
        return sb.toString();
    }

    /**
     * This method creates and renders a density map legend for a species.
     *
     * Used by fieldguide.
     *
     * @throws Exception
     */
    @Deprecated
    @Operation(summary = "Renders a density map legend for a species.", tags = {"Deprecated"})
    @RequestMapping(value = "/density/legend", method = RequestMethod.GET)
    public @ResponseBody void speciesDensityLegend(SpatialSearchRequestDTO requestParams,
                                                   @RequestParam(value = "forceRefresh", required = false, defaultValue = "false") boolean forceRefresh,
                                                   @RequestParam(value = "pointHeatMapThreshold", required = false, defaultValue = "500") Integer pointHeatMapThreshold,
                                                   HttpServletRequest request,
                                                   HttpServletResponse response) throws Exception {

        File baseDir = new File(heatmapOutputDir);

        String outputHMFile = getOutputFile(request);

        //Does file exist on disk?
        File f = new File(baseDir + "/" + "legend_" + outputHMFile);

        if (!f.isFile() || !f.exists() || forceRefresh) {
            //If not, generate
            logger.debug("regenerating heatmap legend");
            generateStaticHeatmapImages(requestParams, true, false, pointHeatMapThreshold, "0000ff", null, null, 1.0f, request);
        } else {
            logger.debug("legend file already exists on disk, sending file back to user");
        }

        //read file off disk and send back to user
        try {
            File file = new File(baseDir + "/" + "legend_" + outputHMFile);
            //only send the image back if it actually exists - a legend won't exist if we create the map based on points
            if (file.exists()) {
                BufferedImage img = ImageIO.read(file);
                streamImage(img, response);
            }

        } catch (Exception e) {
            logger.error("Unable to write image.", e);
        }
    }

    private String getOutputFile(HttpServletRequest request) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        return getQueryHash(request) + "_hm.png";
    }

    /**
     * Generate heatmap image (and associated legend if applicable)
     * @param requestParams
     */
    public void generateStaticHeatmapImages(
            SpatialSearchRequestDTO requestParams,
            boolean generateLegend,
            boolean forcePointsDisplay,
            Integer pointHeatMapThreshold,
            String defaultPointColour,
            String[] colourByFq,
            String[] colours,
            Float opacity,
            HttpServletRequest request) throws Exception {
        
        File baseDir = new File(heatmapOutputDir);
        logger.debug("Heatmap output directory is " + heatmapOutputDir);
        String outputHMFile = getOutputFile(request);

        PointType pointType = PointType.POINT_001;

        double[] points = retrievePoints(requestParams, pointType);
        
        HeatMap hm = new HeatMap();

        //heatmap versus points
        if (forcePointsDisplay || points.length == 0 || (points.length / 2) < pointHeatMapThreshold) {
            hm.setLegendImage(null);
            if (!generateLegend && colourByFq != null){
                String[] originalFq = requestParams.getFq();
                for(int k = 0; k < colourByFq.length; k++){
                    if(originalFq != null){
                        requestParams.setFq(ArrayUtils.add(originalFq, colourByFq[k]));
                    } else {
                        requestParams.setFq(new String[]{colourByFq[k]});
                    }
                    if(forcePointsDisplay && points.length > 0 && (points.length / 2 < pointHeatMapThreshold) ){
                        pointType = PointType.POINT_01;
                    }

                    double[] pointsForFacet = retrievePoints(requestParams, pointType);
                    Color pointColor = ColorUtil.getColor(colours[k], opacity);

                    String facetDisplayString = queryFormatUtils.formatQueryTerm(colourByFq[k], null)[0];
                    hm.generatePoints(pointsForFacet, pointColor, facetDisplayString);
                }
            } else {
                Color pointColor = ColorUtil.getColor(defaultPointColour, opacity);
                hm.generatePoints(points, pointColor, heatmapLegendOccurrenceLabel);
            }
            hm.drawOutput(baseDir + "/" + outputHMFile, false);
            hm.drawLegend(baseDir + "/legend_" + outputHMFile);
        } else {
            hm.generateClasses(points); //this will create legend
            if (generateLegend){
                hm.drawLegend(baseDir + "/legend_" + outputHMFile);
            } else {
                hm.drawOutput(baseDir + "/" + outputHMFile, true);
            }
        }
    }

    /**
     * Returns an array of points in the format [lat1,long1,lat2,long2,.....]
     *
     * @param requestParams
     * @param pointType
     * @return returns an empty array if none found.
     */
    private double[] retrievePoints(SpatialSearchRequestDTO requestParams, PointType pointType) {

        double[] points = new double[0];
        try {
            requestParams.setQ(requestParams.getQ());
            List<OccurrencePoint> occ_points = searchDAO.getFacetPoints(requestParams, pointType);
            if (logger.isDebugEnabled()) {
                logger.debug("Points search for " + pointType.getLabel() + " - found: " + occ_points.size());
            }

            int totalItems = 0;
            for (int i = 0; i < occ_points.size(); i++) {
                OccurrencePoint pt = occ_points.get(i);
                totalItems = (int) (totalItems + pt.getCount());
            }

            logger.debug("total number of occurrence points is " + totalItems);

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
            logger.error("An error occurred getting heatmap points", e);
        }
        return points;
    }
}
