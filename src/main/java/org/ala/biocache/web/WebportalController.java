package org.ala.biocache.web;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.util.*;
import org.ala.biocache.dto.TaxaCountDTO;
import org.ala.biocache.util.ParamsCache;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import javax.imageio.ImageIO;
import javax.inject.Inject;
import javax.servlet.ServletConfig;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.ala.biocache.dao.SearchDAO;
import org.ala.biocache.dto.DataProviderCountDTO;
import org.ala.biocache.dto.OccurrencePoint;
import org.ala.biocache.dto.PointType;
import org.ala.biocache.dto.SearchResultDTO;
import org.ala.biocache.dto.SpatialSearchRequestParams;
import org.ala.biocache.util.LegendItem;
import org.ala.biocache.util.SearchUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.context.ServletConfigAware;

/**
 * Webportal specific services.
 *
 */
@Controller
public class WebportalController implements ServletConfigAware {

    /** webportal results limit */
    private final int DEFAULT_PAGE_SIZE = 1000000;
    /** categorical colours */
    private final int[] colourList = {0x003366CC, 0x00DC3912, 0x00FF9900, 0x00109618, 0x00990099, 0x000099C6, 0x00DD4477, 0x0066AA00, 0x00B82E2E, 0x00316395, 0x00994499, 0x0022AA99, 0x00AAAA11, 0x006633CC, 0x00E67300, 0x008B0707, 0x00651067, 0x00329262, 0x005574A6, 0x003B3EAC, 0x00B77322, 0x0016D620, 0x00B91383, 0x00F4359E, 0x009C5935, 0x00A9C413, 0x002A778D, 0x00668D1C, 0x00BEA413, 0x000C5922, 0x00743411};
    private final int DEFAULT_COLOUR = 0x00000000;
    /** legend limits */
    private final String NULL_NAME = "Unknown";
    /** max uncertainty mappable in m */
    private final double MAX_UNCERTAINTY = 30000;
    private String baseMapPath = "/images/mapaus1_white.png";
    private ServletConfig cfg;
    /** Logger initialisation */
    private final static Logger logger = Logger.getLogger(WebportalController.class);
    /** Fulltext search DAO */
    @Inject
    protected SearchDAO searchDAO;
    /** Data Resource DAO */
    @Inject
    protected SearchUtils searchUtils;
    /** Load a smaller 256x256 png than java.image produces */
    final static byte[] blankImageBytes;

    static {
        byte[] b = null;
        try {
            RandomAccessFile raf = new RandomAccessFile(WebportalController.class.getResource("/blank.png").getFile(), "r");
            b = new byte[(int) raf.length()];
            raf.read(b);
            raf.close();
        } catch (Exception e) {
            logger.error("Unable to open blank image file");
        }
        blankImageBytes = b;
    }

    /**
     * Store query params list
     */
    @RequestMapping(value = {"/webportal/params"}, method = RequestMethod.POST)
    public
    @ResponseBody
    Long storeParams(SpatialSearchRequestParams requestParams,
            @RequestParam(value = "bbox", required = false, defaultValue = "false") String bbox) throws Exception {
        //cleanup Q by running a query
        //searchDAO.getSourcesForQuery(requestParams);

        //get bbox (also cleans up Q)
        double[] bb = null;
        if(bbox != null && bbox.equals("true")) {
            bb = getBBox(requestParams);
        }

        //store
        return ParamsCache.put(requestParams.getQ(), requestParams.getDisplayString(), requestParams.getWkt(), bb);
    }

    /**
     * Test presence of query params {id} in params store.
     */
    @RequestMapping(value = {"/webportal/params/{id}"}, method = RequestMethod.GET)
    public
    @ResponseBody
    Boolean storeParams(@PathVariable("id") Long id) throws Exception {
        return ParamsCache.get(id) != null;
    }

    /**
     *
     * JSON web service that returns a list of species and record counts for a given location search
     *
     * @param SpatialSearchRequestParams requestParams
     * @throws Exception
     */
    @RequestMapping(value = "/webportal/species", method = RequestMethod.GET)
    public
    @ResponseBody
    List<TaxaCountDTO> listSpecies(
            SpatialSearchRequestParams requestParams) throws Exception {

        return searchDAO.findAllSpecies(requestParams);
    }

    /**
     *
     * List of species for webportal as csv.
     *
     * @param SpatialSearchRequestParams
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = "/webportal/species.csv", method = RequestMethod.GET)
    public void listSpeciesCsv(
            SpatialSearchRequestParams requestParams,
            HttpServletResponse response) throws Exception {

        List<TaxaCountDTO> list = searchDAO.findAllSpecies(requestParams);

        //format as csv
        StringBuilder sb = new StringBuilder();
        sb.append("Family,Scientific name,Common name,Taxon rank,LSID,# Occurrences");
        for (TaxaCountDTO d : list) {
            String family = d.getFamily();
            String name = d.getName();
            String commonName = d.getCommonName();
            String guid = d.getGuid();
            String rank = d.getRank();

            if (family == null) {
                family = "";
            }
            if (name == null) {
                name = "";
            }
            if (commonName == null) {
                commonName = "";
            }

            if (d.getGuid() == null) {
                //when guid is empty name contains name_lsid value.
                if (d.getName() != null) {
                    //parse name
                    String[] nameLsid = d.getName().split("\\|");
                    if (nameLsid.length >= 2) {
                        name = nameLsid[0];
                        guid = nameLsid[1];
                        rank = "scientific name";

                        if (nameLsid.length >= 3) {
                            commonName = nameLsid[2];
                        }
//                        if(nameLsid.length >= 4) {
//                            kingdom = nameLsid[3];
//                        }
                    } else {
                        name = NULL_NAME;
                    }
                }
            }
            if (d.getCount() != null && guid != null) {
                sb.append("\n\"").append(family.replace("\"", "\"\"").trim()).append("\",\"").append(name.replace("\"", "\"\"").trim()).append("\",\"").append(commonName.replace("\"", "\"\"").trim()).append("\",").append(rank).append(",").append(guid).append(",").append(d.getCount());
            }
        }

        writeBytes(response, sb.toString().getBytes("UTF-8"));
    }

    /**
     * Get csv legend for a query and facet field (colourMode).
     *
     * @param requestParams
     * @param colourMode
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = "/webportal/legend", method = RequestMethod.GET)
    public void legend(
            SpatialSearchRequestParams requestParams,
            @RequestParam(value = "cm", required = false, defaultValue = "") String colourMode,
            HttpServletResponse response)
            throws Exception {

        //test for cutpoints on the back of colourMode
        String[] s = colourMode.split(",");
        String[] cutpoints = null;
        if (s.length > 1) {
            cutpoints = new String[s.length - 1];
            System.arraycopy(s, 1, cutpoints, 0, cutpoints.length);
        }
        List<LegendItem> legend = searchDAO.getLegend(requestParams, s[0], cutpoints);
        if (cutpoints == null) {
            java.util.Collections.sort(legend);
        }
        StringBuilder sb = new StringBuilder();
        sb.append("name,red,green,blue,count");
        int i = 0;
        //add legend entries.
        for (i = 0; i < legend.size(); i++) {
            String name = legend.get(i).getName();
            if (name == null) {
                name = NULL_NAME;
            }
            int colour = DEFAULT_COLOUR;
            if (cutpoints == null) {
                colour = colourList[Math.min(i, colourList.length - 1)];
            } else if (cutpoints != null && i < cutpoints.length - 1) {    //only NULL should be >= cutpoints.length -1
                colour = getRangedColour(i, cutpoints);
            }
            sb.append("\n\"").append(name.replace("\"","\"\"")).append("\",").append(getRGB(colour)) //repeat last colour if required
                    .append(",").append(legend.get(i).getCount());
        }

        writeBytes(response, sb.toString().getBytes("UTF-8"));
    }

    /**
     * List data providers for a query.
     *
     * @param requestParams
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/webportal/dataProviders", method = RequestMethod.GET)
    @ResponseBody
    public List<DataProviderCountDTO> queryInfo(
            SpatialSearchRequestParams requestParams)
            throws Exception {

        return searchDAO.getDataProviderList(requestParams);
    }

    /**
     * Get query bounding box as csv containing:
     *  min longitude, min latitude, max longitude, max latitude
     *
     * @param requestParams
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = "/webportal/bbox", method = RequestMethod.GET)
    public void boundingBox(
            SpatialSearchRequestParams requestParams,
            HttpServletResponse response)
            throws Exception {

        double[] bbox = null;

        String q = requestParams.getQ();
        if (q.startsWith("qid:")) {
            try {
                bbox = ParamsCache.get(Long.parseLong(q.substring(4))).getBbox();
            } catch (Exception e) {
            }
        }

        if (bbox == null) {
            bbox = getBBox(requestParams);
        }

        writeBytes(response, (bbox[0] + "," + bbox[1] + "," + bbox[2] + "," + bbox[3]).getBytes("UTF-8"));
    }

    /**
     * Get occurrences by query as JSON.
     *
     * @param requestParams
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = "/webportal/occurrences*", method = RequestMethod.GET)
    @ResponseBody
    public SearchResultDTO occurrences(
            SpatialSearchRequestParams requestParams,
            Model model) throws Exception {

        SearchResultDTO searchResult = new SearchResultDTO();

        if (StringUtils.isEmpty(requestParams.getQ())) {
            return searchResult;
        }

        //searchUtils.updateSpatial(requestParams);
        searchResult = searchDAO.findByFulltextSpatialQuery(requestParams);
        model.addAttribute("searchResult", searchResult);

        if (logger.isDebugEnabled()) {
            logger.debug("Returning results set with: " + searchResult.getTotalRecords());
        }

        return searchResult;
    }

    /**
     * Get occurrences by query as gzipped csv.
     *
     * @param requestParams
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = "/webportal/occurrences.gz", method = RequestMethod.GET)
    public void occurrenceGz(
            SpatialSearchRequestParams requestParams,
            HttpServletResponse response)
            throws Exception {

        response.setContentType("text/plain");
        response.setCharacterEncoding("gzip");

        ServletOutputStream outStream = response.getOutputStream();
        java.util.zip.GZIPOutputStream gzip = new java.util.zip.GZIPOutputStream(outStream);

        writeOccurrencesCsvToStream(requestParams, gzip);

        gzip.flush();
        gzip.close();

        
    }

    private void writeOccurrencesCsvToStream(SpatialSearchRequestParams requestParams, OutputStream stream) throws Exception {
        SolrDocumentList sdl = searchDAO.findByFulltext(requestParams);

        byte [] bComma = ",".getBytes("UTF-8");
        byte [] bNewLine = "\n".getBytes("UTF-8");
        byte [] bDblQuote = "\"".getBytes("UTF-8");

        if (sdl != null && sdl.size() > 0) {
            //header field identification
            ArrayList<String> header = new ArrayList<String>();
            if (requestParams.getFl() != null || requestParams.getFl().isEmpty()) {
                TreeSet<String> unique = new TreeSet<String>();
                for (int i = 0; i < sdl.size(); i++) {
                    unique.addAll(sdl.get(i).getFieldNames());
                }
                header = new ArrayList<String>(unique);
            } else {
                String[] fields = requestParams.getFl().split(",");
                for (int i = 0; i < fields.length; i++) {
                    if (fields[i].length() > 0) {
                        header.add(fields[i]);
                    }
                }
            }

            //write header
            for (int i = 0; i < header.size(); i++) {
                if (i > 0) {
                    stream.write(bComma);
                }
                stream.write(header.get(i).getBytes("UTF-8"));
            }

            //write records
            for (int i = 0; i < sdl.size(); i++) {
                stream.write(bNewLine);
                for (int j = 0; j < header.size(); j++) {
                    if (j > 0) {
                        stream.write(bComma);
                    }
                    if (sdl.get(i).containsKey(header.get(j))) {
                        stream.write(bDblQuote);
                        stream.write(String.valueOf(sdl.get(i).getFieldValue(header.get(j))).replace("\"", "\"\"").getBytes("UTF-8"));
                        stream.write(bDblQuote);
                    }
                }
            }
        }
    }

    private void writeBytes(HttpServletResponse response, byte[] bytes) throws IOException {
        response.setContentType("text/plain");
        response.setCharacterEncoding("UTF-8");
        ServletOutputStream outStream = response.getOutputStream();
        outStream.write(bytes);
        outStream.flush();
        outStream.close();
    }

    /**
     * WMS service for webportal.
     *
     * @param cql_filter q value.
     * @param env ';' delimited field:value pairs.  See Env
     * @param bboxString
     * @param width
     * @param height
     * @param request
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = "/webportal/wms/reflect", method = RequestMethod.GET)
    public void wms(
            @RequestParam(value = "CQL_FILTER", required = false, defaultValue = "") String cql_filter,
            @RequestParam(value = "ENV", required = false, defaultValue = "") String env,
            @RequestParam(value = "BBOX", required = false, defaultValue = "") String bboxString,
            @RequestParam(value = "WIDTH", required = false, defaultValue = "256") Integer width,
            @RequestParam(value = "HEIGHT", required = false, defaultValue = "256") Integer height,
            HttpServletRequest request, HttpServletResponse response)
            throws Exception {

        response.setHeader("Cache-Control", "max-age=86400"); //age == 1 day
        response.setContentType("image/png"); //only png images generated

        WmsEnv vars = new WmsEnv(env);
        double[] mbbox = new double[4];
        double[] bbox = new double[4];
        double[] pbbox = new double[4];
        getBBoxes(bboxString, width, height, vars.size, vars.uncertainty, mbbox, bbox, pbbox);
        String q = getQ(cql_filter);

        String[] filterQuery = new String[2];
        filterQuery[0] = String.format("longitude:[%f TO %f]", bbox[0], bbox[2]);
        filterQuery[1] = String.format("latitude:[%f TO %f]", bbox[1], bbox[3]);

        PointType pointType = getPointTypeForDegreesPerPixel(Math.min(
                (bbox[2] - bbox[0]) / (double) width,
                (bbox[3] - bbox[1]) / (double) height));

        int pointWidth = vars.size * 2;
        double width_mult = (width / (pbbox[2] - pbbox[0]));
        double height_mult = (height / (pbbox[1] - pbbox[3]));

        //build request
        SpatialSearchRequestParams requestParams = new SpatialSearchRequestParams();
        requestParams.setQ(q);

        //bounding box test (q must be 'qid:' + number)
        if (q.startsWith("qid:")) {
            double[] queryBBox = ParamsCache.get(Long.parseLong(q.substring(4))).getBbox();
            if (queryBBox != null && (queryBBox[0] > bbox[2] || queryBBox[2] < bbox[0]
                    || queryBBox[1] > bbox[3] || queryBBox[3] < bbox[1])) {
                displayBlankImage(response);
                return;
            }
        }

        requestParams.setFq(filterQuery);

        //colour mapping
        List<LegendItem> colours = (vars.colourMode.equals("-1") || vars.colourMode.equals("grid")) ? null : getColours(q, vars.colourMode);
        int sz = colours == null ? 1 : colours.size() + 1;
        int x, y;

        List<List<OccurrencePoint>> points = new ArrayList<List<OccurrencePoint>>(sz);
        List<Integer> pColour = new ArrayList<Integer>(sz);

        ArrayList<String> forNulls = new ArrayList<String>(sz);
        String[] fqs = new String[3];
        fqs[0] = filterQuery[0];
        fqs[1] = filterQuery[1];
        if (colours != null) {
            //get facet points
            for (int i = 0; i < colours.size(); i++) {
                LegendItem li = colours.get(i);
                fqs[2] = li.getFq();
                if (li.getName() == null) {
                    //li.getFq() is of the form "-(...)"
                    forNulls.add(fqs[2].substring(1));
                } else {
                    if(fqs[2].charAt(0) == '-'){
                        forNulls.add(fqs[2].substring(1));
                    } else {
                        forNulls.add("-" + fqs[2]);
                    }
                }
                requestParams.setFq(fqs);
                points.add(searchDAO.getFacetPoints(requestParams, pointType));
                pColour.add(li.getColour() | (vars.alpha << 24));
            }
        }
        //get points for occurrences not in colours.
        if (colours == null || colours.isEmpty()) {
            requestParams.setFq(filterQuery); //only filter by bounding box
            points.add(searchDAO.getFacetPoints(requestParams, pointType));
            pColour.add(vars.colour);
        } else if(colours.size() >= colourList.length - 1) {
            fqs = new String[forNulls.size()];
            forNulls.toArray(fqs);
            requestParams.setFq(fqs);
            points.add(searchDAO.getFacetPoints(requestParams, pointType));
            pColour.add(colourList[colourList.length - 1] | (vars.alpha << 24));
        }

        BufferedImage img = null;
        Graphics2D g = null;

        //grid setup
        int divs = 16; //number of x & y divisions in the WIDTH/HEIGHT
        int[][] gridCounts = new int[divs][divs];
        int xstep = 256 / divs;
        int ystep = 256 / divs;
        double grid_width_mult = (width / (pbbox[2] - pbbox[0])) / (width / divs);
        double grid_height_mult = (height / (pbbox[1] - pbbox[3])) / (height / divs);

        for (int j = 0; j < points.size(); j++) {
            List<OccurrencePoint> ps = points.get(j);

            if (ps == null || ps.isEmpty()) {
                continue;
            }

            if (img == null) {
                img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
                g = (Graphics2D) img.getGraphics();
                g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            }

            if (vars.colourMode.equals("grid")) {
                //populate grid
                for (int i = 0; i < ps.size(); i++) {
                    OccurrencePoint pt = ps.get(i);
                    float lng = pt.getCoordinates().get(0).floatValue();
                    float lat = pt.getCoordinates().get(1).floatValue();

                    x = (int) ((convertLngToPixel(lng) - pbbox[0]) * grid_width_mult);
                    y = (int) ((convertLatToPixel(lat) - pbbox[3]) * grid_height_mult);

                    if (x >= 0 && x < divs && y >= 0 && y < divs) {
                        gridCounts[x][y]+=pt.getCount();
                    }
                }
            } else {
                g.setPaint(new Color(pColour.get(j), true));

                for (int i = 0; i < ps.size(); i++) {
                    OccurrencePoint pt = ps.get(i);
                    float lng = pt.getCoordinates().get(0).floatValue();
                    float lat = pt.getCoordinates().get(1).floatValue();

                    x = (int) ((convertLngToPixel(lng) - pbbox[0]) * width_mult);
                    y = (int) ((convertLatToPixel(lat) - pbbox[3]) * height_mult);

                    g.fillOval(x - vars.size, y - vars.size, pointWidth, pointWidth);
                }
            }
        }

        //no points
        if (img == null) {
            displayBlankImage(response);
            return;
        }

        if (vars.colourMode.equals("grid")) {
            //draw grid
            for (x = 0; x < divs; x++) {
                for (y = 0; y < divs; y++) {
                    int v = gridCounts[x][y];
                    if (v > 0) {
                        if (v > 500) {
                            v = 500;
                        }
                        int colour = (((500 - v) / 2) << 8) | (vars.alpha << 24) | 0x00FF0000;
                        g.setColor(new Color(colour));
                        g.fillRect(x * xstep, y * ystep, xstep, ystep);
                    }
                }
            }
        } else {
            //draw uncertainty circles
            double hmult = (height / (mbbox[3] - mbbox[1]));

            //only draw uncertainty if max radius will be > 5 pixels
            if (vars.uncertainty && MAX_UNCERTAINTY * hmult > 5) {

                //uncertainty colour/fq/radius, [0]=map, [1]=not specified, [2]=too large
                Color[] uncertaintyColours = {new Color(255, 255, 255, vars.alpha), new Color(255, 255, 100, vars.alpha), new Color(100, 255, 100, vars.alpha)};
                //TODO: don't assume MAX_UNCERTAINTY > default_uncertainty
                String[] uncertaintyFqs = {"coordinate_uncertainty:[* TO " + MAX_UNCERTAINTY + "] AND -assertions:uncertaintyNotSpecified", "assertions:uncertaintyNotSpecified", "coordinate_uncertainty:[" + MAX_UNCERTAINTY + " TO *]"};
                double[] uncertaintyR = {-1, MAX_UNCERTAINTY, MAX_UNCERTAINTY};

                fqs = new String[3];
                fqs[0] = filterQuery[0];
                fqs[1] = filterQuery[1];

                requestParams.setPageSize(DEFAULT_PAGE_SIZE);

                for (int j = 0; j < uncertaintyFqs.length; j++) {
                    //do not display for [1]=not specified
                    if (j == 1) {
                        continue;
                    }

                    fqs[2] = uncertaintyFqs[j];
                    requestParams.setFq(fqs);
                    requestParams.setFl("longitude,latitude,coordinate_uncertainty"); //only retrieve longitude and latitude

                    //TODO: paging
                    SolrDocumentList sdl = searchDAO.findByFulltext(requestParams);

                    double lng, lat;
                    int uncertaintyRadius = (int) Math.ceil(uncertaintyR[j] * hmult);
                    if (sdl != null && sdl.size() > 0) {
                        g.setColor(uncertaintyColours[j]);
                        for (int i = 0; i < sdl.size(); i++) {
                            if (uncertaintyR[j] < 0) {
                                uncertaintyRadius = (int) Math.ceil((Double) sdl.get(i).getFieldValue("coordinate_uncertainty") * hmult);
                            }
                            
                            lng = (Double) sdl.get(i).getFieldValue("longitude");
                            lat = (Double) sdl.get(i).getFieldValue("latitude");

                            x = (int) ((convertLngToPixel(lng) - pbbox[0]) * width_mult);
                            y = (int) ((convertLatToPixel(lat) - pbbox[3]) * height_mult);

                            if (uncertaintyRadius > 0) {
                                g.drawOval(x - uncertaintyRadius, y - uncertaintyRadius, uncertaintyRadius * 2, uncertaintyRadius * 2);
                            } else {
                                g.drawRect(x, y, 1, 1);
                            }
                        }
                    }
                }
            }

            //highlight
            if (vars.highlight != null) {
                fqs = new String[3];
                fqs[0] = filterQuery[0];
                fqs[1] = filterQuery[1];
                fqs[2] = vars.highlight;

                requestParams.setFq(fqs);
                List<OccurrencePoint> ps = searchDAO.getFacetPoints(requestParams, pointType);

                if (ps != null && ps.size() > 0) {
                    g.setColor(new Color(255, 0, 0, vars.alpha));
                    for (int i = 0; i < ps.size(); i++) {
                        OccurrencePoint pt = ps.get(i);
                        float lng = pt.getCoordinates().get(0).floatValue();
                        float lat = pt.getCoordinates().get(1).floatValue();

                        x = (int) ((convertLngToPixel(lng) - pbbox[0]) * width_mult);
                        y = (int) ((convertLatToPixel(lat) - pbbox[3]) * height_mult);

                        g.drawOval(x - vars.size, y - vars.size, pointWidth, pointWidth);
                    }
                }
            }
        }

        if (g != null) {
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
        } else {
            displayBlankImage(response);
        }
    }
    /** 4326 to 900913 pixel and m conversion */
    private int map_offset = 268435456; // half the Earth's circumference at zoom level 21
    private double map_radius = map_offset / Math.PI;

    int convertLatToPixel(double lat) {
        return (int) Math.round(map_offset - map_radius
                * Math.log((1 + Math.sin(lat * Math.PI / 180))
                / (1 - Math.sin(lat * Math.PI / 180))) / 2);
    }

    int convertLngToPixel(double lng) {
        return (int) Math.round(map_offset + map_radius * lng * Math.PI / 180);
    }

    double convertMetersToLng(double meters) {
        return meters / 20037508.342789244 * 180;
    }

    double convertMetersToLat(double meters) {
        return 180.0 / Math.PI * (2 * Math.atan(Math.exp(meters / 20037508.342789244 * Math.PI)) - Math.PI / 2.0);
    }

    /**
     * Map a zoom level to a coordinate accuracy level
     *
     * @param zoomLevel
     * @return
     */
    protected PointType getPointTypeForDegreesPerPixel(double resolution) {
        PointType pointType = null;
        // Map zoom levels to lat/long accuracy levels
        if (resolution >= 1) {
            pointType = PointType.POINT_1;
        } else if (resolution >= 0.1) {
            pointType = PointType.POINT_01;
        } else if (resolution >= 0.01) {
            pointType = PointType.POINT_001;
        } else if (resolution >= 0.001) {
            pointType = PointType.POINT_0001;
        } else if (resolution >= 0.0001) {
            pointType = PointType.POINT_00001;
        } else {
            pointType = PointType.POINT_RAW;
        }
        return pointType;
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

    void displayBlankImage(HttpServletResponse response) {
        try {
            ServletOutputStream outStream = response.getOutputStream();
            outStream.write(blankImageBytes);
            outStream.flush();
            outStream.close();
        } catch (Exception e) {
            logger.error("Unable to write image", e);
        }
    }

    private void getBBoxes(String bboxString, int width, int height, int size, boolean uncertainty, double[] mbbox, double[] bbox, double[] pbbox) {
        int i = 0;
        for (String s : bboxString.split(",")) {
            try {
                mbbox[i] = Double.parseDouble(s);
                i++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //adjust bbox extents with half pixel width/height
        double pixelWidth = (mbbox[2] - mbbox[0]) / width;
        double pixelHeight = (mbbox[3] - mbbox[1]) / height;
        mbbox[0] += pixelWidth / 2;
        mbbox[2] -= pixelWidth / 2;
        mbbox[1] += pixelHeight / 2;
        mbbox[3] -= pixelHeight / 2;

        //offset for points bounding box by dot size
        double xoffset = (mbbox[2] - mbbox[0]) / (double) width * size;
        double yoffset = (mbbox[3] - mbbox[1]) / (double) height * size;

        //check offset for points bb by maximum uncertainty
        if (uncertainty) {
            if (xoffset < MAX_UNCERTAINTY) {
                xoffset = MAX_UNCERTAINTY;
            }
            if (yoffset < MAX_UNCERTAINTY) {
                yoffset = MAX_UNCERTAINTY;
            }
        }

        //adjust offset for pixel height/width
        xoffset += pixelWidth;
        yoffset += pixelHeight;

        pbbox[0] = convertLngToPixel(convertMetersToLng(mbbox[0]));
        pbbox[1] = convertLatToPixel(convertMetersToLat(mbbox[1]));
        pbbox[2] = convertLngToPixel(convertMetersToLng(mbbox[2]));
        pbbox[3] = convertLatToPixel(convertMetersToLat(mbbox[3]));

        bbox[0] = convertMetersToLng(mbbox[0] - xoffset);
        bbox[1] = convertMetersToLat(mbbox[1] - yoffset);
        bbox[2] = convertMetersToLng(mbbox[2] + xoffset);
        bbox[3] = convertMetersToLat(mbbox[3] + yoffset);
    }

    private String getQ(String cql_filter) {
        String q = cql_filter;
        int p1 = cql_filter.indexOf("qid:");
        if (p1 >= 0) {
            int p2 = cql_filter.indexOf('&', p1 + 1);
            if (p2 < 0) {
                p2 = cql_filter.indexOf(';', p1 + 1);
            }
            if (p2 < 0) {
                p2 = cql_filter.length();
            }
            q = cql_filter.substring(p1, p2);
        }
        return q;
    }

    /**
     * Get legend items for the first colourList.length-1 items only.
     *
     * @param q
     * @param colourMode
     * @return
     * @throws Exception
     */
    private List<LegendItem> getColours(String q, String colourMode) throws Exception {
        List<LegendItem> colours = new ArrayList<LegendItem>();
        if (colourMode.equals("grid")) {
            for (int i = 0; i <= 500; i += 100) {
                LegendItem li;
                if (i == 0) {
                    li = new LegendItem(">0", 0, null);
                } else {
                    li = new LegendItem(String.valueOf(i), 0, null);
                }
                li.setColour((((500 - i) / 2) << 8) | 0x00FF0000);
                colours.add(li);
            }
        } else {
            SpatialSearchRequestParams requestParams = new SpatialSearchRequestParams();
            requestParams.setQ(q);

            //test for cutpoints on the back of colourMode
            String[] s = colourMode.split(",");
            String[] cutpoints = null;
            if (s.length > 1) {
                cutpoints = new String[s.length - 1];
                System.arraycopy(s, 1, cutpoints, 0, cutpoints.length);
            }
            List<LegendItem> legend = searchDAO.getLegend(requestParams, s[0], cutpoints);
            if (cutpoints == null) {     //do not sort if cutpoints are provided
                java.util.Collections.sort(legend);
            }
            int i = 0;
            for (i = 0; i < legend.size() && i < colourList.length - 1; i++) {
                colours.add(new LegendItem(legend.get(i).getName(), legend.get(i).getCount(), legend.get(i).getFq()));
                int colour = DEFAULT_COLOUR;
                if (cutpoints == null) {
                    colour = colourList[i];
                } else if (cutpoints != null && i < cutpoints.length - 1) {    //only NULL should be >= cutpoints.length -1
                    colour = getRangedColour(i, cutpoints);
                }
                colours.get(colours.size() - 1).setColour(colour);
            }
        }

        return colours;
    }

    int getRangedColour(int pos, String[] cutpoints) {
        int[] colourRange = {0x00002DD0, 0x00005BA2, 0x00008C73, 0x0000B944, 0x0000E716, 0x00A0FF00, 0x00FFFF00, 0x00FFC814, 0x00FFA000, 0x00FF5B00, 0x00FF0000};

        double step = 1 / (double) colourRange.length;
        double p = pos / (double) (cutpoints.length - 1);
        double dist = p / step;

        int minI = (int) Math.floor(dist);
        int maxI = (int) Math.ceil(dist);
        if (maxI >= colourRange.length) {
            maxI = colourRange.length - 1;
        }

        double minorP = p - (minI * step);
        double minorDist = minorP / step;

        //scale RGB individually
        int colour = 0x00000000;
        for (int i = 0; i < 3; i++) {
            int minC = (colourRange[minI] >> (i * 8)) & 0x000000ff;
            int maxC = (colourRange[maxI] >> (i * 8)) & 0x000000ff;
            int c = Math.min((int) ((maxC - minC) * minorDist + minC), 255);

            colour = colour | ((c & 0x000000ff) << (i * 8));
        }

        return colour;
    }

    String getRGB(int colour) {
        return ((colour >> 16) & 0x000000ff) + ","
                + ((colour >> 8) & 0x000000ff) + ","
                + (colour & 0x000000ff);
    }

    /**
     * Get bounding box for a query.
     *
     * @param requestParams
     * @return
     * @throws Exception
     */
    double[] getBBox(SpatialSearchRequestParams requestParams) throws Exception {
        double[] bbox = new double[4];
        String[] sort = {"longitude", "latitude", "longitude", "latitude"};
        String[] dir = {"asc", "asc", "desc", "desc"};

        //remove instances of null longitude or latitude
        String[] fq = (String[])ArrayUtils.addAll(requestParams.getFq(),new String[]{"longitude:[* TO *]", "latitude:[* TO *]"});
        requestParams.setFq(fq);        
        requestParams.setPageSize(10);        
        
        for (int i = 0; i < sort.length; i++) {
            requestParams.setSort(sort[i]);
            requestParams.setDir(dir[i]);
            requestParams.setFl(sort[i]);

            SolrDocumentList sdl = searchDAO.findByFulltext(requestParams);
            if (sdl != null && sdl.size() > 0) {
                if (sdl.get(0) != null) {
                    bbox[i] = (Double) sdl.get(0).getFieldValue(sort[i]);
                } else {
                    logger.error("searchDAO.findByFulltext returning SolrDocumentList with null records");
                }
            }
        }

        return bbox;
    }
}

class WmsEnv {

    public int red, green, blue, alpha, size, colour;
    public boolean uncertainty;
    public String colourMode, highlight;

    /**
     * Get WMS ENV values from String, or use defaults.
     *
     * @param env
     */
    public WmsEnv(String env) {
        try {
            env = URLDecoder.decode(env, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        red = green = blue = alpha = 0;
        size = 4;
        uncertainty = false;
        highlight = null;
        colourMode = "-1";
        colour = 0x00000000;

        for (String s : env.split(";")) {
            String[] pair = s.split(":");
            if (pair[0].equals("color")) {
                while (pair[1].length() < 6) {
                    pair[1] = "0" + pair[1];
                }
                red = Integer.parseInt(pair[1].substring(0, 2), 16);
                green = Integer.parseInt(pair[1].substring(2, 4), 16);
                blue = Integer.parseInt(pair[1].substring(4), 16);
                colour = (red << 16) | (green << 8) | blue;
            } else if (pair[0].equals("size")) {
                size = Integer.parseInt(pair[1]);
            } else if (pair[0].equals("opacity")) {
                alpha = (int) (255 * Double.parseDouble(pair[1]));
            } else if (pair[0].equals("uncertainty")) {
                uncertainty = true;
            } else if (pair[0].equals("sel")) {
                highlight = s.replace("sel:", "");
            } else if (pair[0].equals("colormode")) {
                colourMode = pair[1];
            }
        }

        colour = colour | (alpha << 24);
    }
}
