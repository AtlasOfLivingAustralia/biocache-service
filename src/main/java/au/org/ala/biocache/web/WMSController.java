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

import au.org.ala.biocache.dao.QidCacheDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dao.TaxonDAO;
import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.model.Qid;
import au.org.ala.biocache.util.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.googlecode.ehcache.annotations.Cacheable;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import javax.imageio.ImageIO;
import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;
import java.util.List;

/**
 * This controller provides mapping services which include WMS services. Includes support for:
 *
 * <ul>
 *    <li>GetCapabilities</li>
 *    <li>GetMap</li>
 *    <li>GetFeatureInfo</li>
 *    <li>GetMetadata</li>
 * </ul>
 */
@Controller
public class WMSController {

    /**
     * webportal results limit
     */
    private final int DEFAULT_PAGE_SIZE = 1000000;
    /**
     * categorical colours
     */
    private final int[] colourList = {0x003366CC, 0x00DC3912, 0x00FF9900, 0x00109618, 0x00990099, 0x000099C6, 0x00DD4477,
            0x0066AA00, 0x00B82E2E, 0x00316395, 0x00994499, 0x0022AA99, 0x00AAAA11, 0x006633CC, 0x00E67300, 0x008B0707,
            0x00651067, 0x00329262, 0x005574A6, 0x003B3EAC, 0x00B77322, 0x0016D620, 0x00B91383, 0x00F4359E, 0x009C5935,
            0x00A9C413, 0x002A778D, 0x00668D1C, 0x00BEA413, 0x000C5922, 0x00743411};
    //For WMS services
    final String[] colorsNames = new String[]{
            "DarkRed", "IndianRed", "DarkSalmon", "SaddleBrown", "Chocolate", "SandyBrown", "Orange", "DarkGreen", "Green", "Lime", "LightGreen", "MidnightBlue", "Blue",
            "SteelBlue", "CadetBlue", "Aqua", "PowderBlue", "DarkOliveGreen", "DarkKhaki", "Yellow", "Moccasin", "Indigo", "Purple", "Fuchsia", "Plum", "Black", "White"
    };
    final String[] colorsCodes = new String[]{
            "8b0000", "FF0000", "CD5C5C", "E9967A", "8B4513", "D2691E", "F4A460", "FFA500", "006400", "008000", "00FF00", "90EE90", "191970", "0000FF",
            "4682B4", "5F9EA0", "00FFFF", "B0E0E6", "556B2F", "BDB76B", "FFFF00", "FFE4B5", "4B0082", "800080", "FF00FF", "DDA0DD", "000000", "FFFFFF"
    };

    @Value("${wms.colour:0x00000000}")
    private int DEFAULT_COLOUR;
    /**
     * webportal image max pixel count
     */
    @Value("${wms.image.pixel.count:36000000}")
    private int MAX_IMAGE_PIXEL_COUNT; //this is slightly larger than 600dpi A4
    /**
     * legend limits
     */
    private final String NULL_NAME = "Unknown";
    /**
     * max uncertainty mappable in m
     */
    @Value("${wms.uncertainty.max:30000}")
    private double MAX_UNCERTAINTY;
    /**
     * add pixel radius for wms highlight circles
     */
    @Value("${wms.highlight.radius:3}")
    private static int HIGHLIGHT_RADIUS;
    /**
     * Global wms cache enable. WMS requests can disable adding to the cache using CACHE=off. WMS CACHE=off will still
     * read from the cache.
     */
    @Value("${wms.cache.enabled:true}")
    private boolean wmsCacheEnabled;
    /**
     * Logger initialisation
     */
    private final static Logger logger = Logger.getLogger(WMSController.class);
    /**
     * Fulltext search DAO
     */
    @Inject
    protected SearchDAO searchDAO;
    @Inject
    protected TaxonDAO taxonDAO;
    @Inject
    protected SearchUtils searchUtils;
    @Inject
    protected QidCacheDAO qidCacheDAO;
    @Inject
    protected WMSCache wmsCache;
    /**
     * Load a smaller 256x256 png than java.image produces
     */
    final static byte[] blankImageBytes;

    @Value("${webservices.root:http://biocache.ala.org.au/ws}")
    protected String baseWsUrl;
    @Value("${geoserver.url:http://spatial.ala.org.au/geoserver}")
    protected String geoserverUrl;

    @Value("${organizationName:Atlas of Living Australia}")
    protected String organizationName;
    @Value("${orgCity:Canberra}")
    protected String orgCity;
    @Value("${orgStateProvince:ACT}")
    protected String orgStateProvince;
    @Value("${orgPostcode:2601}")
    protected String orgPostcode;
    @Value("${orgCountry:Australia}")
    protected String orgCountry;
    @Value("${orgPhone:+61 (0) 2 6246 4400}")
    protected String orgPhone;
    @Value("${orgFax:+61 (0) 2 6246 4400}")
    protected String orgFax;
    @Value("${orgEmail:support@ala.org.au}")
    protected String orgEmail;

    @Value("${service.bie.ws.url:http://bie.ala.org.au/ws}")
    protected String bieWebService;

    @Value("${service.bie.ui.url:http://bie.ala.org.au}")
    protected String bieUiUrl;

    /**
     * Limit WKT complexity to reduce index query time for qids.
     */
    @Value("${qid.wkt.maxPoints:5000}")
    private int maxWktPoints;

    /**
     * Threshold for caching a whole PointType for a query or only caching the current bounding box.
     */
    @Value("${wms.cache.maxLayerPoints:100000}")
    private int wmsCacheMaxLayerPoints;

    /**
     * Occurrence count where < uses pivot and > uses facet for retrieving points. Can be fine tuned with
     * multiple queries and comparing DEBUG *
     */
    @Value("${wms.facetPivotCutoff:2000}")
    private int wmsFacetPivotCutoff;

    @Inject
    protected WMSUtils wmsUtils;

    //Stores query hashes + occurrence counts, and, query hashes + pointType + point counts
    private LRUMap countsCache = new LRUMap(10000);
    private Object countLock = new Object();

    @Inject
    protected WMSOSGridController wmsosGridController;


    static {
        byte[] b = null;
        try {
            RandomAccessFile raf = new RandomAccessFile(WMSController.class.getResource("/blank.png").getFile(), "r");
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
    @RequestMapping(value = {"/webportal/params", "/mapping/params"}, method = RequestMethod.POST)
    public void storeParams(SpatialSearchRequestParams requestParams,
                            @RequestParam(value = "bbox", required = false, defaultValue = "false") String bbox,
                            @RequestParam(value = "title", required = false) String title,
                            @RequestParam(value = "maxage", required = false, defaultValue = "-1") Long maxage,
                            @RequestParam(value = "source", required = false) String source,
                            HttpServletResponse response) throws Exception {

        //simplify wkt
        String wkt = requestParams.getWkt();
        if (wkt != null && wkt.length() > 0) {
            //TODO: Is this too slow? Do not want to send large WKT to SOLR.
            wkt = fixWkt(wkt);

            if (wkt == null) {
                //wkt too large and simplification failed, do not produce qid
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "WKT provided has more than " + maxWktPoints + " points and failed to be simplified.");
                return;
            }

            //set wkt
            requestParams.setWkt(wkt);
        }

        //get bbox (also cleans up Q)
        double[] bb = null;
        if (bbox != null && bbox.equals("true")) {
            bb = getBBox(requestParams);
        } else {
            //get a formatted Q by running a query
            requestParams.setPageSize(0);
            requestParams.setFacet(false);
            searchDAO.findByFulltext(requestParams);
        }

        //store the title if necessary
        if (title == null)
            title = requestParams.getDisplayString();
        String[] fqs = wmsUtils.getFq(requestParams);
        if (fqs != null && fqs.length == 1 && fqs[0].length() == 0) {
            fqs = null;
        }
        String qid = qidCacheDAO.put(requestParams.getFormattedQuery(), title, requestParams.getWkt(), bb, fqs, maxage, source);

        response.setContentType("text/plain");
        writeBytes(response, qid.getBytes());
    }

    @Cacheable(cacheName = "fixWkt")
    private String fixWkt(String wkt) {
        return SpatialUtils.simplifyWkt(wkt, maxWktPoints);
    }

    /**
     * Test presence of query params {id} in params store.
     */
    @RequestMapping(value = {"/webportal/params/{id}", "/mapping/params/{id}"}, method = RequestMethod.GET)
    public
    @ResponseBody
    Boolean storeParams(@PathVariable("id") Long id) throws Exception {
        return qidCacheDAO.get(String.valueOf(id)) != null;
    }

    /**
     * Allows the details of a cached query to be viewed.
     *
     * @param id
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {"/webportal/params/details/{id}", "/mapping/params/details/{id}"}, method = RequestMethod.GET)
    public
    @ResponseBody
    Qid getQid(@PathVariable("id") Long id) throws Exception {
        return qidCacheDAO.get(String.valueOf(id));
    }

    /**
     * JSON web service that returns a list of species and record counts for a given location search
     *
     * @throws Exception
     */
    @RequestMapping(value = {"/webportal/species", "/mapping/species"}, method = RequestMethod.GET)
    public
    @ResponseBody
    List<TaxaCountDTO> listSpecies(SpatialSearchRequestParams requestParams) throws Exception {
        return searchDAO.findAllSpecies(requestParams);
    }

    /**
     * List of species for webportal as csv.
     *
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = {"/webportal/species.csv", "/mapping/species.csv"}, method = RequestMethod.GET)
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
     * Get legend for a query and facet field (colourMode).
     *
     * if "Accept" header is application/json return json otherwise
     *
     * @param requestParams
     * @param colourMode
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = {"/webportal/legend", "/mapping/legend"}, method = RequestMethod.GET)
    @ResponseBody
    public List<LegendItem> legend(
            SpatialSearchRequestParams requestParams,
            @RequestParam(value = "cm", required = false, defaultValue = "") String colourMode,
            @RequestParam(value = "type", required = false, defaultValue = "application/csv") String returnType,
            HttpServletRequest request,
            HttpServletResponse response)
            throws Exception {

        String[] acceptableTypes = new String[]{"application/json", "application/csv"};

        String accepts = request.getHeader("Accept");
        //only allow a single format to be supplied in the header otherwise use the default returnType
        returnType = StringUtils.isNotEmpty(accepts) && !accepts.contains(",") ? accepts : returnType;
        if (!Arrays.asList(acceptableTypes).contains(returnType)) {
            response.sendError(response.SC_NOT_ACCEPTABLE, "Unable to produce a legend in the supplied \"Accept\" format: " + returnType);
            return null;
        }
        boolean isCsv = returnType.equals("application/csv");
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
        if (isCsv) {
            sb.append("name,red,green,blue,count");
        }
        int i = 0;
        //add legend entries.
        int offset = 0;
        for (i = 0; i < legend.size(); i++) {
            LegendItem li = legend.get(i);
            String name = li.getName();
            if (StringUtils.isEmpty(name)) {
                name = NULL_NAME;
            }
            int colour = DEFAULT_COLOUR;
            if (cutpoints == null) {
                colour = colourList[Math.min(i, colourList.length - 1)];
            } else if (cutpoints != null && i - offset < cutpoints.length) {
                if (name.equals(NULL_NAME) || name.startsWith("-")) {
                    offset++;
                    colour = DEFAULT_COLOUR;
                } else {
                    colour = getRangedColour(i - offset, cutpoints.length / 2);
                }
            }
            li.setRGB(colour);
            if (isCsv) {
                sb.append("\n\"").append(name.replace("\"", "\"\"")).append("\",").append(getRGB(colour)) //repeat last colour if required
                        .append(",").append(legend.get(i).getCount());
            }
        }

        //now generate the JSON if necessary
        if (returnType.equals("application/json")) {
            return legend;
        } else {
            writeBytes(response, sb.toString().getBytes("UTF-8"));
            return null;
        }
    }

    /**
     * List data providers for a query.
     *
     * @param requestParams
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {"/webportal/dataProviders", "/mapping/dataProviders"}, method = RequestMethod.GET)
    @ResponseBody
    public List<DataProviderCountDTO> queryInfo(
            SpatialSearchRequestParams requestParams)
            throws Exception {
        return searchDAO.getDataProviderList(requestParams);
    }

    /**
     * Get query bounding box as csv containing:
     * min longitude, min latitude, max longitude, max latitude
     *
     * @param requestParams
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = {"/webportal/bbox", "/mapping/bbox"}, method = RequestMethod.GET)
    public void boundingBox(
            SpatialSearchRequestParams requestParams,
            HttpServletResponse response)
            throws Exception {

        double[] bbox = null;

        if (bbox == null) {
            bbox = getBBox(requestParams);
        }

        writeBytes(response, (bbox[0] + "," + bbox[1] + "," + bbox[2] + "," + bbox[3]).getBytes("UTF-8"));
    }

    /**
     * Get query bounding box as JSON array containing:
     * min longitude, min latitude, max longitude, max latitude
     *
     * @param requestParams
     * @param response
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {"/webportal/bounds", "/mapping/bounds"}, method = RequestMethod.GET)
    public
    @ResponseBody
    double[] jsonBoundingBox(
            SpatialSearchRequestParams requestParams,
            HttpServletResponse response)
            throws Exception {

        double[] bbox = null;

        String q = requestParams.getQ();
        //when requestParams only contain a qid, get the bbox from the qidCache
        if (q.startsWith("qid:") && StringUtils.isEmpty(requestParams.getWkt()) &&
                (requestParams.getFq().length == 0 ||
                        (requestParams.getFq().length == 1 && StringUtils.isEmpty(requestParams.getFq()[0])))) {
            try {
                bbox = qidCacheDAO.get(q.substring(4)).getBbox();
            } catch (Exception e) {
            }
        }

        if (bbox == null) {
            bbox = getBBox(requestParams);
        }

        return bbox;
    }

    /**
     * Get occurrences by query as JSON.
     *
     * @param requestParams
     * @throws Exception
     */
    @RequestMapping(value = {"/webportal/occurrences*", "/mapping/occurrences*"}, method = RequestMethod.GET)
    @ResponseBody
    public SearchResultDTO occurrences(
            SpatialSearchRequestParams requestParams,
            Model model) throws Exception {

        SearchResultDTO searchResult = new SearchResultDTO();

        if (StringUtils.isEmpty(requestParams.getQ())) {
            return searchResult;
        }

        //searchUtils.updateSpatial(requestParams);
        searchResult = searchDAO.findByFulltextSpatialQuery(requestParams, null);
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
    @RequestMapping(value = {"/webportal/occurrences.gz", "/mapping/occurrences.gz"}, method = RequestMethod.GET)
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

        byte[] bComma = ",".getBytes("UTF-8");
        byte[] bNewLine = "\n".getBytes("UTF-8");
        byte[] bDblQuote = "\"".getBytes("UTF-8");

        if (sdl != null && sdl.size() > 0) {
            //header field identification
            ArrayList<String> header = new ArrayList<String>();
            if (requestParams.getFl() == null || requestParams.getFl().isEmpty()) {
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
     * 4326 to 900913 pixel and m conversion
     */
    private int map_offset = 268435456; // half the Earth's circumference at zoom level 21
    private double map_radius = map_offset / Math.PI;

    int convertLatToPixel(double lat) {
        return (int) Math.round(map_offset - map_radius
                * Math.log((1 + Math.sin(lat * Math.PI / 180))
                / (1 - Math.sin(lat * Math.PI / 180))) / 2);
    }

    int convertLatToPixel4326(double lat, double top, double bottom, int pixelHeight) {
        return (int) (((lat - top) / (bottom - top)) * pixelHeight);
    }

    int convertLngToPixel4326(double lng, double left, double right, int pixelWidth) {
        return (int) (((lng - left) / (right - left)) * pixelWidth);
    }

    int convertLngToPixel(double lng) {
        return (int) Math.round(map_offset + map_radius * lng * Math.PI / 180);
    }

    double convertMetersToLng(double meters) {
        return meters / 20037508.342789244 * 180;
    }

    //    //http://mapsforge.googlecode.com/svn-history/r1841/trunk/mapsforge-map-writer/src/main/java/org/mapsforge/map/writer/model/MercatorProjection.java
    double convertLngToMeters(double lng) {
        return 6378137.0 * Math.PI / 180 * lng;
    }

    //
    //    public static final double WGS_84_EQUATORIALRADIUS = 6378137.0;
    double convertLatToMeters(double lat) {
        return 6378137.0 * Math.log(Math.tan(Math.PI / 180 * (45 + lat / 2.0)));
    }

    double convertMetersToLat(double meters) {
        return 180.0 / Math.PI * (2 * Math.atan(Math.exp(meters / 20037508.342789244 * Math.PI)) - Math.PI / 2.0);
    }

    /**
     * Map a zoom level to a coordinate accuracy level
     *
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

    /**
     * @param bboxString
     * @param width
     * @param height
     * @param size
     * @param uncertainty
     * @param mbbox       the mbbox to initialise
     * @param bbox        the bbox to initialise
     * @param pbbox       the pbbox to initialise
     * @return
     */
    private double getBBoxes(String bboxString, int width, int height, int size, boolean uncertainty, double[] mbbox, double[] bbox, double[] pbbox, double[] tilebbox) {
        int i = 0;
        for (String s : bboxString.split(",")) {
            try {
                tilebbox[i] = Double.parseDouble(s);
                mbbox[i] = tilebbox[i];
                i++;
            } catch (Exception e) {
                logger.error("Problem parsing BBOX: '" + bboxString + "'", e);
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

        double degreesPerPixel = Math.min((convertMetersToLng(mbbox[2]) - convertMetersToLng(mbbox[0])) / (double) width,
                (convertMetersToLng(mbbox[3]) - convertMetersToLng(mbbox[1])) / (double) height);
        return degreesPerPixel;
    }

    /**
     * @param bboxString
     * @param width
     * @param height
     * @param size
     * @param uncertainty
     * @param mbbox       the mbbox to initialise
     * @param bbox        the bbox to initialise
     * @param pbbox       the pbbox to initialise
     * @return
     */
    private double getBBoxes4326(String bboxString, int width, int height, int size, boolean uncertainty, double[] mbbox, double[] bbox, double[] pbbox, double[] tilebbox) {
        int i = 0;
        for (String s : bboxString.split(",")) {
            try {
                tilebbox[i] = Double.parseDouble(s);
                mbbox[i] = tilebbox[i];
                i++;
            } catch (Exception e) {
                logger.error("Problem parsing BBOX: '" + bboxString + "'", e);
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

        //adjust offset for pixel height/width
        xoffset += pixelWidth;
        yoffset += pixelHeight;

        //actual bounding box
        bbox[0] = mbbox[0] - xoffset;
        bbox[1] = mbbox[1] - yoffset;
        bbox[2] = mbbox[2] + xoffset;
        bbox[3] = mbbox[3] + yoffset;

        double degreesPerPixel = Math.min(pixelWidth, pixelHeight);
        return degreesPerPixel;
    }

    /**
     * Get legend items for the first colourList.length-1 items only.
     *
     * @param colourMode
     * @throws Exception
     */
    @Cacheable(cacheName = "getColours")
    private List<LegendItem> getColours(SpatialSearchRequestParams request, String colourMode) throws Exception {
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
            requestParams.setQ(request.getQ());
            requestParams.setQc(request.getQc());
            requestParams.setFq(wmsUtils.getFq(request));
            requestParams.setFoffset(-1);

            //test for cutpoints on the back of colourMode
            String[] s = colourMode.split(",");
            String[] cutpoints = null;
            if (s.length > 1) {
                cutpoints = new String[s.length - 1];
                System.arraycopy(s, 1, cutpoints, 0, cutpoints.length);
            }
            if (s[0].equals("-1") || s[0].equals("grid")) {
                return null;
            } else {
                List<LegendItem> legend = searchDAO.getLegend(requestParams, s[0], cutpoints);

                if (cutpoints == null) {     //do not sort if cutpoints are provided
                    java.util.Collections.sort(legend);
                }
                int i = 0;
                int offset = 0;
                for (i = 0; i < legend.size() && i < colourList.length - 1; i++) {
                    colours.add(new LegendItem(legend.get(i).getName(), legend.get(i).getCount(), legend.get(i).getFq()));
                    int colour = DEFAULT_COLOUR;
                    if (cutpoints == null) {
                        colour = colourList[i];
                    } else if (cutpoints != null && i - offset < cutpoints.length) {
                        if (StringUtils.isEmpty(legend.get(i).getName()) || legend.get(i).getName().equals(NULL_NAME) || legend.get(i).getName().startsWith("-")) {
                            offset++;
                        } else {
                            colour = getRangedColour(i - offset, cutpoints.length / 2);
                        }
                    }
                    colours.get(colours.size() - 1).setColour(colour);
                }
            }
        }

        return colours;
    }

    int getRangedColour(int pos, int length) {
        int[] colourRange = {0x00002DD0, 0x00005BA2, 0x00008C73, 0x0000B944, 0x0000E716, 0x00A0FF00, 0x00FFFF00,
                0x00FFC814, 0x00FFA000, 0x00FF5B00, 0x00FF0000};

        double step = 1 / (double) colourRange.length;
        double p = pos / (double) (length);
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

        //Filter for -180 +180 longitude and -90 +90 latitude to match WMS request bounds.
        String[] fq = (String[]) ArrayUtils.addAll(wmsUtils.getFq(requestParams), new String[]{"longitude:[-180 TO 180]", "latitude:[-90 TO 90]"});
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

    private String convertBBox4326To900913(String bbox) {
        int i = 0;
        Double[] mbbox = new Double[4];
        for (String s : bbox.split(",")) {
            if (i % 2 == 0) mbbox[i] = convertLngToMeters(Double.parseDouble(s));
            else mbbox[i] = convertLatToMeters(Double.parseDouble(s));
            i++;
        }
        return StringUtils.join(mbbox, ",");
    }

    // add this to the GetCapabilities...
    @RequestMapping(value = {"/ogc/getMetadata"}, method = RequestMethod.GET)
    public String getMetadata(
            @RequestParam(value = "LAYER", required = false, defaultValue = "") String layer,
            @RequestParam(value = "q", required = false, defaultValue = "") String query,
            HttpServletRequest request,
            HttpServletResponse response,
            Model model
    ) throws Exception {

        String taxonName = "";
        String rank = "";
        String q = "";
        if (StringUtils.trimToNull(layer) != null) {
            String[] parts = layer.split(":");
            taxonName = parts[parts.length - 1];
            if (parts.length > 1) {
                rank = parts[0];
            }
            q = layer;
        } else if (StringUtils.trimToNull(query) != null) {
            String[] parts = query.split(":");
            taxonName = parts[parts.length - 1];
            if (parts.length > 1) {
                rank = parts[0];
            }
            q = query;
        } else {
            response.sendError(400);
        }

        ObjectMapper om = new ObjectMapper();
        String guid = null;
        JsonNode guidLookupNode = om.readTree(new URL(bieWebService + "/guid/" + URLEncoder.encode(taxonName, "UTF-8")));
        //NC: Fixed the ArraryOutOfBoundsException when the lookup fails to yield a result
        if (guidLookupNode.isArray() && guidLookupNode.size() > 0) {
            JsonNode idNode = guidLookupNode.get(0).get("acceptedIdentifier");//NC: changed to used the acceptedIdentifier because this will always hold the guid for the accepted taxon concept whether or not a synonym name is provided
            guid = idNode != null ? idNode.asText() : null;
        }
        String newQuery = "raw_name:" + taxonName;
        if (guid != null) {

            model.addAttribute("guid", guid);
            model.addAttribute("speciesPageUrl", bieUiUrl + "/species/" + guid);
            JsonNode node = om.readTree(new URL(bieWebService + "/species/info/" + guid + ".json"));
            JsonNode tc = node.get("taxonConcept");
            JsonNode imageNode = tc.get("smallImageUrl");
            String imageUrl = imageNode != null ? imageNode.asText() : null;
            if (imageUrl != null) {
                model.addAttribute("imageUrl", imageUrl);
                JsonNode imageMetadataNode = node.get("taxonConcept").get("imageMetadataUrl");
                String imageMetadataUrl = imageMetadataNode != null ? imageMetadataNode.asText() : null;

                //image metadata
                JsonNode imageMetadata = om.readTree(new URL(imageMetadataUrl));
                if (imageMetadata != null) {
                    if (imageMetadata.get("http://purl.org/dc/elements/1.1/creator") != null)
                        model.addAttribute("imageCreator", imageMetadata.get("http://purl.org/dc/elements/1.1/creator").asText());
                    if (imageMetadata.get("http://purl.org/dc/elements/1.1/license") != null)
                        model.addAttribute("imageLicence", imageMetadata.get("http://purl.org/dc/elements/1.1/license").asText());
                    if (imageMetadata.get("http://purl.org/dc/elements/1.1/source") != null)
                        model.addAttribute("imageSource", imageMetadata.get("http://purl.org/dc/elements/1.1/source").asText());
                }
            }

            JsonNode leftNode = tc.get("left");
            JsonNode rightNode = tc.get("right");
            newQuery = leftNode != null && rightNode != null ? "lft:[" + leftNode.asText() + " TO " + rightNode.asText() + "]" : "taxon_concept_lsid:" + guid;
            logger.debug("The new query : " + newQuery);

            //common name
            JsonNode commonNameNode = tc.get("commonNameSingle");
            if (commonNameNode != null) {
                model.addAttribute("commonName", commonNameNode.asText());
                logger.debug("retrieved name: " + commonNameNode.asText());
            }

            //name
            JsonNode nameNode = tc.get("nameComplete");
            if (nameNode != null) {
                model.addAttribute("name", nameNode.asText());
                logger.debug("retrieved name: " + nameNode.asText());
            }

            //authorship
            JsonNode authorshipNode = node.get("taxonConcept").get("author");
            if (authorshipNode != null) model.addAttribute("authorship", authorshipNode.asText());

            //taxonomic information
            JsonNode node2 = om.readTree(new URL(bieWebService + "/species/" + guid + ".json"));
            JsonNode classificationNode = node2.get("classification");
            model.addAttribute("kingdom", StringUtils.capitalize(classificationNode.get("kingdom").asText().toLowerCase()));
            model.addAttribute("phylum", StringUtils.capitalize(classificationNode.get("phylum").asText().toLowerCase()));
            model.addAttribute("clazz", StringUtils.capitalize(classificationNode.get("clazz").asText().toLowerCase()));
            model.addAttribute("order", StringUtils.capitalize(classificationNode.get("order").asText().toLowerCase()));
            model.addAttribute("family", StringUtils.capitalize(classificationNode.get("family").asText().toLowerCase()));
            model.addAttribute("genus", classificationNode.get("genus").asText());

            JsonNode taxonNameNode = node2.get("taxonName");
            if (taxonNameNode != null && taxonNameNode.get("specificEpithet") != null) {
                model.addAttribute("specificEpithet", taxonNameNode.get("specificEpithet").asText());
            }
        }

        SpatialSearchRequestParams searchParams = new SpatialSearchRequestParams();
        searchParams.setQ(newQuery);
        searchParams.setFacets(new String[]{"data_resource"});
        searchParams.setPageSize(0);
        List<FacetResultDTO> facets = searchDAO.getFacetCounts(searchParams);
        model.addAttribute("query", newQuery); //need a facet on data providers
        model.addAttribute("dataProviders", facets.get(0).getFieldResult()); //need a facet on data providers
        return "metadata/mcp";
    }

    @RequestMapping(value = {"/ogc/getFeatureInfo"}, method = RequestMethod.GET)
    public String getFeatureInfo(
            @RequestParam(value = "CQL_FILTER", required = false, defaultValue = "") String cql_filter,
            @RequestParam(value = "ENV", required = false, defaultValue = "") String env,
            @RequestParam(value = "BBOX", required = true, defaultValue = "0,-90,180,0") String bboxString,
            @RequestParam(value = "WIDTH", required = true, defaultValue = "256") Integer width,
            @RequestParam(value = "HEIGHT", required = true, defaultValue = "256") Integer height,
            @RequestParam(value = "STYLES", required = false, defaultValue = "") String styles,
            @RequestParam(value = "SRS", required = false, defaultValue = "") String srs,
            @RequestParam(value = "QUERY_LAYERS", required = false, defaultValue = "") String queryLayers,
            @RequestParam(value = "X", required = true, defaultValue = "0") Double x,
            @RequestParam(value = "Y", required = true, defaultValue = "0") Double y,
            HttpServletRequest request,
            HttpServletResponse response,
            Model model) throws Exception {

        logger.debug("WMS - GetFeatureInfo requested for: " + queryLayers);

        if ("EPSG:4326".equals(srs))
            bboxString = convertBBox4326To900913(bboxString);    // to work around a UDIG bug

        WmsEnv vars = new WmsEnv(env, styles);
        double[] mbbox = new double[4];
        double[] bbox = new double[4];
        double[] pbbox = new double[4];
        double[] tilebbox = new double[4];
        int size = vars.size + (vars.highlight != null ? HIGHLIGHT_RADIUS * 2 + (int) (vars.size * 0.2) : 0) + 5;  //bounding box buffer

        //what is the size of the dot in degrees
        double resolution = getBBoxes(bboxString, width, height, size, vars.uncertainty, mbbox, bbox, pbbox, tilebbox);

        //resolution should be a value < 1
        PointType pointType = getPointTypeForDegreesPerPixel(resolution);

        double longitude = bbox[0] + (((bbox[2] - bbox[0]) / width) * x);
        double latitude = bbox[3] - (((bbox[3] - bbox[1]) / height) * y);

        //round to the correct point size
        double roundedLongitude = pointType.roundToPointType(longitude);
        double roundedLatitude = pointType.roundToPointType(latitude);

        //get the pixel size of the circles
        double minLng = pointType.roundDownToPointType(roundedLongitude - (pointType.getValue() * 2 * (size + 3)));
        double maxLng = pointType.roundUpToPointType(roundedLongitude + (pointType.getValue() * 2 * (size + 3)));
        double minLat = pointType.roundDownToPointType(roundedLatitude - (pointType.getValue() * 2 * (size + 3)));
        double maxLat = pointType.roundUpToPointType(roundedLatitude + (pointType.getValue() * 2 * (size + 3)));

        //do the SOLR query
        SpatialSearchRequestParams requestParams = new SpatialSearchRequestParams();
        String q = WMSUtils.convertLayersParamToQ(queryLayers);
        requestParams.setQ(WMSUtils.convertLayersParamToQ(queryLayers));  //need to derive this from the layer name
        logger.debug("WMS GetFeatureInfo for " + queryLayers + ", longitude:[" + minLng + " TO " + maxLng + "],  latitude:[" + minLat + " TO " + maxLat + "]");

        String[] fqs = new String[]{"longitude:[" + minLng + " TO " + maxLng + "]", "latitude:[" + minLat + " TO " + maxLat + "]"};
        requestParams.setFq(fqs);
        //requestParams.setFq(new String[]{"point-"+pointType.getValue()+":"+roundedLatitude+","+roundedLongitude});
        requestParams.setFacet(false);

        //TODO: paging
        SolrDocumentList sdl = searchDAO.findByFulltext(requestParams);
        //send back the results.
        String body = "";
        if (sdl != null && sdl.size() > 0) {
            SolrDocument doc = sdl.get(0);
            model.addAttribute("record", doc.getFieldValueMap());
            model.addAttribute("totalRecords", sdl.getNumFound());
        }

        model.addAttribute("uriUrl", "http://biocache.ala.org.au/occurrences/search?q=" +
                URLEncoder.encode(q, "UTF-8")
                + "&fq=" + URLEncoder.encode(fqs[0], "UTF-8")
                + "&fq=" + URLEncoder.encode(fqs[1], "UTF-8")
        );


        model.addAttribute("pointType", pointType.name());
        model.addAttribute("minLng", minLng);
        model.addAttribute("maxLng", maxLng);
        model.addAttribute("minLat", minLat);
        model.addAttribute("maxLat", maxLat);
        model.addAttribute("latitudeClicked", latitude);
        model.addAttribute("longitudeClicked", longitude);

        return "metadata/getFeatureInfo";
    }



    @RequestMapping(value = {"/ogc/legendGraphic"}, method = RequestMethod.GET)
    public void getLegendGraphic(
            @RequestParam(value = "ENV", required = false, defaultValue = "") String env,
            @RequestParam(value = "STYLE", required = false, defaultValue = "8b0000;opacity=1;size=5") String style,
            @RequestParam(value = "WIDTH", required = false, defaultValue = "30") Integer width,
            @RequestParam(value = "HEIGHT", required = false, defaultValue = "20") Integer height,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {

        try {
            if (StringUtils.trimToNull(env) == null && StringUtils.trimToNull(style) == null) {
                style = "8b0000;opacity=1;size=5";
            }

            WmsEnv wmsEnv = new WmsEnv(env, style);
            BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
            Graphics2D g = (Graphics2D) img.getGraphics();
            int size = width > height ? height : width;
            Paint fill = new Color(wmsEnv.colour | wmsEnv.alpha << 24);
            g.setPaint(fill);
            g.fillOval(0, 0, size, size);
            OutputStream out = response.getOutputStream();
            logger.debug("WMS - GetLegendGraphic requested : " + request.getQueryString());
            response.setContentType("image/png");
            ImageIO.write(img, "png", out);
            out.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Returns a get capabilities response by default.
     *
     * @param requestParams
     * @param cql_filter
     * @param env
     * @param srs
     * @param styles
     * @param style
     * @param bboxString
     * @param width
     * @param height
     * @param cache
     * @param requestString
     * @param outlinePoints
     * @param outlineColour
     * @param layers
     * @param query
     * @param filterQueries
     * @param x
     * @param y
     * @param spatiallyValidOnly
     * @param marineOnly
     * @param terrestrialOnly
     * @param limitToFocus
     * @param useSpeciesGroups
     * @param request
     * @param response
     * @param model
     * @throws Exception
     */
    @RequestMapping(value = {"/ogc/ows", "/ogc/capabilities"}, method = RequestMethod.GET)
    public void getCapabilities(
            SpatialSearchRequestParams requestParams,
            @RequestParam(value = "CQL_FILTER", required = false, defaultValue = "") String cql_filter,
            @RequestParam(value = "ENV", required = false, defaultValue = "") String env,
            @RequestParam(value = "SRS", required = false, defaultValue = "EPSG:900913") String srs, //default to google mercator
            @RequestParam(value = "STYLES", required = false, defaultValue = "") String styles,
            @RequestParam(value = "STYLE", required = false, defaultValue = "") String style,
            @RequestParam(value = "BBOX", required = false, defaultValue = "") String bboxString,
            @RequestParam(value = "WIDTH", required = false, defaultValue = "256") Integer width,
            @RequestParam(value = "HEIGHT", required = false, defaultValue = "256") Integer height,
            @RequestParam(value = "CACHE", required = false, defaultValue = "default") String cache,
            @RequestParam(value = "REQUEST", required = false, defaultValue = "") String requestString,
            @RequestParam(value = "OUTLINE", required = false, defaultValue = "false") boolean outlinePoints,
            @RequestParam(value = "OUTLINECOLOUR", required = false, defaultValue = "0x000000") String outlineColour,
            @RequestParam(value = "LAYERS", required = false, defaultValue = "") String layers,
            @RequestParam(value = "q", required = false, defaultValue = "*:*") String query,
            @RequestParam(value = "fq", required = false) String[] filterQueries,
            @RequestParam(value = "X", required = true, defaultValue = "0") Double x,
            @RequestParam(value = "Y", required = true, defaultValue = "0") Double y,
            @RequestParam(value = "spatiallyValidOnly", required = false, defaultValue = "true") boolean spatiallyValidOnly,
            @RequestParam(value = "marineSpecies", required = false, defaultValue = "false") boolean marineOnly,
            @RequestParam(value = "terrestrialSpecies", required = false, defaultValue = "false") boolean terrestrialOnly,
            @RequestParam(value = "limitToFocus", required = false, defaultValue = "true") boolean limitToFocus,
            @RequestParam(value = "useSpeciesGroups", required = false, defaultValue = "false") boolean useSpeciesGroups,
            @RequestParam(value = "GRIDDETAIL", required = false, defaultValue = "16") int gridDivisionCount,
            HttpServletRequest request,
            HttpServletResponse response,
            Model model)
            throws Exception {

        if ("GetMap".equalsIgnoreCase(requestString)) {
            generateWmsTile(
                    requestParams,
                    cql_filter,
                    env,
                    srs,
                    styles,
                    bboxString,
                    width,
                    height,
                    cache,
                    requestString,
                    outlinePoints,
                    outlineColour,
                    layers,
                    null,
                    gridDivisionCount,
                    request,
                    response);
            return;
        }

        if ("GetLegendGraphic".equalsIgnoreCase(requestString)) {
            getLegendGraphic(env, style, 30, 20, request, response);
            return;
        }

        if ("GetFeatureInfo".equalsIgnoreCase(requestString)) {
            getFeatureInfo(
                    cql_filter,
                    env,
                    bboxString,
                    width,
                    height,
                    styles,
                    srs,
                    layers,
                    x,
                    y,
                    request,
                    response,
                    model);
            return;
        }

        //add the get capabilities request

        response.setContentType("text/xml");
        response.setHeader("Content-Description", "File Transfer");
        response.setHeader("Content-Disposition", "attachment; filename=GetCapabilities.xml");
        response.setHeader("Content-Transfer-Encoding", "binary");
        try {
            //webservicesRoot
            String biocacheServerUrl = request.getSession().getServletContext().getInitParameter("webservicesRoot");
            PrintWriter writer = response.getWriter();
            writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<!DOCTYPE WMT_MS_Capabilities SYSTEM \"http://spatial.ala.org.au/geoserver/schemas/wms/1.1.1/WMS_MS_Capabilities.dtd\">\n" +
                    "<WMT_MS_Capabilities version=\"1.1.1\" updateSequence=\"28862\">\n" +
                    "  <Service>\n" +
                    "    <Name>OGC:WMS</Name>\n" +
                    "    <Title>" + organizationName + "(WMS) - Species occurrences</Title>\n" +
                    "    <Abstract>WMS services for species occurrences.</Abstract>\n" +
                    "    <KeywordList>\n" +
                    "      <Keyword>WMS</Keyword>\n" +
                    "      <Keyword>Species occurrence data</Keyword>\n" +
                    "      <Keyword>ALA</Keyword>\n" +
                    "      <Keyword>CRIS</Keyword>\n" +
                    "    </KeywordList>\n" +
                    "    <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\"" + biocacheServerUrl + "/ogc/wms\"/>\n" +
                    "    <ContactInformation>\n" +
                    "      <ContactPersonPrimary>\n" +
                    "        <ContactPerson>ALA Support</ContactPerson>\n" +
                    "        <ContactOrganization>" + organizationName + "</ContactOrganization>\n" +
                    "      </ContactPersonPrimary>\n" +
                    "      <ContactPosition>Support Manager</ContactPosition>\n" +
                    "      <ContactAddress>\n" +
                    "        <AddressType></AddressType>\n" +
                    "        <Address/>\n" +
                    "        <City>" + orgCity + "</City>\n" +
                    "        <StateOrProvince>" + orgStateProvince + "</StateOrProvince>\n" +
                    "        <PostCode>" + orgPostcode + "</PostCode>\n" +
                    "        <Country>" + orgCountry + "</Country>\n" +
                    "      </ContactAddress>\n" +
                    "      <ContactVoiceTelephone>" + orgPhone + "</ContactVoiceTelephone>\n" +
                    "      <ContactFacsimileTelephone>" + orgFax + "</ContactFacsimileTelephone>\n" +
                    "      <ContactElectronicMailAddress>" + orgEmail + "</ContactElectronicMailAddress>\n" +
                    "    </ContactInformation>\n" +
                    "    <Fees>NONE</Fees>\n" +
                    "    <AccessConstraints>NONE</AccessConstraints>\n" +
                    "  </Service>\n" +
                    "  <Capability>\n" +
                    "    <Request>\n" +
                    "      <GetCapabilities>\n" +
                    "        <Format>application/vnd.ogc.wms_xml</Format>\n" +
                    "        <DCPType>\n" +
                    "          <HTTP>\n" +
                    "            <Get>\n" +
                    "              <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\"" + baseWsUrl + "/ogc/capabilities?SERVICE=WMS&amp;\"/>\n" +
                    "            </Get>\n" +
                    "            <Post>\n" +
                    "              <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\"" + baseWsUrl + "/ogc/capabilities?SERVICE=WMS&amp;\"/>\n" +
                    "            </Post>\n" +
                    "          </HTTP>\n" +
                    "        </DCPType>\n" +
                    "      </GetCapabilities>\n" +
                    "      <GetMap>\n" +
                    "        <Format>image/png</Format>\n" +
                    "        <DCPType>\n" +
                    "          <HTTP>\n" +
                    "            <Get>\n" +
                    "              <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\"" + baseWsUrl + "/ogc/wms/reflect?SERVICE=WMS&amp;OUTLINE=TRUE&amp;\"/>\n" +
                    "            </Get>\n" +
                    "          </HTTP>\n" +
                    "        </DCPType>\n" +
                    "      </GetMap>\n" +
                    "      <GetFeatureInfo>\n" +
                    "        <Format>text/html</Format>\n" +
                    "        <DCPType>\n" +
                    "          <HTTP>\n" +
                    "            <Get>\n" +
                    "              <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\"" + baseWsUrl + "/ogc/getFeatureInfo\"/>\n" +
                    "            </Get>\n" +
                    "            <Post>\n" +
                    "              <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\"" + baseWsUrl + "/ogc/getFeatureInfo\"/>\n" +
                    "            </Post>\n" +
                    "          </HTTP>\n" +
                    "        </DCPType>\n" +
                    "      </GetFeatureInfo>\n" +
                    "      <GetLegendGraphic>\n" +
                    "        <Format>image/png</Format>\n" +
                    "        <Format>image/jpeg</Format>\n" +
                    "        <Format>image/gif</Format>\n" +
                    "        <DCPType>\n" +
                    "          <HTTP>\n" +
                    "            <Get>\n" +
                    "              <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\"" + baseWsUrl + "/ogc/legendGraphic\"/>\n" +
                    "            </Get>\n" +
                    "          </HTTP>\n" +
                    "        </DCPType>\n" +
                    "      </GetLegendGraphic>\n" +
                    "    </Request>\n" +
                    "    <Exception>\n" +
                    "      <Format>application/vnd.ogc.se_xml</Format>\n" +
                    "      <Format>application/vnd.ogc.se_inimage</Format>\n" +
                    "    </Exception>\n" +
                    "    <Layer>\n" +
                    "      <Title>" + organizationName + " - Species occurrence layers</Title>\n" +
                    "      <Abstract>Custom WMS services for " + organizationName + " species occurrences</Abstract>\n" +
                    "      <SRS>EPSG:900913</SRS>\n" +
                    "      <SRS>EPSG:4326</SRS>\n" +
                    "     <LatLonBoundingBox minx=\"-179.9\" miny=\"-89.9\" maxx=\"179.9\" maxy=\"89.9\"/>\n"
            );

            writer.write(generateStylesForPoints());

            if (spatiallyValidOnly) {
                filterQueries = org.apache.commons.lang3.ArrayUtils.add(filterQueries, "geospatial_kosher:true");
            }

            if (marineOnly) {
                filterQueries = org.apache.commons.lang3.ArrayUtils.add(filterQueries, "species_habitats:Marine OR species_habitats:\"Marine and Non-marine\"");
            }

            if (terrestrialOnly) {
                filterQueries = org.apache.commons.lang3.ArrayUtils.add(filterQueries, "species_habitats:\"Non-marine\" OR species_habitats:Limnetic");
            }

            if (limitToFocus) {
                //TODO retrieve focus from config file
                filterQueries = org.apache.commons.lang3.ArrayUtils.add(filterQueries, "latitude:[-89 TO -8] AND longitude:[100 TO 165]");
            }

            query = searchUtils.convertRankAndName(query);
            logger.debug("GetCapabilities query in use: " + query);

            if (useSpeciesGroups) {
                taxonDAO.extractBySpeciesGroups(baseWsUrl + "/ogc/getMetadata", query, filterQueries, writer);
            } else {
                taxonDAO.extractHierarchy(baseWsUrl + "/ogc/getMetadata", query, filterQueries, writer);
            }

            writer.write("</Layer></Capability></WMT_MS_Capabilities>\n");

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public String generateStylesForPoints() {
        //need a better listings of colours
        String[] sizes = new String[]{"5", "10", "2"};
        String[] sizesNames = new String[]{"medium", "large", "small"};
        String[] opacities = new String[]{"0.5", "1", "0.2"};
        String[] opacitiesNames = new String[]{"medium", "opaque", "transparency"};
        StringBuffer sb = new StringBuffer();
        int colorIdx = 0;
        int sizeIdx = 0;
        int opIdx = 0;
        for (String color : colorsNames) {
            for (String size : sizes) {
                for (String opacity : opacities) {
                    sb.append(
                            "<Style>\n" +
                                    "<Name>" + colorsCodes[colorIdx] + ";opacity=" + opacity + ";size=" + size + "</Name> \n" +
                                    "<Title>" + color + ";opacity=" + opacitiesNames[opIdx] + ";size=" + sizesNames[sizeIdx] + "</Title> \n" +
                                    "</Style>\n"
                    );
                    opIdx++;
                }
                opIdx = 0;
                sizeIdx++;
            }
            sizeIdx = 0;
            colorIdx++;
        }
        return sb.toString();
    }

    /**
     * WMS service for webportal.
     *
     * @param cql_filter q value.
     * @param env        ';' delimited field:value pairs.  See Env
     * @param bboxString
     * @param width
     * @param height
     * @param cache      'on' = use cache, 'off' = do not use cache this
     *                   also removes any related cache data.
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = {"/webportal/wms/reflect", "/ogc/wms/reflect", "/mapping/wms/reflect"}, method = RequestMethod.GET)
    public void generateWmsTile(
            SpatialSearchRequestParams requestParams,
            @RequestParam(value = "CQL_FILTER", required = false, defaultValue = "") String cql_filter,
            @RequestParam(value = "ENV", required = false, defaultValue = "") String env,
            @RequestParam(value = "SRS", required = false, defaultValue = "EPSG:900913") String srs, //default to google mercator
            @RequestParam(value = "STYLES", required = false, defaultValue = "") String styles,
            @RequestParam(value = "BBOX", required = true, defaultValue = "") String bboxString,
            @RequestParam(value = "WIDTH", required = true, defaultValue = "256") Integer width,
            @RequestParam(value = "HEIGHT", required = true, defaultValue = "256") Integer height,
            @RequestParam(value = "CACHE", required = true, defaultValue = "default") String cache,
            @RequestParam(value = "REQUEST", required = true, defaultValue = "") String requestString,
            @RequestParam(value = "OUTLINE", required = true, defaultValue = "false") boolean outlinePoints,
            @RequestParam(value = "OUTLINECOLOUR", required = true, defaultValue = "0x000000") String outlineColour,
            @RequestParam(value = "LAYERS", required = false, defaultValue = "") String layers,
            @RequestParam(value = "HQ", required = false) String[] hqs,
            @RequestParam(value = "GRIDDETAIL", required = false, defaultValue = "16") Integer gridDivisionCount,
            HttpServletRequest request,
            HttpServletResponse response)
            throws Exception {

        //for OS Grids, hand over to WMS OS controller
        if(env != null && env.contains("osgrid")){
            wmsosGridController.generateWmsTile(requestParams, cql_filter, env, srs, styles, bboxString, layers, width, height, outlinePoints, outlineColour, request, response);
            return;
        }

        //correct cache value
        if ("default".equals(cache)) cache = wmsCacheEnabled ? "on" : "off";

        //Some WMS clients are ignoring sections of the GetCapabilities....
        if ("GetLegendGraphic".equalsIgnoreCase(requestString)) {
            getLegendGraphic(env, styles, 30, 20, request, response);
            return;
        }

        Set<Integer> hq = new HashSet<Integer>();
        if (hqs != null && hqs.length > 0) {
            for (String h : hqs) {
                hq.add(Integer.parseInt(h));
            }
        }

        logger.debug("WMS tile: " + request.getQueryString());

        response.setHeader("Cache-Control", "max-age=86400"); //age == 1 day
        response.setContentType("image/png"); //only png images generated

        boolean is4326 = false;
        WmsEnv vars = new WmsEnv(env, styles);
        double[] mbbox = new double[4];
        double[] bbox = new double[4];
        double[] pbbox = new double[4];
        double[] tilebbox = new double[4];

        //bbox adjustment for WMSCache is better with a stepped size
        int steppedSize = (int) (Math.ceil(vars.size / 20.0) * 20);
        int size = steppedSize + (vars.highlight != null ? HIGHLIGHT_RADIUS * 2 + (int) (steppedSize * 0.2) : 0) + 5;  //bounding box buffer

        double resolution;
        if ("EPSG:4326".equals(srs)) {
            is4326 = true;
            //bboxString = convertBBox4326To900913(bboxString);    // to work around a UDIG bug

            resolution = getBBoxes4326(bboxString, width, height, size, vars.uncertainty, mbbox, bbox, pbbox, tilebbox);
        } else {
            resolution = getBBoxes(bboxString, width, height, size, vars.uncertainty, mbbox, bbox, pbbox, tilebbox);
        }

        PointType pointType = getPointTypeForDegreesPerPixel(resolution);
        logger.debug("Rendering: " + pointType.name());

        String q = "";

        //CQL Filter takes precedence of the layer
        if (StringUtils.trimToNull(cql_filter) != null) {
            q = WMSUtils.getQ(cql_filter);
        } else if (StringUtils.trimToNull(layers) != null && !"ALA:Occurrences".equalsIgnoreCase(layers)) {
            q = WMSUtils.convertLayersParamToQ(layers);
        }

        String[] boundingBoxFqs = new String[2];
        boundingBoxFqs[0] = String.format("longitude:[%f TO %f]", bbox[0], bbox[2]);
        boundingBoxFqs[1] = String.format("latitude:[%f TO %f]", bbox[1], bbox[3]);

        int pointWidth = vars.size * 2;
        double width_mult = (width / (pbbox[2] - pbbox[0]));
        double height_mult = (height / (pbbox[1] - pbbox[3]));

        //build request
        if (q.length() > 0) {
            requestParams.setQ(q);
        } else {
            q = requestParams.getQ();
        }

        //bounding box test (requestParams must be 'qid:' + number only)
        if (q.startsWith("qid:") && StringUtils.isEmpty(requestParams.getWkt()) &&
                (requestParams.getFq().length == 0 ||
                        (requestParams.getFq().length == 1 && StringUtils.isEmpty(requestParams.getFq()[0])))) {
            double[] queryBBox = qidCacheDAO.get(q.substring(4)).getBbox();
            if (queryBBox != null && (queryBBox[0] > bbox[2] || queryBBox[2] < bbox[0]
                    || queryBBox[1] > bbox[3] || queryBBox[3] < bbox[1])) {
                displayBlankImage(response);
                return;
            }
        }

        String[] originalFqs = wmsUtils.getFq(requestParams);

        //get from cache, or make it
        boolean canCache = wmsCache.isEnabled() && cache.equalsIgnoreCase("on");
        WMSTile wco = getWMSCacheObject(requestParams, vars, pointType, bbox, originalFqs, boundingBoxFqs, canCache);

        //correction for gridDivisionCount
        boolean isGrid = vars.colourMode.equals("grid");
        if (isGrid) {
            if (gridDivisionCount > Math.min(width, height)) gridDivisionCount = Math.min(width, height);
            if (gridDivisionCount < 0) gridDivisionCount = 1;

            //gridDivisionCount correction
            while (width % gridDivisionCount > 0 || height % gridDivisionCount > 0) {
                gridDivisionCount--;
            }
        }

        ImgObj imgObj = wco.getPoints() == null ? null : wmsCached(wco, requestParams, vars, pointType, pbbox, bbox, mbbox,
                width, height, width_mult, height_mult, pointWidth,
                originalFqs, hq, boundingBoxFqs, outlinePoints, outlineColour, response, is4326, tilebbox, gridDivisionCount);

        if (imgObj != null && imgObj.g != null) {
            imgObj.g.dispose();
            try {
                ServletOutputStream outStream = response.getOutputStream();
                ImageIO.write(imgObj.img, "png", outStream);
                outStream.flush();
                outStream.close();
            } catch (Exception e) {
                logger.debug("Unable to write image", e);
            }
        } else {
            displayBlankImage(response);
        }
    }

    /**
     * Method that produces the downloadable map integrated in AVH/OZCAM/Biocache.
     *
     * @param requestParams
     * @param format
     * @param extents
     * @param widthMm
     * @param pointRadiusMm
     * @param pradiusPx
     * @param pointColour
     * @param pointOpacity
     * @param baselayer
     * @param scale
     * @param dpi
     * @param outlinePoints
     * @param outlineColour
     * @param fileName
     * @param request
     * @param response
     * @throws Exception
     */
    @RequestMapping(value = {"/webportal/wms/image", "/mapping/wms/image"}, method = RequestMethod.GET)
    public void generatePublicationMap(
            SpatialSearchRequestParams requestParams,
            @RequestParam(value = "format", required = false, defaultValue = "jpg") String format,
            @RequestParam(value = "extents", required = true) String extents,
            @RequestParam(value = "widthmm", required = false, defaultValue = "60") Double widthMm,
            @RequestParam(value = "pradiusmm", required = false, defaultValue = "2") Double pointRadiusMm,
            @RequestParam(value = "pradiuspx", required = false) Integer pradiusPx,
            @RequestParam(value = "pcolour", required = false, defaultValue = "FF0000") String pointColour,
            @RequestParam(value = "popacity", required = false, defaultValue = "0.8") Double pointOpacity,
            @RequestParam(value = "baselayer", required = false, defaultValue = "world") String baselayer,
            @RequestParam(value = "scale", required = false, defaultValue = "off") String scale,
            @RequestParam(value = "dpi", required = false, defaultValue = "300") Integer dpi,
            @RequestParam(value = "outline", required = true, defaultValue = "false") boolean outlinePoints,
            @RequestParam(value = "outlineColour", required = true, defaultValue = "#000000") String outlineColour,
            @RequestParam(value = "fileName", required = false) String fileName,
            HttpServletRequest request, HttpServletResponse response) throws Exception {

        String[] bb = extents.split(",");

        double long1 = Double.parseDouble(bb[0]);
        double lat1 = Double.parseDouble(bb[1]);
        double long2 = Double.parseDouble(bb[2]);
        double lat2 = Double.parseDouble(bb[3]);

        if (lat1 <= -90) {
            lat1 = -89.999;
        }
        if (lat2 >= 90) {
            lat2 = 89.999;
        }

        int pminx = convertLngToPixel(long1);
        int pminy = convertLatToPixel(lat1);
        int pmaxx = convertLngToPixel(long2);
        int pmaxy = convertLatToPixel(lat2);

        int width = (int) ((dpi / 25.4) * widthMm);
        int height = (int) Math.round(width * ((pminy - pmaxy) / (double) (pmaxx - pminx)));

        if (height * width > MAX_IMAGE_PIXEL_COUNT) {
            String errorMessage = "Image size in pixels " + width + "x" + height + " exceeds " + MAX_IMAGE_PIXEL_COUNT + " pixels.  Make the image smaller";
            response.sendError(response.SC_NOT_ACCEPTABLE, errorMessage);
            throw new Exception(errorMessage);
        }

        int pointSize = -1;
        if (pradiusPx != null) {
            pointSize = (int) pradiusPx;
        } else {
            pointSize = (int) ((dpi / 25.4) * pointRadiusMm);
        }

        double[] boundingBox = transformBbox4326To900913(Double.parseDouble(bb[0]), Double.parseDouble(bb[1]), Double.parseDouble(bb[2]), Double.parseDouble(bb[3]));

        //"http://biocache.ala.org.au/ws/webportal/wms/reflect?
        //q=macropus&ENV=color%3Aff0000%3Bname%3Acircle%3Bsize%3A3%3Bopacity%3A1
        //&BBOX=12523443.0512,-2504688.2032,15028131.5936,0.33920000120997&WIDTH=256&HEIGHT=256");
        String speciesAddress = baseWsUrl
                + "/ogc/wms/reflect?"
                + "ENV=color%3A" + pointColour
                + "%3Bname%3Acircle%3Bsize%3A" + pointSize
                + "%3Bopacity%3A" + pointOpacity
                + "&BBOX=" + boundingBox[0] + "," + boundingBox[1] + "," + boundingBox[2] + "," + boundingBox[3]
                + "&WIDTH=" + width + "&HEIGHT=" + height + "&OUTLINE=" + outlinePoints + "&OUTLINECOLOUR=" + outlineColour
                + "&" + request.getQueryString();

        URL speciesURL = new URL(speciesAddress);
        BufferedImage speciesImage = ImageIO.read(speciesURL);

        //"http://spatial.ala.org.au/geoserver/wms/reflect?
        //LAYERS=ALA%3Aworld&SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&STYLES=
        //&FORMAT=image%2Fjpeg&SRS=EPSG%3A900913&BBOX=12523443.0512,-1252343.932,13775787.3224,0.33920000004582&WIDTH=256&HEIGHT=256"
        String layout = "";
        if (!scale.equals("off")) {
            layout += "layout:scale";
        }
        String basemapAddress = geoserverUrl + "/wms/reflect?"
                + "LAYERS=ALA%3A" + baselayer
                + "&SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&STYLES="
                + "&FORMAT=image%2Fpng&SRS=EPSG%3A900913"     //specify the mercator projection
                + "&BBOX=" + boundingBox[0] + "," + boundingBox[1] + "," + boundingBox[2] + "," + boundingBox[3]
                + "&WIDTH=" + width + "&HEIGHT=" + height + "&OUTLINE=" + outlinePoints
                + "&format_options=dpi:" + dpi + ";" + layout;

        BufferedImage basemapImage = ImageIO.read(new URL(basemapAddress));

        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D combined = (Graphics2D) img.getGraphics();

        combined.drawImage(basemapImage, 0, 0, Color.WHITE, null);
        //combined.setComposite(AlphaComposite.getInstance(AlphaComposite.SRC_OVER, pointOpacity.floatValue()));
        combined.setComposite(AlphaComposite.getInstance(AlphaComposite.SRC_OVER, 1.0f));
        combined.drawImage(speciesImage, null, 0, 0);
        combined.dispose();

        //if filename supplied, force a download
        if (fileName != null) {
            response.setContentType("application/octet-stream");
            response.setHeader("Content-Description", "File Transfer");
            response.setHeader("Content-Disposition", "attachment; filename=" + fileName);
            response.setHeader("Content-Transfer-Encoding", "binary");
        } else if (format.equalsIgnoreCase("png")) {
            response.setContentType("image/png");
        } else {
            response.setContentType("image/jpeg");
        }

        if (format.equalsIgnoreCase("png")) {
            OutputStream os = response.getOutputStream();
            ImageIO.write(img, format, os);
            os.close();
        } else {
            //handle jpeg + BufferedImage.TYPE_INT_ARGB
            BufferedImage img2;
            Graphics2D c2;
            (c2 = (Graphics2D) (img2 = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)).getGraphics()).drawImage(img, 0, 0, Color.WHITE, null);
            c2.dispose();
            OutputStream os = response.getOutputStream();
            ImageIO.write(img2, format, os);
            os.close();
        }
    }

    /**
     * @return
     * @throws Exception
     */
    private ImgObj wmsCached(WMSTile wco, SpatialSearchRequestParams requestParams,
                             WmsEnv vars, PointType pointType, double[] pbbox,
                             double[] bbox, double[] mbbox, int width, int height, double width_mult,
                             double height_mult, int pointWidth, String[] originalFqs, Set<Integer> hq,
                             String[] boundingBoxFqs, boolean outlinePoints,
                             String outlineColour,
                             HttpServletResponse response,
                             boolean is4326, double[] tilebbox, int gridDivisionCount) throws Exception {

        ImgObj imgObj = null;

        //grid setup
        boolean isGrid = vars.colourMode.equals("grid");
        int divs = gridDivisionCount; //number of x & y divisions in the WIDTH/HEIGHT
        int[][] gridCounts = isGrid ? new int[divs][divs] : null;
        int xstep = width / divs;
        int ystep = height / divs;
        double grid_width_mult = (width / (pbbox[2] - pbbox[0])) / (width / divs);
        double grid_height_mult = (height / (pbbox[1] - pbbox[3])) / (height / divs);

        int x, y;

        //if not transparent and zero size, render dots
        if (vars.alpha > 0 && vars.size > 0) {
            List<float[]> points = wco.getPoints();
            List<int[]> counts = wco.getCounts();
            List<Integer> pColour = wco.getColours();
            if (pColour.size() == 1 && vars.colourMode.equals("-1")) {
                pColour.set(0, vars.colour | (vars.alpha << 24));
            }

            //initialise the image object
            imgObj = ImgObj.create(width, height);

            for (int j = 0; j < points.size(); j++) {

                if (hq != null && hq.contains(j)) {
                    //dont render these points
                    continue;
                }

                float[] ps = points.get(j);

                if (ps == null || ps.length == 0) {
                    continue;
                }

                //for 4326
                double top = tilebbox[3];
                double bottom = tilebbox[1];
                double left = tilebbox[0];
                double right = tilebbox[2];

                if (isGrid) {
                    //render grids
                    int[] count = counts.get(j);

                    //populate grid
                    for (int i = 0; i < ps.length; i += 2) {
                        float lng = ps[i];
                        float lat = ps[i + 1];
                        if (lng >= bbox[0] && lng <= bbox[2]
                                && lat >= bbox[1] && lat <= bbox[3]) {
                            if (is4326) {
                                x = convertLngToPixel4326(lng, left, right, width);
                                y = convertLatToPixel4326(lat, top, bottom, height);
                            } else {
                                x = (int) ((convertLngToPixel(lng) - pbbox[0]) * grid_width_mult);
                                y = (int) ((convertLatToPixel(lat) - pbbox[3]) * grid_height_mult);
                            }

                            if (x >= 0 && x < divs && y >= 0 && y < divs) {
                                gridCounts[x][y] += count[i / 2];
                            }
                        }
                    }
                } else {
                    renderPoints(vars, bbox, pbbox, width_mult, height_mult, pointWidth, outlinePoints, outlineColour, pColour, imgObj, j, ps, is4326, tilebbox, height, width);
                }
            }
        }

        //no points
        if (imgObj == null || imgObj.img == null) {
            if (vars.highlight == null) {
                displayBlankImage(response);
                return null;
            }
        } else if (isGrid) {
            //draw grid
            for (x = 0; x < divs; x++) {
                for (y = 0; y < divs; y++) {
                    int v = gridCounts[x][y];
                    if (v > 0) {
                        if (v > 500) {
                            v = 500;
                        }
                        int colour = (((500 - v) / 2) << 8) | (vars.alpha << 24) | 0x00FF0000;
                        imgObj.g.setColor(new Color(colour));
                        imgObj.g.fillRect(x * xstep, y * ystep, xstep, ystep);
                    }
                }
            }
        } else {
            drawUncertaintyCircles(requestParams, vars, height, width, pbbox, mbbox, bbox, width_mult, height_mult, imgObj.g,
                    originalFqs, boundingBoxFqs, is4326, tilebbox, pointType);
        }

        //highlight
        if (vars.highlight != null) {
            imgObj = drawHighlight(requestParams, vars, pointType, width, height, pbbox, width_mult, height_mult, imgObj,
                    originalFqs, boundingBoxFqs, is4326, tilebbox);
        }

        return imgObj;
    }

    void drawUncertaintyCircles(SpatialSearchRequestParams requestParams, WmsEnv vars, int height, int width,
                                double[] pbbox, double[] mbbox, double[] bbox, double width_mult, double height_mult, Graphics2D g,
                                String[] originalFqs, String[] boundingBoxFqs, boolean is4326, double[] tilebbox,
                                PointType pointType) throws Exception {
        //draw uncertainty circles
        double hmult = (height / (mbbox[3] - mbbox[1]));

        //min uncertainty for current resolution and dot size
        double min_uncertainty = (vars.size + 1) / hmult;

        //only draw uncertainty if max radius will be > dot size
        if (vars.uncertainty && MAX_UNCERTAINTY > min_uncertainty) {

            //uncertainty colour/fq/radius, [0]=map, [1]=not specified, [2]=too large
            Color[] uncertaintyColours = {new Color(255, 170, 0, vars.alpha), new Color(255, 255, 100, vars.alpha), new Color(50, 255, 50, vars.alpha)};
            //TODO: don't assume MAX_UNCERTAINTY > default_uncertainty
            String[] uncertaintyFqs = {"coordinate_uncertainty:[" + min_uncertainty + " TO " + MAX_UNCERTAINTY + "] AND -assertions:uncertaintyNotSpecified", "assertions:uncertaintyNotSpecified", "coordinate_uncertainty:[" + MAX_UNCERTAINTY + " TO *]"};
            double[] uncertaintyR = {-1, MAX_UNCERTAINTY, MAX_UNCERTAINTY};

            String[] fqs = new String[originalFqs.length + 3];
            System.arraycopy(originalFqs, 0, fqs, 3, originalFqs.length);

            //expand bounding box to cover MAX_UNCERTAINTY radius (m to degrees)
            fqs[1] = "longitude:[" + (bbox[0] - MAX_UNCERTAINTY / 100000.0) + " TO " + (bbox[2] + MAX_UNCERTAINTY / 100000.0) + "]";
            fqs[2] = "latitude:[" + (bbox[1] - MAX_UNCERTAINTY / 100000.0) + " TO " + (bbox[3] + MAX_UNCERTAINTY / 100000.0) + "]";

            requestParams.setPageSize(DEFAULT_PAGE_SIZE);

            for (int j = 0; j < uncertaintyFqs.length; j++) {
                //do not display for [1]=not specified
                if (j == 1) {
                    continue;
                }

                fqs[0] = uncertaintyFqs[j];
                requestParams.setFq(fqs);

                //There can be performance issues with pivot when too many distinct coordinate_uncertainty values
                requestParams.setFacets(new String[]{"coordinate_uncertainty", pointType.getLabel()});
                requestParams.setFlimit(-1);
                List<FacetPivotResultDTO> qr = searchDAO.searchPivot(requestParams);

                if (qr != null && qr.size() > 0) {
                    List<FacetPivotResultDTO> piv = qr.get(0).getPivotResult();

                    //for 4326
                    double top = tilebbox[3];
                    double bottom = tilebbox[1];
                    double left = tilebbox[0];
                    double right = tilebbox[2];

                    double lng, lat;
                    int x, y;
                    int uncertaintyRadius = (int) Math.ceil(uncertaintyR[j] * hmult);

                    g.setColor(uncertaintyColours[j]);
                    for (FacetPivotResultDTO r : piv) {
                        if (uncertaintyR[j] < 0) {
                            uncertaintyRadius = (int) Math.ceil(Double.parseDouble(r.getValue()) * hmult);
                        }

                        for (FacetPivotResultDTO point : r.getPivotResult()) {
                            String[] lat_lng = point.getValue().split(",");

                            lng = Double.parseDouble(lat_lng[1]);
                            lat = Double.parseDouble(lat_lng[0]);

                            if (is4326) {
                                x = convertLngToPixel4326(lng, left, right, width);
                                y = convertLatToPixel4326(lat, top, bottom, height);
                            } else {
                                x = (int) ((convertLngToPixel(lng) - pbbox[0]) * width_mult);
                                y = (int) ((convertLatToPixel(lat) - pbbox[3]) * height_mult);
                            }

                            if (uncertaintyRadius > 0) {
                                g.drawOval(x - uncertaintyRadius, y - uncertaintyRadius, uncertaintyRadius * 2, uncertaintyRadius * 2);
                            } else {
                                g.drawRect(x, y, 1, 1);
                            }
                        }
                    }
                }
            }
        }
    }

    ImgObj drawHighlight(SpatialSearchRequestParams requestParams, WmsEnv vars, PointType pointType,
                         int width, int height, double[] pbbox, double width_mult,
                         double height_mult, ImgObj imgObj, String[] originalFqs, String[] boundingBoxFqs,
                         boolean is4326, double[] tilebbox) throws Exception {
        String[] fqs = new String[originalFqs.length + 3];
        System.arraycopy(originalFqs, 0, fqs, 3, originalFqs.length);
        fqs[0] = vars.highlight;
        fqs[1] = boundingBoxFqs[0];
        fqs[2] = boundingBoxFqs[1];

        requestParams.setFq(fqs);
        requestParams.setFlimit(-1);
        FacetField ps = searchDAO.getFacetPointsShort(requestParams, pointType.getLabel());

        if (ps != null && ps.getValueCount() > 0) {
            if (imgObj == null || imgObj.img == null) {  //when vars.alpha == 0 img is null
                imgObj = ImgObj.create(width, height);
            }

            int highightRadius = vars.size + HIGHLIGHT_RADIUS;
            int highlightWidth = highightRadius * 2;

            imgObj.g.setStroke(new BasicStroke(2));
            imgObj.g.setColor(new Color(255, 0, 0, 255));
            int x, y;

            //for 4326
            double top = tilebbox[3];
            double bottom = tilebbox[1];
            double left = tilebbox[0];
            double right = tilebbox[2];

            for (int i = 0; i < ps.getValueCount(); i++) {
                //extract lat lng
                if (ps.getValues().get(i).getName() != null) {
                    String[] lat_lng = ps.getValues().get(i).getName().split(",");
                    float lng = Float.parseFloat(lat_lng[1]);
                    float lat = Float.parseFloat(lat_lng[0]);

                    if (is4326) {
                        x = convertLngToPixel4326(lng, left, right, width);
                        y = convertLatToPixel4326(lat, top, bottom, height);
                    } else {
                        x = (int) ((convertLngToPixel(lng) - pbbox[0]) * width_mult);
                        y = (int) ((convertLatToPixel(lat) - pbbox[3]) * height_mult);
                    }

                    imgObj.g.drawOval(x - highightRadius, y - highightRadius, highlightWidth, highlightWidth);
                }
            }
        }

        return imgObj;
    }

    /**
     * Returns the wms cache object and initialises it if required.
     *
     * @param vars
     * @param pointType
     * @param requestParams
     * @param bbox
     * @return
     * @throws Exception
     */
    WMSTile getWMSCacheObject(SpatialSearchRequestParams requestParams,
                              WmsEnv vars, PointType pointType,
                              double[] bbox, String[] originalFqs,
                              String[] boundingBoxFqs, boolean canCache) throws Exception {
        // do not cache this query if the cache is disabled or full
        if (wmsCache.isFull() || !wmsCache.isEnabled()) {
            canCache = false;
        }

        //caching is perTile
        String[] origAndBBoxFqs = null;
        if (originalFqs == null || originalFqs.length == 0) {
            origAndBBoxFqs = boundingBoxFqs;
        } else {
            origAndBBoxFqs = new String[originalFqs.length + 2];
            System.arraycopy(originalFqs, 0, origAndBBoxFqs, 2, originalFqs.length);
            origAndBBoxFqs[0] = boundingBoxFqs[0];
            origAndBBoxFqs[1] = boundingBoxFqs[1];
        }

        //replace qid with values for more cache hits
        String qparam = requestParams.getQ();
        if (qparam.startsWith("qid:")) {
            try {
                Qid qid = qidCacheDAO.get(qparam.substring(4));
                if (qid != null) {
                    qparam = qid.getQ() + qid.getWkt() + (qid.getFqs() != null ? StringUtils.join(qid.getFqs(), ",") : "");
                }
            } catch (Exception e) {
            }
        }

        String qfull = qparam + StringUtils.join(requestParams.getFq(), ",") + requestParams.getQc() +
                requestParams.getWkt() + requestParams.getRadius() + requestParams.getLat() + requestParams.getLon();

        //qfull can be long if there is WKT
        String q = String.valueOf(qfull.hashCode());

        //grid and -1 colour modes have the same data
        String cm = (vars.colourMode.equals("-1") || vars.colourMode.equals("grid")) ? "-1" : vars.colourMode;

        //if too many points, cache with bbox string
        boolean[] useBbox = new boolean[1];
        Integer count = 0;
        Integer pointsCount = 0;
        if (canCache) {
            //count docs
            count = getCachedCount(true, requestParams, q, pointType);
            if (count == 0) {
                return new WMSTile();
            }

            //count unique points, if necessary
            if (count > wmsCacheMaxLayerPoints && pointType.getValue() > 0) {
                pointsCount = getCachedCount(false, requestParams, q, pointType);

                //use bbox when too many points
                if (pointsCount > wmsCacheMaxLayerPoints) {
                    q += StringUtils.join(origAndBBoxFqs, ",");

                    requestParams.setFq(origAndBBoxFqs);
                    count = getCachedCount(true, requestParams, q, pointType);
                    requestParams.setFq(originalFqs);

                    if (count == 0) {
                        return new WMSTile();
                    }

                    useBbox[0] = true;
                }
            }
        }

        List<LegendItem> colours = null;
        int sz = 0;
        WMSTile wco = null;
        if (canCache) {
            //iterate from lower value pointTypes up to this one
            for (int i = 0; !useBbox[0] && i < PointType.values().length && wco == null; i++) {
                if (PointType.values()[i].getValue() < pointType.getValue()) {
                    wco = wmsCache.getTest(q, cm, PointType.values()[i]);
                }
            }

            //not found, create it
            if (wco == null) {
                requestParams.setFlimit(-1);
                colours = cm.equals("-1") ? null : getColours(requestParams, vars.colourMode);
                sz = colours == null ? 1 : colours.size() + 1;

                wco = wmsCache.get(q, cm, pointType);
            }

            if (wco.getCached()) {
                return wco;
            }
        } else {
            wco = new WMSTile();
        }

        //still need colours when cannot cache
        if (colours == null && !cm.equals("-1")) colours = getColours(requestParams, vars.colourMode);

        //build only once
        synchronized (wco) {
            if (wco.getCached()) {
                return wco;
            }

            // when there is only one colour, return the result for colourMode=="-1"
            if ((colours == null || colours.size() == 1) && !cm.equals("-1")) {
                String prevColourMode = vars.colourMode;
                vars.colourMode = "-1";
                WMSTile equivalentTile = getWMSCacheObject(requestParams, vars, pointType, bbox, originalFqs, boundingBoxFqs, canCache);
                vars.colourMode = prevColourMode;

                //use the correct colour
                List<Integer> pColour = new ArrayList<Integer>(1);
                pColour.add(colours != null ? colours.get(0).getColour() | (vars.alpha << 24) : vars.colour);
                wco.setColours(pColour);

                wco.setBbox(bbox);
                wco.setColourmode(vars.colourMode);
                wco.setCounts(equivalentTile.getCounts());
                wco.setPoints(equivalentTile.getPoints());
                wco.setQuery(q);
            } else {
                //query with the bbox when it cannot be cached
                if (!canCache || useBbox[0]) requestParams.setFq(origAndBBoxFqs);

                List<Integer> pColour = new ArrayList<Integer>(sz);
                List<float[]> pointsArrays = new ArrayList<float[]>(sz);
                List<int[]> countsArrays = cm.equals("-1") ? new ArrayList<int[]>(sz) : null;

                queryTile(requestParams, vars, pointType, countsArrays, pointsArrays, colours, pColour,
                        bbox, originalFqs, boundingBoxFqs, canCache, count);

                wco.setBbox(bbox);
                wco.setColourmode(vars.colourMode);
                wco.setColours(pColour);
                if (cm.equals("-1")) wco.setCounts(countsArrays);
                wco.setPoints(pointsArrays);
                wco.setQuery(q);
            }

            if (canCache) {
                wmsCache.put(q, cm, pointType, wco);
            }

            return wco;
        }
    }

    private Integer getCachedCount(boolean docCount, SpatialSearchRequestParams requestParams, String q, PointType pointType) throws Exception {

        Integer count = null;

        String tag = docCount ? "" : pointType.getLabel();

        synchronized (countLock) {
            count = (Integer) countsCache.get(q + tag);
        }
        if (count == null) {
            requestParams.setPageSize(0);
            requestParams.setFacet(true);
            requestParams.setFlimit(0);
            requestParams.setFacets(new String[]{pointType.getLabel()});
            if (docCount) {
                SolrDocumentList result = searchDAO.findByFulltext(requestParams);
                if (result != null) {
                    synchronized (countLock) {
                        count = (int) result.getNumFound();
                        countsCache.put(q + tag, count);
                    }
                }
            } else {
                List<GroupFacetResultDTO> result = searchDAO.searchGroupedFacets(requestParams);
                if (result != null && result.size() > 0) {
                    synchronized (countLock) {
                        count = result.get(0).getCount();
                        countsCache.put(q + tag, count);
                    }
                }
            }
        }

        return count;
    }

    private void queryTile(SpatialSearchRequestParams requestParams, WmsEnv vars, PointType pointType, List<int[]> countsArrays,
                           List<float[]> pointsArrays, List<LegendItem> colours, List<Integer> pColour,
                           double[] bbox, String[] originalFqs,
                           String[] boundingBoxFqs, boolean canCache, int docCount) throws Exception {

        if (colours != null && colours.size() > 1) {
            long t1 = System.currentTimeMillis();
            String[] origFqs = requestParams.getFq();

            int colrmax = -1;
            long colrmaxtime = -1;

            boolean numericalFacetCategories = vars.colourMode.contains(",");

            //in some instances querying each colour's facet, one by one, is more suitable than pivoting
            if (numericalFacetCategories || docCount > wmsFacetPivotCutoff || !canCache) {
                //iterate
                String[] fqs = new String[requestParams.getFq() == null ? 1 : requestParams.getFq().length + 1];
                if (requestParams.getFq() != null && requestParams.getFq().length > 0) {
                    System.arraycopy(requestParams.getFq(), 0, fqs, 1, requestParams.getFq().length);
                }
                List<String> fqsDone = new ArrayList<String>(colours != null ? colours.size() : 0);

                //draw grouped points before drawing other points.
                boolean otherPointsAdded = false;
                pointsArrays.add(null);
                pColour.add(null);

                for (int i = 0; i < colours.size(); i++) {
                    LegendItem li = colours.get(i);

                    fqs[0] = li.getFq();

                    //invert fq
                    if (StringUtils.isEmpty(li.getName())) {
                        //li.getFq() is of the form "-(...)"
                        fqsDone.add(fqs[0].substring(1));
                    } else {
                        if (fqs[0].charAt(0) == '-') {
                            fqsDone.add(fqs[0].substring(1));
                        } else {
                            fqsDone.add("-" + fqs[0]);
                        }
                    }

                    requestParams.setFq(fqs);
                    long ms = System.currentTimeMillis();
                    requestParams.setFlimit(-1);

                    makePointsFromFacet(searchDAO.getFacetPointsShort(requestParams, pointType.getLabel()), pointsArrays, countsArrays);
                    pColour.add(li.getColour() | (vars.alpha << 24));
                    colrmaxtime = (System.currentTimeMillis() - ms);

                    //in the last iteration check for more and batch.
                    if (i == colourList.length - 2 && colours.size() == colourList.length - 1) {
                        colrmax = i;

                        fqs = new String[(requestParams.getFq() == null ? 0 : requestParams.getFq().length) + fqsDone.size()];
                        if (requestParams.getFq() != null && requestParams.getFq().length > 0) {
                            System.arraycopy(requestParams.getFq(), 0, fqs, fqsDone.size(), requestParams.getFq().length);
                        }
                        System.arraycopy(fqsDone.toArray(new String[]{}), 0, fqs, 0, fqsDone.size());

                        //do full query
                        requestParams.setFq(origFqs);
                        String prevColourMode = vars.colourMode;
                        vars.colourMode = "-1";
                        WMSTile equivalentTile = getWMSCacheObject(requestParams, vars, pointType, bbox, originalFqs, boundingBoxFqs, canCache);
                        vars.colourMode = prevColourMode;

                        if (equivalentTile.getPoints() != null && equivalentTile.getPoints().size() > 0 && equivalentTile.getPoints().get(0).length > 0) {
                            pointsArrays.set(0, equivalentTile.getPoints().get(0));
                            //countsArrays.add(equivalentTile.getCounts().get(0));
                            pColour.set(0, colourList[colourList.length - 1] | (vars.alpha << 24));
                            otherPointsAdded = true;
                        }
                    }
                }

                if (!otherPointsAdded) {
                    pointsArrays.remove(0);
                    pColour.remove(0);
                }

                //restore the altered fqs
                requestParams.setFq(origFqs);
            }

            long t2 = System.currentTimeMillis();

            //pivot
            if (!numericalFacetCategories && docCount <= wmsFacetPivotCutoff) {
                requestParams.setFacets(new String[]{vars.colourMode + "," + pointType.getLabel()});
                requestParams.setFlimit(-1);
                //get pivot and drill to colourMode level
                List<FacetPivotResultDTO> qr = searchDAO.searchPivot(requestParams);
                if (qr != null && qr.size() > 0) {
                    List<FacetPivotResultDTO> piv = qr.get(0).getPivotResult();

                    //last colour
                    int lastColour = colourList[colourList.length - 1] | (vars.alpha << 24);

                    //get facet points
                    for (int j = 0; j < piv.size(); j++) {
                        FacetPivotResultDTO p = piv.get(j);
                        boolean added = false;
                        for (int i = 0; i < colours.size(); i++) {
                            LegendItem li = colours.get(i);
                            if ((StringUtils.isEmpty(li.getName()) && StringUtils.isEmpty(p.getValue()))
                                    || (StringUtils.isNotEmpty(li.getName()) && li.getName().equals(p.getValue()))) {
                                makePointsFromPivot(p.getPivotResult(), pointsArrays, countsArrays);
                                pColour.add(li.getColour() | (vars.alpha << 24));
                                added = true;
                                break;
                            }
                        }
                        if (!added) {
                            makePointsFromPivot(p.getPivotResult(), pointsArrays, countsArrays);
                            pColour.add(lastColour);
                        }
                    }
                }
            }

            long t3 = System.currentTimeMillis();

            if (logger.isDebugEnabled()) {
                int occurrences = 0;
                int points = 0;
                int colourMatches = pointsArrays.size();
                for (int i = 0; i < pointsArrays.size(); i++) {
                    points += pointsArrays.get(i).length;
                    if (countsArrays != null) {
                        for (int j = 0; j < pointsArrays.get(i).length; j++) {
                            occurrences += countsArrays.get(i)[j];
                        }
                    }
                }

                logger.debug("wms timings: many queries=" + (t2 - t1) + "ms, pivot=" + (t3 - t2) + "ms, " +
                        "colours=" + colours.size() + ", points=" + points + ", occurrences=" + occurrences + ", " +
                        "matchedColours=" + colourMatches + ", max colour idx=" + colrmax + ", " +
                        "max colour request time=" + colrmaxtime + "ms, query docCount=" + docCount);
            }

        }
        //get points for occurrences not in colours.
        if (colours == null || colours.isEmpty() || colours.size() == 1) {
            requestParams.setFlimit(-1);
            makePointsFromFacet(searchDAO.getFacetPointsShort(requestParams, pointType.getLabel()), pointsArrays, countsArrays);
            if (colours == null || colours.isEmpty()) {
                pColour.add(vars.colour);
            } else {
                pColour.add(colours.get(0).getColour() | (vars.alpha << 24));
            }
        }
    }

    private void makePointsFromPivot(List<FacetPivotResultDTO> pivotResult, List gPoints, List gCount) {
        float[] points = new float[2 * pivotResult.size()];
        int[] count = new int[pivotResult.size()];
        int i = 0;
        int j = 0;
        for (FacetPivotResultDTO fpr : pivotResult) {
            String v = fpr.getValue();
            if (StringUtils.isNotEmpty(v)) {
                int p = v.indexOf(',');
                points[i++] = Float.parseFloat(v.substring(p + 1));
                points[i++] = Float.parseFloat(v.substring(0, p));
                count[j++] = fpr.getCount();
            } else {
                points[i++] = Float.NaN;
                i++;
                count[j++] = 0;
            }
        }

        gPoints.add(points);
        if (gCount != null) gCount.add(count);
    }

    private void makePointsFromFacet(FacetField facet, List gPoints, List gCount) {
        float[] points = new float[2 * facet.getValues().size()];
        int[] count = new int[facet.getValues().size()];
        int i = 0;
        int j = 0;
        for (FacetField.Count s : facet.getValues()) {
            try {
                String v = s.getName();
                if (v != null) {
                    int p = v.indexOf(',');
                    float lng = Float.parseFloat(v.substring(p + 1));
                    float lat = Float.parseFloat(v.substring(0, p));
                    int c = (int) s.getCount();
                    points[i++] = lng;
                    points[i++] = lat;
                    count[j++] = c;
                } else {
                    points[i++] = Float.NaN;
                    i++;
                    count[j++] = 0;
                }
            } catch (Exception e) {
            }
        }

        //duplicate the last point in case of errors above
        while (i < points.length && i > 1) {
            points[i] = points[i - 2];
            i++;
        }
        while (j < count.length && j > 0) {
            count[j] = 0;
            j++;
        }

        gPoints.add(points);
        if (gCount != null) gCount.add(count);
    }

    private void renderPoints(WmsEnv vars, double[] bbox, double[] pbbox, double width_mult, double height_mult, int pointWidth, boolean outlinePoints, String outlineColour, List<Integer> pColour, ImgObj imgObj, int j, float[] ps, boolean is4326, double[] tilebbox, int height, int width) {
        int x;
        int y;
        Paint currentFill = new Color(pColour.get(j), true);
        imgObj.g.setPaint(currentFill);
        Color oColour = Color.decode(outlineColour);

        //for 4326
        double top = tilebbox[3];
        double bottom = tilebbox[1];
        double left = tilebbox[0];
        double right = tilebbox[2];

        for (int i = 0; i < ps.length; i += 2) {
            float lng = ps[i];
            float lat = ps[i + 1];

            if (lng >= bbox[0] && lng <= bbox[2]
                    && lat >= bbox[1] && lat <= bbox[3]) {

                if (is4326) {
                    x = convertLngToPixel4326(lng, left, right, width);
                    y = convertLatToPixel4326(lat, top, bottom, height);
                } else {
                    x = (int) ((convertLngToPixel(lng) - pbbox[0]) * width_mult);
                    y = (int) ((convertLatToPixel(lat) - pbbox[3]) * height_mult);
                }

                //System.out.println("Drawing an oval.....");
                imgObj.g.fillOval(x - vars.size, y - vars.size, pointWidth, pointWidth);
                if (outlinePoints) {
                    imgObj.g.setPaint(oColour);
                    imgObj.g.drawOval(x - vars.size, y - vars.size, pointWidth, pointWidth);
                    imgObj.g.setPaint(currentFill);
                }
            }
        }
    }

    //method from 1.3.3.1 Mercator (Spherical) http://www.epsg.org/guides/docs/g7-2.pdf
    //constant from EPSG:900913
    private double[] transformBbox4326To900913(double long1, double lat1, double long2, double lat2) {
        return new double[]{
                6378137.0 * long1 * Math.PI / 180.0,
                6378137.0 * Math.log(Math.tan(Math.PI / 4.0 + lat1 * Math.PI / 360.0)),
                6378137.0 * long2 * Math.PI / 180.0,
                6378137.0 * Math.log(Math.tan(Math.PI / 4.0 + lat2 * Math.PI / 360.0))
        };
    }

    public void setTaxonDAO(TaxonDAO taxonDAO) {
        this.taxonDAO = taxonDAO;
    }

    public void setSearchDAO(SearchDAO searchDAO) {
        this.searchDAO = searchDAO;
    }

    public void setSearchUtils(SearchUtils searchUtils) {
        this.searchUtils = searchUtils;
    }

    public void setBaseWsUrl(String baseWsUrl) {
        this.baseWsUrl = baseWsUrl;
    }

    public void setOrganizationName(String organizationName) {
        this.organizationName = organizationName;
    }

    public void setOrgCity(String orgCity) {
        this.orgCity = orgCity;
    }

    public void setOrgStateProvince(String orgStateProvince) {
        this.orgStateProvince = orgStateProvince;
    }

    public void setOrgPostcode(String orgPostcode) {
        this.orgPostcode = orgPostcode;
    }

    public void setOrgCountry(String orgCountry) {
        this.orgCountry = orgCountry;
    }

    public void setOrgPhone(String orgPhone) {
        this.orgPhone = orgPhone;
    }

    public void setOrgFax(String orgFax) {
        this.orgFax = orgFax;
    }

    public void setOrgEmail(String orgEmail) {
        this.orgEmail = orgEmail;
    }
}

class ImgObj {

    Graphics2D g;
    BufferedImage img;

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