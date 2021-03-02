/**************************************************************************
 *  Copyright (C) 2017 Atlas of Living Australia
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
import au.org.ala.biocache.util.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.geotools.geometry.GeneralDirectPosition;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.DefaultCoordinateOperationFactory;
import org.opengis.geometry.DirectPosition;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.CoordinateOperation;
import org.opengis.referencing.operation.TransformException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.ehcache.EhCacheManagerFactoryBean;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import javax.imageio.ImageIO;
import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

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
public class WMSController extends AbstractSecureController{

    /**
     * webportal results limit
     */
    @Value("${wms.pagesize:1000000}")
    private int DEFAULT_PAGE_SIZE = 1000000;

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
    protected QueryFormatUtils queryFormatUtils;
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

    @Value("${webservices.root:https://biocache-ws.ala.org.au/ws}")
    protected String baseWsUrl;

    @Value("${biocache.ui.url:https://biocache.ala.org.au}")
    protected String baseUiUrl;

    @Value("${geoserver.url:https://spatial.ala.org.au/geoserver}")
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

    @Value("${service.bie.ws.url:https://bie-ws.ala.org.au/ws}")
    protected String bieWebService;

    @Value("${service.bie.ui.url:https://bie.ala.org.au}")
    protected String bieUiUrl;

    @Value("${wms.capabilities.focus:latitude:[-90 TO 90] AND longitude:[-180 TO 180]}")
    protected String limitToFocusValue10;

    @Value("${wms.capabilities.focus.2.0:decimalLatitude:[-90 TO 90] AND decimaLongitude:[-180 TO 180]}")
    protected String limitToFocusValue20;

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

    /**
     * The public or private value to use in the Cache-Control HTTP header for WMS tiles. Defaults to public
     */
    @Value("${wms.cache.cachecontrol.publicorprivate:public}")
    private String wmsCacheControlHeaderPublicOrPrivate;

    /**
     * The max-age value to use in the Cache-Control HTTP header for WMS tiles. Defaults to 86400, equivalent to 1 day
     */
    @Value("${wms.cache.cachecontrol.maxage:86400}")
    private String wmsCacheControlHeaderMaxAge;

    private final AtomicReference<String> wmsETag = new AtomicReference<String>(UUID.randomUUID().toString());

    //Stores query hashes + occurrence counts, and, query hashes + pointType + point counts
    private LRUMap countsCache = new LRUMap(10000);
    private final Object countLock = new Object();

    @Inject
    protected WMSOSGridController wmsosGridController;

    static {
        // cache blank image bytes
        byte[] b = null;
        try (RandomAccessFile raf = new RandomAccessFile(WMSController.class.getResource("/blank.png").getFile(), "r");) {
            b = new byte[(int) raf.length()];
            raf.read(b);
        } catch (Exception e) {
            logger.error("Unable to open blank image file", e);
        }
        blankImageBytes = b;

        // configure geotools to use x/y order for SRS operations
        System.setProperty("org.geotools.referencing.forceXY", "true");
    }

    @Inject
    EhCacheManagerFactoryBean cacheManager;

    @RequestMapping(value = {"/webportal/params", "/mapping/params"}, method = RequestMethod.POST)
    public void storeParams(SpatialSearchRequestParams requestParams,
                            @RequestParam(value = "bbox", required = false, defaultValue = "false") String bbox,
                            @RequestParam(value = "title", required = false) String title,
                            @RequestParam(value = "maxage", required = false, defaultValue = "-1") Long maxage,
                            @RequestParam(value = "source", required = false) String source,
                            HttpServletResponse response) throws Exception {

        //set default values for parameters not stored in the qid.
        requestParams.setFl("");
        requestParams.setFacets(new String[] {});
        requestParams.setStart(0);
        requestParams.setFacet(false);
        requestParams.setFlimit(0);
        requestParams.setPageSize(0);
        requestParams.setSort("score");
        requestParams.setDir("asc");
        requestParams.setFoffset(0);
        requestParams.setFprefix("");
        requestParams.setFsort("");
        requestParams.setIncludeMultivalues(false);

        //move qc to fq
        if (StringUtils.isNotEmpty(requestParams.getQc())) {
            queryFormatUtils.addFqs(new String[]{requestParams.getQc()}, requestParams);
            requestParams.setQc("");
        }

        //move lat/lon/radius to fq
        if (requestParams.getLat() != null) {
            String fq = queryFormatUtils.buildSpatialQueryString(requestParams);
            queryFormatUtils.addFqs(new String[]{fq}, requestParams);
            requestParams.setLat(null);
            requestParams.setLon(null);
            requestParams.setRadius(null);
        }

        String qid = qidCacheDAO.generateQid(requestParams, bbox, title, maxage, source);
        if (qid == null) {
            if(StringUtils.isEmpty(requestParams.getWkt())){
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unable to generate QID for query");
            } else {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "WKT provided failed to be simplified.");
            }
        } else {
            response.setContentType("text/plain");
            writeBytes(response, qid.getBytes());
        }
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
     * Test presence of query params {id} in params store.
     */
    @RequestMapping(value = {"/qid/{id}", "/mapping/qid/{id}"}, method = RequestMethod.GET)
    public
    @ResponseBody
    Qid showQid(@PathVariable("id") Long id) throws Exception {
        return qidCacheDAO.get(String.valueOf(id));
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

        response.setHeader("Cache-Control", wmsCacheControlHeaderPublicOrPrivate + ", max-age=" + wmsCacheControlHeaderMaxAge);
        response.setHeader("ETag", wmsETag.get());

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
        requestParams.setFormattedQuery(null);
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
                colour = ColorUtil.colourList[Math.min(i, ColorUtil.colourList.length - 1)];
            } else if (cutpoints != null && i - offset < cutpoints.length) {
                if (name.equals(NULL_NAME) || name.startsWith("-")) {
                    offset++;
                    colour = DEFAULT_COLOUR;
                } else {
                    colour = ColorUtil.getRangedColour(i - offset, cutpoints.length / 2);
                }
            }
            li.setRGB(colour);
            if (isCsv) {
                sb.append("\n\"").append(name.replace("\"", "\"\"")).append("\",").append(ColorUtil.getRGB(colour)) //repeat last colour if required
                        .append(",").append(legend.get(i).getCount());
            }
        }

        response.setHeader("Cache-Control", wmsCacheControlHeaderPublicOrPrivate + ", max-age=" + wmsCacheControlHeaderMaxAge);
        response.setHeader("ETag", wmsETag.get());

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

        response.setHeader("Cache-Control", wmsCacheControlHeaderPublicOrPrivate + ", max-age=" + wmsCacheControlHeaderMaxAge);
        response.setHeader("ETag", wmsETag.get());

        double[] bbox = null;

        if (bbox == null) {
            bbox = searchDAO.getBBox(requestParams);
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

        response.setHeader("Cache-Control", wmsCacheControlHeaderPublicOrPrivate + ", max-age=" + wmsCacheControlHeaderMaxAge);
        response.setHeader("ETag", wmsETag.get());

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
            bbox = searchDAO.getBBox(requestParams);
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
            Model model,
            HttpServletResponse response) throws Exception {

        if (StringUtils.isEmpty(requestParams.getQ())) {
            return new SearchResultDTO();
        }

        //searchUtils.updateSpatial(requestParams);
        SearchResultDTO searchResult = searchDAO.findByFulltextSpatialQuery(requestParams, null);
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
            HttpServletResponse response) {

        response.setContentType("text/plain");
//        response.setCharacterEncoding("gzip");

        try {
            ServletOutputStream outStream = response.getOutputStream();
            java.util.zip.GZIPOutputStream gzip = new java.util.zip.GZIPOutputStream(outStream);

            writeOccurrencesCsvToStream(requestParams, gzip);

            gzip.flush();
            gzip.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
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
        try {
            response.setContentType("text/plain");
            response.setCharacterEncoding("UTF-8");
            ServletOutputStream outStream = response.getOutputStream();
            outStream.write(bytes);
            outStream.flush();
            outStream.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    int scaleLatitudeForImage(double lat, double top, double bottom, int pixelHeight) {
        return (int) (((lat - top) / (bottom - top)) * pixelHeight);
    }

    int scaleLongitudeForImage(double lng, double left, double right, int pixelWidth) {
        return (int) (((lng - left) / (right - left)) * pixelWidth);
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
     *
     * @param transformTo4326 TransformOp to convert from the target SRS to the coordinates in SOLR (EPSG:4326)
     * @param bboxString getMap bbox parameter with the tile extents in the target SRS as min x, min y, max x, max y
     * @param width getMap width value in pixels
     * @param height getMap height value in pixels
     * @param size dot radius in pixels used to calculate a larger bounding box for the request to SOLR for coordinates
     * @param uncertainty boolean to trigger a larger bounding box for the request to SOLR for coordinates (??)
     * @param mbbox bounding box in metres (spherical mercator). Includes pixel correction buffer.
     * @param bbox bounding box for the SOLR request. This is the bboxString transformed to EPSG:4326 with buffer of
     *            dot size + max uncertainty radius + pixel correction
     * @param pbbox bounding box in the target SRS corresponding to the output pixel positions. Includes pixel correction buffer.
     * @param tilebbox raw coordinates from the getMap bbox parameter
     * @return degrees per pixel to determine which SOLR coordinate field to facet upon
     */
    private double getBBoxesSRS(CoordinateOperation transformTo4326, String bboxString, int width, int height, int size, boolean uncertainty, double[] mbbox, double[] bbox, double[] pbbox, double[] tilebbox) throws TransformException {
        String[] splitBBox = bboxString.split(",");
        for (int i = 0; i < 4; i++) {
            try {
                tilebbox[i] = Double.parseDouble(splitBBox[i]);
                mbbox[i] = tilebbox[i];
            } catch (Exception e) {
                logger.error("Problem parsing BBOX: '" + bboxString + "' at position " + i, e);
                tilebbox[i] = 0.0d;
                mbbox[i] = 0.0d;
            }
        }

        // pixel correction buffer: adjust bbox extents with half pixel width/height
        double pixelWidthInTargetSRS = (mbbox[2] - mbbox[0]) / (double) width;
        double pixelHeightInTargetSRS = (mbbox[3] - mbbox[1]) / (double) height;
        mbbox[0] += pixelWidthInTargetSRS / 2.0;
        mbbox[2] -= pixelWidthInTargetSRS / 2.0;
        mbbox[1] += pixelHeightInTargetSRS / 2.0;
        mbbox[3] -= pixelHeightInTargetSRS / 2.0;

        // when an SRS is not aligned with EPSG:4326 the dot size may be too small.
        double srsOffset = 10;

        //offset for points bounding box by dot size
        double xoffset = pixelWidthInTargetSRS * (size + 1) * srsOffset;
        double yoffset = pixelHeightInTargetSRS * (size + 1) * srsOffset;

        pbbox[0] = mbbox[0];
        pbbox[1] = mbbox[1];
        pbbox[2] = mbbox[2];
        pbbox[3] = mbbox[3];

        GeneralDirectPosition directPositionSW = new GeneralDirectPosition(mbbox[0] - xoffset, mbbox[1] - yoffset);
        GeneralDirectPosition directPositionNE = new GeneralDirectPosition(mbbox[2] + xoffset, mbbox[3] + yoffset);
        GeneralDirectPosition directPositionSE = new GeneralDirectPosition(mbbox[2] - xoffset, mbbox[1] - yoffset);
        GeneralDirectPosition directPositionNW = new GeneralDirectPosition(mbbox[0] + xoffset, mbbox[3] + yoffset);
        DirectPosition sw4326 = transformTo4326.getMathTransform().transform(directPositionSW, null);
        DirectPosition ne4326 = transformTo4326.getMathTransform().transform(directPositionNE, null);
        DirectPosition se4326 = transformTo4326.getMathTransform().transform(directPositionSE, null);
        DirectPosition nw4326 = transformTo4326.getMathTransform().transform(directPositionNW, null);

        bbox[0] = Math.min(Math.min(Math.min(sw4326.getOrdinate(0), ne4326.getOrdinate(0)), se4326.getOrdinate(0)), nw4326.getOrdinate(0));
        bbox[1] = Math.min(Math.min(Math.min(sw4326.getOrdinate(1), ne4326.getOrdinate(1)), se4326.getOrdinate(1)), nw4326.getOrdinate(1));
        bbox[2] = Math.max(Math.max(Math.max(sw4326.getOrdinate(0), ne4326.getOrdinate(0)), se4326.getOrdinate(0)), nw4326.getOrdinate(0));
        bbox[3] = Math.max(Math.max(Math.max(sw4326.getOrdinate(1), ne4326.getOrdinate(1)), se4326.getOrdinate(1)), nw4326.getOrdinate(1));

        double degreesPerPixel = Math.min((bbox[2] - bbox[0]) / (double) width,
                (bbox[3] - bbox[1]) / (double) height);

        return degreesPerPixel;
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
        String newQuery = OccurrenceIndex.RAW_NAME + ":" + taxonName;
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
            newQuery = leftNode != null && rightNode != null ? "lft:[" + leftNode.asText() + " TO " + rightNode.asText() + "]" : "taxonConceptID:" + guid;
            if (logger.isDebugEnabled()) {
                logger.debug("The new query : " + newQuery);
            }

            //common name
            JsonNode commonNameNode = tc.get("commonNameSingle");
            if (commonNameNode != null) {
                model.addAttribute("commonName", commonNameNode.asText());
                if (logger.isDebugEnabled()) {
                    logger.debug("retrieved name: " + commonNameNode.asText());
                }
            }

            //name
            JsonNode nameNode = tc.get("nameComplete");
            if (nameNode != null) {
                model.addAttribute("name", nameNode.asText());
                if (logger.isDebugEnabled()) {
                    logger.debug("retrieved name: " + nameNode.asText());
                }
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
        searchParams.setFacets(new String[]{OccurrenceIndex.DATA_RESOURCE_NAME});
        searchParams.setPageSize(0);
        List<FacetResultDTO> facets = searchDAO.getFacetCounts(searchParams);
        model.addAttribute("query", newQuery); //need a facet on data providers
        model.addAttribute("dataProviders", facets.get(0).getFieldResult()); //need a facet on data providers
        return "metadata/mcp";
    }

    @RequestMapping(value = {"/ogc/getFeatureInfo"}, method = RequestMethod.GET)
    public String getFeatureInfo(
            @RequestParam(value = "ENV", required = false, defaultValue = "") String env,
            @RequestParam(value = "BBOX", required = true, defaultValue = "0,-90,180,0") String bboxString,
            @RequestParam(value = "WIDTH", required = true, defaultValue = "256") Integer width,
            @RequestParam(value = "HEIGHT", required = true, defaultValue = "256") Integer height,
            @RequestParam(value = "STYLES", required = false, defaultValue = "") String styles,
            @RequestParam(value = "SRS", required = false, defaultValue = "EPSG:3857") String srs,
            @RequestParam(value = "QUERY_LAYERS", required = false, defaultValue = "") String queryLayers,
            @RequestParam(value = "X", required = true, defaultValue = "0") Double x,
            @RequestParam(value = "Y", required = true, defaultValue = "0") Double y,
            Model model) throws Exception {

        if (logger.isDebugEnabled()) {
            logger.debug("WMS - GetFeatureInfo requested for: " + queryLayers);
        }

        WmsEnv vars = new WmsEnv(env, styles);
        double[] mbbox = new double[4];
        double[] bbox = new double[4];
        double[] pbbox = new double[4];
        double[] tilebbox = new double[4];
        int size = vars.size + (vars.highlight != null ? HIGHLIGHT_RADIUS * 2 + (int) (vars.size * 0.2) : 0) + 5;  //bounding box buffer

        CRSAuthorityFactory factory = CRS.getAuthorityFactory(true);
        CoordinateReferenceSystem sourceCRS = factory.createCoordinateReferenceSystem(srs);
        CoordinateReferenceSystem targetCRS = factory.createCoordinateReferenceSystem("EPSG:4326");
        CoordinateOperation transformTo4326 = new DefaultCoordinateOperationFactory().createOperation(sourceCRS, targetCRS);

        double resolution;

        // support for any srs
        resolution = getBBoxesSRS(transformTo4326, bboxString, width, height, size, vars.uncertainty, mbbox, bbox, pbbox, tilebbox);

        //resolution should be a value < 1
        PointType pointType = getPointTypeForDegreesPerPixel(resolution);

        double longitude = bbox[0] + (((bbox[2] - bbox[0]) / width.doubleValue()) * x);
        double latitude = bbox[3] - (((bbox[3] - bbox[1]) / height.doubleValue()) * y);

        //round to the correct point size
//        double roundedLongitude = pointType.roundToPointType(longitude);
//        double roundedLatitude = pointType.roundToPointType(latitude);
        double roundedLongitude = longitude;
        double roundedLatitude = latitude;

        //get the pixel size of the circles
        double minLng = pointType.roundDownToPointType(roundedLongitude - (pointType.getValue() * 2 * (size + 3)));
        double maxLng = pointType.roundUpToPointType(roundedLongitude + (pointType.getValue() * 2 * (size + 3)));
        double minLat = pointType.roundDownToPointType(roundedLatitude - (pointType.getValue() * 2 * (size + 3)));
        double maxLat = pointType.roundUpToPointType(roundedLatitude + (pointType.getValue() * 2 * (size + 3)));

        //do the SOLR query
        SpatialSearchRequestParams requestParams = new SpatialSearchRequestParams();

        String longitudeField = OccurrenceIndex.LONGITUDE;
        String latitudeField = OccurrenceIndex.LATITUDE;

        String q = WMSUtils.convertLayersParamToQ(queryLayers);
        requestParams.setQ(WMSUtils.convertLayersParamToQ(queryLayers));  //need to derive this from the layer name
        if (logger.isDebugEnabled()) {
            logger.debug("WMS GetFeatureInfo for " + queryLayers + ", " + longitudeField + ":[" + minLng + " TO " + maxLng + "],  " + latitudeField + ":[" + minLat + " TO " + maxLat + "]");
        }

        String[] fqs = new String[]{longitudeField + ":[" + minLng + " TO " + maxLng + "]", latitudeField + ":[" + minLat + " TO " + maxLat + "]"};
        requestParams.setFq(fqs);
        requestParams.setFacet(false);

        //TODO: paging
        SolrDocumentList sdl = searchDAO.findByFulltext(requestParams);

        //send back the results.
        if (sdl != null && sdl.size() > 0) {
            SolrDocument doc = sdl.get(0);
            model.addAttribute("record", doc.getFieldValueMap());
            model.addAttribute("totalRecords", sdl.getNumFound());
        }

        model.addAttribute("uriUrl", baseUiUrl + "/occurrences/search?q=" +
                URLEncoder.encode(q == null ? "" : q, "UTF-8")
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
            if (logger.isDebugEnabled()) {
                logger.debug("WMS - GetLegendGraphic requested : " + request.getQueryString());
            }
            try (OutputStream out = response.getOutputStream();) {
                response.setContentType("image/png");
                response.setHeader("Cache-Control", wmsCacheControlHeaderPublicOrPrivate + ", max-age=" + wmsCacheControlHeaderMaxAge);
                response.setHeader("ETag", wmsETag.get());
                ImageIO.write(img, "png", out);
            }
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
            @RequestParam(value = "SRS", required = false, defaultValue = "EPSG:3857") String srs, //default to google mercator
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

            // Depricated RequestParam
            @RequestParam(value = "spatiallyValidOnly", required = false, defaultValue = "true") boolean spatiallyValidOnly,

            @RequestParam(value = "marineSpecies", required = false, defaultValue = "false") boolean marineOnly,
            @RequestParam(value = "terrestrialSpecies", required = false, defaultValue = "false") boolean terrestrialOnly,
            @RequestParam(value = "limitToFocus", required = false, defaultValue = "false") boolean limitToFocus,
            @RequestParam(value = "useSpeciesGroups", required = false, defaultValue = "false") boolean useSpeciesGroups,
            @RequestParam(value = "GRIDDETAIL", required = false, defaultValue = "16") int gridDivisionCount,
            HttpServletRequest request,
            HttpServletResponse response,
            Model model)
            throws Exception {

        if ("GetMap".equalsIgnoreCase(requestString)) {
            generateWmsTileViaHeatmap(
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
                    env,
                    bboxString,
                    width,
                    height,
                    styles,
                    srs,
                    layers,
                    x,
                    y,
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

            String supportedCodes = "";
            for (String code : CRS.getSupportedCodes("EPSG")) {
                supportedCodes += "      <SRS>EPSG:" + code + "</SRS>\n";
            }

            writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<!DOCTYPE WMT_MS_Capabilities SYSTEM \"https://spatial.ala.org.au/geoserver/schemas/wms/1.1.1/WMS_MS_Capabilities.dtd\">\n" +
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
                    supportedCodes +
                    "     <LatLonBoundingBox minx=\"-179.9\" miny=\"-89.9\" maxx=\"179.9\" maxy=\"89.9\"/>\n"
            );

            writer.write(generateStylesForPoints());

            if (marineOnly) {
                filterQueries = org.apache.commons.lang3.ArrayUtils.add(filterQueries, OccurrenceIndex.SPECIES_HABITATS + ":Marine OR " + OccurrenceIndex.SPECIES_HABITATS + ":\"Marine and Non-marine\"");
            }

            if (terrestrialOnly) {
                filterQueries = org.apache.commons.lang3.ArrayUtils.add(filterQueries, OccurrenceIndex.SPECIES_HABITATS + ":\"Non-marine\" OR " + OccurrenceIndex.SPECIES_HABITATS + ":Limnetic");
            }

            if (limitToFocus) {
                //TODO retrieve focus from config file
                filterQueries = org.apache.commons.lang3.ArrayUtils.add(filterQueries, limitToFocusValue20);
            }

            query = searchUtils.convertRankAndName(query);
            if (logger.isDebugEnabled()) {
                logger.debug("GetCapabilities query in use: " + query);
            }

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
        for (String color : ColorUtil.colorsNames) {
            for (String size : sizes) {
                for (String opacity : opacities) {
                    sb.append(
                            "<Style>\n" +
                                    "<Name>" + ColorUtil.colorsCodes[colorIdx] + ";opacity=" + opacity + ";size=" + size + "</Name> \n" +
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

    @RequestMapping(value = {"/webportal/wms/clearCache", "/ogc/wms/clearCache", "/mapping/wms/clearCache"}, method = RequestMethod.GET)
    public void clearWMSCache(HttpServletResponse response,
                              @RequestParam(value = "apiKey") String apiKey) throws Exception {
        if (isValidKey(apiKey)) {
            wmsCache.empty();
            response.setStatus(200);
            regenerateWMSETag();
        } else {
            response.setStatus(401);
        }
    }

    /**
     * Regenerate the ETag after clearing the WMS cache so that cached responses are identified as out of date
     */
    private void regenerateWMSETag() {
        wmsETag.set(UUID.randomUUID().toString());
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
    public ModelAndView generateWmsTileViaHeatmap(
            SpatialSearchRequestParams requestParams,
            @RequestParam(value = "CQL_FILTER", required = false, defaultValue = "") String cql_filter,
            @RequestParam(value = "ENV", required = false, defaultValue = "") String env,
            @RequestParam(value = "SRS", required = false, defaultValue = "EPSG:3857") String srs, //default to google mercator
            @RequestParam(value = "STYLES", required = false, defaultValue = "") String styles,
            @RequestParam(value = "BBOX", required = true, defaultValue = "") String bboxString,
            @RequestParam(value = "WIDTH", required = true, defaultValue = "256") Integer width,
            @RequestParam(value = "HEIGHT", required = true, defaultValue = "256") Integer height,
            @RequestParam(value = "CACHE", required = true, defaultValue = "default") String cache,
            @RequestParam(value = "REQUEST", required = true, defaultValue = "") String requestString,
            @RequestParam(value = "OUTLINE", required = true, defaultValue = "true") boolean outlinePoints,
            @RequestParam(value = "OUTLINECOLOUR", required = true, defaultValue = "0x000000") String outlineColour,
            @RequestParam(value = "LAYERS", required = false, defaultValue = "") String layers,
            @RequestParam(value = "HQ", required = false) String[] hqs,
            @RequestParam(value = "GRIDDETAIL", required = false, defaultValue = "16") Integer gridDivisionCount,
            HttpServletRequest request,
            HttpServletResponse response)
            throws Exception {

        //for OS Grids, hand over to WMS OS controller
        if (env != null && env.contains("osgrid")) {
            wmsosGridController.generateWmsTile(requestParams, cql_filter, env, srs, styles, bboxString, layers, width, height, outlinePoints, outlineColour, request, response);
            return null;
        }

        //Some WMS clients are ignoring sections of the GetCapabilities....
        if ("GetLegendGraphic".equalsIgnoreCase(requestString)) {
            getLegendGraphic(env, styles, 30, 20, request, response);
            return null;
        }

        if (StringUtils.isBlank(bboxString)) {
            return sendWmsError(response, 400, "MissingOrInvalidParameter",
                    "Missing valid BBOX parameter");
        }

        // Used to hide certain values from the layer
        Set<Integer> hiddenFacets = new HashSet<Integer>();
        if (hqs != null && hqs.length > 0) {
            for (String h : hqs) {
                hiddenFacets.add(Integer.parseInt(h));
            }
        }

        WmsEnv vars = new WmsEnv(env, styles);

        String[] splitBBox = bboxString.split(",");
        double[] tilebbox = new double[4];
        for (int i = 0; i < 4; i++) {
            try {
                tilebbox[i] = Double.parseDouble(splitBBox[i]);
            } catch (Exception e) {
                return sendWmsError(response, 400, "MissingOrInvalidParameter",
                        "Missing valid BBOX parameter");
            }
        }

        double[] bbox = reprojectBBox(tilebbox, srs);

        boolean isGrid = vars.colourMode.equals("grid");
        logger.info("vars.colourMode = " + vars.colourMode);

        // add a buffer of 10px
        // add buffer
        float pointWidth = (float) (vars.size * 2);

        // format the query -  this will deal with radius / wkt
        queryFormatUtils.formatSearchQuery(requestParams, true);

        //retrieve legend
        List<LegendItem> legend = searchDAO.getColours(requestParams, vars.colourMode);

        if (!isGrid) {
            // increase BBOX by pointWidth either side
            bbox[0] = bbox[0] - ((pointWidth / (256f)) * (bbox[2] - bbox[0]));
            bbox[1] = bbox[1] - ((pointWidth / (256f)) * (bbox[3] - bbox[1]));
            bbox[2] = bbox[2] + ((pointWidth / (256f)) * (bbox[2] - bbox[0]));
            bbox[3] = bbox[3] + ((pointWidth / (256f)) * (bbox[3] - bbox[1]));
        }

        HeatmapDTO heatmapDTO = searchDAO.getHeatMap(requestParams, bbox[0], bbox[1], bbox[2], bbox[3], legend, hiddenFacets, isGrid);

        if (heatmapDTO.layers == null) {
            displayBlankImage(response);
            return null;
        }

        // render PNG...
        ImgObj tile = renderHeatmap(heatmapDTO,
                vars,
                (int) pointWidth,
                outlinePoints,
                outlineColour,
                width,
                height
        );

        if (tile != null && tile.g != null) {
            tile.g.dispose();
            try (ServletOutputStream outStream = response.getOutputStream();) {
                ImageIO.write(tile.img, "png", outStream);
                outStream.flush();
            } catch (Exception e) {
                logger.debug("Unable to write image", e);
            }
        } else {
            displayBlankImage(response);
        }
        return null;
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
    @RequestMapping(value = {"/webportal/v1wms/reflect", "/ogc/v1/wms/reflect", "/mapping/v1/wms/reflect"}, method = RequestMethod.GET)
    public ModelAndView generateWmsTile(
            SpatialSearchRequestParams requestParams,
            @RequestParam(value = "CQL_FILTER", required = false, defaultValue = "") String cql_filter,
            @RequestParam(value = "ENV", required = false, defaultValue = "") String env,
            @RequestParam(value = "SRS", required = false, defaultValue = "EPSG:3857") String srs, //default to google mercator
            @RequestParam(value = "STYLES", required = false, defaultValue = "") String styles,
            @RequestParam(value = "BBOX", required = true, defaultValue = "") String bboxString,
            @RequestParam(value = "WIDTH", required = true, defaultValue = "256") Integer width,
            @RequestParam(value = "HEIGHT", required = true, defaultValue = "256") Integer height,
            @RequestParam(value = "CACHE", required = true, defaultValue = "default") String cache,
            @RequestParam(value = "REQUEST", required = true, defaultValue = "") String requestString,
            @RequestParam(value = "OUTLINE", required = true, defaultValue = "true") boolean outlinePoints,
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
            return null;
        }

        //correct cache value
        if ("default".equals(cache)) cache = wmsCacheEnabled ? "on" : "off";

        //Some WMS clients are ignoring sections of the GetCapabilities....
        if ("GetLegendGraphic".equalsIgnoreCase(requestString)) {
            getLegendGraphic(env, styles, 30, 20, request, response);
            return null;
        }

        if (StringUtils.isBlank(bboxString)){
            return sendWmsError(response, 400, "MissingOrInvalidParameter",
                    "Missing valid BBOX parameter");
        }

        Set<Integer> hq = new HashSet<Integer>();
        if (hqs != null && hqs.length > 0) {
            for (String h : hqs) {
                hq.add(Integer.parseInt(h));
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("WMS tile: " + request.getQueryString());
        }

        response.setHeader("Cache-Control", wmsCacheControlHeaderPublicOrPrivate + ", max-age=" + wmsCacheControlHeaderMaxAge);
        response.setHeader("ETag", wmsETag.get());
        response.setContentType("image/png"); //only png images generated

        WmsEnv vars = new WmsEnv(env, styles);
        double[] mbbox = new double[4];
        double[] bbox = new double[4];
        double[] pbbox = new double[4];
        double[] tilebbox = new double[4];

        //bbox adjustment for WMSCache is better with a stepped size
        int steppedSize = (int) (Math.ceil(vars.size / 20.0) * 20);
        int size = steppedSize + (vars.highlight != null ? HIGHLIGHT_RADIUS * 2 + (int) (steppedSize * 0.2) : 0) + 5;  //bounding box buffer

        CRSAuthorityFactory factory = CRS.getAuthorityFactory(true);
        CoordinateReferenceSystem sourceCRS = factory.createCoordinateReferenceSystem(srs);
        CoordinateReferenceSystem targetCRS = factory.createCoordinateReferenceSystem("EPSG:4326");
        CoordinateOperation transformTo4326 = new DefaultCoordinateOperationFactory().createOperation(sourceCRS, targetCRS);
        CoordinateOperation transformFrom4326 = new DefaultCoordinateOperationFactory().createOperation(targetCRS, sourceCRS);

        double resolution;

        // support for any srs
        resolution = getBBoxesSRS(transformTo4326, bboxString, width, height, size, vars.uncertainty, mbbox, bbox, pbbox, tilebbox);

        PointType pointType = getPointTypeForDegreesPerPixel(resolution);
        if (logger.isDebugEnabled()) {
            logger.debug("Rendering: " + pointType.name());
        }

        String longitudeField = OccurrenceIndex.LONGITUDE;
        String latitudeField = OccurrenceIndex.LATITUDE;


        String[] boundingBoxFqs = new String[2];
        boundingBoxFqs[0] = String.format(Locale.ROOT, longitudeField + ":[%f TO %f]", bbox[0], bbox[2]);
        boundingBoxFqs[1] = String.format(Locale.ROOT, latitudeField + ":[%f TO %f]", bbox[1], bbox[3]);

        int pointWidth = vars.size * 2;
        double width_mult = (width / (pbbox[2] - pbbox[0]));
        double height_mult = (height / (pbbox[1] - pbbox[3]));


        //CQL Filter takes precedence of the layer
        String q = "";
        if (StringUtils.trimToNull(cql_filter) != null) {
            q = WMSUtils.getQ(cql_filter);
        } else if (StringUtils.trimToNull(layers) != null && !"ALA:Occurrences".equalsIgnoreCase(layers)) {
            q = WMSUtils.convertLayersParamToQ(layers);
        }

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
                return null;
            }
        }

        String[] originalFqs = qidCacheDAO.getFq(requestParams);

        //get from cache, or make it
        boolean canCache = wmsCache.isEnabled() && "on".equalsIgnoreCase(cache);
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

        ImgObj imgObj = wco.getPoints() == null ? null :
                wmsCached(wco, requestParams, vars, pointType, pbbox, bbox, mbbox, width, height, width_mult,
                        height_mult, pointWidth, originalFqs, hq, boundingBoxFqs, outlinePoints, outlineColour,
                        response, tilebbox, gridDivisionCount, transformFrom4326);

        if (imgObj != null && imgObj.g != null) {
            imgObj.g.dispose();
            try (ServletOutputStream outStream = response.getOutputStream();){
                ImageIO.write(imgObj.img, "png", outStream);
                outStream.flush();
            } catch (Exception e) {
                logger.debug("Unable to write image", e);
            }
        } else {
            displayBlankImage(response);
        }
        return null;
    }

    private ModelAndView sendWmsError(HttpServletResponse response, int status, String errorType, String errorDescription) {
        response.setStatus(status);
        Map<String,String> model = new HashMap<String,String>();
        model.put("errorType", errorType);
        model.put("errorDescription", errorDescription);
        return new ModelAndView("wms/error", model);
    }

    private void transformBBox(CoordinateOperation op, String bbox, double[] source, double[] target) throws TransformException {

        String[] bb = bbox.split(",");

        source[0] = Double.parseDouble(bb[0]);
        source[1] = Double.parseDouble(bb[1]);
        source[2] = Double.parseDouble(bb[2]);
        source[3] = Double.parseDouble(bb[3]);

        GeneralDirectPosition sw = new GeneralDirectPosition(source[0], source[1]);
        GeneralDirectPosition ne = new GeneralDirectPosition(source[2], source[3]);
        GeneralDirectPosition se = new GeneralDirectPosition(source[2], source[1]);
        GeneralDirectPosition nw = new GeneralDirectPosition(source[0], source[3]);
        DirectPosition targetSW = op.getMathTransform().transform(sw, null);
        DirectPosition targetNE = op.getMathTransform().transform(ne, null);
        DirectPosition targetSE = op.getMathTransform().transform(se, null);
        DirectPosition targetNW = op.getMathTransform().transform(nw, null);

        target[0] = Math.min(Math.min(Math.min(targetSW.getOrdinate(0), targetNE.getOrdinate(0)), targetSE.getOrdinate(0)), targetNW.getOrdinate(0));
        target[1] = Math.min(Math.min(Math.min(targetSW.getOrdinate(1), targetNE.getOrdinate(1)), targetSE.getOrdinate(1)), targetNW.getOrdinate(1));
        target[2] = Math.max(Math.max(Math.max(targetSW.getOrdinate(0), targetNE.getOrdinate(0)), targetSE.getOrdinate(0)), targetNW.getOrdinate(0));
        target[3] = Math.max(Math.max(Math.max(targetSW.getOrdinate(1), targetNE.getOrdinate(1)), targetSE.getOrdinate(1)), targetNW.getOrdinate(1));
    }

    /**
     * Method that produces the downloadable map integrated in AVH/OZCAM/Biocache.
     *
     * @param requestParams
     * @param format
     * @param extents bounding box in decimal degrees
     * @param bboxString bounding box in target SRS
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
            @RequestParam(value = "extents", required = false) String extents,
            @RequestParam(value = "bbox", required = false) String bboxString,
            @RequestParam(value = "widthmm", required = false, defaultValue = "60") Double widthMm,
            @RequestParam(value = "pradiusmm", required = false, defaultValue = "2") Double pointRadiusMm,
            @RequestParam(value = "pradiuspx", required = false) Integer pradiusPx,
            @RequestParam(value = "pcolour", required = false, defaultValue = "FF0000") String pointColour,
            @RequestParam(value = "ENV", required = false, defaultValue = "") String env,
            @RequestParam(value = "SRS", required = false, defaultValue = "EPSG:3857") String srs,
            @RequestParam(value = "popacity", required = false, defaultValue = "0.8") Double pointOpacity,
            @RequestParam(value = "baselayer", required = false, defaultValue = "world") String baselayer,
            @RequestParam(value = "scale", required = false, defaultValue = "off") String scale,
            @RequestParam(value = "dpi", required = false, defaultValue = "300") Integer dpi,
            @RequestParam(value = "baselayerStyle", required = false, defaultValue = "") String baselayerStyle,
            @RequestParam(value = "outline", required = true, defaultValue = "false") boolean outlinePoints,
            @RequestParam(value = "outlineColour", required = true, defaultValue = "#000000") String outlineColour,
            @RequestParam(value = "fileName", required = false) String fileName,
            @RequestParam(value = "baseMap", required = false, defaultValue = "ALA") String baseMap,
            HttpServletRequest request, HttpServletResponse response) throws Exception {

        // convert extents from EPSG:4326 into target SRS
        CRSAuthorityFactory factory = CRS.getAuthorityFactory(true);
        CoordinateReferenceSystem sourceCRS = factory.createCoordinateReferenceSystem(srs);
        CoordinateReferenceSystem targetCRS = factory.createCoordinateReferenceSystem("EPSG:4326");
        CoordinateOperation transformTo4326 = new DefaultCoordinateOperationFactory().createOperation(sourceCRS, targetCRS);
        CoordinateOperation transformFrom4326 = new DefaultCoordinateOperationFactory().createOperation(targetCRS, sourceCRS);
        double[] bbox4326 = new double[4];     // extents in EPSG:4326
        double[] bboxSRS = new double[4];      //extents in target SRS
        if (bboxString != null) {
            transformBBox(transformTo4326, bboxString, bboxSRS, bbox4326);
        } else {
            transformBBox(transformFrom4326, extents, bbox4326, bboxSRS);
            bboxString = bboxSRS[0] + "," + bboxSRS[1] + "," + bboxSRS[2] + "," + bboxSRS[3];
        }

        int width = (int) ((dpi / 25.4) * widthMm);
        int height = (int) Math.round(width * ((bboxSRS[3] - bboxSRS[1]) / (bboxSRS[2] - bboxSRS[0])));

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

        String rendering = "ENV=color%3A" + pointColour + "%3Bname%3Acircle%3Bsize%3A" + pointSize
                + "%3Bopacity%3A" + pointOpacity;
        if(StringUtils.isNotEmpty(env)){
            rendering = "ENV=" + env;
        }

        //"https://biocache-ws.ala.org.au/ws/webportal/wms/reflect?
        //q=macropus&ENV=color%3Aff0000%3Bname%3Acircle%3Bsize%3A3%3Bopacity%3A1
        //&BBOX=12523443.0512,-2504688.2032,15028131.5936,0.33920000120997&WIDTH=256&HEIGHT=256");
        String speciesAddress = baseWsUrl
                + "/ogc/wms/reflect?"
                + rendering
                + "&SRS=" + srs
                + "&BBOX=" + bboxString
                + "&WIDTH=" + width + "&HEIGHT=" + height
                + "&OUTLINE=" + outlinePoints + "&OUTLINECOLOUR=" + outlineColour;

        String serialisedQueryParameters = requestParams.getEncodedParams();

        if (!serialisedQueryParameters.isEmpty()) {
            speciesAddress += "&";
        }

        // add query parameters
        speciesAddress += serialisedQueryParameters;

        URL speciesURL = new URL(speciesAddress);
        BufferedImage speciesImage = ImageIO.read(speciesURL);

        //"https://spatial.ala.org.au/geoserver/wms/reflect?
        //LAYERS=ALA%3Aworld&SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&STYLES=
        //&FORMAT=image%2Fjpeg&SRS=EPSG%3A900913&BBOX=12523443.0512,-1252343.932,13775787.3224,0.33920000004582&WIDTH=256&HEIGHT=256"
        String layout = "";
        if (!scale.equals("off")) {
            layout += "layout:scale";
        }
        String basemapAddress = geoserverUrl + "/wms/reflect?"
                + "LAYERS=ALA%3A" + baselayer
                + "&SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&STYLES=" + baselayerStyle
                + "&FORMAT=image%2Fpng&SRS=" + srs     //specify the mercator projection
                + "&BBOX=" + bboxString
                + "&WIDTH=" + width + "&HEIGHT=" + height + "&OUTLINE=" + outlinePoints
                + "&format_options=dpi:" + dpi + ";" + layout;

        BufferedImage basemapImage;

        if ("roadmap".equalsIgnoreCase(baseMap) || "satellite".equalsIgnoreCase(baseMap) ||
                "hybrid".equalsIgnoreCase(baseMap) || "terrain".equalsIgnoreCase(baseMap)){
            basemapImage = basemapGoogle(width, height, bboxSRS, baseMap);
        } else {
            basemapImage = ImageIO.read(new URL(basemapAddress));
        }

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
        response.setHeader("Cache-Control", wmsCacheControlHeaderPublicOrPrivate + ", max-age=" + wmsCacheControlHeaderMaxAge);
        response.setHeader("ETag", wmsETag.get());

        try {
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
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private BufferedImage basemapGoogle(int width, int height, double [] extents, String maptype) throws Exception {

        double[] resolutions = {
                156543.03390625,
                78271.516953125,
                39135.7584765625,
                19567.87923828125,
                9783.939619140625,
                4891.9698095703125,
                2445.9849047851562,
                1222.9924523925781,
                611.4962261962891,
                305.74811309814453,
                152.87405654907226,
                76.43702827453613,
                38.218514137268066,
                19.109257068634033,
                9.554628534317017,
                4.777314267158508,
                2.388657133579254,
                1.194328566789627,
                0.5971642833948135};

        //nearest resolution
        int imgSize = 640;
        int gScale = 2;
        double actualWidth = extents[2] - extents[0];
        double actualHeight = extents[3] - extents[1];
        int res = 0;
        while (res < resolutions.length - 1 && resolutions[res + 1] * imgSize > actualWidth
                && resolutions[res + 1] * imgSize > actualHeight) {
            res++;
        }

        int centerX = (int) ((extents[2] - extents[0]) / 2 + extents[0]);
        int centerY = (int) ((extents[3] - extents[1]) / 2 + extents[1]);
        double latitude = convertMetersToLat(centerY);
        double longitude = convertMetersToLng(centerX);

        //need to change the size requested so the extents match the output extents.
        int imgWidth = (int) ((extents[2] - extents[0]) / resolutions[res]);
        int imgHeight = (int) ((extents[3] - extents[1]) / resolutions[res]);

        String uri = "http://maps.googleapis.com/maps/api/staticmap?";
        String parameters = "center=" + latitude + "," + longitude + "&zoom=" + res + "&scale=" + gScale + "&size=" + imgWidth + "x" + imgHeight + "&maptype=" + maptype;

        BufferedImage img = ImageIO.read(new URL(uri + parameters));

        BufferedImage tmp = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        tmp.getGraphics().drawImage(img, 0, 0, width, height, 0, 0, imgWidth * gScale, imgHeight * gScale, null);

        return tmp;
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
                             double[] tilebbox, int gridDivisionCount,
                             CoordinateOperation transformFrom4326) throws Exception {

        ImgObj imgObj = null;

        //grid setup
        boolean isGrid = vars.colourMode.equals("grid");
        int divs = gridDivisionCount; //number of x & y divisions in the WIDTH/HEIGHT
        int[][] gridCounts = isGrid ? new int[divs][divs] : null;
        int xstep = width / divs;
        int ystep = height / divs;

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
                            try {
                                GeneralDirectPosition sourceCoords = new GeneralDirectPosition(lng, lat);
                                DirectPosition targetCoords = transformFrom4326.getMathTransform().transform(sourceCoords, null);
                                x = scaleLongitudeForImage(targetCoords.getOrdinate(0), left, right, divs);
                                y = scaleLatitudeForImage(targetCoords.getOrdinate(1), top, bottom, divs);

                                if (x >= 0 && x < divs && y >= 0 && y < divs) {
                                    gridCounts[x][y] += count[i / 2];
                                }
                            } catch (MismatchedDimensionException e) {
                            } catch (TransformException e) {
                                // failure to transform a coordinate will result in it not rendering
                            }
                        }
                    }
                } else {
                    renderPoints(vars, bbox, pbbox, width_mult, height_mult, pointWidth, outlinePoints, outlineColour, pColour, imgObj, j, ps, tilebbox, height, width, transformFrom4326);
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
            drawUncertaintyCircles(requestParams, vars, height, width, mbbox, bbox, imgObj.g,
                    originalFqs, tilebbox, pointType, transformFrom4326);
        }

        //highlight
        if (vars.highlight != null) {
            imgObj = drawHighlight(requestParams, vars, pointType, width, height, imgObj,
                    originalFqs, boundingBoxFqs, tilebbox, transformFrom4326);
        }

        return imgObj;
    }

    void drawUncertaintyCircles(SpatialSearchRequestParams requestParams, WmsEnv vars, int height, int width,
                                double[] mbbox, double[] bbox, Graphics2D g,
                                String[] originalFqs, double[] tilebbox,
                                PointType pointType, CoordinateOperation transformFrom4326) throws Exception {
        //draw uncertainty circles
        double hmult = (height / (mbbox[3] - mbbox[1]));

        //min uncertainty for current resolution and dot size
        double min_uncertainty = (vars.size + 1) / hmult;

        //for image scaling
        double top = tilebbox[3];
        double bottom = tilebbox[1];
        double left = tilebbox[0];
        double right = tilebbox[2];

        //only draw uncertainty if max radius will be > dot size
        if (vars.uncertainty && MAX_UNCERTAINTY > min_uncertainty) {

            String coordinateUncertainty = OccurrenceIndex.COORDINATE_UNCERTAINTY;
            //uncertainty colour/fq/radius, [0]=map, [1]=not specified, [2]=too large
            Color[] uncertaintyColours = {new Color(255, 170, 0, vars.alpha), new Color(255, 255, 100, vars.alpha), new Color(50, 255, 50, vars.alpha)};
            //TODO: don't assume MAX_UNCERTAINTY > default_uncertainty
            String[] uncertaintyFqs = {coordinateUncertainty + ":[" + min_uncertainty + " TO " + MAX_UNCERTAINTY + "] AND -assertions:uncertaintyNotSpecified", "assertions:uncertaintyNotSpecified", coordinateUncertainty + ":[" + MAX_UNCERTAINTY + " TO *]"};
            double[] uncertaintyR = {-1, MAX_UNCERTAINTY, MAX_UNCERTAINTY};

            int originalFqsLength = originalFqs != null ? originalFqs.length : 0;

            String[] fqs = new String[originalFqsLength + 3];

            if (originalFqsLength > 0) {
                System.arraycopy(originalFqs, 0, fqs, 3, originalFqsLength);
            }

            String longitudeField = OccurrenceIndex.LONGITUDE;
            String latitudeField = OccurrenceIndex.LATITUDE;


            //expand bounding box to cover MAX_UNCERTAINTY radius (m to degrees)
            fqs[1] = longitudeField + ":[" + (bbox[0] - MAX_UNCERTAINTY / 100000.0) + " TO " + (bbox[2] + MAX_UNCERTAINTY / 100000.0) + "]";
            fqs[2] = latitudeField + ":[" + (bbox[1] - MAX_UNCERTAINTY / 100000.0) + " TO " + (bbox[3] + MAX_UNCERTAINTY / 100000.0) + "]";

            requestParams.setPageSize(DEFAULT_PAGE_SIZE);

            for (int j = 0; j < uncertaintyFqs.length; j++) {
                //do not display for [1]=not specified
                if (j == 1) {
                    continue;
                }

                fqs[0] = uncertaintyFqs[j];
                requestParams.setFq(fqs);

                //There can be performance issues with pivot when too many distinct coordinate_uncertainty values
                requestParams.setFacets(new String[]{coordinateUncertainty, pointType.getLabel()});
                requestParams.setFlimit(-1);
                requestParams.setFormattedQuery(null);
                List<FacetPivotResultDTO> qr = searchDAO.searchPivot(requestParams);

                if (qr != null && qr.size() > 0) {
                    List<FacetPivotResultDTO> piv = qr.get(0).getPivotResult();

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

                            try {
                                GeneralDirectPosition sourceCoords = new GeneralDirectPosition(lng, lat);
                                DirectPosition targetCoords = transformFrom4326.getMathTransform().transform(sourceCoords, null);
                                x = scaleLongitudeForImage(targetCoords.getOrdinate(0), left, right, width);
                                y = scaleLatitudeForImage(targetCoords.getOrdinate(1), top, bottom, height);

                                if (uncertaintyRadius > 0) {
                                    g.drawOval(x - uncertaintyRadius, y - uncertaintyRadius, uncertaintyRadius * 2, uncertaintyRadius * 2);
                                } else {
                                    g.drawRect(x, y, 1, 1);
                                }
                            } catch (MismatchedDimensionException e) {
                            } catch (TransformException e) {
                                // failure to transform a coordinate will result in it not rendering
                            }
                        }
                    }
                }
            }
        }
    }

    ImgObj drawHighlight(SpatialSearchRequestParams requestParams, WmsEnv vars, PointType pointType,
                         int width, int height, ImgObj imgObj, String[] originalFqs, String[] boundingBoxFqs,
                         double[] tilebbox, CoordinateOperation transformFrom4326) throws Exception {
        String[] fqs = new String[3 + (originalFqs != null ? originalFqs.length : 0)];

        if (originalFqs != null) {
            System.arraycopy(originalFqs, 0, fqs, 3, originalFqs.length);
        }

        fqs[0] = vars.highlight;
        fqs[1] = boundingBoxFqs[0];
        fqs[2] = boundingBoxFqs[1];

        requestParams.setFq(fqs);
        requestParams.setFlimit(-1);
        requestParams.setFormattedQuery(null);
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

            //for image scaling
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

                    try {
                        GeneralDirectPosition sourceCoords = new GeneralDirectPosition(lng, lat);
                        DirectPosition targetCoords = transformFrom4326.getMathTransform().transform(sourceCoords, null);
                        x = scaleLongitudeForImage(targetCoords.getOrdinate(0), left, right, width);
                        y = scaleLatitudeForImage(targetCoords.getOrdinate(1), top, bottom, height);

                        imgObj.g.drawOval(x - highightRadius, y - highightRadius, highlightWidth, highlightWidth);
                    } catch (MismatchedDimensionException e) {
                    } catch (TransformException e) {
                        // failure to transform a coordinate will result in it not rendering
                    }
                }
            }
        }

        return imgObj;
    }

    /**
     * Returns the wms cache object and initialises it if required.
     *
     * @param vars
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

        String qfull = qparam +
                StringUtils.join(requestParams.getFq(), ",") +
                requestParams.getQc() +
                requestParams.getWkt() +
                requestParams.getRadius() +
                requestParams.getLat() +
                requestParams.getLon();

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
            count = getCachedCount(true, requestParams, q, pointType, useBbox);
            if (count == null || count == 0) {
                return new WMSTile();
            }

            //count unique points, if necessary
            if (count > wmsCacheMaxLayerPoints && pointType.getValue() > 0) {
                pointsCount = getCachedCount(false, requestParams, q, pointType, useBbox);

                //use bbox when too many points
                if (pointsCount != null && pointsCount > wmsCacheMaxLayerPoints) {
                    q += StringUtils.join(origAndBBoxFqs, ",");

                    requestParams.setFq(origAndBBoxFqs);
                    count = getCachedCount(true, requestParams, q, pointType, useBbox);
                    requestParams.setFq(originalFqs);

                    if (count == null || count == 0) {
                        return new WMSTile();
                    }

                    useBbox[0] = true;
                }
            }
        } else {
            queryFormatUtils.formatSearchQuery(requestParams, false);
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
                requestParams.setFormattedQuery(null);
                colours = cm.equals("-1") ? null : searchDAO.getColours(requestParams, vars.colourMode);
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
        requestParams.setFormattedQuery(null);
        if (colours == null && !cm.equals("-1")) {
            requestParams.setFlimit(-1);
            colours = searchDAO.getColours(requestParams, vars.colourMode);
            sz = colours == null ? 1 : colours.size() + 1;
        }

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

    private Integer getCachedCount(boolean docCount, SpatialSearchRequestParams requestParams, String q, PointType pointType, boolean[] useBbox) throws Exception {

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
            requestParams.setFormattedQuery(null);
            if (docCount) {
                requestParams.setFacet(false);
                SolrDocumentList result = searchDAO.findByFulltext(requestParams);
                if (result != null) {
                    synchronized (countLock) {
                        count = (int) result.getNumFound();
                        countsCache.put(q + tag, count);
                    }
                }
            } else {
                List<FieldStatsItem> result = searchDAO.searchStat(requestParams, pointType.getLabel(), null,
                        Arrays.asList("countDistinct"));
                if (result != null && result.size() > 0) {
                    synchronized (countLock) {
                        count = result.get(0).getCountDistinct().intValue();
                        countsCache.put(q + tag, count);
                    }
                }
            }
        } else {
            queryFormatUtils.formatSearchQuery(requestParams, false);
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

                    requestParams.setFormattedQuery(null);
                    makePointsFromFacet(searchDAO.getFacetPointsShort(requestParams, pointType.getLabel()), pointsArrays, countsArrays);
                    pColour.add(li.getColour() | (vars.alpha << 24));
                    colrmaxtime = (System.currentTimeMillis() - ms);

                    //in the last iteration check for more and batch.
                    if (i == ColorUtil.colourList.length - 2 && colours.size() == ColorUtil.colourList.length - 1) {
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
                            pColour.set(0, ColorUtil.colourList[ColorUtil.colourList.length - 1] | (vars.alpha << 24));
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
            if (!numericalFacetCategories && docCount <= wmsFacetPivotCutoff && canCache) {
                requestParams.setFacets(new String[]{vars.colourMode + "," + pointType.getLabel()});
                requestParams.setFlimit(-1);
                requestParams.setFormattedQuery(null);
                //get pivot and drill to colourMode level
                List<FacetPivotResultDTO> qr = searchDAO.searchPivot(requestParams);
                if (qr != null && qr.size() > 0) {
                    List<FacetPivotResultDTO> piv = qr.get(0).getPivotResult();

                    //last colour
                    int lastColour = ColorUtil.colourList[ColorUtil.colourList.length - 1] | (vars.alpha << 24);

                    //get facet points, retain colours order so it does not break hq
                    for (int i = 0; i < colours.size(); i++) {
                        LegendItem li = colours.get(i);

                        int j = 0;
                        while (!piv.isEmpty() && j < piv.size()) {
                            FacetPivotResultDTO p = piv.get(j);
                            if ((StringUtils.isEmpty(li.getName()) && StringUtils.isEmpty(p.getValue()))
                                    || (StringUtils.isNotEmpty(li.getName()) && li.getName().equals(p.getValue()))) {
                                // TODO: What to do when this is null?
                                List<FacetPivotResultDTO> pivotResult = p.getPivotResult();
                                makePointsFromPivot(pivotResult, pointsArrays, countsArrays);
                                pColour.add(li.getColour() | (vars.alpha << 24));
                                piv.remove(j);
                                break;
                            } else {
                                j++;
                            }
                        }
                    }

                    // ensure all point/colour pairs are added
                    while (!piv.isEmpty()) {
                        // TODO: What to do when this is null?
                        List<FacetPivotResultDTO> pivotResult = piv.get(0).getPivotResult();
                        makePointsFromPivot(pivotResult, pointsArrays, countsArrays);
                        pColour.add(lastColour);
                        piv.remove(0);
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
            requestParams.setFormattedQuery(null);
            makePointsFromFacet(searchDAO.getFacetPointsShort(requestParams, pointType.getLabel()), pointsArrays, countsArrays);
            if (colours == null || colours.isEmpty()) {
                pColour.add(vars.colour);
            } else {
                pColour.add(colours.get(0).getColour() | (vars.alpha << 24));
            }
        }
    }

    private void makePointsFromPivot(List<FacetPivotResultDTO> pivotResult, List<float[]> gPoints, List<int[]> gCount) {
        Objects.requireNonNull(pivotResult, "Pivot result was null");
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

    private ImgObj renderHeatmap(HeatmapDTO heatmapDTO,
                               WmsEnv vars,
                               float pointWidth,
                               boolean outlinePoints,
                               String outlineColour,
                               int tileWidthInPx,
                               int tileHeightInPx
    )  {

        List<List<List<Integer>>> layers = heatmapDTO.layers;

        if (layers.isEmpty()){
            return null;
        }

        logger.info("Image width:" + tileWidthInPx + ", height:" + tileHeightInPx);

        ImgObj imgObj = ImgObj.create( (int)(tileWidthInPx ), (int) (tileHeightInPx ));

        int layerIdx = 0;

        // render layers remainder layers
        for (List<List<Integer>> rows : layers){

            if (heatmapDTO.legend !=null && heatmapDTO.legend.get(layerIdx) != null && heatmapDTO.legend.get(layerIdx).isRemainder()) {
                renderLayer(heatmapDTO,
                        vars,
                        pointWidth,
                        outlinePoints,
                        outlineColour,
                        (float) tileWidthInPx,
                        (float) tileHeightInPx,
                        imgObj,
                        layerIdx,
                        rows);
            }
            layerIdx++;
        }

        layerIdx = 0;

        // render layers
        for (List<List<Integer>> rows : layers){

            if (heatmapDTO.legend == null || heatmapDTO.legend.get(layerIdx) == null || !heatmapDTO.legend.get(layerIdx).isRemainder()) {
                renderLayer(heatmapDTO,
                        vars,
                        pointWidth,
                        outlinePoints,
                        outlineColour,
                        (float) tileWidthInPx,
                        (float) tileHeightInPx,
                        imgObj,
                        layerIdx,
                        rows);
            }
            layerIdx++;
        }

        return imgObj;
    }

    private void renderLayer(HeatmapDTO heatmapDTO, WmsEnv vars, float pointWidth, boolean outlinePoints, String outlineColour, float tileWidthInPx, float tileHeightInPx, ImgObj imgObj, int layerIdx, List<List<Integer>> rows) {
        if (rows != null && !rows.isEmpty()) {

            final int numberOfRows = rows.size();
            Integer nofOfColumns = -1;
            for (List<Integer> row : rows) {
                if (row != null) {
                    nofOfColumns = row.size();
                    break;
                }
            }

            logger.info("Rows:" + numberOfRows + ", columns:" + nofOfColumns);

            float cellWidth = tileWidthInPx / (float) nofOfColumns;
            float cellHeight = tileHeightInPx / (float) numberOfRows;

            logger.info("cellWidth:" + cellWidth + ", cellHeight:" + cellHeight);

            Color oColour = Color.decode(outlineColour);

            for (int row = 0; row < numberOfRows; row++) {

                List<Integer> columns = rows.get(row);

                if (columns != null) {
                    // render each column with a point
                    for (int column = 0; column < columns.size(); column++) {
                        Integer cellValue = columns.get(column);

                        if (cellValue > 0) {

                            if (heatmapDTO.isGrid) {

                                int x = -1;
                                int y = -1;

                                if (column == 0) {
                                    x = 0;
                                } else {
                                    x = Math.round(cellWidth * (float) column);
                                }
                                if (row == 0) {
                                    y = 0;
                                } else {
                                    y = Math.round(cellHeight * (float) row);
                                }

                                int v = cellValue;
                                if (v > 500) {
                                    v = 500;
                                }
                                int colour = (((500 - v) / 2) << 8) | (vars.alpha << 24) | 0x00FF0000;
                                imgObj.g.setColor(new Color(colour));
                                imgObj.g.fillRect(x, y, (int) Math.ceil(cellWidth), (int) Math.ceil(cellHeight));
                                if (outlinePoints) {
                                    imgObj.g.setPaint(Color.BLACK);
                                    imgObj.g.drawRect(x, y, (int) Math.ceil(cellWidth), (int) Math.ceil(cellHeight));
                                }
                            } else {

                                // cell width / height adjustment to take in padding
                                float cellWidthWithPadding = (tileWidthInPx + (pointWidth * 2f)) / (float) nofOfColumns;
                                float cellHeightWithPadding = (tileHeightInPx + (pointWidth * 2f)) / (float) numberOfRows;

                                int x = (int) ((cellWidthWithPadding  * (float) column) - pointWidth);
                                int y = (int) ((cellHeightWithPadding * (float) row)    - pointWidth);

                                Paint currentFill = null;
                                if (heatmapDTO.legend != null && !heatmapDTO.legend.isEmpty()){
                                    currentFill = new Color(heatmapDTO.legend.get(layerIdx).getColour() | (vars.alpha << 24));
                                } else {
                                    currentFill = Color.RED;
                                }
                                imgObj.g.setPaint(currentFill);

                                imgObj.g.fillOval(
                                        x, //+ (int) (( pointWidth) / 2),
                                        y, // + (int) (( pointWidth) / 2),
                                        (int) pointWidth,
                                        (int) pointWidth);

                                if (outlinePoints) {
                                    imgObj.g.setPaint(oColour);
                                    imgObj.g.drawOval(
                                        x, //   + (int) (( pointWidth) / 2),
                                        y, // + (int) (( pointWidth) / 2),
                                        (int) pointWidth,
                                        (int) pointWidth);
                                    imgObj.g.setPaint(currentFill);
                                }
                            }
                        }
                    }
                }
            }
        }
    }


    private void renderPoints(WmsEnv vars, double[] bbox, double[] pbbox, double width_mult, double height_mult, int pointWidth, boolean outlinePoints, String outlineColour, List<Integer> pColour, ImgObj imgObj, int j, float[] ps, double[] tilebbox, int height, int width, CoordinateOperation transformFrom4326) throws TransformException {
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

                try {
                    GeneralDirectPosition sourceCoords = new GeneralDirectPosition(lng, lat);
                    DirectPosition targetCoords = transformFrom4326.getMathTransform().transform(sourceCoords, null);
                    x = scaleLongitudeForImage(targetCoords.getOrdinate(0), left, right, width);
                    y = scaleLatitudeForImage(targetCoords.getOrdinate(1), top, bottom, height);

                    //System.out.println("Drawing an oval.....");
                    imgObj.g.fillOval(x - vars.size, y - vars.size, pointWidth, pointWidth);
                    if (outlinePoints) {
                        imgObj.g.setPaint(oColour);
                        imgObj.g.drawOval(x - vars.size, y - vars.size, pointWidth, pointWidth);
                        imgObj.g.setPaint(currentFill);
                    }
                } catch (MismatchedDimensionException e) {
                } catch (TransformException e) {
                    // failure to transform a coordinate will result in it not rendering
                }
            }
        }
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

    final Graphics2D g;
    final BufferedImage img;

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
