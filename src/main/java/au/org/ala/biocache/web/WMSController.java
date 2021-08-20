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
import au.org.ala.biocache.stream.StreamAsCSV;
import au.org.ala.biocache.util.*;
import au.org.ala.biocache.util.solr.FieldMappingUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static au.org.ala.biocache.dto.OccurrenceIndex.*;

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
public class WMSController extends AbstractSecureController {

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
    public static int HIGHLIGHT_RADIUS;

    /**
     * max WMS point width in pixels. This makes better use of the searchDAO.getHeatMap cache.
     */
    @Value("${wms.cache.point.width.max:15}")
    private Integer wmsMaxPointWidth;
    /**
     * uncertainty distance grouping used by WMS
     */
    @Value("${wms.uncertainty.grouping:0,1000,2000,4000,8000,16000,30000}")
    private String uncertaintyGroupingStr;

    private double[] uncertaintyGrouping;

    private double[] getUncertaintyGrouping() {
        if (uncertaintyGrouping == null) {
            try {
                uncertaintyGrouping = Arrays.stream(uncertaintyGroupingStr.split(",")).mapToDouble((a -> Double.parseDouble(a))).toArray();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return uncertaintyGrouping;
    }

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
    public FieldMappingUtil fieldMappingUtil;

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

    @RequestMapping(value = {"/webportal/params", "/mapping/params"}, method = RequestMethod.POST)
    public void storeParams(SpatialSearchRequestParams requestParams,
                            @RequestParam(value = "bbox", required = false, defaultValue = "false") String bbox,
                            @RequestParam(value = "title", required = false) String title,
                            @RequestParam(value = "maxage", required = false, defaultValue = "-1") Long maxage,
                            @RequestParam(value = "source", required = false) String source,
                            HttpServletResponse response) throws Exception {

        //set default values for parameters not stored in the qid.
        requestParams.setFl("");
        requestParams.setFacets(new String[]{});
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
            if (StringUtils.isEmpty(requestParams.getWkt())) {
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
    @RequestMapping(value = {
            "/webportal/params/{id}",
            "/webportal/params/{id}.json",
            "/mapping/params/{id}",
            "/mapping/params/{id}.json"}, method = RequestMethod.GET)
    public
    @ResponseBody
    Boolean storeParams(@PathVariable("id") Long id) throws Exception {
        return qidCacheDAO.get(String.valueOf(id)) != null;
    }

    /**
     * Allows the details of a cached query to be viewed.
     */
    @RequestMapping(value = {
            "/qid/{id}",
            "/qid/{id}.json",
            "/mapping/qid/{id}",
            "/mapping/qid/{id}.json",
            "/webportal/params/details/{id}",
            "/webportal/params/details/{id}.json",
            "/mapping/params/details/{id}",
            "/mapping/params/details/{id}json"}, method = RequestMethod.GET)
    public
    @ResponseBody
    Qid showQid(@PathVariable("id") Long id) throws Exception {
        return qidCacheDAO.get(String.valueOf(id));
    }

    /**
     * JSON web service that returns a list of species and record counts for a given location search
     *
     * @throws Exception
     */
    @RequestMapping(value = {
            "/webportal/species",
            "/webportal/species.json",
            "/mapping/species",
            "/mapping/species.json"}, method = RequestMethod.GET)
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

        writeBytes(response, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Get legend for a query and facet field (colourMode).
     * <p>
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
        String[] colourModes = colourMode.split(",");
        String[] cutpoints = null;
        if (colourModes.length > 1) {
            cutpoints = new String[colourModes.length - 1];
            System.arraycopy(colourModes, 1, cutpoints, 0, cutpoints.length);
        }
        requestParams.setFormattedQuery(null);
        List<LegendItem> legend = searchDAO.getLegend(requestParams, colourModes[0], cutpoints);

        StringBuilder sb = new StringBuilder();
        if (isCsv) {
            sb.append("name,red,green,blue,count");
            for (int i = 0; i < legend.size(); i++) {
                LegendItem li = legend.get(i);
                String name = li.getName();
                if (StringUtils.isEmpty(name)) {
                    name = NULL_NAME;
                }
                sb.append("\n\"").append(name.replace("\"", "\"\"")).append("\",").append(ColorUtil.getRGB(li.getColour()))
                        .append(",").append(legend.get(i).getCount());
            }
        }

        response.setHeader("Cache-Control", wmsCacheControlHeaderPublicOrPrivate + ", max-age=" + wmsCacheControlHeaderMaxAge);
        response.setHeader("ETag", wmsETag.get());

        //now generate the JSON if necessary
        if (returnType.equals("application/json")) {
            return legend;
        } else {
            writeBytes(response, sb.toString().getBytes(StandardCharsets.UTF_8));
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
    @RequestMapping(value = {
            "/webportal/dataProviders",
            "/mapping/dataProviders.json",
            "/webportal/dataProviders",
            "/mapping/dataProviders.json" }, method = RequestMethod.GET)
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

        writeBytes(response, (bbox[0] + "," + bbox[1] + "," + bbox[2] + "," + bbox[3]).getBytes(StandardCharsets.UTF_8));
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
    @RequestMapping(value = {
            "/webportal/bounds",
            "/mapping/bounds.json",
            "/webportal/bounds",
            "/mapping/bounds.json" }, method = RequestMethod.GET)
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
    @RequestMapping(value = {
            "/webportal/occurrences*",
            "/mapping/occurrences.json*",
            "/webportal/occurrences*",
            "/mapping/occurrences.json*" }, method = RequestMethod.GET)
    @ResponseBody
    public SearchResultDTO occurrences(
            SpatialSearchRequestParams requestParams,
            Model model,
            HttpServletResponse response) throws Exception {

        if (StringUtils.isEmpty(requestParams.getQ())) {
            return new SearchResultDTO();
        }

        //searchUtils.updateSpatial(requestParams);
        SearchResultDTO searchResult = searchDAO.findByFulltextSpatialQuery(requestParams, false, null);
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

        try {
            ServletOutputStream outStream = response.getOutputStream();
            java.util.zip.GZIPOutputStream gzip = new java.util.zip.GZIPOutputStream(outStream);

            // Override page size to return all records with a single request
            requestParams.setPageSize(-1);

            // All records are returned when start == 0 therefore treat the download as finished when start > 0
            if (requestParams.getStart() == 0) {
                writeOccurrencesCsvToStream(requestParams, gzip);
            }

            gzip.flush();
            gzip.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void writeOccurrencesCsvToStream(SpatialSearchRequestParams requestParams, OutputStream stream) throws Exception {
        searchDAO.streamingQuery(requestParams, new StreamAsCSV(fieldMappingUtil, stream, requestParams), null);
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

    double convertMetersToLat(double meters) {
        return 180.0 / Math.PI * (2 * Math.atan(Math.exp(meters / 20037508.342789244 * Math.PI)) - Math.PI / 2.0);
    }

    /**
     * Map a zoom level to a coordinate accuracy level
     *
     * @return
     */
    protected PointType getPointTypeForDegreesPerPixel(double resolution) {
        // Map zoom levels to lat/long accuracy levels
        if (resolution >= 1) {
            return PointType.POINT_1;
        } else if (resolution >= 0.1) {
            return PointType.POINT_01;
        } else if (resolution >= 0.01) {
            return PointType.POINT_001;
        } else if (resolution >= 0.001) {
            return PointType.POINT_0001;
        } else if (resolution >= 0.0001) {
            return PointType.POINT_00001;
        } else {
            return PointType.POINT_RAW;
        }
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
     * @param transformTo4326 TransformOp to convert from the target SRS to the coordinates in SOLR (EPSG:4326)
     * @param bboxString      getMap bbox parameter with the tile extents in the target SRS as min x, min y, max x, max y
     * @param width           getMap width value in pixels
     * @param height          getMap height value in pixels
     * @param size            dot radius in pixels used to calculate a larger bounding box for the request to SOLR for coordinates
     * @param uncertainty     boolean to trigger a larger bounding box for the request to SOLR for coordinates (??)
     * @param mbbox           bounding box in metres (spherical mercator). Includes pixel correction buffer.
     * @param bbox            bounding box for the SOLR request. This is the bboxString transformed to EPSG:4326 with buffer of
     *                        dot size + max uncertainty radius + pixel correction
     * @param pbbox           bounding box in the target SRS corresponding to the output pixel positions. Includes pixel correction buffer.
     * @param tilebbox        raw coordinates from the getMap bbox parameter
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
    @RequestMapping(value = { "/ogc/getMetadata", "/ogc/getMetadata.json" }, method = RequestMethod.GET)
    public String getMetadata(
            @RequestParam(value = "LAYER", required = false, defaultValue = "") String layer,
            @RequestParam(value = "q", required = false, defaultValue = "") String query,
            HttpServletRequest request,
            HttpServletResponse response,
            Model model
    ) throws Exception {

        String taxonName = "";
        if (StringUtils.trimToNull(layer) != null) {
            String[] parts = layer.split(":");
            taxonName = parts[parts.length - 1];

        } else if (StringUtils.trimToNull(query) != null) {
            String[] parts = query.split(":");
            taxonName = parts[parts.length - 1];
        } else {
            response.sendError(400);
        }

        ObjectMapper om = new ObjectMapper();
        String guid = null;
        JsonNode guidLookupNode = om.readTree(new URL(bieWebService + "/guid/" + URLEncoder.encode(taxonName, "UTF-8")));
        //NC: Fixed the ArrayOutOfBoundsException when the lookup fails to yield a result
        if (guidLookupNode.isArray() && guidLookupNode.size() > 0) {
            JsonNode idNode = guidLookupNode.get(0).get("acceptedIdentifier");//NC: changed to used the acceptedIdentifier because this will always hold the guid for the accepted taxon concept whether or not a synonym name is provided
            guid = idNode != null ? idNode.asText() : null;
        }
        String newQuery = OccurrenceIndex.RAW_NAME + ":" + taxonName;
        if (guid != null) {

            model.addAttribute("guid", guid);
            model.addAttribute("speciesPageUrl", bieUiUrl + "/species/" + guid);
            JsonNode node = om.readTree(new URL(bieWebService + "/species/" + guid));
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

    @RequestMapping(value = { "/ogc/getFeatureInfo", "/ogc/getFeatureInfo.json" }, method = RequestMethod.GET)
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
    @RequestMapping(value = {
            "/ogc/ows",
            "/ogc/ows.xml",
            "/ogc/capabilities",
            "/ogc/capabilities.xml" }, method = RequestMethod.GET)
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
            // Deprecated RequestParam
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
    @RequestMapping(value = {
            "/webportal/wms/reflect",
            "/webportal/wms/reflect.png",
            "/ogc/wms/reflect",
            "/ogc/wms/reflect.png",
            "/mapping/wms/reflect",
            "/mapping/wms/reflect.png" }, method = RequestMethod.GET)
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

        // replace requestParams.q with cql_filter or layers if present
        if (StringUtils.trimToNull(cql_filter) != null) {
            requestParams.setQ(WMSUtils.getQ(cql_filter));
        } else if (StringUtils.trimToNull(layers) != null && !"ALA:Occurrences".equalsIgnoreCase(layers)) {
            requestParams.setQ(WMSUtils.convertLayersParamToQ(layers));
        }

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
        if (logger.isDebugEnabled()) {
            logger.debug("vars.colourMode = " + vars.colourMode);
        }

        float pointWidth = (float) (vars.size * 2);

        // format the query -  this will deal with radius / wkt
        queryFormatUtils.formatSearchQuery(requestParams, true);

        //retrieve legend
        List<LegendItem> legend = searchDAO.getColours(requestParams, vars.colourMode);

        // Increase size of area requested to incude occurrences around the edge that overlap with the target area when drawn.
        int additionalBuffer = 2;   // hide rounding errors and some projection errors
        double bWidth = ((bbox[2] - bbox[0]) / (double) width) * (Math.max(wmsMaxPointWidth, pointWidth) + additionalBuffer);
        double bHeight = ((bbox[3] - bbox[1]) / (double) height) * (Math.max(wmsMaxPointWidth, pointWidth) + additionalBuffer);

        HeatmapDTO heatmapDTO = searchDAO.getHeatMap(requestParams.getFormattedQuery(), requestParams.getFormattedFq(), bbox[0] - bWidth, bbox[1] - bHeight, bbox[2] + bWidth, bbox[3] + bHeight, legend, isGrid ? (int) Math.ceil(width / (double) gridDivisionCount) : 1);
        heatmapDTO.setTileExtents(bbox);

        // getHeatMap is cached. The process to trigger hiddenFacets is:
        // 1. map all facets
        // 2. nominate facets to hide
        // As the heatmapDTO is cached no additional SOLR requests are required when only adding hiddenFacets (HQ)
        if (hiddenFacets != null) {
            for (Integer hf : hiddenFacets) {
                if (hf < heatmapDTO.layers.size()) {
                    heatmapDTO.layers.set(hf, null);
                }
            }
        }

        HeatmapDTO uncertaintyHeatmap = null;
        if (!isGrid && vars.uncertainty) {
            List<LegendItem> uncertaintyLegend = new ArrayList();
            Double lastDistance = null;
            double widthInDecimalDegrees = bbox[2] - bbox[0];
            for (Double d : getUncertaintyGrouping()) {
                LegendItem li;
                // skip if uncertainty radius is < pointWidth * 1.5 or radius is > tile width
                if (widthInDecimalDegrees / (double) width * pointWidth * 1.5 < (d / 100000.0) &&
                        widthInDecimalDegrees > (d / 100000.0)) {
                    if (lastDistance == null) {
                        li = new LegendItem(
                                null, null, null, 0, "coordinateUncertaintyInMeters:[* TO " + d + "]");
                        li.setColour(0xffaa00);
                    } else if (lastDistance == getUncertaintyGrouping()[getUncertaintyGrouping().length - 2]) {
                        li =
                                new LegendItem(null, null, null, lastDistance.intValue(), "coordinateUncertaintyInMeters:{" + d + " TO *]");
                        li.setColour(0x32ff32);
                    } else {
                        li =
                                new LegendItem(null, null, null, lastDistance.intValue(), "coordinateUncertaintyInMeters:{" + lastDistance + " TO " + d + "]");
                        li.setColour(0xffaa00);
                    }
                    uncertaintyLegend.add(li);
                }
                lastDistance = d;
            }

            if (uncertaintyLegend.size() > 0) {
                // approximately a 30km buffer
                double buffer = lastDistance / 100000.0 * 1.01;
                uncertaintyHeatmap = searchDAO.getHeatMap(requestParams.getFormattedQuery(), requestParams.getFormattedFq(), bbox[0] - buffer, bbox[1] - buffer, bbox[2] + buffer, bbox[3] + buffer, uncertaintyLegend, 1);
                uncertaintyHeatmap.setTileExtents(bbox);
            }
        }

        if (heatmapDTO.layers == null) {
            displayBlankImage(response);
            return null;
        }

        CRSAuthorityFactory factory = CRS.getAuthorityFactory(true);
        CoordinateReferenceSystem sourceCRS = factory.createCoordinateReferenceSystem(srs);
        CoordinateReferenceSystem targetCRS = factory.createCoordinateReferenceSystem("EPSG:4326");
        CoordinateOperation transformFrom4326 = new DefaultCoordinateOperationFactory().createOperation(targetCRS, sourceCRS);

        // render PNG...
        ImgObj tile = renderHeatmap(heatmapDTO,
                vars,
                (int) pointWidth,
                outlinePoints,
                outlineColour,
                width,
                height, transformFrom4326, tilebbox,
                uncertaintyHeatmap
        );

        if (tile != null && tile.g != null) {
            tile.g.dispose();
            try (ServletOutputStream outStream = response.getOutputStream();) {
                response.setContentType("image/png");
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

    private double[] reprojectBBox(double[] tilebbox, String srs) throws Exception {

        CRSAuthorityFactory factory = CRS.getAuthorityFactory(true);
        CoordinateReferenceSystem sourceCRS = factory.createCoordinateReferenceSystem(srs);
        CoordinateReferenceSystem targetCRS = factory.createCoordinateReferenceSystem("EPSG:4326");
        CoordinateOperation transformTo4326 = new DefaultCoordinateOperationFactory().createOperation(sourceCRS, targetCRS);

        // pixel correction buffer: adjust bbox extents with half pixel width/height
        GeneralDirectPosition directPositionSW = new GeneralDirectPosition(tilebbox[0], tilebbox[1]);
        GeneralDirectPosition directPositionNE = new GeneralDirectPosition(tilebbox[2], tilebbox[3]);

        DirectPosition sw4326 = transformTo4326.getMathTransform().transform(directPositionSW, null);
        DirectPosition ne4326 = transformTo4326.getMathTransform().transform(directPositionNE, null);

        double[] bbox = new double[4];
        bbox[0] = sw4326.getCoordinate()[0];
        bbox[1] = sw4326.getCoordinate()[1];
        bbox[2] = ne4326.getCoordinate()[0];
        bbox[3] = ne4326.getCoordinate()[1];
        return bbox;
    }

    private ModelAndView sendWmsError(HttpServletResponse response, int status, String errorType, String errorDescription) {
        response.setStatus(status);
        Map<String, String> model = new HashMap<String, String>();
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
     * @param extents       bounding box in decimal degrees
     * @param bboxString    bounding box in target SRS
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

        if (Strings.isNullOrEmpty(request.getQueryString())) {
            response.sendError(400, "No parameters supplied for this request");
            return;
        }

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
        if (StringUtils.isNotEmpty(env)) {
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
                "hybrid".equalsIgnoreCase(baseMap) || "terrain".equalsIgnoreCase(baseMap)) {
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

    private BufferedImage basemapGoogle(int width, int height, double[] extents, String maptype) throws Exception {

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


    private ImgObj renderHeatmap(HeatmapDTO heatmapDTO,
                                 WmsEnv vars,
                                 float pointWidth,
                                 boolean outlinePoints,
                                 String outlineColour,
                                 int tileWidthInPx,
                                 int tileHeightInPx, CoordinateOperation transformFrom4326, double[] tilebbox,
                                 HeatmapDTO uncertaintyHeatmap
    ) {

        List<List<List<Integer>>> layers = heatmapDTO.layers;

        if (layers.isEmpty()) {
            return null;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Image width:" + tileWidthInPx + ", height:" + tileHeightInPx);
        }

        ImgObj imgObj = ImgObj.create((int) (tileWidthInPx), (int) (tileHeightInPx));

        int layerIdx = 0;

        // render layers remainder
        for (List<List<Integer>> rows : layers) {
            if (heatmapDTO.legend != null && heatmapDTO.legend.get(layerIdx) != null && heatmapDTO.legend.get(layerIdx).isRemainder()) {
                renderLayer(heatmapDTO,
                        vars,
                        pointWidth,
                        outlinePoints,
                        outlineColour,
                        true,
                        (float) tileWidthInPx,
                        (float) tileHeightInPx,
                        imgObj,
                        layerIdx,
                        rows, transformFrom4326, tilebbox);
            }
            layerIdx++;
        }

        layerIdx = 0;

        // render layers
        for (List<List<Integer>> rows : layers) {
            if (heatmapDTO.legend == null || heatmapDTO.legend.get(layerIdx) == null || !heatmapDTO.legend.get(layerIdx).isRemainder()) {
                renderLayer(heatmapDTO,
                        vars,
                        pointWidth,
                        outlinePoints,
                        outlineColour,
                        true,
                        (float) tileWidthInPx,
                        (float) tileHeightInPx,
                        imgObj,
                        layerIdx,
                        rows, transformFrom4326, tilebbox);
            }
            layerIdx++;
        }

        // render uncertainty layer
        if (uncertaintyHeatmap != null && uncertaintyHeatmap.layers != null) {
            layerIdx = 0;
            try {
                for (List<List<Integer>> rows : uncertaintyHeatmap.layers) {
                    if (rows != null) {
                        // approximate conversion of meters to decimal degrees (1:100000) followed by conversion to pixels
                        GeneralDirectPosition coord1 = new GeneralDirectPosition(uncertaintyHeatmap.legend.get(layerIdx).getCount() / 100000.0, 0);
                        GeneralDirectPosition coord2 = new GeneralDirectPosition(0, 0);
                        DirectPosition pos1 = transformFrom4326.getMathTransform().transform(coord1, null);
                        DirectPosition pos2 = transformFrom4326.getMathTransform().transform(coord2, null);
                        int px1 = scaleLongitudeForImage(pos1.getOrdinate(0), tilebbox[0], tilebbox[2], (int) tileWidthInPx);
                        int px2 = scaleLatitudeForImage(pos2.getOrdinate(0), tilebbox[0], tilebbox[2], (int) tileWidthInPx);
                        int uncertaintyWidthInPixels = Math.abs(px1 - px2);

                        // legacy colours for uncertainty circles
                        String colour = String.format("0x%06X", uncertaintyHeatmap.legend.get(layerIdx).getColour());

                        renderLayer(
                                uncertaintyHeatmap,
                                vars,
                                uncertaintyWidthInPixels,
                                true,
                                colour,
                                false,
                                (float) tileWidthInPx,
                                (float) tileHeightInPx,
                                imgObj,
                                layerIdx,
                                rows,
                                transformFrom4326,
                                tilebbox
                        );
                    }
                    layerIdx++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return imgObj;
    }

    private void renderLayer(HeatmapDTO heatmapDTO, WmsEnv vars, float pointWidth, boolean outlinePoints, String outlineColour, boolean drawPointFill,
                             float tileWidthInPx,
                             float tileHeightInPx, ImgObj imgObj, int layerIdx, List<List<Integer>> rows, CoordinateOperation transformFrom4326, double[] tilebbox) {

        if (rows != null && !rows.isEmpty()) {

            final int numberOfRows = rows.size();

            // heatmap cell size
            double cellWidth = heatmapDTO.columnWidth();
            double cellHeight = heatmapDTO.rowHeight();

            Color oColour = Color.decode(outlineColour);

            // default colour
            Paint currentFill = null;

            if (heatmapDTO.legend == null || heatmapDTO.legend.isEmpty()) {
                currentFill = new Color(vars.colour);
                imgObj.g.setPaint(currentFill);
            }

            int rowStep = 1;
            int columnStep = 1;

            // determine if grid cells should be aggregated
            if (heatmapDTO.gridSizeInPixels > 1) {
                while (tileWidthInPx / (double) heatmapDTO.gridSizeInPixels < heatmapDTO.columns / columnStep) {
                    columnStep++;
                }
                while (tileHeightInPx / (double) heatmapDTO.gridSizeInPixels < heatmapDTO.rows / rowStep) {
                    rowStep++;
                }
            }

            // heatmap contains counts of an underlying grid
            for (int row = 0; row < numberOfRows; row += rowStep) {
                // heatmap grid cell centre latitude
                double lat = heatmapDTO.maxy - (cellHeight * (row + 0.5));

                List<Integer> columns = rows.get(row);

                if (columns != null) {
                    // render each column with a point
                    for (int column = 0; column < columns.size(); column += columnStep) {

                        Integer cellValue = columns.get(column);

                        // aggregate grid cells
                        if (rowStep > 1 || columnStep > 1) {
                            cellValue = 0;
                            for (int r = 0; r < rowStep && row + r < rows.size(); r++) {
                                List<Integer> rw = rows.get(row + r);
                                for (int c = 0; rw != null && c < columnStep && column + c < rw.size(); c++) {
                                    cellValue += rw.get(column + c);
                                }
                            }
                        }

                        if (cellValue > 0) {
                            try {
                                if (heatmapDTO.gridSizeInPixels > 1) {
                                    double minLat = heatmapDTO.maxy - (cellHeight * (row + rowStep));
                                    double maxLat = heatmapDTO.maxy - (cellHeight * (row));
                                    double minLng = heatmapDTO.minx + cellWidth * (column);
                                    double maxLng = heatmapDTO.minx + cellWidth * (column + columnStep);

                                    // Correct for date line (180 degree) issue that occurrs when tile extents
                                    // differ from the heatmap extents by 360 degrees (minx > tileMaxx)
                                    if (heatmapDTO.minx > heatmapDTO.tileMaxx) {
                                        maxLng -= 360;
                                        minLng -= 360;
                                    }

                                    // make coordinates to match target SRS
                                    GeneralDirectPosition sourceCoordsBottomLeft = new GeneralDirectPosition(minLng, minLat);
                                    GeneralDirectPosition sourceCoordsTopRight = new GeneralDirectPosition(maxLng, maxLat);
                                    DirectPosition targetCoordsBottomLeft = transformFrom4326.getMathTransform().transform(sourceCoordsBottomLeft, null);
                                    DirectPosition targetCoordsTopRight = transformFrom4326.getMathTransform().transform(sourceCoordsTopRight, null);
                                    int px1 = scaleLongitudeForImage(targetCoordsBottomLeft.getOrdinate(0), tilebbox[0], tilebbox[2], (int) tileWidthInPx);
                                    int py1 = scaleLatitudeForImage(targetCoordsBottomLeft.getOrdinate(1), tilebbox[3], tilebbox[1], (int) tileHeightInPx);
                                    int px2 = scaleLongitudeForImage(targetCoordsTopRight.getOrdinate(0), tilebbox[0], tilebbox[2], (int) tileWidthInPx);
                                    int py2 = scaleLatitudeForImage(targetCoordsTopRight.getOrdinate(1), tilebbox[3], tilebbox[1], (int) tileHeightInPx);

                                    int v = cellValue;
                                    if (v > 500) {
                                        v = 500;
                                    }
                                    int colour = (((500 - v) / 2) << 8) | (vars.alpha << 24) | 0x00FF0000;
                                    if (drawPointFill) {
                                        imgObj.g.setColor(new Color(colour));
                                        imgObj.g.fillRect(
                                                Math.min(px1, px2),
                                                Math.min(py1, py2),
                                                Math.abs(px2 - px1),
                                                Math.abs(py2 - py1));
                                    }
                                    if (outlinePoints) {
                                        imgObj.g.setPaint(oColour);
                                        imgObj.g.drawRect(Math.min(px1, px2), Math.min(py1, py2), Math.abs(px2 - px1), Math.abs(py2 - py1));
                                    }
                                } else {

                                    // heatmap grid cell centre longitude
                                    double lng = heatmapDTO.minx + cellWidth * (column + 0.5);

                                    // Correct for date line (180 degree) that occurrs when tile extents differ from
                                    // the heatmap extents by 360 degrees (minx > tileMaxx)
                                    if (heatmapDTO.minx > heatmapDTO.tileMaxx) {
                                        lng -= 360;
                                    }

                                    // make coordinates to match target SRS
                                    GeneralDirectPosition sourceCoords = new GeneralDirectPosition(lng, lat);
                                    DirectPosition targetCoords = transformFrom4326.getMathTransform().transform(sourceCoords, null);
                                    int px = scaleLongitudeForImage(targetCoords.getOrdinate(0), tilebbox[0], tilebbox[2], (int) tileWidthInPx);
                                    int py = scaleLatitudeForImage(targetCoords.getOrdinate(1), tilebbox[3], tilebbox[1], (int) tileHeightInPx);

                                    if (drawPointFill) {
                                        if (heatmapDTO.legend != null && !heatmapDTO.legend.isEmpty()) {
                                            currentFill = new Color(heatmapDTO.legend.get(layerIdx).getColour() | (vars.alpha << 24));
                                            imgObj.g.setPaint(currentFill);
                                        }

                                        imgObj.g.fillOval(
                                                px - (int) (pointWidth / 2),
                                                py - (int) (pointWidth / 2),
                                                (int) pointWidth,
                                                (int) pointWidth);
                                    }

                                    if (outlinePoints) {
                                        imgObj.g.setPaint(oColour);
                                        imgObj.g.drawOval(
                                                px - (int) (pointWidth / 2),
                                                py - (int) (pointWidth / 2),
                                                (int) pointWidth,
                                                (int) pointWidth);
                                        imgObj.g.setPaint(currentFill);
                                    }
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
}
