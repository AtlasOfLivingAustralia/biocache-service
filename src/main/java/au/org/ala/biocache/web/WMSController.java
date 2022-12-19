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
import au.org.ala.biocache.util.converter.FqField;
import au.org.ala.biocache.util.solr.FieldMappingUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.swagger.annotations.*;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
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
import org.springdoc.api.annotations.ParameterObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
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
import com.fasterxml.jackson.annotation.JsonInclude;

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
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WMSController extends AbstractSecureController {

    /**
     * Logger initialisation
     */
    private final static Logger logger = Logger.getLogger(WMSController.class);
    private static final String SPECIES_LIST_CSV_HEADER = "Family,Scientific name,Common name,Taxon rank,LSID,# Occurrences";
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
    protected String  baseWsUrl;

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
     * add pixel radius for wms highlight circles
     */
    @Value("${wms.highlight.radius:6}")
    public int HIGHLIGHT_RADIUS;

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

    /**
     * buffer for hiding rounding errors and some projection errors
     */
    int additionalBuffer = 2;

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


    @Operation(summary = "Create a query ID", tags = "Query ID", description = "Add query details to a cache to reduce the size of the query params that are being passed around. This is particularly useful if you requests are too large for a GET.\n" +
            "\n" +
            "Returns a text identification for the query that has been cached. This identification can be used as part of the value for a search q. ie q=qid:")
    @Tag(name = "Query ID", description = "Services for creation and retrieval of queries and query ids in a cache for occurrence search")
    @RequestMapping(value = {
            "/qid"
    }, method = RequestMethod.POST)
    public void storeParams(@ParameterObject SpatialSearchRequestParams params,
                            @RequestParam(value = "bbox", required = false, defaultValue = "false") String bbox,
                            @RequestParam(value = "title", required = false) String title,
                            @RequestParam(value = "maxage", required = false, defaultValue = "-1") Long maxage,
                            @RequestParam(value = "source", required = false) String source,
                            HttpServletResponse response) throws Exception {

        SpatialSearchRequestDTO requestParams = SpatialSearchRequestDTO.create(params);

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

    @Deprecated
    @Operation(summary = "Deprecated  - use /qid", tags = "Deprecated")
    @RequestMapping(value = {
            "/webportal/params",
            "/mapping/params",
    }, method = RequestMethod.POST)
    public void storeParamsDeprecated(@ParameterObject SpatialSearchRequestParams params,
                            @RequestParam(value = "bbox", required = false, defaultValue = "false") String bbox,
                            @RequestParam(value = "title", required = false) String title,
                            @RequestParam(value = "maxage", required = false, defaultValue = "-1") Long maxage,
                            @RequestParam(value = "source", required = false) String source,
                            HttpServletResponse response) throws Exception {
        storeParams(params, bbox, title, maxage, source, response);
    }



    /**
     * Allows the details of a cached query to be viewed.
     */
    @Operation(summary = "Lookup a query ID", tags = "Query ID", description = "Lookup a cached query based on its query id")
    @RequestMapping(value = {
            "/qid/{queryID}"
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiParam(value = "queryID", required = true)
    public @ResponseBody Qid showQid(@PathVariable("queryID") Long id) throws Exception {
        return qidCacheDAO.get(String.valueOf(id));
    }

    /**
     * Allows the details of a cached query to be viewed.
     */
    @Deprecated
    @Operation(summary = "Deprecated use /qid/{queryID}", tags = "Deprecated")
    @RequestMapping(value = {
            "/mapping/qid/{queryID}",
            "/mapping/qid/{queryID}.json",
            "/webportal/params/details/{queryID}",  // used by spatial portal
            "/mapping/params/details/{queryID}"
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiParam(value = "queryID", required = true)
    public @ResponseBody Qid showQidDeprecated(@PathVariable("queryID") Long id) throws Exception {
        return showQid(id);
    }

    /**
     * JSON web service that returns a list of species and record counts for a given location search
     *
     * @throws Exception
     */
    @Operation(summary = "JSON web service that returns a list of species and record counts for a given location search", tags = "Mapping")
    @RequestMapping(value = {
            "/mapping/species"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public
    @ResponseBody
    List<TaxaCountDTO> listSpecies(@ParameterObject SpatialSearchRequestParams params) throws Exception {
        return searchDAO.findAllSpecies(SpatialSearchRequestDTO.create(params));
    }

    @Deprecated
    @Operation(summary = "JSON web service that returns a list of species and record counts for a given location search", tags = "Mapping")
    @RequestMapping(value = {
            "/webportal/species"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public
    @ResponseBody
    List<TaxaCountDTO> listSpeciesDeprecated(@ParameterObject SpatialSearchRequestParams params) throws Exception {
        return searchDAO.findAllSpecies(SpatialSearchRequestDTO.create(params));
    }

    /**
     * List of species for webportal as csv.
     *
     * @param response
     * @throws Exception
     */
    @Operation(summary = "Download a set of counts for the supplied query", tags = "Download")
    @RequestMapping(value = {
            "/mapping/species.csv"
    }, method = RequestMethod.GET, produces = {"text/csv", "text/plain"})
    public void listSpeciesCsv(
            @ParameterObject SpatialSearchRequestParams requestParams,
            HttpServletResponse response) throws Exception {

        SpatialSearchRequestDTO dto = SpatialSearchRequestDTO.create(requestParams);
        List<TaxaCountDTO> list = searchDAO.findAllSpecies(dto);

        //format as csv
        StringBuilder sb = new StringBuilder();
        sb.append(SPECIES_LIST_CSV_HEADER);
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
     * List of species for webportal as csv.
     *
     * @param response
     * @throws Exception
     */
    @Deprecated
    @Operation(summary = "Download a set of counts for the supplied query. Deprecated use /mapping/species.csv", tags = "Deprecated")
    @RequestMapping(value = {
            "/webportal/species.csv"
    }, method = RequestMethod.GET, produces = {"text/csv", "text/plain"})
    public void listSpeciesCsvDeprecated(
            @ParameterObject SpatialSearchRequestParams requestParams,
            HttpServletResponse response) throws Exception {
        listSpeciesCsv(requestParams, response);
    }

    /**
     * Get legend for a query and facet field (colourMode).
     * <p>
     * if "Accept" header is application/json return json otherwise
     *
     * @param colourMode
     * @param response
     * @throws Exception
     */
    @Operation(summary = "Get legend for a query and facet field (colourMode).", tags = "Mapping")
    @Tag(name = "Mapping", description = "Services for creating maps with WMS services, static heat maps")
    @RequestMapping(value = {
            "/mapping/legend"
    }, method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE, "text/plain"})
    @ResponseBody
    public List<LegendItem> legend(
            @ParameterObject SpatialSearchRequestParams params,
            @RequestParam(value = "cm", required = false, defaultValue = "") String colourMode,
            @RequestParam(value = "type", required = false, defaultValue = "application/csv") String returnType,
            HttpServletRequest request,
            HttpServletResponse response)
            throws Exception {

        SpatialSearchRequestDTO dto = SpatialSearchRequestDTO.create(params);

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
        dto.setFormattedQuery(null);
        List<LegendItem> legend = searchDAO.getLegend(dto, colourModes[0], cutpoints);

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
     * Get legend for a query and facet field (colourMode).
     * <p>
     * if "Accept" header is application/json return json otherwise
     *
     * @param colourMode
     * @param response
     * @throws Exception
     */
    @Deprecated
    @Operation(summary = "Get legend for a query and facet field (colourMode). Deprecated use /mapping/legend", tags = "Deprecated")
    @RequestMapping(value = {
            "/webportal/legend"
    }, method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE, "text/plain"})
    @ResponseBody
    public List<LegendItem> legendDeprecated(
            @ParameterObject SpatialSearchRequestParams params,
            @RequestParam(value = "cm", required = false, defaultValue = "") String colourMode,
            @RequestParam(value = "type", required = false, defaultValue = "application/csv") String returnType,
            HttpServletRequest request,
            HttpServletResponse response)
            throws Exception {
        return legend(params, colourMode, returnType, request, response);
    }

    /**
     * List data providers for a query.
     *
     * @param requestParams
     * @return
     * @throws Exception
     */
    @Hidden
    @RequestMapping(value = {
            "/mapping/dataProviders"
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public List<DataProviderCountDTO> queryInfo(
            @ParameterObject SpatialSearchRequestParams requestParams)
            throws Exception {
        return searchDAO.getDataProviderList(SpatialSearchRequestDTO.create(requestParams));
    }

    /**
     * List data providers for a query.
     *
     * @param requestParams
     * @return
     * @throws Exception
     */
    @Deprecated
    @RequestMapping(value = {
            "/webportal/dataProviders",
            "/mapping/dataProviders.json",
            "/webportal/dataProviders",
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public List<DataProviderCountDTO> queryInfoDeprecated(
            @ParameterObject SpatialSearchRequestParams requestParams)
            throws Exception {
        return queryInfo(requestParams);
    }

    /**
     * Get query bounding box as csv containing:
     * min longitude, min latitude, max longitude, max latitude
     *
     * @param requestParams
     * @param response
     * @throws Exception
     */
    @Operation(summary = "Get query bounding box as csv containing: min longitude, min latitude, max longitude, max latitude", tags = "Mapping")
    @RequestMapping(value = {
            "/mapping/bbox"}, method = RequestMethod.GET, produces = "text/plain")
    public void boundingBox(
            @ParameterObject SpatialSearchRequestParams requestParams,
            HttpServletResponse response)
            throws Exception {

        response.setHeader("Cache-Control", wmsCacheControlHeaderPublicOrPrivate + ", max-age=" + wmsCacheControlHeaderMaxAge);
        response.setHeader("ETag", wmsETag.get());

        double[] bbox = searchDAO.getBBox(SpatialSearchRequestDTO.create(requestParams));

        writeBytes(response, (bbox[0] + "," + bbox[1] + "," + bbox[2] + "," + bbox[3]).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Get query bounding box as csv containing:
     * min longitude, min latitude, max longitude, max latitude
     *
     * @param requestParams
     * @param response
     * @throws Exception
     */
    @Deprecated
    @Operation(summary = "Get query bounding box as csv containing: min longitude, min latitude, max longitude, max latitude - Deprecated use /mapping/bbox", tags = "Deprecated")
    @RequestMapping(value = {
            "/webportal/bbox"
       }, method = RequestMethod.GET, produces = "text/plain")
    public void boundingBoxDeprecated(
            @ParameterObject SpatialSearchRequestParams requestParams,
            HttpServletResponse response)
            throws Exception {
        boundingBox(requestParams, response);
    }

    /**
     * Get query bounding box as JSON array containing:
     * min longitude, min latitude, max longitude, max latitude
     *
     * @param response
     * @return
     * @throws Exception
     */
    @Operation(summary = "Get query bounding box as JSON", tags = "Mapping")
    @RequestMapping(value = {"/mapping/bounds" }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public
    @ResponseBody
    double[] jsonBoundingBox(
            @ParameterObject SpatialSearchRequestParams params,
            HttpServletResponse response)
            throws Exception {

        SpatialSearchRequestDTO dto = SpatialSearchRequestDTO.create(params);
        response.setHeader("Cache-Control", wmsCacheControlHeaderPublicOrPrivate + ", max-age=" + wmsCacheControlHeaderMaxAge);
        response.setHeader("ETag", wmsETag.get());

        double[] bbox = null;

        String q = dto.getQ();
        //when requestParams only contain a qid, get the bbox from the qidCache
        if (q.startsWith("qid:") && StringUtils.isEmpty(dto.getWkt()) &&
                (dto.getFq().length == 0 ||
                        (dto.getFq().length == 1 && StringUtils.isEmpty(dto.getFq()[0])))) {
            try {
                bbox = qidCacheDAO.get(q.substring(4)).getBbox();
            } catch (Exception e) {
            }
        }

        if (bbox == null) {
            bbox = searchDAO.getBBox(dto);
        }

        return bbox;
    }

    /**
     * Get query bounding box as JSON array containing:
     * min longitude, min latitude, max longitude, max latitude
     *
     * @param response
     * @return
     * @throws Exception
     */
    @Deprecated
    @Operation(summary = "Deprecated use /mapping/bounds", tags = "Deprecated")
    @RequestMapping(value = {
            "/mapping/bounds.json",
            "/webportal/bounds.json",
            "/webportal/bounds"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public
    @ResponseBody
    double[] jsonBoundingBoxDeprecated(
            @ParameterObject SpatialSearchRequestParams params,
            HttpServletResponse response)
            throws Exception {
        return jsonBoundingBox(params, response);
    }

    /**
     * Get occurrences by query as JSON.
     *
     * @param requestParams
     * @throws Exception
     */
    @Operation(summary = "Get query bounding box as JSON", tags = "Deprecated")
    @Deprecated
    @RequestMapping(value = {
            "/webportal/occurrences",
            "/mapping/occurrences.json",
            "/webportal/occurrences",
            "/mapping/occurrences.json"
    }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public SearchResultDTO occurrences(
            @ParameterObject SpatialSearchRequestParams requestParams,
            Model model) throws Exception {

        if (StringUtils.isEmpty(requestParams.getQ())) {
            return new SearchResultDTO();
        }

        SearchResultDTO searchResult = searchDAO.findByFulltextSpatialQuery(
                SpatialSearchRequestDTO.create(requestParams),
                false,
                null);
        model.addAttribute("searchResult", searchResult);

        if (logger.isDebugEnabled()) {
            logger.debug("Returning results set with: " + searchResult.getTotalRecords());
        }

        return searchResult;
    }

    /**
     * Get occurrences by query as gzipped csv.
     *
     * @param response
     * @throws Exception
     */
    @SecurityRequirement(name="JWT")
    @Secured({"ROLE_USER", "ROLE_ADMIN"})
    @Operation(summary = "Get occurrences by query as gzipped csv.", tags = "Deprecated")
    @Deprecated
    @RequestMapping(value = {
            "/mapping/occurrences.gz"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public void occurrenceGz(
            @ParameterObject SpatialSearchRequestParams params,
            HttpServletResponse response) {

        response.setContentType("text/plain"); //why ???

        SpatialSearchRequestDTO dto = SpatialSearchRequestDTO.create(params);
        try {
            ServletOutputStream outStream = response.getOutputStream();
            java.util.zip.GZIPOutputStream gzip = new java.util.zip.GZIPOutputStream(outStream);

            // Override page size to return all records with a single request
            dto.setPageSize(-1);

            // All records are returned when start == 0 therefore treat the download as finished when start > 0
            if (dto.getStart() == 0) {
                writeOccurrencesCsvToStream(SpatialSearchRequestDTO.create(params), gzip);
            }

            gzip.flush();
            gzip.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Deprecated
    @RequestMapping(value = {
            "/webportal/occurrences.gz"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public void occurrenceGzDeprecated(
            @ParameterObject SpatialSearchRequestParams params,
            HttpServletResponse response) {
        occurrenceGz(params, response);
    }

    private void writeOccurrencesCsvToStream(SpatialSearchRequestDTO requestParams, OutputStream stream) throws Exception {
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
    @Operation(summary = "Get metadata request", tags = "OGC")
    @Tag(name = "OGC", description = "Services for providing OGC compliant mapping functionalities")
    @RequestMapping(value = { "/ogc/getMetadata"
    }, method = RequestMethod.GET, produces = "text/xml")
    public String getMetadata(
            @RequestParam(value = "LAYER", required = false, defaultValue = "") String layer,
            @RequestParam(value = "q", required = false, defaultValue = "") String query,
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

        SpatialSearchRequestDTO searchParams = new SpatialSearchRequestDTO();
        searchParams.setQ(newQuery);
        searchParams.setFacets(new String[]{OccurrenceIndex.DATA_RESOURCE_NAME});
        searchParams.setPageSize(0);
        List<FacetResultDTO> facets = searchDAO.getFacetCounts(searchParams);
        model.addAttribute("query", newQuery); //need a facet on data providers
        model.addAttribute("dataProviders", facets.get(0).getFieldResult()); //need a facet on data providers
        return "metadata/mcp";
    }

    @Operation(summary = "Get feature request", tags = "OGC")
    @RequestMapping(value = {
            "/ogc/getFeatureInfo"
    }, method = RequestMethod.GET, produces="text/html")
    public String getFeatureInfo(
            @RequestParam(value = "ENV", required = false, defaultValue = "") String env,
            @RequestParam(value = "BBOX", required = false, defaultValue = "0,-90,180,0") String bboxString,
            @RequestParam(value = "WIDTH", required = false, defaultValue = "256") Integer width,
            @RequestParam(value = "HEIGHT", required = false, defaultValue = "256") Integer height,
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
        SpatialSearchRequestDTO requestParams = new SpatialSearchRequestDTO();

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

    @Operation(summary = "Get Legend Graphic", tags = "OGC")
    @RequestMapping(value = {"/ogc/legendGraphic"}, method = RequestMethod.GET, produces = "image/png")
    public void getLegendGraphic(
            @RequestParam(value = "ENV", required = false, defaultValue = "") String env,
            @RequestParam(value = "STYLE", required = false, defaultValue = "8b0000;opacity=1;size=5") String style,
            @RequestParam(value = "WIDTH", required = false, defaultValue = "30") Integer width,
            @RequestParam(value = "HEIGHT", required = false, defaultValue = "20") Integer height,
            HttpServletRequest request,
            HttpServletResponse response) {

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
     * @param requestString
     * @param outlinePoints
     * @param outlineColour
     * @param layers
     * @param query
     * @param filterQueries
     * @param x
     * @param y
     * @param marineOnly
     * @param terrestrialOnly
     * @param limitToFocus
     * @param useSpeciesGroups
     * @param request
     * @param response
     * @param model
     * @throws Exception
     */
    @Operation(summary = "Get Capabilities OGC request", tags = "OGC")
    @RequestMapping(value = {
            "/ogc/ows",
            "/ogc/ows.xml",
            "/ogc/capabilities",
            "/ogc/capabilities.xml",
            "/ogc/getCapabilities"
    }, method = RequestMethod.GET, produces="text/xml")
    public void getCapabilities(
            @ParameterObject SpatialSearchRequestParams requestParams,
            @RequestParam(value = "CQL_FILTER", required = false, defaultValue = "") String cql_filter,
            @RequestParam(value = "ENV", required = false, defaultValue = "") String env,
            @RequestParam(value = "SRS", required = false, defaultValue = "EPSG:3857") String srs, //default to google mercator
            @RequestParam(value = "STYLES", required = false, defaultValue = "") String styles,
            @RequestParam(value = "STYLE", required = false, defaultValue = "") String style,
            @RequestParam(value = "BBOX", required = false, defaultValue = "") String bboxString,
            @RequestParam(value = "WIDTH", required = false, defaultValue = "256") Integer width,
            @RequestParam(value = "HEIGHT", required = false, defaultValue = "256") Integer height,
            @RequestParam(value = "REQUEST", required = false, defaultValue = "GetCapabilities") String requestString,
            @RequestParam(value = "OUTLINE", required = false, defaultValue = "false") boolean outlinePoints,
            @RequestParam(value = "OUTLINECOLOUR", required = false, defaultValue = "0x000000") String outlineColour,
            @RequestParam(value = "LAYERS", required = false, defaultValue = "") String layers,
            @RequestParam(value = "q", required = false, defaultValue = "*:*") String query,
            @FqField @RequestParam(value = "fq", required = false) String[] filterQueries,
            @RequestParam(value = "X", required = false, defaultValue = "0") Double x,
            @RequestParam(value = "Y", required = false, defaultValue = "0") Double y,
            @RequestParam(value = "GRIDDETAIL", required = false, defaultValue = "16") int gridDivisionCount,
            @RequestParam(value = "HQ", required = false) String[] hqs,
            @RequestParam(value = "marineSpecies", required = false, defaultValue = "false") boolean marineOnly,
            @RequestParam(value = "terrestrialSpecies", required = false, defaultValue = "false") boolean terrestrialOnly,
            @RequestParam(value = "limitToFocus", required = false, defaultValue = "false") boolean limitToFocus,
            @RequestParam(value = "useSpeciesGroups", required = false, defaultValue = "false") boolean useSpeciesGroups,
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
                    requestString,
                    outlinePoints,
                    outlineColour,
                    layers,
                    hqs,
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
            // webservices root
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
                    "              <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\"" + baseWsUrl + "/ogc/getCapabilities?SERVICE=WMS&amp;\"/>\n" +
                    "            </Get>\n" +
                    "            <Post>\n" +
                    "              <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\"" + baseWsUrl + "/ogc/getCapabilities?SERVICE=WMS&amp;\"/>\n" +
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
                filterQueries = org.apache.commons.lang3.ArrayUtils.add(filterQueries, OccurrenceIndex.BIOME + ":Marine OR " + OccurrenceIndex.BIOME + ":\"Marine and Non-marine\"");
            }

            if (terrestrialOnly) {
                filterQueries = org.apache.commons.lang3.ArrayUtils.add(filterQueries, OccurrenceIndex.BIOME + ":\"Non-marine\" OR " + OccurrenceIndex.BIOME + ":Limnetic");
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

    private String generateStylesForPoints() {
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

    @Deprecated
    @Operation(summary = "Web Mapping Service", tags = {"Deprecated"})
    @GetMapping(value = {
            "/webportal/wms/reflect",
    }, produces = "image/png")
    public void generateWmsTileViaHeatmapDeprecated(
            @ParameterObject SpatialSearchRequestParams params,
            @RequestParam(value = "CQL_FILTER", required = false, defaultValue = "") String cql_filter,
            @RequestParam(value = "ENV", required = false, defaultValue = "") String env,
            @RequestParam(value = "SRS", required = false, defaultValue = "EPSG:3857") String srs, //default to google mercator
            @RequestParam(value = "STYLES", required = false, defaultValue = "") String styles,
            @RequestParam(value = "BBOX", required = true, defaultValue = "") String bboxString,
            @RequestParam(value = "WIDTH", required = false, defaultValue = "256") Integer width,
            @RequestParam(value = "HEIGHT", required = false, defaultValue = "256") Integer height,
            @RequestParam(value = "REQUEST", required = false, defaultValue = "GetMap") String requestString,
            @RequestParam(value = "OUTLINE", required = false, defaultValue = "true") boolean outlinePoints,
            @RequestParam(value = "OUTLINECOLOUR", required = false, defaultValue = "0x000000") String outlineColour,
            @RequestParam(value = "LAYERS", required = false, defaultValue = "") String layers,
            @RequestParam(value = "HQ", required = false) String[] hqs,
            @RequestParam(value = "GRIDDETAIL", required = false, defaultValue = "16") Integer gridDivisionCount,
            HttpServletRequest request,
            HttpServletResponse response)
            throws Exception {
        generateWmsTileViaHeatmap(params, cql_filter, env, srs, styles,bboxString, width, height,
                requestString, outlinePoints, outlineColour, layers, hqs, gridDivisionCount,
                request, response);
    }

    /**
     * WMS services
     *
     * @param cql_filter q value.
     * @param env        ';' delimited field:value pairs.  See Env
     * @param bboxString
     * @param width
     * @param height
     * @param response
     * @throws Exception
     */
    @Operation(summary = "Web Mapping Service", tags = {"Mapping", "OGC"}, description = "WMS services for point occurrence data")
    @GetMapping(value = {
            "/ogc/wms/reflect",
            "/mapping/wms/reflect",
    }, produces = "image/png")
    public void generateWmsTileViaHeatmap(
            @ParameterObject SpatialSearchRequestParams params,
            @RequestParam(value = "CQL_FILTER", required = false, defaultValue = "") String cql_filter,
            @RequestParam(value = "ENV", required = false, defaultValue = "") String env,
            @RequestParam(value = "SRS", required = false, defaultValue = "EPSG:3857") String srs, //default to google mercator
            @RequestParam(value = "STYLES", required = false, defaultValue = "") String styles,
            @RequestParam(value = "BBOX", required = true, defaultValue = "") String bboxString,
            @RequestParam(value = "WIDTH", required = false, defaultValue = "256") Integer width,
            @RequestParam(value = "HEIGHT", required = false, defaultValue = "256") Integer height,
            @RequestParam(value = "REQUEST", required = false, defaultValue = "GetMap") String requestString,
            @RequestParam(value = "OUTLINE", required = false, defaultValue = "true") boolean outlinePoints,
            @RequestParam(value = "OUTLINECOLOUR", required = false, defaultValue = "0x000000") String outlineColour,
            @RequestParam(value = "LAYERS", required = false, defaultValue = "") String layers,
            @RequestParam(value = "HQ", required = false) String[] hqs,
            @RequestParam(value = "GRIDDETAIL", required = false, defaultValue = "16") Integer gridDivisionCount,
            HttpServletRequest request,
            HttpServletResponse response)
            throws Exception {

        SpatialSearchRequestDTO requestParams = SpatialSearchRequestDTO.create(params);

        // replace requestParams.q with cql_filter or layers if present
        if (StringUtils.trimToNull(cql_filter) != null) {
            requestParams.setQ(WMSUtils.getQ(cql_filter));
        } else if (StringUtils.trimToNull(layers) != null && !"ALA:Occurrences".equalsIgnoreCase(layers)) {
            requestParams.setQ(WMSUtils.convertLayersParamToQ(layers));
        }

        //for OS Grids, hand over to WMS OS controller
        if (env != null && env.contains("osgrid")) {
            wmsosGridController.generateWmsTile(requestParams, cql_filter, env, srs, styles, bboxString, layers, width, height, outlinePoints, outlineColour, request, response);
            return;
        }

        //Some WMS clients are ignoring sections of the GetCapabilities....
        if ("GetLegendGraphic".equalsIgnoreCase(requestString)) {
            getLegendGraphic(env, styles, 30, 20, request, response);
            return;
        }

        if (StringUtils.isBlank(bboxString)) {
            sendWmsError(response, 400, "MissingOrInvalidParameter",
                    "Missing valid BBOX parameter");
            return;
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
                sendWmsError(response, 400, "MissingOrInvalidParameter",
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

        if (heatmapDTO.layers == null) {
            displayBlankImage(response);
            return;
        }

        // circles from uncertainty distances or requested highlight
        HeatmapDTO circlesHeatmap = getCirclesHeatmap(vars, bbox, requestParams, width, height, pointWidth);

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
                circlesHeatmap
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
    }

    /**
     * Get HeatmapDTO of the circles that occur around the mapped points.
     * <p>
     * 1. Uncertainty circles.
     * 2. Highlight circles.
     *
     * @param vars
     * @param bbox
     * @param requestParams
     * @param width
     * @param height
     * @param pointWidth
     * @return
     * @throws Exception
     */
    private HeatmapDTO getCirclesHeatmap(WmsEnv vars, double[] bbox, SpatialSearchRequestDTO requestParams, int width, int height, float pointWidth) throws Exception {
        boolean isGrid = vars.colourMode.equals("grid");

        List<LegendItem> circlesLegend = new ArrayList();
        Double lastDistance = 0.0;
        if (!isGrid && vars.uncertainty) {
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
                    circlesLegend.add(li);
                }
                lastDistance = d;
            }
        }

        //highlight
        double hWidth = 0;
        double hHeight = 0;
        if (!isGrid && vars.highlight != null) {
            LegendItem li = new LegendItem(null, null, null, 0, vars.highlight);    // count=0 is used to indicate that HIGHLIGHT_RADIUS is the used to draw the circle.
            li.setColour(0xff0000);
            circlesLegend.add(li);
            hWidth = ((bbox[2] - bbox[0]) / (double) width) * (Math.max(wmsMaxPointWidth, pointWidth) + additionalBuffer + HIGHLIGHT_RADIUS);
            hHeight = ((bbox[3] - bbox[1]) / (double) height) * (Math.max(wmsMaxPointWidth, pointWidth) + additionalBuffer + HIGHLIGHT_RADIUS);
        }

        if (circlesLegend.size() > 0) {
            // approximately a 30km buffer for uncertanty
            double buffer = lastDistance / 100000.0 * 1.01;

            // use the largest of the uncertainty buffer or highlight buffer
            double bufferWidth = Math.max(buffer, hWidth);
            double bufferHeight = Math.max(buffer, hHeight);

            HeatmapDTO circlesHeatmap = searchDAO.getHeatMap(requestParams.getFormattedQuery(), requestParams.getFormattedFq(), bbox[0] - bufferWidth, bbox[1] - bufferHeight, bbox[2] + bufferWidth, bbox[3] + bufferHeight, circlesLegend, 1);
            circlesHeatmap.setTileExtents(bbox);
            return circlesHeatmap;
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
    @Operation(summary = "Produces a downloadable map", tags = "Mapping")
    @RequestMapping(value = {
            "/mapping/wms/image"}, method = RequestMethod.GET, produces="image/png")
    public void generatePublicationMap(
            @ParameterObject SpatialSearchRequestParams params,
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
            @RequestParam(value = "outline", defaultValue = "false") boolean outlinePoints,
            @RequestParam(value = "outlineColour", defaultValue = "#000000") String outlineColour,
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

        SpatialSearchRequestDTO dto = SpatialSearchRequestDTO.create(params);
        String serialisedQueryParameters = dto.getEncodedParams();

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
            response.setContentType("application/octet-stream;charset=UTF-8");
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

    /**
     * Method that produces the downloadable map integrated in AVH/OZCAM/Biocache.
     *
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
    @Deprecated
    @Operation(summary = "Produces a downloadable map - Deprecated use /mapping/wms/image", tags = "Deprecated")
    @RequestMapping(value = {
            "/webportal/wms/image",
    }, method = RequestMethod.GET, produces="image/png")
    public void generatePublicationMapDeprecated(
            @ParameterObject SpatialSearchRequestParams params,
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
            @RequestParam(value = "outline", defaultValue = "false") boolean outlinePoints,
            @RequestParam(value = "outlineColour", defaultValue = "#000000") String outlineColour,
            @RequestParam(value = "fileName", required = false) String fileName,
            @RequestParam(value = "baseMap", required = false, defaultValue = "ALA") String baseMap,
            HttpServletRequest request, HttpServletResponse response) throws Exception {

        generatePublicationMap(params, format, extents, bboxString, widthMm, pointRadiusMm, pradiusPx, pointColour, env, srs, pointOpacity, baselayer, scale, dpi, baselayerStyle, outlinePoints, outlineColour, fileName, baseMap, request, response);
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
                                 HeatmapDTO cirlesHeatmap
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

        // render circle layers
        if (cirlesHeatmap != null && cirlesHeatmap.layers != null) {
            layerIdx = 0;
            try {
                for (List<List<Integer>> rows : cirlesHeatmap.layers) {
                    if (rows != null) {
                        // approximate conversion of meters to decimal degrees (1:100000) followed by conversion to pixels
                        double dist = cirlesHeatmap.legend.get(layerIdx).getCount();    // count value is the radius in meters for the circle.
                        int circleWidthInPixels = (int) pointWidth + HIGHLIGHT_RADIUS;  // count==0 indicates that a highlight circle is required.
                        if (dist > 0) {
                            // convert radius in meters to radius in pixels
                            GeneralDirectPosition coord1 = new GeneralDirectPosition(dist / 100000.0, 0);
                            GeneralDirectPosition coord2 = new GeneralDirectPosition(0, 0);
                            DirectPosition pos1 = transformFrom4326.getMathTransform().transform(coord1, null);
                            DirectPosition pos2 = transformFrom4326.getMathTransform().transform(coord2, null);
                            int px1 = scaleLongitudeForImage(pos1.getOrdinate(0), tilebbox[0], tilebbox[2], (int) tileWidthInPx);
                            int px2 = scaleLatitudeForImage(pos2.getOrdinate(0), tilebbox[0], tilebbox[2], (int) tileWidthInPx);
                            circleWidthInPixels = Math.abs(px1 - px2);
                        }

                        // legacy colours for uncertainty circles
                        String colour = String.format("0x%06X", cirlesHeatmap.legend.get(layerIdx).getColour());

                        renderLayer(
                                cirlesHeatmap,
                                vars,
                                circleWidthInPixels,
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
