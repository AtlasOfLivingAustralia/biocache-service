package au.org.ala.biocache.web;

import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.util.GISPoint;
import au.org.ala.biocache.util.GISUtil;
import au.org.ala.biocache.util.GridRef;
import au.org.ala.biocache.util.GridUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.geotools.geometry.GeneralDirectPosition;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.DefaultCoordinateOperationFactory;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.CoordinateOperation;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import scala.Option;

import javax.imageio.ImageIO;
import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.text.MessageFormat;
import java.util.*;
import java.util.List;

/**
 * WMS controller that supports OS grid rendering.
 */
@Controller
public class WMSOSGridController {

    private final static Logger logger = Logger.getLogger(WMSOSGridController.class);

    @Inject
    protected SearchDAO searchDAO;

    @Inject
    protected WMSUtils wmsUtils;


    @RequestMapping(value = {"/osgrid/lookup.json"}, method = RequestMethod.GET)
    public @ResponseBody Map<String, Object> parseGridReference(
            @RequestParam(value = "q", required = true) String gridReference){

        Map<String, Object> response = new LinkedHashMap<String, Object>();

        //validation
        if(gridReference == null || StringUtils.isBlank(gridReference)){
            response.put("valid", false);
            return response;
        }

        String cleanedRef = gridReference.replaceAll("[^a-zA-Z0-9]+","").toUpperCase();

        Option<GISPoint> point = GridUtil.processGridReference(cleanedRef);
        if(!point.isEmpty()){
            response.put("valid", true);
            Map<String, String> resolutions = GridUtil.getGridRefAsResolutions(cleanedRef);
            response.put("decimalLatitude", point.get().latitude());
            response.put("decimalLongitude", point.get().longitude());
            response.put("coordinateUncertaintyInMeters", point.get().coordinateUncertaintyInMeters());
            response.put("geodeticDatum", point.get().datum());
            response.put("bbox", point.get().bboxString());
            response.put("easting", point.get().easting());
            response.put("northing", point.get().northing());
            response.put("minLat", point.get().minLatitude());
            response.put("minLong", point.get().minLongitude());
            response.put("maxLat", point.get().maxLatitude());
            response.put("maxLong", point.get().maxLongitude());
            response.put("resolutions", resolutions);
        } else {
            response.put("valid", false);
        }
        return response;
    }

    /**
     * Query for a record count at:
     * - 100m grid, stopping if > 0
     * - 1000m grid, stopping if > 0
     * - 2000m grid, stopping if > 0
     * - 10000m grid
     *
     * @param requestParams
     * @param request
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {"/osgrid/feature.json"}, method = RequestMethod.GET)
    public @ResponseBody Map<String, Object> getFeatureInfo(
            SpatialSearchRequestParams requestParams,
            HttpServletRequest request) throws Exception {

        try {

            String qc = requestParams.getQc();
            if(StringUtils.isNotEmpty(qc)){
                String[] newFqs = new String[requestParams.getFq().length + 1];
                System.arraycopy(requestParams.getFq(), 0, newFqs, 0, requestParams.getFq().length);
                newFqs[newFqs.length - 1] = qc;
                requestParams.setFq(newFqs);
                requestParams.setQc(null);
            }

            String lat = request.getParameter("lat");
            String lng = request.getParameter("lng") != null ? request.getParameter("lng") : request.getParameter("lon");

            //determine the zoom level
            double[] eastingNorthing = convertWGS84ToEastingNorthing(
                    Double.parseDouble(lat),
                    Double.parseDouble(lng)
            );

            ParsedGridRef osGrid = convertEastingNorthingToOSGrid(eastingNorthing[0], eastingNorthing[1]);

            Map<String, Object> map = new HashMap<String, Object>();

            long count = getRecordCountForGridRef(requestParams, osGrid.getGridRef100(), 100, false);
            if (count > 0) {
                map.put("gridRef", osGrid.getGridRef100());
                map.put("gridSize", 100);
                map.put("recordCount", count);
                map.put("filterQuery", getFilterQuery(osGrid.getGridRef100(), 100));
            }
            logger.info(osGrid.getGridRef100() + " = " + count);

            if (count == 0) {
                count = getRecordCountForGridRef(requestParams, osGrid.getGridRef1000(), 1000, true);
                if (count > 0) {
                    map.put("gridRef", osGrid.getGridRef1000());
                    map.put("gridSize", 1000);
                    map.put("recordCount", count);
                    map.put("filterQuery", getFilterQuery(osGrid.getGridRef1000(), 1000));
                }
            }
            logger.info(osGrid.getGridRef1000() + " = " + count);

            if (count == 0) {
                count = getRecordCountForGridRef(requestParams, osGrid.getGridRef2000(), 2000, true);
                if (count > 0) {
                    map.put("gridRef", osGrid.getGridRef2000());
                    map.put("gridSize", 2000);
                    map.put("recordCount", count);
                    map.put("filterQuery", getFilterQuery(osGrid.getGridRef2000(), 2000));
                }
            }
            logger.info(osGrid.getGridRef2000() + " = " + count);

            if (count == 0) {
                count = getRecordCountForGridRef(requestParams, osGrid.getGridRef10000(), 10000, true);
                if (count > 0) {
                    map.put("gridRef", osGrid.getGridRef10000());
                    map.put("gridSize", 10000);
                    map.put("recordCount", count);
                    map.put("filterQuery", getFilterQuery(osGrid.getGridRef10000(), 10000));
                }
            }
            logger.info(osGrid.getGridRef10000() + " = " + count);

            if (count == 0) {
                count = getRecordCountForGridRef(requestParams, osGrid.getGridRef100000(), 100000, true);
                if (count > 0) {
                    map.put("gridRef", osGrid.getGridRef100000());
                    map.put("gridSize", 100000);
                    map.put("recordCount", count);
                    map.put("filterQuery", getFilterQuery(osGrid.getGridRef100000(), 100000));
                }
            }

            logger.info(osGrid.getGridRef100000() + " = " + count);

            if (count > 0) {
                int gridSize = (Integer) map.get("gridSize");
                String gridRef = (String) map.get("gridRef");

                Option<GridRef> result = GridUtil.gridReferenceToEastingNorthing(gridRef);
                if(!result.isEmpty()){

                    int[] enForGrid = new int[]{result.get().easting(), result.get().northing()};

                    double[] sw = reprojectPoint((double) enForGrid[0], (double) enForGrid[1], GridUtil.OSGB_CRS(), GISUtil.WGS84_EPSG_Code());
                    double[] se = reprojectPoint((double) enForGrid[0] + gridSize, (double) enForGrid[1], GridUtil.OSGB_CRS(), GISUtil.WGS84_EPSG_Code());
                    double[] nw = reprojectPoint((double) enForGrid[0], (double) enForGrid[1] + gridSize, GridUtil.OSGB_CRS(), GISUtil.WGS84_EPSG_Code());
                    double[] ne = reprojectPoint((double) enForGrid[0] + gridSize, (double) enForGrid[1] + gridSize, GridUtil.OSGB_CRS(), GISUtil.WGS84_EPSG_Code());

                    map.put("sw", sw);
                    map.put("se", se);
                    map.put("nw", nw);
                    map.put("ne", ne);
                }
            }
            return map;
        } catch (Exception e){
            e.printStackTrace();
            return new HashMap<String, Object>();
        }
    }

    /**
     * Performs a quick count query for this query and grid reference.
     *
     * @param requestParams
     * @param gridRef
     * @param gridSize
     * @param replaceLast
     * @return
     */
    private long getRecordCountForGridRef(SpatialSearchRequestParams requestParams, String gridRef, int gridSize, boolean replaceLast){

        try {
            String fq = getFilterQuery(gridRef, gridSize);

            if (replaceLast) {
                String[] fqs = requestParams.getFq();
                fqs[fqs.length - 1] = fq;
            } else {
                String[] newFqs = new String[requestParams.getFq().length + 1];
                System.arraycopy(requestParams.getFq(), 0, newFqs, 0, requestParams.getFq().length);
                newFqs[newFqs.length - 1] = fq;
                requestParams.setFq(newFqs);
            }

            requestParams.setLat(null);
            requestParams.setLon(null);

            requestParams.setPageSize(0);
            requestParams.setFacet(false);

            SearchResultDTO resultDTO = searchDAO.findByFulltextSpatialQuery(requestParams, new HashMap<String, String[]>());
            return resultDTO.getTotalRecords();
        } catch (Exception e){
            e.printStackTrace();
            return 0;
        }
    }

    private String getFilterQuery(String gridRef, int gridSize) {
        String field = "grid_ref" + (gridSize > 0 ? "_" + gridSize : "");
        return field + ":" + gridRef;
    }

    /**
     * - render grid cell sized with different colours (with legend support)
     * - opacity support
     * - allow show all grid cells option (i.e. always include 10km grids)
     * - enable,disable outline
     * - only show 1km grids or smaller
     *
     * Default behaviour for zoom levels
     *
     * 313km - just show 10km grids
     * 156km  - just show 10km grids
     * 78km  - just show 10km grids
     * 39km - 1km grids (TODO  - show 2km grids as well)
     * 19km - 1km grids (TODO  - show 2km grids as well)
     * 9km - every resolution (excluding 10km grids)
     * 4km - every resolution (excluding 10km grids)
     * 2km - every resolution (excluding 10km grids)
     * 1km - every resolution (excluding 10km grids)
     * 0.5 - every resolution (excluding 10km grids)
     * 0.150 - every resolution (excluding 10km grids)
     */
    @RequestMapping(value = {"/osgrid/wms/reflect"}, method = RequestMethod.GET)
    public void generateWmsTile(
            SpatialSearchRequestParams requestParams,
            @RequestParam(value = "CQL_FILTER", required = false, defaultValue = "") String cql_filter,
            @RequestParam(value = "ENV", required = true, defaultValue = "") String env,
            @RequestParam(value = "SRS", required = false, defaultValue = "EPSG:900913") String srs, //default to google mercator
            @RequestParam(value = "STYLES", required = false, defaultValue = "") String styles,
            @RequestParam(value = "BBOX", required = true, defaultValue = "") String bboxString,
            @RequestParam(value = "LAYERS", required = false, defaultValue = "") String layers,
            @RequestParam(value = "WIDTH", required = true, defaultValue = "256") Integer width,
            @RequestParam(value = "HEIGHT", required = true, defaultValue = "256") Integer height,
            @RequestParam(value = "OUTLINE", required = true, defaultValue = "true") boolean outlineGrids,
            @RequestParam(value = "OUTLINECOLOUR", required = true, defaultValue = "0xff000000") String outlineColour,
            HttpServletRequest request,
            HttpServletResponse response)
            throws Exception {


        System.out.println(request.getRequestURI() + "?" + request.getQueryString());

        srs = srs.split(",")[0];


        WMSEnv wmsEnv = new WMSEnv(env, styles);

        if(StringUtils.isEmpty(bboxString)){
            return;
        }

        //CQL Filter takes precedence of the layer
        if (org.apache.commons.lang.StringUtils.trimToNull(cql_filter) != null) {
            requestParams.setQ(wmsUtils.getQ(cql_filter));
        } else if (org.apache.commons.lang.StringUtils.trimToNull(layers) != null && !"ALA:Occurrences".equalsIgnoreCase(layers)) {
            requestParams.setQ(wmsUtils.convertLayersParamToQ(layers));
        }

        //get the requested extent for this tile
        String[] bbox = bboxString.split(",");
        double minx = Double.parseDouble(bbox[0]);
        double miny = Double.parseDouble(bbox[1]);
        double maxx = Double.parseDouble(bbox[2]);
        double maxy = Double.parseDouble(bbox[3]);

        int boundingBoxSizeInKm = (int) (maxx - minx) /1000;
        double buff = 0.0;

        String[] facets = new String[0];
        String[] additionalFqs = new String[0];
        logger.info("Rendering at " + boundingBoxSizeInKm);

        if ("singlegrid".equals(wmsEnv.gridres)){
            if(boundingBoxSizeInKm >= 1000 ) {
                facets = new String[]{"grid_ref_100000"};
                buff = 1.0;
            } else if(boundingBoxSizeInKm > 78 && boundingBoxSizeInKm <1000 ){
                facets = new String[]{"grid_ref_10000"};
                buff = 0.75;
            } else if(boundingBoxSizeInKm > 39 && boundingBoxSizeInKm <= 78) {
                facets = new String[]{"grid_ref_2000"};
                buff = 0.05;
            } else if(boundingBoxSizeInKm > 8 && boundingBoxSizeInKm <= 39) {
                facets = new String[]{"grid_ref_1000"};
                buff = 0.05;
            } else {
                facets = new String[]{"grid_ref_100"};
                buff = 0.05;
            }
        } else if ("10kgrid".equals(wmsEnv.gridres)){
            facets = new String[]{"grid_ref_10000"};
            buff = 0.75; //no problems with buff 1.0
        } else {
            //variable grid
            if(boundingBoxSizeInKm >= 1000 ) {
                facets = new String[]{"grid_ref_100000"};
                buff = 1.0;
            } else if(boundingBoxSizeInKm > 39 && boundingBoxSizeInKm < 1000 ){
                facets = new String[]{"grid_ref_10000"};
                buff = 0.75;
            } else if(boundingBoxSizeInKm >= 19 && boundingBoxSizeInKm <= 39){
                buff = 0.5;
                facets = new String[]{"grid_ref_1000", "grid_ref_10000"};
            } else {
                buff = 0.1;
                facets = new String[]{"grid_ref"};
            }
        }

        double oneUnitInRequestedProjXInPixels = (double) width / (double)(maxx - minx);
        double oneUnitInRequestedProjYInPixels = (double) height / (double)(maxy - miny);

        //get a bounding box in WGS84 decimal latitude/longitude
        double[] minLatLng = convertProjectionToWGS84(minx, miny, srs);
        double[] maxLatLng = convertProjectionToWGS84(maxx, maxy, srs);

        String bboxFilterQuery = "(longitude:[{0} TO {2}] AND latitude:[{1} TO {3}])";

        double minX = minLatLng[1] - buff;
        double maxX = maxLatLng[1] + buff;
        double minY = minLatLng[0] - buff;
        double maxY = maxLatLng[0] + buff;

        if(minX > maxX){
            double tmp = maxX;
            maxX = minX;
            minX = tmp;
        }

        if(minY > maxY){
            double tmp = maxY;
            maxY = minY;
            minY = tmp;
        }

        String fq = MessageFormat.format(bboxFilterQuery, minX, minY, maxX, maxY);
        String[] fqs = wmsUtils.getFq(requestParams);
        if(fqs.length==1 && StringUtils.isBlank(fqs[0])){
            fqs = new String[0];
        }

        String[] newFqs = new String[fqs.length + 1 + additionalFqs.length];
        System.arraycopy(fqs, 0, newFqs, 0, fqs.length);
        System.arraycopy(additionalFqs, 0, newFqs, fqs.length, additionalFqs.length);
        newFqs[newFqs.length - 1] = fq;

        requestParams.setFq(newFqs);
        requestParams.setPageSize(0);
        requestParams.setFacet(true);
        requestParams.setFlimit(-1);
        requestParams.setFacets(facets);

        WMSImg wmsImg = WMSImg.create(width, height);

        requestParams.setFq(newFqs);
        requestParams.setPageSize(0);
        requestParams.setFacet(true);
        requestParams.setFlimit(-1);
        requestParams.setFacets(facets);

        Map<String, Integer> gridsRefs = new HashMap<String, Integer>();

        SearchResultDTO resultsDTO2 = searchDAO.findByFulltextSpatialQuery(requestParams, new HashMap<String,String[]>());
        Collection<FacetResultDTO> results2 = resultsDTO2.getFacetResults();
        for (FacetResultDTO result : results2){
            for (FieldResultDTO fieldResult : result.getFieldResult()) {
                gridsRefs.put(fieldResult.getLabel(), (int) fieldResult.getCount());
            }
        }

        List<String> gridRefsToRender =  Arrays.asList(gridsRefs.keySet().toArray(new String[0]));
        java.util.Collections.sort(gridRefsToRender, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                if(o1.length() > o2.length())
                    return 1;
                if(o1.length() == o2.length())
                    return 0;
                return -1;
            }
        });

        Set<int[]> linesToRender = new HashSet<int[]>();
        List<String> renderedLinesCache = new LinkedList<String>();

        for(String gridRef : gridRefsToRender){

            Set<int[]> renderedLines = renderGrid(wmsImg,
                    gridRef,
                    minx,
                    miny,
                    oneUnitInRequestedProjXInPixels,
                    oneUnitInRequestedProjYInPixels,
                    srs,
                    width,
                    height,
                    wmsEnv,
                    renderedLinesCache
            );

            linesToRender.addAll(renderedLines);
        }

        if(outlineGrids) {
            //grid lines are rendered after cell fills
            renderGridLines(wmsImg, linesToRender, outlineColour);
        }

//        if(tileOutline){
//            //for debugging
//            Paint debugBorder = new Color(0x5500FF00, true);
//            wmsImg.g.setPaint(debugBorder);
//            wmsImg.g.drawRect(0,0, width, height); //debugging border
//        }

        if (wmsImg != null && wmsImg.g != null) {
            wmsImg.g.dispose();
            try {
                ServletOutputStream outStream = response.getOutputStream();
                ImageIO.write(wmsImg.img, "png", outStream);
                outStream.flush();
                outStream.close();
            } catch (Exception e) {
                logger.debug("Unable to write image", e);
                e.printStackTrace();
            }
        }
    }

    public void renderGridLines(WMSImg wmsImg, Collection<int[]> linesToRender, String outlineColour){
        for(int[] line: linesToRender){
            wmsImg.g.setPaint(new Color(0xff000000, true));
            wmsImg.g.setStroke(new BasicStroke(0.8f));
            wmsImg.g.drawLine(line[0],line[1],line[2],line[3]);
        }
    }

    /**
     * Render a single grid reference on the supplied tile
     *
     * //TODO note imageHeight is not used.....
     *
     * @param wmsImg
     * @param gridRef
     * @param minx
     * @param miny
     * @param oneUnitXInPixels
     * @param oneUnitYInPixels
     */
    private Set<int[]> renderGrid(WMSImg wmsImg, String gridRef, double minx, double miny, double oneUnitXInPixels,
                                  double oneUnitYInPixels, String targetSrs, int imageWidth, int imageHeight, WMSEnv wmsEnv, List<String> renderedLines){

        if(StringUtils.isEmpty(gridRef)) return new HashSet<int[]>();

        Option<au.org.ala.biocache.util.GridRef> gridRefOption = GridUtil.gridReferenceToEastingNorthing(gridRef);

        if(gridRefOption.isEmpty()) return new HashSet<int[]>();

        Set<int[]> linesToRender = new HashSet<int[]>();

        au.org.ala.biocache.util.GridRef gr = gridRefOption.get();

        int easting = gr.easting();
        int northing = gr.northing();
        int gridSize = (Integer) gr.coordinateUncertainty().get();

        //coordinates in easting / northing of the nearest 10km grid to the bottom,left of this tile
        Integer minEastingOfGridCell  = easting; //may need to use the minimum of each
        Integer minNorthingOfGridCell = northing;
        Integer maxEastingOfGridCell  = minEastingOfGridCell + gridSize;
        Integer maxNorthingOfGridCell = minNorthingOfGridCell + gridSize;

        double[][] polygonInMercator = convertEastingNorthingToTargetSRS(
                new double[][]{
                        new double[]{minEastingOfGridCell, minNorthingOfGridCell},
                        new double[]{maxEastingOfGridCell, minNorthingOfGridCell},
                        new double[]{maxEastingOfGridCell, maxNorthingOfGridCell},
                        new double[]{minEastingOfGridCell, maxNorthingOfGridCell},
                },
                gr.datum(),
                targetSrs
        );

        int[][] coordinatesForImages = convertUnitsToPixelOffset(polygonInMercator, minx, miny,
                oneUnitXInPixels, oneUnitYInPixels, imageWidth, imageHeight);

        int color;
        if(!StringUtils.isEmpty(wmsEnv.gridres) && !"variablegrid".equals(wmsEnv.gridres)){
            //retrieve the supplied colour
            color = wmsEnv.colour;
        } else {
            if(gridSize == 100000){
                color = 0xFFFFFF00; //1km grids yellow
            }
            else if(gridSize == 10000){
                color = 0xFFFFFF00; //1km grids yellow
            }
            else if(gridSize == 2000){
                color = 0xFF0000FF; //blue
            }
            else if(gridSize == 1000){
                color = 0xFF00FF00; //green
            }
            else {
                color = 0xFFFF0000; //red
            }
        }

        Paint polygonFill = new Color(color, true);
        wmsImg.g.setPaint(polygonFill);


        if(overlapping(coordinatesForImages)){
            wmsImg.g.fillPolygon(
                    new int[]{
                            coordinatesForImages[0][0],
                            coordinatesForImages[1][0],
                            coordinatesForImages[2][0],
                            coordinatesForImages[3][0]
                    },
                    new int[]{
                            coordinatesForImages[0][1],
                            coordinatesForImages[1][1],
                            coordinatesForImages[2][1],
                            coordinatesForImages[3][1]
                    },
                    4
            );
        }

        Paint polygonBorder = new Color(0xFF000000, true);
        wmsImg.g.setPaint(polygonBorder);

        //line 1 -  bottom line
        String key1 = getLineKey(minEastingOfGridCell,minNorthingOfGridCell,maxEastingOfGridCell,minNorthingOfGridCell);
        if(!renderedLines.contains(key1)) {
            linesToRender.add(new int[]{coordinatesForImages[0][0], coordinatesForImages[0][1], coordinatesForImages[1][0], coordinatesForImages[1][1]});
        }

        //line 2 - right line
        String key2 = getLineKey(maxEastingOfGridCell,minNorthingOfGridCell,maxEastingOfGridCell,maxNorthingOfGridCell);
        if(!renderedLines.contains(key2)) {
            linesToRender.add(new int[]{coordinatesForImages[1][0], coordinatesForImages[1][1], coordinatesForImages[2][0], coordinatesForImages[2][1]});
        }

        //line 3 - top line
        String key3 = getLineKey(maxEastingOfGridCell,maxNorthingOfGridCell,minEastingOfGridCell,maxNorthingOfGridCell);
        if(!renderedLines.contains(key3)) {
            linesToRender.add(new int[]{coordinatesForImages[2][0], coordinatesForImages[2][1], coordinatesForImages[3][0], coordinatesForImages[3][1]});
        }

        //line 4 - left line
        String key4 = getLineKey(minEastingOfGridCell,maxNorthingOfGridCell,minEastingOfGridCell,minNorthingOfGridCell);
        if(!renderedLines.contains(key4)) {
            linesToRender.add(new int[]{coordinatesForImages[3][0], coordinatesForImages[3][1], coordinatesForImages[0][0], coordinatesForImages[0][1]});
        }

        if(wmsEnv.gridlabels) {
            Paint textColor = new Color(0xFF000000, true);
            wmsImg.g.setPaint(textColor);
            wmsImg.g.setFont(new Font("Ofliant", Font.PLAIN, 11));

            FontMetrics fm = wmsImg.g.getFontMetrics();
            int relativeStringWidth = fm.stringWidth(gridRef);

            String toDisplay = gridRef;

            if ((relativeStringWidth + 5) <= (coordinatesForImages[1][0] - coordinatesForImages[0][0])) {
                wmsImg.g.drawString(toDisplay,
                        coordinatesForImages[0][0] + (coordinatesForImages[1][0] - coordinatesForImages[0][0]) / 2 - (fm.stringWidth(gridRef) / 2),
                        coordinatesForImages[0][1] + (coordinatesForImages[2][1] - coordinatesForImages[0][1]) / 2
                );
            } else {
                wmsImg.g.setFont(new Font("Ofliant", Font.PLAIN, 9));
                relativeStringWidth = fm.stringWidth(gridRef);
                if ((relativeStringWidth + 5) <= (coordinatesForImages[1][0] - coordinatesForImages[0][0])) {
                    wmsImg.g.drawString(toDisplay,
                            coordinatesForImages[0][0] + (coordinatesForImages[1][0] - coordinatesForImages[0][0]) / 2 - (fm.stringWidth(gridRef) / 2),
                            coordinatesForImages[0][1] + (coordinatesForImages[2][1] - coordinatesForImages[0][1]) / 2
                    );
                }
            }
        }

        return linesToRender;
    }

    public boolean overlapping(int[][] imageCoords){
        for(int i=0; i < imageCoords.length; i++){
            if(imageCoords[i][0] >=0 && imageCoords[i][1] >=0 ){
                return true;
            }
        }
        return false;
    }

    public String getLineKey(int x1, int y1 , int x2 , int y2){

        String key = "";
        if(x1 < x2){
            key = x1 + " " + x2;
        } else {
            key = x2 + " " + x1;
        }

        key += " ";

        if(y1 < y2){
            key += y1 + " " + y2;
        } else {
            key += y2 + " " + y1;
        }

        return key;
    }

    ParsedGridRef convertEastingNorthingToOSGrid(double e, double n){

        Integer digits = 10;

        // get the 100km-grid indices
        Double e100k = Math.floor(e/100000);
        Double n100k = Math.floor(n/100000);

        if (e100k<0 || e100k>6 || n100k<0 || n100k>12) return null;

        // translate those into numeric equivalents of the grid letters
        Double l1 = (19-n100k) - (19-n100k)%5 + Math.floor((e100k+10)/5);
        Double l2 = (19-n100k)*5%25 + e100k%5;

        // compensate for skipped 'I' and build grid letter-pairs
        if (l1 > 7) l1++;
        if (l2 > 7) l2++;
        String letPair = "" + (char)(l1 + 'A') + (char)(l2 + 'A');

        // strip 100km-grid indices from easting & northing, and reduce precision
        e = Math.floor((e%100000)/Math.pow(10, 5-digits/2));
        n = Math.floor((n%100000)/Math.pow(10, 5-digits/2));

        return new ParsedGridRef(letPair, (int) e, (int) n);
    }

    /**
     * Reproject coordinates into target CRS.
     *
     * @param x
     * @param y
     * @param sourceCRSString
     * @param targetCRSString
     * @return
     */
    double[] reprojectPoint(Double x, Double y, String sourceCRSString, String targetCRSString){

        try {

            CoordinateReferenceSystem sourceCRS = CRS.decode(sourceCRSString);
            CoordinateReferenceSystem targetCRS = CRS.decode(targetCRSString);
            CoordinateOperation transformOp = new DefaultCoordinateOperationFactory().createOperation(sourceCRS, targetCRS);
            GeneralDirectPosition directPosition = new GeneralDirectPosition(x, y);
            DirectPosition latLongInTargetCRS = transformOp.getMathTransform().transform(directPosition, null);

            //NOTE - returned coordinates are longitude, latitude, despite the fact that if
            //converting latitude and longitude values, they must be supplied as latitude, longitude.
            //No idea why this is the case.
            Double longitude = latLongInTargetCRS.getOrdinate(0);
            Double latitude = latLongInTargetCRS.getOrdinate(1);

            double[] coords = new double[2];

//            coords[0] = Precision.round(longitude, 10);
//            coords[1] = Precision.round(latitude, 10);

            coords[0] = longitude;
            coords[1] = latitude;


            return coords;

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    double[] convertWGS84ToEastingNorthing(Double coordinate1, Double coordinate2){
        return reprojectPoint(coordinate1, coordinate2,  "EPSG:4326", "EPSG:27700");
    }

    double[] convertProjectionToWGS84(Double coordinate1, Double coordinate2, String sourceProjection){
        return reprojectPoint(coordinate1, coordinate2, sourceProjection, "EPSG:4326");
    }

    /**
     *
     * @param polygon coordinates for the polygon
     * @param minXOfTileInMetres
     * @param minYOfTileInMetres
     * @param onePixelInUnitX
     * @param onePixelInUnitY
     * @return
     */
    int[][] convertUnitsToPixelOffset(double[][] polygon,
                                      double minXOfTileInMetres,
                                      double minYOfTileInMetres,
                                      double onePixelInUnitX,
                                      double onePixelInUnitY,
                                      int tileSizeInPixelsX,
                                      int tileSizeInPixelsY){

        int[][] offsetXYWidthHeights = new int[polygon.length][2];
        for(int i = 0; i < polygon.length; i++){
            int x = (int)((polygon[i][0] - minXOfTileInMetres) * onePixelInUnitX);
            int y = (int)((polygon[i][1] - minYOfTileInMetres) * onePixelInUnitY);
            offsetXYWidthHeights[i] = new int[]{x, tileSizeInPixelsY - y};
        }
        return offsetXYWidthHeights;
    }

    double[][] convertEastingNorthingToTargetSRS(double[][] polygon, String sourceSrs, String targetSrs){
        double[][] converted = new double[polygon.length][2];
        for(int i = 0; i < polygon.length; i++){
            converted[i] = reprojectPoint(polygon[i][0], polygon[i][1], sourceSrs, targetSrs);
        }
        return converted;
    }
}

class WMSImg {

    Graphics2D g;
    BufferedImage img;

    public static WMSImg create(int width, int height) {
        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = (Graphics2D) img.getGraphics();
        g.setRenderingHint(RenderingHints.KEY_ALPHA_INTERPOLATION, RenderingHints.VALUE_ALPHA_INTERPOLATION_QUALITY);
        g.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_STROKE_CONTROL, RenderingHints.VALUE_STROKE_NORMALIZE);
        g.setRenderingHint(RenderingHints.KEY_STROKE_CONTROL, RenderingHints.VALUE_STROKE_PURE);
        return new WMSImg(g, img);
    }

    public WMSImg(Graphics2D g, BufferedImage img) {
        this.g = g;
        this.img = img;
    }
}

final class ParsedGridRef {
    public final String chars;
    public final int northing;
    public final int easting;
    public ParsedGridRef(String chars, int easting, int northing){
        this.chars = chars;
        this.easting = easting;
        this.northing = northing;
    }
    public String getGridRef(){ return chars + (easting) + (northing);}
    public String getGridRef100(){ return chars + pad100(easting/100) + pad100(northing/100);}
    public String getGridRef1000(){ return chars + pad10(easting/1000) + pad10(northing/1000);}
    public String getGridRef2000(){

        int tetradE = (easting  % 10000)/1000;
        int tetradN = (northing % 10000)/1000;

        int code = ((tetradN / 2) + 1) + ((tetradE / 2) * 5);

        int tetrad = 64;

        if(code <= 14){
            tetrad = (char) (tetrad + code);
        } else {
            tetrad = (char) (tetrad + (code + 1));
        }
        return chars + (easting/10000) + (northing/10000) + ((char) tetrad);
    }
    public String getGridRef10000(){ return chars + (easting/10000) + (northing/10000);}

    public String getGridRef100000(){ return new String(chars);}

    public String pad10(int eastingOrNorthing){
        if(eastingOrNorthing <10){
            return  "0" + eastingOrNorthing;
        }
        return "" +  eastingOrNorthing;
    }

    public String pad100(int eastingOrNorthing){
        if(eastingOrNorthing > 0 && eastingOrNorthing <100){
            return  "0" + eastingOrNorthing;
        }
        if(eastingOrNorthing <10){
            return  "00" + eastingOrNorthing;
        }
        return "" +  eastingOrNorthing;
    }
}