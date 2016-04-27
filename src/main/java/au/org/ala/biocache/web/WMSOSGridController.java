package au.org.ala.biocache.web;

import au.org.ala.biocache.Store;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.FacetPivotResultDTO;
import au.org.ala.biocache.dto.PointType;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.processor.LocationProcessor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.MultidimensionalCounter;
import org.apache.commons.math3.util.Precision;
import org.apache.log4j.Logger;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.map.ObjectMapper;
import org.geotools.geometry.GeneralDirectPosition;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.operation.DefaultCoordinateOperationFactory;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.CoordinateOperation;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import scala.Option;
import scala.Tuple7;

import javax.imageio.ImageIO;
import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.*;

@Controller
public class WMSOSGridController {

    private final static Logger logger = Logger.getLogger(WMSOSGridController.class);

    @Inject
    protected SearchDAO searchDAO;
//
//    @RequestMapping(value = {"/osgrid/getFeatureInfo"}, method = RequestMethod.GET)
//    public String getFeatureInfo(
//            @RequestParam(value = "CQL_FILTER", required = false, defaultValue = "") String cql_filter,
//            @RequestParam(value = "ENV", required = false, defaultValue = "") String env,
//            @RequestParam(value = "BBOX", required = true, defaultValue = "0,-90,180,0") String bboxString,
//            @RequestParam(value = "WIDTH", required = true, defaultValue = "256") Integer width,
//            @RequestParam(value = "HEIGHT", required = true, defaultValue = "256") Integer height,
//            @RequestParam(value = "STYLES", required = false, defaultValue = "") String styles,
//            @RequestParam(value = "SRS", required = false, defaultValue = "") String srs,
//            @RequestParam(value = "QUERY_LAYERS", required = false, defaultValue = "") String queryLayers,
//            @RequestParam(value = "X", required = true, defaultValue = "0") Double x,
//            @RequestParam(value = "Y", required = true, defaultValue = "0") Double y,
//            HttpServletRequest request,
//            HttpServletResponse response,
//            Model model) throws Exception {
//
//        logger.debug("WMS - GetFeatureInfo requested for: " + queryLayers);
//
//        double[] eastingNorthing = convertMetersToEastingNorthing(x, y);
//
//        String osGrid = convertEastingNorthingToOSGrid(eastingNorthing[0], eastingNorthing[1]);
//
//        System.out.println(osGrid);
//
//        return "metadata/getFeatureGrid";
//    }

    @RequestMapping(value = {"/osgrid/feature.json"}, method = RequestMethod.GET)
    public @ResponseBody Map<String,String> getFeatureInfo(
            HttpServletRequest request,
            HttpServletResponse response,
            Model model) throws Exception {

        logger.debug("WMS - coordinates " + request.getParameter("lng").toString()
            + ", " +  request.getParameter("lat").toString());

        double[] eastingNorthing = convertWGS84ToEastingNorthing(
                Double.parseDouble(request.getParameter("lat").toString()),
                Double.parseDouble(request.getParameter("lng").toString())
        );

        String osGrid = convertEastingNorthingToOSGrid(eastingNorthing[0], eastingNorthing[1]);

        System.out.println(osGrid);
        Map<String, String> map = new HashMap<String, String>();
        map.put("gridref", osGrid);

        return map;
    }

    /**
     * Zoom levels
     *
     * 313km - just show 10km grids
     * 156km  - just show 10km grids
     * 78km  - just show 10km grids
     * 39km - 1km grids
     * 19km - 1km grids
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
            HttpServletResponse response)
            throws Exception {


        //get the requested extent for this tile
        String[] bbox = bboxString.split(",");
        double minx = Double.parseDouble(bbox[0]);
        double miny = Double.parseDouble(bbox[1]);
        double maxx = Double.parseDouble(bbox[2]);
        double maxy = Double.parseDouble(bbox[3]);


        int boundingBoxSizeInKm = (int) (maxx - minx) /1000;
        logger.info("boundingBoxSizeInKm : " + boundingBoxSizeInKm);

        int gridSize = 1000;

        String facet = "grid_ref_1000";

        if(boundingBoxSizeInKm >= 39){
            facet = "grid_ref_10000";
            gridSize = 10000;
        }

        if(boundingBoxSizeInKm < 19){
            facet = "grid_ref";
        }

        double oneMetreMercatorXInPixels = 256f / (float)(maxx - minx);  //easting & northing
        double oneMetreMercatorYInPixels =  256f / (float)(maxy - miny);


        //get a bounding box in WGS84 decimal latitude/longitude
        double[] minLatLng = convertMetersToWGS84(minx, miny);
        double[] maxLatLng = convertMetersToWGS84(maxx, maxy);

        Map<String, Integer> gridsRefs = new HashMap<String, Integer>();


        //construct the filter query
        String bboxFilterQuery2 =
        "(" +
            "(min_latitude:[{0} TO {1}] AND min_longitude:[{2} TO {3}])" +
            " OR " +
            "(max_latitude:[{0} TO {1}] AND min_longitude:[{2} TO {3}])" +
            " OR " +
            "(min_latitude:[{0} TO {1}] AND max_longitude:[{2} TO {3}])" +
            " OR " +
            "(max_latitude:[{0} TO {1}] AND max_longitude:[{2} TO {3}])" +
        ")" +
        " OR " +
        "(" +
            "latitude:[{0} TO {1}] AND longitude:[{2} TO {3}]" +
        ")";

//        if (a.max.x < b.min.x) return false; // a is left of b
//        if (a.min.x > b.max.x) return false; // a is right of b
//        if (a.max.y < b.min.y) return false; // a is above b
//        if (a.min.y > b.max.y) return false; // a is below b


        String bboxFilterQuery =
                "-((max_longitude:[* TO {0}])" +
                        " OR " +
                        "(min_longitude:[{1} TO *])" +
                        " OR " +
                        "(max_latitude:[* TO {2}])" +
                        " OR " +
                        "(min_latitude:[{3} TO *]))";


        String fq = MessageFormat.format(bboxFilterQuery, minLatLng[1], maxLatLng[1], minLatLng[0], maxLatLng[0]);

        String[] newFqs = new String[requestParams.getFq().length + 1];
        System.arraycopy(requestParams.getFq(), 0, newFqs, 0, requestParams.getFq().length);
        newFqs[newFqs.length - 1] = fq;

        requestParams.setFq(newFqs);
        requestParams.setPageSize(0);
        requestParams.setFacet(true);
        requestParams.setFlimit(-1);
        requestParams.setFacets(new String[]{facet});

        java.util.List<FacetPivotResultDTO> facetPivots = searchDAO.searchPivot(requestParams);

        WMSImg wmsImg = WMSImg.create(width, height);

        if(!facetPivots.isEmpty()){
            for(FacetPivotResultDTO pivot: facetPivots){
                logger.info(pivot.getPivotField() + " : " + pivot.getPivotResult().size());
                for(FacetPivotResultDTO pivotResult : pivot.getPivotResult()){
                    logger.info("pivot: " + pivotResult.getValue() + ", count: " + pivotResult.getCount());
                    String gridReference = pivotResult.getValue();
                    Integer recordCount = pivotResult.getCount();
                    gridsRefs.put(gridReference, recordCount);
                }

            }
        }

/********** FUDGE *************/

        //  easting/northing range with buffer
        double[] minEN = convertMetersToEastingNorthing(minx, miny);
        double[] maxEN = convertMetersToEastingNorthing(maxx, maxy);


        String eastingNorthingFilterQuery =
                "easting:[{0} TO {1}]" +
                " AND " +
                "northing:[{2} TO {3}]";

        int buff = gridSize * 2;
        newFqs[newFqs.length - 1] = MessageFormat.format(eastingNorthingFilterQuery, minEN[0]-buff, maxEN[0]+buff, minEN[1]-buff, maxEN[1]+buff);

        requestParams.setFq(newFqs);
        requestParams.setPageSize(0);
        requestParams.setFacet(true);
        requestParams.setFlimit(-1);
        requestParams.setFacets(new String[]{facet});

        java.util.List<FacetPivotResultDTO> facetPivots2 = searchDAO.searchPivot(requestParams);

        if(!facetPivots2.isEmpty()){
            for(FacetPivotResultDTO pivot: facetPivots2){
                logger.info(pivot.getPivotField() + " : " + pivot.getPivotResult().size());
                for(FacetPivotResultDTO pivotResult : pivot.getPivotResult()){
                    logger.info("pivot: " + pivotResult.getValue() + ", count: " + pivotResult.getCount());
                    String gridReference = pivotResult.getValue();
                    Integer recordCount = pivotResult.getCount();
                    gridsRefs.put(gridReference, recordCount);
                }

            }
        }
/********** END FUDGE *************/


        Iterator<String> iter = gridsRefs.keySet().iterator();
        while(iter.hasNext()){
            String gridRef = iter.next();
            //TODO alter colour with counts
            renderGrid(wmsImg, gridRef, minx, miny, oneMetreMercatorXInPixels, oneMetreMercatorYInPixels);
        }

        Paint debugBorder = new Color(0x5500FF00, true);
        wmsImg.g.setPaint(debugBorder);
        wmsImg.g.drawRect(0,0,256,256); //debugging border

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


    public void renderGrid(WMSImg wmsImg, String gridRef, double minx, double miny, double oneMetreMercatorXInPixels, double oneMetreMercatorYInPixels){

        if(StringUtils.isEmpty(gridRef)) return;

        int[] eastingNorthingGridSize = Store.convertGridReference(gridRef);

        int easting = eastingNorthingGridSize[0];
        int northing = eastingNorthingGridSize[1];
        int gridSize = eastingNorthingGridSize[2];

        //coordinates in easting / northing of the nearest 10km grid to the bottom,left of this tile
        Integer minEastingOfGridCell  = easting; //may need to use the minimum of each
        Integer minNorthingOfGridCell = northing;
        Integer maxEastingOfGridCell  = minEastingOfGridCell + gridSize;
        Integer maxNorthingOfGridCell = minNorthingOfGridCell + gridSize;


        double[][] polygonInMercator = convertEastingNorthingToMercators(new double[][]{
                new double[]{minEastingOfGridCell, minNorthingOfGridCell},
                new double[]{maxEastingOfGridCell, minNorthingOfGridCell},
                new double[]{maxEastingOfGridCell, maxNorthingOfGridCell},
                new double[]{minEastingOfGridCell, maxNorthingOfGridCell}
        });


        int[][] coordinatesForImages = convertMercatorMetersToPixelOffset(polygonInMercator, minx, miny,
                oneMetreMercatorXInPixels, oneMetreMercatorYInPixels, 256);

        Paint polygonFill = new Color(0x55FF0000, true);
        wmsImg.g.setPaint(polygonFill);

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

    String convertEastingNorthingToOSGrid(double e, double n){

        Integer digits = 10;


        // use digits = 0 to return numeric format (in metres)
//        if (digits == 0) return e.pad(6) + ',' + n.pad(6);

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

//        String gridRef = letPair + ' ' + (int) e  + ' ' + (int) n ;

        String gridRef = letPair + " "  + (int) (e / 1000) + " " + (int) (n  / 1000) ;

        return gridRef;
    }

    double[] reproject(Double coordinate1 , Double coordinate2, String sourceCRSString, String targetCRSString){

        try {

            CoordinateReferenceSystem sourceCRS = CRS.decode(sourceCRSString);
            CoordinateReferenceSystem targetCRS = CRS.decode(targetCRSString);
            CoordinateOperation transformOp = new DefaultCoordinateOperationFactory().createOperation(sourceCRS, targetCRS);
            GeneralDirectPosition directPosition = new GeneralDirectPosition(coordinate1, coordinate2);
            DirectPosition wgs84LatLong = transformOp.getMathTransform().transform(directPosition, null);

            //NOTE - returned coordinates are longitude, latitude, despite the fact that if
            //converting latitude and longitude values, they must be supplied as latitude, longitude.
            //No idea why this is the case.
            Double longitude = wgs84LatLong.getOrdinate(0);
            Double latitude = wgs84LatLong.getOrdinate(1);

            double[] coords = new double[2];

            coords[0] = Precision.round(longitude, 10);
            coords[1] = Precision.round(latitude, 10);
            return coords;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    double[] convertWGS84ToEastingNorthing(Double coordinate1 , Double coordinate2){
        return reproject(coordinate1, coordinate2,  "EPSG:4326", "EPSG:27700");
    }

    double[] convertMetersToWGS84(Double coordinate1 , Double coordinate2){
        return reproject(coordinate1, coordinate2, "EPSG:3857", "EPSG:4326");
    }


    double[] convertMetersToEastingNorthing(Double coordinate1 , Double coordinate2){
        return reproject(coordinate1, coordinate2, "EPSG:3857", "EPSG:27700");
    }


    /**
     *
     * @param polygon coordinates for the polygon
     * @param minXOfTileInMercatorMetres
     * @param minYOfTileInMercatorMetres
     * @param onePixelInMetresX
     * @param onePixelInMetresY
     * @param tileSizeInPixels
     * @return
     */
    int[][] convertMercatorMetersToPixelOffset(double[][] polygon, double minXOfTileInMercatorMetres, double minYOfTileInMercatorMetres,
                                             double onePixelInMetresX,
                                             double onePixelInMetresY, int tileSizeInPixels){

        int[][] offsetXYWidthHeights = new int[polygon.length][2];
        for(int i = 0; i < polygon.length; i++){

//            logger.debug("Easting = " + polygon[i][0] + ", Northing = " + polygon[i][1]);

//            logger.debug("Coordinates (Mercator)  x = " + polygon[i][0] + ", y = " + polygon[i][1] +
// ", tile min x = " + minXOfTileInMercatorMetres + ", tile min y = " + minYOfTileInMercatorMetres);

            int x = (int)((polygon[i][0] - minXOfTileInMercatorMetres) * onePixelInMetresX);
            int y = (int)((polygon[i][1] - minYOfTileInMercatorMetres) * onePixelInMetresY);

//            logger.debug("X & Y = " + x + ", " + y);

            offsetXYWidthHeights[i] = new int[]{
                    x,
                    tileSizeInPixels - y ,              //y

            };
        }
        return offsetXYWidthHeights;
    }



    double[][] convertEastingNorthingToMercators(double[][] polygon){
        double[][] converted = new double[polygon.length][2];
        for(int i=0; i< polygon.length; i++){
            converted[i] = convertEastingNorthingToMercator(polygon[i][0], polygon[i][1]);
        }
        return converted;
    }


    double[] convertEastingNorthingToMercator(Double x , Double y){

        try {
            CoordinateReferenceSystem sourceCRS = CRS.decode("EPSG:27700");
            CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:3857");
            CoordinateOperation transformOp = new DefaultCoordinateOperationFactory().createOperation(sourceCRS, targetCRS);
            GeneralDirectPosition directPosition = new GeneralDirectPosition(x, y);
            DirectPosition wgs84LatLong = transformOp.getMathTransform().transform(directPosition, null);

            //NOTE - returned coordinates are longitude, latitude, despite the fact that if
            //converting latitude and longitude values, they must be supplied as latitude, longitude.
            //No idea why this is the case.
            Double longitude = wgs84LatLong.getOrdinate(0);
            Double latitude = wgs84LatLong.getOrdinate(1);

            double[] coords = new double[2];

            coords[0] = Precision.round(longitude, 10);
            coords[1] = Precision.round(latitude, 10);
            return coords;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }



//
//    private class Deg2UTM
//    {
//        double Easting;
//        double Northing;
//        int Zone;
//        char Letter;
//        private  Deg2UTM(double Lat,double Lon)
//        {
//            Zone= (int) Math.floor(Lon/6+31);
//            if (Lat<-72)
//                Letter='C';
//            else if (Lat<-64)
//                Letter='D';
//            else if (Lat<-56)
//                Letter='E';
//            else if (Lat<-48)
//                Letter='F';
//            else if (Lat<-40)
//                Letter='G';
//            else if (Lat<-32)
//                Letter='H';
//            else if (Lat<-24)
//                Letter='J';
//            else if (Lat<-16)
//                Letter='K';
//            else if (Lat<-8)
//                Letter='L';
//            else if (Lat<0)
//                Letter='M';
//            else if (Lat<8)
//                Letter='N';
//            else if (Lat<16)
//                Letter='P';
//            else if (Lat<24)
//                Letter='Q';
//            else if (Lat<32)
//                Letter='R';
//            else if (Lat<40)
//                Letter='S';
//            else if (Lat<48)
//                Letter='T';
//            else if (Lat<56)
//                Letter='U';
//            else if (Lat<64)
//                Letter='V';
//            else if (Lat<72)
//                Letter='W';
//            else
//                Letter='X';
//            Easting=0.5*Math.log((1+Math.cos(Lat*Math.PI/180)*Math.sin(Lon*Math.PI/180-(6*Zone-183)*Math.PI/180))/(1-Math.cos(Lat*Math.PI/180)*Math.sin(Lon*Math.PI/180-(6*Zone-183)*Math.PI/180)))*0.9996*6399593.62/Math.pow((1+Math.pow(0.0820944379, 2)*Math.pow(Math.cos(Lat*Math.PI/180), 2)), 0.5)*(1+ Math.pow(0.0820944379,2)/2*Math.pow((0.5*Math.log((1+Math.cos(Lat*Math.PI/180)*Math.sin(Lon*Math.PI/180-(6*Zone-183)*Math.PI/180))/(1-Math.cos(Lat*Math.PI/180)*Math.sin(Lon*Math.PI/180-(6*Zone-183)*Math.PI/180)))),2)*Math.pow(Math.cos(Lat*Math.PI/180),2)/3)+500000;
//            Easting=Math.round(Easting*100)*0.01;
//            Northing = (Math.atan(Math.tan(Lat*Math.PI/180)/Math.cos((Lon*Math.PI/180-(6*Zone -183)*Math.PI/180)))-Lat*Math.PI/180)*0.9996*6399593.625/Math.sqrt(1+0.006739496742*Math.pow(Math.cos(Lat*Math.PI/180),2))*(1+0.006739496742/2*Math.pow(0.5*Math.log((1+Math.cos(Lat*Math.PI/180)*Math.sin((Lon*Math.PI/180-(6*Zone -183)*Math.PI/180)))/(1-Math.cos(Lat*Math.PI/180)*Math.sin((Lon*Math.PI/180-(6*Zone -183)*Math.PI/180)))),2)*Math.pow(Math.cos(Lat*Math.PI/180),2))+0.9996*6399593.625*(Lat*Math.PI/180-0.005054622556*(Lat*Math.PI/180+Math.sin(2*Lat*Math.PI/180)/2)+4.258201531e-05*(3*(Lat*Math.PI/180+Math.sin(2*Lat*Math.PI/180)/2)+Math.sin(2*Lat*Math.PI/180)*Math.pow(Math.cos(Lat*Math.PI/180),2))/4-1.674057895e-07*(5*(3*(Lat*Math.PI/180+Math.sin(2*Lat*Math.PI/180)/2)+Math.sin(2*Lat*Math.PI/180)*Math.pow(Math.cos(Lat*Math.PI/180),2))/4+Math.sin(2*Lat*Math.PI/180)*Math.pow(Math.cos(Lat*Math.PI/180),2)*Math.pow(Math.cos(Lat*Math.PI/180),2))/3);
//            if (Letter<'M')
//                Northing = Northing + 10000000;
//            Northing=Math.round(Northing*100)*0.01;
//        }
//    }

}

class WMSImg {

    Graphics2D g;
    BufferedImage img;

    public static WMSImg create(int width, int height) {
        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = (Graphics2D) img.getGraphics();
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        return new WMSImg(g, img);
    }

    public WMSImg(Graphics2D g, BufferedImage img) {
        this.g = g;
        this.img = img;
    }
}