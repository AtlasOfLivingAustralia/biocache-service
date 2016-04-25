package au.org.ala.biocache.web;

import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import org.apache.commons.math3.util.Precision;
import org.apache.log4j.Logger;
import org.geotools.geometry.GeneralDirectPosition;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.operation.DefaultCoordinateOperationFactory;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.CoordinateOperation;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.imageio.ImageIO;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.util.Locale;

@Controller
public class WMSOSGridController {

    private final static Logger logger = Logger.getLogger(WMSOSGridController.class);

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
            HttpServletRequest request,
            HttpServletResponse response)
            throws Exception {


        //-939258.2035682457,7827151.69640205,-626172.1357121639,8140237.764258131

        String[] bbox = bboxString.split(",");
        double minx = Double.parseDouble(bbox[0]);
        double miny = Double.parseDouble(bbox[1]);
        double maxx = Double.parseDouble(bbox[2]);
        double maxy = Double.parseDouble(bbox[3]);

        Double tileWidth = (maxx - minx)/1000;
        Double tileHeight = (maxy - miny)/1000;


        //1. convert bounding box to easting northing
//        logger.info("BBOX size = " + Math.floor(tileHeight) + "km by " + Math.floor(tileWidth) + "km");

        //boundary in eastings / northing
        double[] SW = convertMetersToEastingNorthing(minx, miny);
        double[] NW = convertMetersToEastingNorthing(minx, maxy);
        double[] SE = convertMetersToEastingNorthing(maxx, miny);
        double[] NE = convertMetersToEastingNorthing(maxx, maxy);


        Integer minEasting  = (int)(SW[0] / 10000) * 10000; //may need to use the minimum of each
        Integer maxEasting  = (int)(SE[0] / 10000) * 10000;

        Integer minNorthing  = (int)(SE[1] / 10000) * 10000;
        Integer maxNorthing  = (int)(NE[1] / 10000) * 10000;




        Integer nofOf10kmGrids = ((maxEasting - minEasting)  / 10000) * ((maxNorthing - minNorthing)  / 10000);

//        logger.info("easting: " + SW[0] + "->" + SE[0] + ",  northing: "+ SE[1] + "->" + NE[1] + ", nofOf10kmGrids: " + nofOf10kmGrids);
//        logger.info("easting: " + minEasting + "->" + maxEasting+",  northing: "+ minNorthing + "->" + maxNorthing + ", nofOf10kmGrids: " + nofOf10kmGrids);


        logger.info("northing (west): " + SW[1] + "->" + NW[1] +",  northing (west): "+ SE[1] + "->" + NE[1] + ", nofOf10kmGrids: " + nofOf10kmGrids);



        WMSImg wmsImg = WMSImg.create(width, height);

        Paint currentFill = new Color(0x55FF0000, true);
        wmsImg.g.setPaint(currentFill);

        //for 4326

        //what is 10km in pixels
        //256 pixels =  maxEasting - minEasting
        double oneMetreXInPixels = 256f / (float)(SE[0] - SW[0]);
        double oneMetreYInPixels =  256f / (float)(NE[1] - SE[1]);


        //coordinates for the first point - who do we work out the others ????
        int offsetX = (int) ((float) (10000 - (SW[0] % 10000)) * oneMetreXInPixels); // minx
        int offsetY = (int) ((float) (10000 - (SW[1] % 10000)) * oneMetreYInPixels);



        //256 pixels =  maxNorthing - minNorthing (use precise easting northings

        //System.out.println("Drawing an oval.....");
        wmsImg.g.fillRect(offsetX, 256 - offsetY - (int) (oneMetreYInPixels * 10000f), (int) (oneMetreXInPixels * 10000f), (int) (oneMetreYInPixels * 10000f));
//        wmsImg.g.fillRect(0, 256 - (int) (oneMetreYInPixels * 10000f), (int) (oneMetreXInPixels * 10000f), (int) (oneMetreYInPixels * 10000f));





        Paint debugBorder = new Color(0x5500FF00, true);
        wmsImg.g.setPaint(debugBorder);


        wmsImg.g.drawRect(0,0,256,256); //debugging border

//        wmsImg.g.fillPolygon();

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





//        logger.info("GRIDS " + convertEastingNorthingToOSGrid(SE[0], SE[1]) +
//                " Northing " + convertEastingNorthingToOSGrid(NW[0], NW[1]) + " no of 10km grids = " + nofOf10kmGrids);

        //convert easting/northing to grid references

        //How do i draw a grid cell ????

        //how do i get the bounds of a grid cell ???





        //2. generate a list of grids that are intersected....
        // take the BBOX and calculate the intersection with:
        // 100m, 1km, 2km, 10km, 100km grids

        //



        //draw 10km grids....
//        WMSImg wmsImg = WMSImg.create(width, height);








/*
Zoom levels

313km - just show 10km grids
156km  - just show 10km grids
78km  - just show 10km grids
39km - 2km grids
19km - 2km grids
9km - every resolution
4km - every resolution
2km - every resolution
1km - every resolution
0.5 - every resolution
0.150 - every resolution

 */




    }


    String convertEastingNorthingToOSGrid(double e, double n){

//        int eastingKm = (int)(easting / 1000);
//        int northingKm = (int)(northing / 1000);
//
//        char firstChar = (char) -1;
//
//
//        if(northingKm > 0 && northingKm <500){
//            firstChar = 'S';
//        } else if(northingKm >= 500 && northingKm < 1000){
//            firstChar = 'N';
//        } else if(northingKm >= 1000 && northingKm < 1300){
//            firstChar = 'H';
//        } else {
//            firstChar = 'X';
//        }
//
//
//        if(eastingKm > 0 && eastingKm <500){
//            firstChar = 'S';
//        } else if(eastingKm >= 500 && eastingKm < 1000){
//            firstChar = 'N';
//        } else if(eastingKm >= 1000 && eastingKm < 1300){
//            firstChar = 'H';
//        } else {
//            firstChar = 'X';
//        }

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

        String gridRef = letPair  + (int) (e/10000)  + (int) (n/10000) ;

        return gridRef;
    }


    double[] convertMetersToEastingNorthing(Double coordinate1 , Double coordinate2){

        try {

            CoordinateReferenceSystem sourceCRS = CRS.decode("EPSG:3857");
            CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:27700");
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