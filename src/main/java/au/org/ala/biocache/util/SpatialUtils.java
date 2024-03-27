package au.org.ala.biocache.util;

import org.apache.log4j.Logger;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

/**
 * Supplies spatial utilities that can be used for the geospatial seaches
 *
 * @author Natasha Carter
 */
public class SpatialUtils {

    private final static Logger logger = Logger.getLogger(SpatialUtils.class);

    /**
     * Build up a WKT query.  When a geometry collection is provided this is coverted into multiple queries using
     * boolean logic.
     * @param spatialField The SOLR field that is being used to search WKT
     * @param wkt The source WKT value
     * @param negated Whether or not the query should be negated this effects logic operator used
     * @return
     */
    public static String getWKTQuery(String spatialField,String wkt, boolean negated){
        StringBuilder sb = new StringBuilder();
        String operation = negated ? " AND ": " OR ";
        String field = negated ? "-" +spatialField:spatialField;
        if(wkt.startsWith("GEOMETRYCOLLECTION")){
            //the lucene JTS WKT does not support GEOMETRYCOLLECTION http://wiki.apache.org/solr/SolrAdaptersForLuceneSpatial4 so we will add a bunch of "OR"ed intersections
            try{
                WKTReader r = new WKTReader();
                GeometryCollection gc = (GeometryCollection)r.read(wkt);

                //now get the individual components
                sb.append("(");
                for(int i=0;i<gc.getNumGeometries();i++){
                    Geometry g = gc.getGeometryN(i);
                    if(i>0){
                        sb.append(operation);
                    }
                    sb.append(field).append(":\"Intersects(");
                    sb.append(g.toText());
                    sb.append(")\"");
                }
                sb.append(")");

            } catch(Exception e){
                //log the error
                logger.error("failed to parse WKT: " + wkt.substring(0, 255) + "...", e);
            }
        } else {
            sb.append(field).append(":\"Intersects(");
            sb.append(wkt);
            sb.append(")\"");

        }
        return sb.toString();
    }

    /**
     * Attempts to simplify WKT until the number of points is < maxPoints.
     * <p/>
     *
     * @param wkt The WKT to simplify, represented as a string
     * @param maxPoints Set to 0 to disable simplification
     * @return WKT that has less than or equal to maxPoints or null
     */
    public static String simplifyWkt(final String wkt, final int maxPoints) throws Exception {
        WKTReader r = new WKTReader();

        try {
            // quickly count points
            int numPoints = 0;
            for (int i=0;i<wkt.length();i++) {
                if (wkt.charAt(i) == ',') {
                    numPoints++;
                }
            }

            // do not simplify when fewer than maxPoints
            if (maxPoints <= 0 || numPoints <= maxPoints) {
                return wkt;
            }

            Geometry g = r.read(wkt);

            // determine average point to point distance
            double averageDistance = 0;
            if (g.getGeometryType().equals("Polygon")) {
                averageDistance = getPolygonAverageDistance((Polygon) g);
            } else if (g.getGeometryType().equals("MultiPolygon")) {
                averageDistance = getMultipolygonAverageDistance((MultiPolygon) g);
            } else if (g.getGeometryType().equals("GeometryCollection")) {
                averageDistance = getGeometryCollectionAverageDistance(g);
            }

            // determine new average distance required for maxPoints
            double distance = g.getNumPoints() / (double) maxPoints * averageDistance;

            // use this new average distance as the tolerance value
            Geometry newG = TopologyPreservingSimplifier.simplify(g, distance);

            logger.debug("WKT simplified reduced points from " + g.getNumPoints() + " to " + newG.getNumPoints());

            return newG.toString();
        } catch (Exception e) {
            throw new Exception("WKT simplification failed");
        }
    }

    static double getPolygonAverageDistance(Polygon p) {
        Coordinate[] coordinates = p.getExteriorRing().getCoordinates();
        double sumOfSq = 0;
        int count = coordinates.length - 1;
        for (int i=1;i<coordinates.length;i++) {
            sumOfSq += Math.pow(coordinates[i].x - coordinates[i-1].x, 2) + Math.pow(coordinates[i].y - coordinates[i-1].y, 2);
        }

        for (int j=0;j<p.getNumInteriorRing();j++) {
            coordinates = p.getInteriorRingN(j).getCoordinates();
            count += coordinates.length - 1;
            for (int i=1;i<coordinates.length;i++) {
                sumOfSq += Math.pow(coordinates[i].x - coordinates[i-1].x, 2) + Math.pow(coordinates[i].y - coordinates[i-1].y, 2);
            }
        }

        return Math.sqrt(sumOfSq / (double) count);
    }

    static double getMultipolygonAverageDistance(MultiPolygon mp) {
        double sum = 0;
        for (int i=0;i<mp.getNumGeometries();i++) {
            Geometry g1 = mp.getGeometryN(i);
            sum += getPolygonAverageDistance((Polygon) g1);
        }
        return sum / (double) mp.getNumGeometries();
    }

    static double getGeometryCollectionAverageDistance(Geometry g) {
        double sum = 0;
        for (int i=0;i<g.getNumGeometries();i++) {
            Geometry g1 = g.getGeometryN(i);
            if (g1.getGeometryType().equals("Polygon")) {
                sum += getPolygonAverageDistance((Polygon) g);
            } else if (g1.getGeometryType().equals("MultiPolygon")) {
                sum += getMultipolygonAverageDistance((MultiPolygon) g);
            }
        }
        return sum / (double) g.getNumGeometries();
    }
}
