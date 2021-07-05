package au.org.ala.biocache.util;

import org.apache.log4j.Logger;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
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
     * TODO: do something with invalid WKT
     *
     * @param wkt The WKT to simplify, represented as a string
     * @param maxPoints Set to 0 to disable simplification
     * @return WKT that has less than or equal to maxPoints or null
     */
    public static String simplifyWkt(String wkt, int maxPoints) {
        final double distanceFactor = 2.0;
        final double distanceInitialPrecision = 0.0001;
        final double distanceMaxPrecision = 10.0;
        return simplifyWkt(wkt, maxPoints, distanceFactor, distanceInitialPrecision, distanceMaxPrecision);
    }
    
    public static String simplifyWkt(final String wkt, final int maxPoints, final double distanceFactor, final double distanceInitialPrecision, final double distanceMaxPrecision) {
        WKTReader r = new WKTReader();

        try {
            Geometry g = r.read(wkt);

            if (maxPoints <= 0 || g.getNumPoints() <= maxPoints) {
                return wkt;
            }

            double distance = distanceInitialPrecision;
            while (distance < distanceMaxPrecision) {
                g = TopologyPreservingSimplifier.simplify(g, distance);

                if (logger.isDebugEnabled()) {
                    logger.debug("Simplified geometry to " + g.getNumPoints() + " at distance precision " + distance);
                }
                
                distance *= distanceFactor;

                if (g.getNumPoints() <= maxPoints) {
                    return g.toText();
                }
            }

            logger.warn(
                    "WKT simplification failed to achieve the required precision: " +
                            " finalNumberOfPoints=" + g.getNumPoints() +
                            " maxPoints=" + maxPoints + 
                            " distanceFactor=" + distanceFactor +
                            " distanceInitialPrecision=" + distanceInitialPrecision + 
                            " distanceMaxPrecision=" + distanceMaxPrecision);
        } catch (Exception e) {
            logger.error("WKT reduction failed due to an exception: " + e.getMessage());
        }
        // An exception occurred or we were unable to reduce it. In both cases, we return null as part of our contract
        return null;
    }

}
