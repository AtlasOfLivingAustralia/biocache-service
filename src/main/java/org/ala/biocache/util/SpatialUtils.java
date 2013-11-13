package org.ala.biocache.util;

import org.geotools.geometry.jts.JTS;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Supplies spatial utilities that can be used for the geospatial seaches
 * 
 * @author Natasha Carter
 */
public class SpatialUtils {
  
    private static final Geometry THE_WORLD=JTS.toGeometry(new Envelope(-180.0,180.0, -90.0, 90.0));
    private static final double TO_DEG = Math.toDegrees(1.0);
    /**
    * The Authalic mean radius (A<subscript>r</subscript>) of the earth
    * [6371.0072 km] (see <a
    * href="http://en.wikipedia.org/wiki/Earth_radius#Authalic_radius"
    * target="_blank">Wikipedia</a>).
    */
        public static final double EARTH_RADIUS_MEAN = 6371.0072;

        /**
    * The equatorial radius of the earth [6378.1370 km] (see <a
    * href="http://en.wikipedia.org/wiki/Earth_radius#Equatorial_radius"
    * target="_blank">Wikipedia</a>) as derived from the WGS-84 ellipsoid.
    */
        public static final double EARTH_RADIUS_EQUATORIAL = 6378.1370;

        /**
    * The polar radius of the earth [6356.7523 km] (see <a
    * href="http://en.wikipedia.org/wiki/Earth_radius#Polar_radius"
    * target="_blank">Wikipedia</a>) as derived from the WGS-84 ellipsoid.
    */
        public static final double EARTH_RADIUS_POLAR = 6356.7523;
    /**
     * This method return an inverse WKT string by subtractoing the WKT from the earth.
     * 
     * @deprecated No longer necessary because the SOLR spatial support negated areas in the query.
     * 
     * @param wkt
     * @return
     */
    @Deprecated 
    public static String getInverseWKT(String wkt){
        try{
            String text=null;
            if(wkt.startsWith("GEOMETRYCOLLECTION")){
                //the lucene JTS WKT does not support GEOMETRYCOLLECTION http://wiki.apache.org/solr/SolrAdaptersForLuceneSpatial4 \
                //Handle a collection by unioning all the geometries together.
                try{
                    WKTReader r = new WKTReader();
                    GeometryCollection gc = (GeometryCollection)r.read(wkt);
                    //now get the individual components
                    Geometry unionGeo = null;
                    for(int i=0;i<gc.getNumGeometries();i++){
                        Geometry g = gc.getGeometryN(i);
                        if(unionGeo == null){
                            unionGeo = g;
                        } else {
                            unionGeo = unionGeo.union(g);
                        }
                    }
                    Geometry newOne = THE_WORLD.difference(unionGeo);
                    text = newOne.toText();
                } catch(Exception e){
                    //log the error
                    e.printStackTrace();
                }
            } else{
                WKTReader r = new WKTReader();
                Geometry g = r.read(wkt);
                Geometry newOne = THE_WORLD.difference(g);
                text = newOne.toText();           
                
                //The WKTWriter used to create the text from the geometry places extra spaces between coordinates and between the type  
                //System.out.println(newOne.toText());
                
            }
            return text.replaceAll("POLYGON ","POLYGON").replaceAll("MULTIPOLYGON ", "MULTIPOLYGON").replaceAll(", ",",");//.replaceAll(" ", ":");
        }
        catch(Exception e){
            //can't do much about this. The original query is invalid...
            e.printStackTrace();
        }
        return wkt;
  }
  
  public static void main(String[] args){
      System.out.println(getInverseWKT("POLYGON((140 -37,151 -37,151 -26, 140.1310 -26, 140 -37))"));
      System.out.println(convertToDegrees(5f));
      System.out.println(convertToDegrees(1f));
      System.out.println(convertToDegrees(10f));
      System.out.println(degreesLatPerKm(-37, 140));
      String wkt ="GEOMETRYCOLLECTION(POLYGON((142.79455566406 -33.134332591089,143.03076171875 -33.134332591089,143.03076171875 -32.940923212969,142.79455566406 -32.940923212969,142.79455566406 -33.134332591089)),MULTIPOLYGON(((143.96459960936 -32.763258819119,144.28320312499 -32.763258819119,144.28320312499 -32.494938181635,143.96459960936 -32.494938181635,143.96459960936 -32.763258819119)),((144.15136718749 -31.881290617098,144.53588867187 -31.881290617098,144.53588867187 -31.58228046593,144.15136718749 -31.58228046593,144.15136718749 -31.881290617098))))";
                             //MULTIPOLYGON (((142.79455566406 -33.134332591089,142.79455566406 -32.940923212969,143.03076171875 -32.940923212969,143.03076171875 -33.134332591089, 142.79455566406 -33.134332591089)),            ((143.96459960936 -32.763258819119,143.96459960936 -32.494938181635, 144.28320312499 -32.494938181635, 144.28320312499 -32.763258819119, 143.96459960936 -32.763258819119)), ((144.15136718749 -31.881290617098, 144.15136718749 -31.58228046593, 144.53588867187 -31.58228046593, 144.53588867187 -31.881290617098, 144.15136718749 -31.881290617098)))
      System.out.println(getWKTQuery("geohash", wkt, false));
      System.out.println(getWKTQuery("geohash", wkt, true));
      System.out.println(getInverseWKT(wkt));
      
  }
  
  //Borrowed from https://github.com/joshuamckenty/OpenSHA/blob/master/java/org/opensha/commons/geo/GeoTools.java
  /**
  * Returns the radius of the earth at the latitude of the supplied
  * <code>Location</code> (see <a
  * href="http://en.wikipedia.org/wiki/Earth_radius#Authalic_radius"
  * target="_blank">Wikipedia</a> for source).
  *
  * @param p
  * the <code>Location</code> at which to compute the earth's
  * radius
  * @return the earth's radius at the supplied <code>Location</code>
  */
    public static double radiusAtLocation(double lat, double lon) {
        double cosL = Math.cos(lat);
        double sinL = Math.sin(lat);
        double C1 = cosL * EARTH_RADIUS_EQUATORIAL;
        double C2 = C1 * EARTH_RADIUS_EQUATORIAL;
        double C3 = sinL * EARTH_RADIUS_POLAR;
        double C4 = C3 * EARTH_RADIUS_POLAR;
        return Math.sqrt((C2 * C2 + C4 * C4) / (C1 * C1 + C3 * C3));
    }

    /**
    * Returns the number of degrees of latitude per km at a given
    * <code>Location</code>. This can be used to convert between km-based and
    * degree-based grid spacing. The calculation takes into account the shape
    * of the earth (oblate spheroid) and scales the conversion accordingly.
    *
    * @param p
    * the <code>Location</code> at which to conversion value
    * @return the number of decimal degrees latitude per km at a given
    * <code>Location</code>
    * @see #radiusAtLocation(Location)
    */
    public static double degreesLatPerKm(double lat, double lon) {
        return TO_DEG / radiusAtLocation(lat,lon);
    }


  
  /**
   * Turns the number of km in a degreee representation based on a conversion factor in Australia.
   * 
   * TODO consider making the conversion factor more dynamic based on supplied lat and long
   * 
   * @param kilometres
   * @return
   */
    public static Float convertToDegrees(Float kilometres){
        if(kilometres != null){
            return 0.008995582157777997f * kilometres;
            //return kilometres /(40000 /360);
        } else {
            return kilometres;
        }
    }
    //NC: We can't use unions to add areas because it seems to add extra area that is not defined in the WKT
    //String wkt ="GEOMETRYCOLLECTION(POLYGON((142.79455566406 -33.134332591089,143.03076171875 -33.134332591089,143.03076171875 -32.940923212969,142.79455566406 -32.940923212969,142.79455566406 -33.134332591089)),MULTIPOLYGON(((143.96459960936 -32.763258819119,144.28320312499 -32.763258819119,144.28320312499 -32.494938181635,143.96459960936 -32.494938181635,143.96459960936 -32.763258819119)),((144.15136718749 -31.881290617098,144.53588867187 -31.881290617098,144.53588867187 -31.58228046593,144.15136718749 -31.58228046593,144.15136718749 -31.881290617098))))";
    //                         MULTIPOLYGON (((142.79455566406 -33.134332591089,142.79455566406 -32.940923212969,143.03076171875 -32.940923212969,143.03076171875 -33.134332591089, 142.79455566406 -33.134332591089)),            ((143.96459960936 -32.763258819119,143.96459960936 -32.494938181635, 144.28320312499 -32.494938181635, 144.28320312499 -32.763258819119, 143.96459960936 -32.763258819119)), ((144.15136718749 -31.881290617098, 144.15136718749 -31.58228046593, 144.53588867187 -31.58228046593, 144.53588867187 -31.881290617098, 144.15136718749 -31.881290617098)))
    /**
     * Returns a GeometryCollection as a UNION of geometries
     * @param wkt
     * @return
     */
    public static String getWKTAsUnions(String wkt){
        if(wkt.startsWith("GEOMETRYCOLLECTION")){
            try{
                WKTReader r = new WKTReader();
                GeometryCollection gc = (GeometryCollection)r.read(wkt);
                Geometry ugeo = null;
                for(int i=0;i<gc.getNumGeometries();i++){
                    Geometry g = gc.getGeometryN(i);
                    if(ugeo == null){
                        ugeo = g;
                    } else{
                        ugeo = ugeo.union(g);
                    }
                }
                return ugeo.toText();
            } catch(Exception e){
                e.printStackTrace();
                return wkt;
            }
        } else {
            return wkt;
        }
    }
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
                e.printStackTrace();
            }
        } else {
            sb.append(field).append(":\"Intersects(");
            sb.append(wkt);
            sb.append(")\"");
            
        }
        return sb.toString();
    }

}
