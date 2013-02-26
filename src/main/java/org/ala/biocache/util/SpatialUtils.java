package org.ala.biocache.util;

import org.geotools.geometry.jts.JTS;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;


/**
 * 
 * Supplies spatial utilities that can be used for the geospatial seaches
 * 
 * @author Natasha Carter
 *
 */
public class SpatialUtils {
  
    private static final Geometry THE_WORLD=JTS.toGeometry(new Envelope(-180.0,180.0, -90.0, 90.0));
  
    public static String getInverseWKT(String wkt){
        try{
            WKTReader r = new WKTReader();
            Geometry g = r.read(wkt);
            Geometry newOne = THE_WORLD.difference(g);
            String text = newOne.toText();           
            
            //The WKTWriter used to create the text from the geometry places extra spaces between coordinates and between the type  
            //System.out.println(newOne.toText());
            return text.replaceAll("POLYGON ","POLYGON").replaceAll("MULTIPOLYGON ", "MULTIPOLYGON").replaceAll(", ",",").replaceAll(" ", ":");
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
      
  }
  
  public static Float convertToDegrees(Float kilometres){
      if(kilometres != null)
          return kilometres /111;
      else
          return kilometres;
  }
  
}
