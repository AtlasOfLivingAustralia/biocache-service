/**************************************************************************
 *  Copyright (C) 2013 Atlas of Living Australia
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
package au.org.ala.biocache.writer;

import java.io.File;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.*;

import au.org.ala.biocache.util.AlaFileUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.geotools.data.shapefile.*;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import au.org.ala.biocache.RecordWriter;

/**
 * A record writer that produces a shapefile.
 * 
 * @author Natasha Carter
 */
public class ShapeFileRecordWriter implements RecordWriter {
	
    private final static Logger logger = LoggerFactory.getLogger(ShapeFileRecordWriter.class);
    
    private static final String tmpDownloadDirectory = "/data/biocache-download/tmp";
    private ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
    private SimpleFeatureBuilder featureBuilder;
    private SimpleFeatureType simpleFeature;
    private OutputStream outputStream;
    private File temporaryShapeFile;
    private int latIdx,longIdx;
    private SimpleFeatureCollection collection = FeatureCollections.newCollection();
    private Map<String,String> headerMappings = null;

    /**
     * GeometryFactory will be used to create the geometry attribute of each feature (a Point
     * object for the location)
     */
    GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
    
    public ShapeFileRecordWriter(String filename,OutputStream out, String[] header){
        //perform the header mappings so that features are only 10 characters long.
        headerMappings = AlaFileUtils.generateShapeHeader(header); 
        //set the outputStream
        outputStream = out;
        //initialise a temporary file that can used to write the shape file
        temporaryShapeFile = new File(tmpDownloadDirectory+File.separator+System.currentTimeMillis()+File.separator+filename+File.separator+filename +".shp");
        try{
            FileUtils.forceMkdir(temporaryShapeFile.getParentFile());
            //get the indices for the lat and long
            latIdx = ArrayUtils.indexOf(header, "latitude");
            longIdx = ArrayUtils.indexOf(header, "longitude");
            if(latIdx <0 || longIdx<0){
                latIdx = ArrayUtils.indexOf(header, "decimalLatitude.p");
                longIdx = ArrayUtils.indexOf(header, "decimalLongitude.p");
            }
            
            simpleFeature = createFeatureType(headerMappings.keySet(), null);
            featureBuilder = new SimpleFeatureBuilder(simpleFeature);
            
            if(latIdx <0 || longIdx<0){
                logger.error("The invalid header..." + StringUtils.join(header, "|"));
                throw new IllegalArgumentException("A Shape File Export needs to include latitude and longitude in the headers.");
            }
            
        } catch (java.io.IOException e){
            logger.error("Unable to create the temporary file necessary for ShapeFile exporting.",e);
        }
    }
    
    /**
     * dynamically creates the feature type based on the headers for the download
     * @param features
     * @return
     */
    private SimpleFeatureType createFeatureType(Set<String> features, Class[] types) {

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("Occurrence");
        builder.setCRS(DefaultGeographicCRS.WGS84); // <- Coordinate reference system

        // add attributes in order
        builder.add("Location", Point.class);
        int i =0;
        for(String feature : features){
            Class type = types != null ? types[i]:String.class;
            String lcFeature = feature.toLowerCase();
            //if((!lcFeature.equals("latitude") && !lcFeature.equals("longitude")) || (!lcFeature.equals("decimalLatitude.p") && !lcFeature.equals("decimalLongitude.p"))){
            if(i != longIdx && i!= latIdx ){
                builder.add(feature, type);
                //SimpleFeatureType test = builder.buildFeatureType();
                //logger.error(test.getAttributeCount()+ " : " + test.getAttributeDescriptors());
            }
            i++;        
        }
        //builder.length(15).add("Name", String.class); // <- 15 chars width for name field

        // build the type
        final SimpleFeatureType LOCATION = builder.buildFeatureType();
        logger.debug("FEATURES IN HEADER::: " + StringUtils.join(features, "|"));
        logger.debug("LOCATION INFO:::" +LOCATION.getAttributeCount() +" " + i +" " + LOCATION.getAttributeDescriptors());
        return LOCATION;
    }
    
    /**
     * Indicates that the download has completed and the shape file should be generated and 
     * written to the supplied output stream.
     */
    @Override
    public void finalise() {
        // stream the contents of the file into the supplied outputStream
      //Properties for the shape file construction
        java.io.FileInputStream inputStream=null;
        try{
            Map<String, Serializable> params = new HashMap<String, Serializable>();
            params.put("url", temporaryShapeFile.toURI().toURL());
            params.put("create spatial index", Boolean.TRUE);
            
            ShapefileDataStore newDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
            newDataStore.createSchema(simpleFeature);
            String typeName = newDataStore.getTypeNames()[0];
            SimpleFeatureSource featureSource = newDataStore.getFeatureSource(typeName);
            //inputStream = new java.io.FileInputStream(temporaryShapeFile);
            
            if (featureSource instanceof SimpleFeatureStore) {
                SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;

               // featureStore.setTransaction(transaction);
                try {
                    featureStore.addFeatures(collection);
                 //   transaction.commit();

                } catch (Exception problem) {
                    logger.error(problem.getMessage(), problem);
                   // transaction.rollback();

                } finally {
                   // transaction.close();
                }
                //zip the parent directory
                String targetZipFile = temporaryShapeFile.getParentFile().getParent()+File.separator+temporaryShapeFile.getName().replace(".shp", ".zip");                
                AlaFileUtils.createZip(temporaryShapeFile.getParent(), targetZipFile);
                inputStream = new java.io.FileInputStream(targetZipFile);
                //write the shapefile to the supplied output stream
                logger.info("Copying Shape zip file to outputstream");
                IOUtils.copy(inputStream,outputStream);
                //now remove the temporary directory
                FileUtils.deleteDirectory(temporaryShapeFile.getParentFile().getParentFile());
                
            } else {
                logger.error(typeName + " does not support read/write access");                
            }
            
        } catch (java.io.IOException e){
            logger.error("Unable to create ShapeFile", e);
        } finally {
            try {
                outputStream.flush();
                IOUtils.closeQuietly(inputStream);                
            } catch(Exception e){
                logger.error("Unable to flush the file " , e);
            }
        }
    }
    
    /**
     * Writes a new record to the download. As a shape file each of the fields are added as a feature. 
     */
    @Override
    public void write(String[] record) {
        //check to see if there are values for latitudes and longitudes
        if(StringUtils.isNotBlank(record[longIdx]) && StringUtils.isNotBlank(record[latIdx])) {
            double longitude = Double.parseDouble(record[longIdx]);
            double latitude = Double.parseDouble(record[latIdx]);
            /* Longitude (= x coord) first ! */
            Point point = geometryFactory.createPoint(new Coordinate(longitude, latitude));
            featureBuilder.add(point);
            
            //now add all the applicable features
            int i = 0;
            
            //logger.debug("FEATURE ATTRIBUT COUNT" + simpleFeature.getAttributeCount());
            int max = simpleFeature.getAttributeCount() + 2;//+2 is the lat and long...
            for(String value : record){
                if(i != longIdx && i!= latIdx && i<max){
                    // add the value as a feature
                    featureBuilder.add(value);
                    //logger.debug(value + " " + i + " " + max);
                }
                i++;
            }
            //build the feature and add it to the collection
            SimpleFeature feature = featureBuilder.buildFeature(null);
            collection.add(feature);
        } else {
            logger.debug("Not adding record with missing lat/long: " + record[0]);
        }
    }
    
    /**
     * @return the headerMappings
     */
    public Map<String, String> getHeaderMappings() {
        return headerMappings;
    }
}
