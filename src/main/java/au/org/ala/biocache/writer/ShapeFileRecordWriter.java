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

import au.org.ala.biocache.stream.OptionalZipOutputStream;
import au.org.ala.biocache.util.AlaFileUtils;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A record writer that produces a shapefile.
 *
 * For the purpose of keeping the stream open a csv with lat,lng will also be produced.
 *
 * @author Natasha Carter
 */
public class ShapeFileRecordWriter implements RecordWriterError {

    private final static Logger logger = LoggerFactory.getLogger(ShapeFileRecordWriter.class);

    /** limit memory usage */
    private final int maxCollectionSize = 10000;

    private String tmpDownloadDirectory;
    private ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
    private SimpleFeatureBuilder featureBuilder;
    private SimpleFeatureType simpleFeature;
    private OutputStream outputStream;
    private File temporaryShapeFile;
    private int latIdx, longIdx;
    private ListFeatureCollection collection = null;
    private Map<String, String> headerMappings = null;

    private final AtomicBoolean finalised = new AtomicBoolean(false);
    private final AtomicBoolean finalisedComplete = new AtomicBoolean(false);

    private boolean writerError = false;

    private ShapefileDataStore newDataStore;
    private String typeName;
    private SimpleFeatureSource featureSource;
    private SimpleFeatureStore featureStore;

    /**
     * GeometryFactory will be used to create the geometry attribute of each feature (a Point
     * object for the location)
     */
    GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);

    public ShapeFileRecordWriter(String tmpdir, String filename, OutputStream out, String[] header) {
        tmpDownloadDirectory = tmpdir;
        //perform the header mappings so that features are only 10 characters long.
        headerMappings = AlaFileUtils.generateShapeHeader(header);
        //set the outputStream
        outputStream = out;
        //initialise a temporary file that can used to write the shape file
        temporaryShapeFile = new File(tmpDownloadDirectory + File.separator + System.currentTimeMillis() + File.separator + filename + File.separator + filename + ".shp");
        try {
            FileUtils.forceMkdir(temporaryShapeFile.getParentFile());
            //get the indices for the lat and long
            latIdx = ArrayUtils.indexOf(header, "latitude");
            longIdx = ArrayUtils.indexOf(header, "longitude");
            if (latIdx < 0 || longIdx < 0) {
                latIdx = ArrayUtils.indexOf(header, "decimalLatitude.p");
                longIdx = ArrayUtils.indexOf(header, "decimalLongitude.p");
            }

            simpleFeature = createFeatureType(headerMappings.keySet(), null);
            featureBuilder = new SimpleFeatureBuilder(simpleFeature);

            collection = new ListFeatureCollection(featureBuilder.getFeatureType());

            if (latIdx < 0 || longIdx < 0) {
                logger.error("The invalid header..." + StringUtils.join(header, "|"));
                throw new IllegalArgumentException("A Shape File Export needs to include latitude and longitude in the headers.");
            }

        } catch (java.io.IOException e) {
            writerError = true;
        }

        initShapefile();
    }

    private void initShapefile() {
        // stream the contents of the file into the supplied outputStream
        //Properties for the shape file construction
        try {
            Map<String, Serializable> params = new HashMap<String, Serializable>();
            params.put("url", temporaryShapeFile.toURI().toURL());
            params.put("create spatial index", Boolean.TRUE);

            newDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
            newDataStore.createSchema(simpleFeature);
            typeName = newDataStore.getTypeNames()[0];
            featureSource = newDataStore.getFeatureSource(typeName);

            if (featureSource instanceof SimpleFeatureStore) {
                featureStore = (SimpleFeatureStore) featureSource;
            } else {
                writerError = true;
                logger.error(typeName + " does not support read/write access");
            }

            //lat,lng csv header
            if (outputStream instanceof OptionalZipOutputStream) {
                outputStream.write(("latitude,longitude\n").getBytes("UTF-8"));
            }

        } catch (java.io.IOException e) {
            logger.error("Unable to create ShapeFile", e);
            writerError = true;
        }
    }

    /**
     * dynamically creates the feature type based on the headers for the download
     *
     * @param features
     * @return
     */
    private SimpleFeatureType createFeatureType(Set<String> features, Class[] types) {

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("Occurrence");
        builder.setCRS(DefaultGeographicCRS.WGS84); // <- Coordinate reference system

        // add attributes in order
        builder.add("Location", Point.class);
        int i = 0;
        for (String feature : features) {
            Class type = types != null ? types[i] : String.class;

            if (i != longIdx && i != latIdx) {
                builder.add(feature, type);
            }
            i++;
        }

        // build the type
        final SimpleFeatureType LOCATION = builder.buildFeatureType();
        if (logger.isDebugEnabled()) {
            logger.debug("FEATURES IN HEADER::: " + StringUtils.join(features, "|"));
            logger.debug("LOCATION INFO:::" + LOCATION.getAttributeCount() + " " + i + " " + LOCATION.getAttributeDescriptors());
        }
        return LOCATION;
    }

    public ShapeFileRecordWriter() {
        super();
    }

    /**
     * Indicates that the download has completed and the shape file should be generated and
     * written to the supplied output stream.
     */
    @Override
    public void finalise() {
        if (finalised.compareAndSet(false, true)) {
            // stream the contents of the file into the supplied outputStream
            //Properties for the shape file construction
            try {

                //close csv before writing shapefile zip
                OptionalZipOutputStream os = null;
                if (outputStream instanceof OptionalZipOutputStream) {
                    //filename
                    os = (OptionalZipOutputStream) outputStream;
                    String name = os.getCurrentEntry();
                    if (name.contains(".")) {
                        name = name.substring(0, name.lastIndexOf('.'));
                    }
                    os = (OptionalZipOutputStream) outputStream;
                    outputStream.flush();
                    os.closeEntry();
                    os.putNextEntry(name + ".zip");
                }

                //zip the parent directory
                String targetZipFile = temporaryShapeFile.getParentFile().getParent() + File.separator + temporaryShapeFile.getName().replace(".shp", ".zip");
                AlaFileUtils.createZip(temporaryShapeFile.getParent(), targetZipFile);
                try (java.io.FileInputStream inputStream = new java.io.FileInputStream(targetZipFile);) {
                    //write the shapefile to the supplied output stream
                    logger.info("Copying Shape zip file to outputstream");
                    IOUtils.copy(inputStream, outputStream);
                    //now remove the temporary directory
                    FileUtils.deleteDirectory(temporaryShapeFile.getParentFile().getParentFile());
                }

                outputStream.flush();
            } catch (java.io.IOException e) {
                logger.error("Unable to create ShapeFile", e);
                writerError = true;
            } finally {
                finalisedComplete.set(true);
            }
        }
    }

    /**
     * Writes a new record to the download. As a shape file each of the fields are added as a feature.
     */
    @Override
    public void write(String[] record) {
        //check to see if there are values for latitudes and longitudes
        if (StringUtils.isNotBlank(record[longIdx]) && StringUtils.isNotBlank(record[latIdx])) {
            double longitude = Double.parseDouble(record[longIdx]);
            double latitude = Double.parseDouble(record[latIdx]);
            /* Longitude (= x coord) first ! */
            Point point = geometryFactory.createPoint(new Coordinate(longitude, latitude));
            featureBuilder.add(point);

            //now add all the applicable features
            int i = 0;

            int max = simpleFeature.getAttributeCount() + 2;//+2 is the lat and long...
            for (String value : record) {
                if (i != longIdx && i != latIdx && i < max) {
                    // add the value as a feature
                    featureBuilder.add(value);
                }
                i++;
            }
            //build the feature and add it to the collection
            SimpleFeature feature = featureBuilder.buildFeature(null);
            collection.add(feature);

            try {
                if (collection.size() > maxCollectionSize) {
                    featureStore.addFeatures(collection);
                }

                //lat,lng csv entry
                if (outputStream instanceof OptionalZipOutputStream) {
                    outputStream.write((latitude + "," + longitude + "\n").getBytes("UTF-8"));
                }
            } catch (IOException e) {
                writerError = true;
            } finally {
                if (collection.size() > maxCollectionSize) {
                    collection.clear();
                }
            }

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

    @Override
    public boolean finalised() {
        return finalisedComplete.get();
    }

    @Override
    public boolean hasError() {
        return writerError;
    }

    @Override
    public void flush() {
        try {
            outputStream.flush();
        } catch (Exception e) {
            writerError = true;
        }
    }
}
