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
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    /**
     * GeometryFactory will be used to create the geometry attribute of each feature (a Point
     * object for the location)
     */
    private final GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);

    private final ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();

    private final String tmpFilename;
    private final String tmpDownloadDirectory;
    private final SimpleFeatureBuilder featureBuilder;
    private final SimpleFeatureType simpleFeature;
    private final OutputStream outputStream;
    private final ListFeatureCollection collection;
    private final Map<String, String> headerMappings;

    private final AtomicBoolean initialised = new AtomicBoolean(false);
    private final AtomicBoolean finalised = new AtomicBoolean(false);
    private final AtomicBoolean finalisedComplete = new AtomicBoolean(false);

    private final AtomicBoolean writerError = new AtomicBoolean(false);
    private final List<Throwable> errors = new ArrayList<>();

    private final int latIdx, longIdx;

    // Resources that are created during initialise because they may cause Exception's
    private volatile Transaction transaction;
    private volatile File temporaryShapeFile;
    private volatile ShapefileDataStore newDataStore;
    private volatile String typeName;
    private volatile SimpleFeatureSource featureSource;
    private volatile SimpleFeatureStore featureStore;

    public ShapeFileRecordWriter(String tmpdir, String filename, OutputStream out, String[] header) {
        tmpDownloadDirectory = tmpdir;
        tmpFilename = filename;
        //perform the header mappings so that features are only 10 characters long.
        headerMappings = AlaFileUtils.generateShapeHeader(header);
        //set the outputStream
        outputStream = out;
        //get the indices for the lat and long
        if (ArrayUtils.indexOf(header, "latitude") < 0 || ArrayUtils.indexOf(header, "longitude") < 0) {
            latIdx = ArrayUtils.indexOf(header, "decimalLatitude_p");
            longIdx = ArrayUtils.indexOf(header, "decimalLongitude_p");
        } else {
            latIdx = ArrayUtils.indexOf(header, "latitude");
            longIdx = ArrayUtils.indexOf(header, "longitude");
        }

        simpleFeature = createFeatureType(headerMappings.keySet(), null);
        featureBuilder = new SimpleFeatureBuilder(simpleFeature);

        collection = new ListFeatureCollection(featureBuilder.getFeatureType());

        if (latIdx < 0 || longIdx < 0) {
            logger.error("The invalid header..." + StringUtils.join(header, "|"));
            throw new IllegalArgumentException("A Shape File Export needs to include latitude and longitude in the headers: " + StringUtils.join(header, "|"));
        }

    }

    /**
     * dynamically creates the feature type based on the headers for the download
     *
     * @param features
     * @return
     */
    private SimpleFeatureType createFeatureType(Set<String> features, Class<?>[] types) {

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("Occurrence");
        builder.setCRS(DefaultGeographicCRS.WGS84); // <- Coordinate reference system

        // add attributes in order
        builder.add("the_geom", Point.class);
        int i = 0;
        for (String feature : features) {
            Class<?> type = types != null ? types[i] : String.class;

            if (i != longIdx && i != latIdx) {
                builder.add(feature, type);
            }
            i++;
        }

        builder.setDefaultGeometry("the_geom");

        // build the type
        final SimpleFeatureType LOCATION = builder.buildFeatureType();
        if (logger.isDebugEnabled()) {
            logger.debug("FEATURES IN HEADER::: " + StringUtils.join(features, "|"));
            logger.debug("LOCATION INFO:::" + LOCATION.getAttributeCount() + " " + i + " " + LOCATION.getAttributeDescriptors());
        }
        return LOCATION;
    }

	@Override
	public void initialise() {
		if(initialised.compareAndSet(false, true)) {
	        try {
	            //initialise a temporary file that can used to write the shape file
	            temporaryShapeFile = new File(tmpDownloadDirectory + File.separator + System.currentTimeMillis() + File.separator + tmpFilename + File.separator + tmpFilename + ".shp");
	                FileUtils.forceMkdir(temporaryShapeFile.getParentFile());
	            Map<String, Serializable> params = new HashMap<String, Serializable>();
	            params.put("url", temporaryShapeFile.toURI().toURL());
	            params.put("create spatial index", Boolean.TRUE);
	
	            transaction = new DefaultTransaction("create");
	            newDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
	            newDataStore.createSchema(simpleFeature);
	            typeName = newDataStore.getTypeNames()[0];
	            featureSource = newDataStore.getFeatureSource(typeName);
	
	            if (featureSource instanceof SimpleFeatureStore) {
	                featureStore = (SimpleFeatureStore) featureSource;
	                featureStore.setTransaction(transaction);
	            } else {
	                writerError.set(true);
	                logger.error(typeName + " is not currently supported for read/write access");
	            }
	
	            //lat,lng csv header
	            // FIXME: What relevant does OptionalZipOutputStream have to whether the CSV header line is written?
	            if (outputStream instanceof OptionalZipOutputStream) {
	                outputStream.write(("latitude,longitude\n").getBytes(StandardCharsets.UTF_8));
	            }
	
	        } catch (java.io.IOException e) {
	            logger.error("Unable to create ShapeFile", e);
	            writerError.set(true);
	            errors.add(e);
	        }
		}
	}
	
    /**
     * Indicates that the download has completed and the shape file should be generated and
     * written to the supplied output stream.
     */
    @Override
    public void finalise() {
        if (finalised.compareAndSet(false, true)) {
            try {
                try {
                    //write & clear the current batch
                    if (!collection.isEmpty()) {
                        SimpleFeatureStore toAddFeatureStore = featureStore;
						if (toAddFeatureStore != null) {
                            toAddFeatureStore.addFeatures(collection);
                        }
                        collection.clear();
                    }
                } finally {
                    try {
                    	// Dereference the non-final field to ensure we don't have another thread setting it between the null check and the commit call
                    	Transaction toCommitTransaction = transaction;
                    	if (toCommitTransaction != null) {
                    		toCommitTransaction.commit();
                    	}
                    } finally {
                        try {
                        	Transaction toCloseTransaction = transaction;
                        	if (toCloseTransaction != null) {
                        		toCloseTransaction.close();
                        	}
                        } finally {
                            try {
                                ShapefileDataStore toCloseDataStore = newDataStore;
								if(toCloseDataStore != null) {
                                    toCloseDataStore.dispose();
                                }
                            } finally {
                                try {
                                    // Allow for future cases where this isn't equivalent to the statements above
                                    SimpleFeatureStore toDisposeFeatureStore = featureStore;
									if (toDisposeFeatureStore != null) {
                                        toDisposeFeatureStore.getDataStore().dispose();
                                    }
                                } finally {
                                    try {
                                        // Allow for future cases where this isn't equivalent to the statements above
                                        SimpleFeatureSource toDisposeFeatureSource = featureSource;
										if (toDisposeFeatureSource != null) {
                                            toDisposeFeatureSource.getDataStore().dispose();
                                        }
                                    } finally {
                                        try {
                                            //close csv before writing shapefile zip
                                            if (outputStream instanceof OptionalZipOutputStream) {
                                                //filename
                                                OptionalZipOutputStream os = (OptionalZipOutputStream) outputStream;
                                                String name = os.getCurrentEntry();
                                                if (name.contains(".")) {
                                                    name = name.substring(0, name.lastIndexOf('.'));
                                                }
                                                os.closeEntry();
                                                outputStream.flush();
                                                os.putNextEntry(name + ".zip");
                                            }

                                            File toCopyTemporaryShapeFile = temporaryShapeFile;
											if (toCopyTemporaryShapeFile != null) {
                                                //zip the parent directory
                                                String targetZipFile = toCopyTemporaryShapeFile.getParentFile().getParent() + File.separator + toCopyTemporaryShapeFile.getName().replace(".shp", ".zip");
                                                AlaFileUtils.createZip(toCopyTemporaryShapeFile.getParent(), targetZipFile);
                                                //write the shapefile to the supplied output stream
                                                logger.info("Copying Shape zip file to outputstream");
                                                try (final InputStream inputStream = Files.newInputStream(Paths.get(targetZipFile));) {
                                                    IOUtils.copy(inputStream, outputStream);
                                                    outputStream.flush();
                                                }
                                            }
                                        } finally {
                                            File toDeleteTemporaryShapeFile = temporaryShapeFile;
											if (toDeleteTemporaryShapeFile != null) {
                                                //now remove the temporary directory
                                                FileUtils.deleteDirectory(toDeleteTemporaryShapeFile.getParentFile().getParentFile());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (java.io.IOException e) {
                logger.error("Unable to create ShapeFile", e);
                writerError.set(true);
                errors.add(e);
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
        if (!initialised.get()) {
        	throw new IllegalStateException("Must call initialise method before calling write.");
        }
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
                    if (featureStore != null) {
                        featureStore.addFeatures(collection);
                    }
                    collection.clear();
                }

                //lat,lng csv entry
                if (outputStream instanceof OptionalZipOutputStream) {
                    outputStream.write((latitude + "," + longitude + "\n").getBytes("UTF-8"));
                }
                // ArrayIndexOutOfBoundsException is sometimes thrown by AbstractFeatureStore.addFeatures, 
                // so handle it as if it is a writer error
            } catch (ArrayIndexOutOfBoundsException | IOException e) {
                logger.error("Unable to write an entry to Shapefile", e);
                errors.add(e);
                writerError.set(true);
            }

        } else {
            logger.debug("Not adding record with missing lat/long: {}", record[0]);
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
        return writerError.get();
    }

    @Override
    public List<Throwable> getErrors() {
        return errors;
    }

    @Override
    public void flush() {
        try {
            outputStream.flush();
        } catch (Exception e) {
            writerError.set(true);
            errors.add(e);
        }
    }

	@Override
	public void close() throws IOException {
		finalise();
	}

}
