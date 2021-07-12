/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
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
package au.org.ala.biocache.dao;

import au.org.ala.biocache.dto.Qid;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.util.QidMissingException;
import au.org.ala.biocache.util.QidSizeException;

import java.util.regex.Pattern;

/**
 * Manage cache of POST'ed search parameter q in memory and in db.
 *
 * @author Adam
 */
public interface QidCacheDAO {

    public static Pattern qidPattern = Pattern.compile("qid:(\")?[0-9]*(\")?");

    /**
     * Store search params and return key.
     *
     * @param q            Search parameter q to store.
     * @param displayQ     Search display q to store.
     * @param wkt          wkt to store
     * @param bbox         bounding box to store as double array [min longitude, min latitude, max longitude, max latitude]
     * @param fqs          fqs to store
     * @param maxAge        -1 or expected qid life in ms
     * @param source       name of app that created this qid
     * @return id to retrieve stored value as long.
     */
    String put(String q, String displayQ, String wkt, double[] bbox, String[] fqs, long maxAge, String source) throws QidSizeException;

    /**
     * Retrive search parameter object
     *
     * @param key id returned by put as long.
     * @return search parameter q as String, or null if not in memory
     * or in file storage.
     */
    Qid get(String key) throws QidMissingException;

    /*
     * cache management. defaults set in qid.properties
     */

    void setMaxCacheSize(long sizeInBytes);

    long getMaxCacheSize();

    void setMinCacheSize(long sizeInBytes);

    long getMinCacheSize();

    void setLargestCacheableSize(long sizeInBytes);

    long getLargestCacheableSize();

    long getSize();

    String[] getFq(SpatialSearchRequestParams requestParams);

    String generateQid(SpatialSearchRequestParams requestParams, String bbox, String title, Long maxage, String source);

    void clear();
}
