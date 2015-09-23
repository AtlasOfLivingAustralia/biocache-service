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
package au.org.ala.biocache.service;

import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.*;
import org.apache.commons.collections.map.LRUMap;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;

/**
 * Caches species counts using left/right values and an optional fq term.
 *
 * Created by Adam Collins on 21/09/15.
 */
@Component("SpeciesCountsService")
public class SpeciesCountsService {

    /**
     * log4 j logger
     */
    private static final Logger logger = Logger.getLogger(SpeciesCountsService.class);

    /**
     * Fulltext search DAO
     */
    @Inject
    protected SearchDAO searchDAO;

    @Value("${species.counts.cache.minage:1800000}")
    private Long cacheMinAge;

    /**
     * Permit disabling of cached species counts
     */
    @Value("${autocomplete.species.counts.enabled:true}")
    private Boolean enabled;

    //left and left counts by q, fq, qc
    Object cacheLock = new Object();
    LRUMap cache = new LRUMap();
    Object updatelock = new Object();

    /**
     * retrieve left + count + index version
     *
     * @return
     */
    public SpeciesCountDTO getCounts(String[] filterQuery) {
        if (!enabled) return null;

        //lft counts for the query
        SpatialSearchRequestParams params = new SpatialSearchRequestParams();
        StringBuilder fq = new StringBuilder();
        if (filterQuery == null || filterQuery.length == 0) {
            params.setQ("*:*");
            fq.append("*:*");
        } else {
            params.setQ(filterQuery[0]);
            fq.append(filterQuery[0]);
            if (filterQuery.length > 1) {
                String[] fqs = Arrays.copyOfRange(filterQuery, 1, filterQuery.length);
                params.setFacets(fqs);
                for (String s : fqs) {
                    fq.append(s);
                }
            }
        }
        params.setPageSize(0);
        params.setFacet(true);
        params.setFacets(new String[]{"lft"});
        params.setFlimit(-1);

        //hash code
        int hashCode = fq.toString().hashCode();

        SpeciesCountDTO counts;
        synchronized (cacheLock) {
            counts = (SpeciesCountDTO) cache.get(hashCode);
        }

        //refresh if cache missing and not refreshed recently (cacheMinAge)
        long indexVersion = searchDAO.getIndexVersion(false);
        if (counts == null || (cacheMinAge + counts.getAge() < System.currentTimeMillis() && indexVersion != counts.getIndexVersion())) {
            synchronized (updatelock) {
                //check if another has already updated this query
                synchronized (cacheLock) {
                    counts = (SpeciesCountDTO) cache.get(hashCode);
                }
                //still not found, update now
                if (counts == null || (cacheMinAge + counts.getAge() < System.currentTimeMillis() && indexVersion != counts.getIndexVersion())) {
                    try {
                        long startTime = System.currentTimeMillis();
                        logger.debug("start refresh");

                        SearchResultDTO qr = searchDAO.findByFulltextSpatialQuery(params, null);

                        //get lft and count
                        Map<Long, Long> map = new HashMap<Long, Long>();
                        for (FacetResultDTO fr : qr.getFacetResults()) {
                            for (FieldResultDTO r : fr.getFieldResult()) {
                                map.put(Long.parseLong(r.getLabel()), r.getCount());
                            }
                        }

                        //sort keys
                        long[] left = new long[map.size()];
                        List<Long> keys = new ArrayList<Long>(map.keySet());
                        for (int i = 0; i < left.length; i++) {
                            left[i] = keys.get(i);
                        }
                        java.util.Arrays.sort(left);

                        //get sorted values
                        long[] leftCount = new long[map.size()];
                        for (int i = 0; i < leftCount.length; i++) {
                            leftCount[i] = map.get(left[i]);
                        }

                        counts = new SpeciesCountDTO(left, leftCount, indexVersion);

                        //store in map
                        synchronized (cacheLock) {
                            cache.put(hashCode, counts);
                        }

                        logger.debug("time to refresh SpeciesCountsService fq: " + fq.toString() + ", " + (System.currentTimeMillis() - startTime) + "ms");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        return counts;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public long getCount(SpeciesCountDTO counts, long left, long right) {
        if (counts == null || counts.getLft() == null) {
            return 0;
        }
        long[] lft = counts.getLft();
        long[] count = counts.getCounts();

        int pos = java.util.Arrays.binarySearch(lft, left);
        if (pos < 0) {
            pos = -1 * pos - 1;
        }

        long sum = 0;
        while (pos < lft.length && lft[pos] < right) {
            sum += count[pos++];
        }
        return sum;
    }

    public void resetCache() {
        synchronized (cacheLock) {
            cache.clear();
        }
    }

}
