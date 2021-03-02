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

import au.org.ala.biocache.dao.IndexDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.*;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Caches species counts using left/right values and an optional fq term.
 *
 * Created by Adam Collins on 21/09/15.
 */
@Component("SpeciesCountsService")
public class SpeciesCountsService {

    /**
     * Fulltext search DAO
     */
    @Inject
    protected SearchDAO searchDAO;

    @Inject
    protected IndexDAO indexDao;

    /**
     * Refresh cache every 30 minutes.
     */
    @Value("${species.counts.async.updates:false}")
    protected Boolean asyncUpdates;

    /**
     * Refresh cache every 30 minutes.
     */
    @Value("${species.counts.cache.minage:1800000}")
    protected Long cacheMinAge;

    /**
     * Permit disabling of cached species counts
     */
    @Value("${autocomplete.species.counts.enabled:true}")
    private Boolean enabled;

    //left and left counts by q, fq, qc
    final Object cacheLock = new Object();
    LRUMap cache = new LRUMap();
    final Object updatelock = new Object();

    //record of updates in queue
    final Object updatingLock = new Object();
    Map<Integer, Boolean> updatingList = new ConcurrentHashMap<Integer, Boolean>();

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
        long indexVersion = indexDao.getIndexVersion(false);
        if (counts == null || (cacheMinAge + counts.getAge() < System.currentTimeMillis() && indexVersion != counts.getIndexVersion())) {
            //old counts that need one update scheduled
            synchronized (updatingLock) {
                Boolean updating = updatingList.get(hashCode);

                if (updating == null) {
                    updatingList.put(hashCode, true);

                    if (asyncUpdates){
                        Thread updateThread = new UpdateThread(this, hashCode, params);
                        updateThread.start();
                    } else {
                        //run synchronously...
                        UpdateThread updateThread = new UpdateThread(this, hashCode, params);
                        updateThread.update();
                        counts = (SpeciesCountDTO) cache.get(hashCode);
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
        while (pos < lft.length && lft[pos] <= right) {
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

class UpdateThread extends Thread {

    private static final Logger logger = Logger.getLogger(UpdateThread.class);

    SpeciesCountsService speciesCountsService;
    int hashCode;
    SpatialSearchRequestParams params;

    public UpdateThread(SpeciesCountsService speciesCountsService, int hashCode, SpatialSearchRequestParams params) {
        this.speciesCountsService = speciesCountsService;
        this.hashCode = hashCode;
        this.params = params;
    }

    @Override
    public void run() {
        update();
    }

    public void update() {
        synchronized (speciesCountsService.updatelock) {
            //check if another has already updated this query
            synchronized (speciesCountsService.cacheLock) {
                if (speciesCountsService.cache.get(hashCode) != null) {
                    //remove this from the update list
                    synchronized (speciesCountsService.updatingLock) {
                        speciesCountsService.updatingList.remove(hashCode);
                    }
                    return;
                }
            }

            //not found, update now
            try {
                logger.debug("updating species counts for query: " + params.toString());
                SearchResultDTO qr = speciesCountsService.searchDAO.findByFulltextSpatialQuery(params, null);

                //get lft and count
                Map<Long, Long> map = new HashMap<Long, Long>();
                for (FacetResultDTO fr : qr.getFacetResults()) {
                    for (FieldResultDTO r : fr.getFieldResult()) {
                        if (StringUtils.isNotEmpty(r.getLabel())) {
                            try {
                                map.put(Long.parseLong(r.getLabel()), r.getCount());
                            } catch (NumberFormatException e){
                                //for non numeric
                            }
                        }
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

                SpeciesCountDTO counts = new SpeciesCountDTO(left, leftCount, speciesCountsService.indexDao.getIndexVersion(false));

                //store in map
                synchronized (speciesCountsService.cacheLock) {
                    speciesCountsService.cache.put(hashCode, counts);
                }

            } catch (Exception e) {
                logger.error("Failed to update species counts for : " + params.toString() + " " + e.getMessage(), e);
            }
        }
        //remove this from the update list
        synchronized (speciesCountsService.updatingLock) {
            speciesCountsService.updatingList.remove(hashCode);
        }
    }
}