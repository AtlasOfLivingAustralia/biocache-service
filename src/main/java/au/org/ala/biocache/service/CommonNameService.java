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
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Convert indexed common names to until names index returns a stored common name
 *
 * This does not wait while cache is first built.
 *
 * Created by Adam Collins on 21/09/15.
 */
@Component("CommonNameService")
public class CommonNameService {

    /**
     * log4 j logger
     */
    private static final Logger logger = Logger.getLogger(CommonNameService.class);

    /**
     * Fulltext search DAO
     */
    @Inject
    protected SearchDAO searchDAO;

    //common name translation, should get this from name index
    private Object translationLock = new Object();
    private Map<String, String> commonNamesTranslation;
    private boolean updatingTranslation = false;

    //common name lookup, should not be required
    Object lookupLock = new Object();
    private Map<String, String> commonNamesLookup;
    private boolean updatingLookup = false;

    Thread updateTranslationThread = new TranslationThread();

    class TranslationThread extends Thread {
        @Override
        public void run() {
            SpatialSearchRequestParams params = new SpatialSearchRequestParams();
            params.setPageSize(0);
            params.setFacet(true);
            params.setFacets(new String[]{"common_name"});
            params.setFlimit(-1);
            try {
                long startTime = System.currentTimeMillis();
                logger.debug("start refresh commonNamesTranslation");
                SearchResultDTO qr = searchDAO.findByFulltextSpatialQuery(params, null);
                Map m = new HashMap<String, String>();
                for (FacetResultDTO fr : qr.getFacetResults()) {
                    for (FieldResultDTO r : fr.getFieldResult()) {
                        m.put(r.getLabel().toLowerCase().replaceAll("[^a-z]", ""), r.getLabel());
                    }
                }

                logger.debug("time to refresh commonNamesTranslation: " + (System.currentTimeMillis() - startTime) + "ms");

                synchronized (translationLock) {
                    updatingTranslation = false;
                    commonNamesTranslation = m;
                }
            } catch (Exception e) {
            }
        }
    };

    Thread updateLookupThread = new LookupThread();

    class LookupThread extends Thread {
        @Override
        public void run() {
            try {
                long startTime = System.currentTimeMillis();
                logger.debug("start refresh commonNamesLookup");

                //fallback to index for lsid to common name
                Map<String, String> lookup = new HashMap<String, String>();
                SpatialSearchRequestParams params = new SpatialSearchRequestParams();
                params.setPageSize(1);
                params.setFl("common_name");
                params.setFacets(new String[]{"taxon_concept_lsid"});
                params.setFlimit(-1);
                List<GroupFacetResultDTO> result = searchDAO.searchGroupedFacets(params);
                for (GroupFacetResultDTO gr : result) {
                    for (GroupFieldResultDTO gf : gr.getFieldResult()) {
                        if (gf.getOccurrences() != null && gf.getOccurrences().size() > 0) {
                            lookup.put(gf.getLabel(), gf.getOccurrences().get(0).getNamesLsid());
                        }
                    }
                }

                logger.debug("time to refresh commonNamesLookup: " + (System.currentTimeMillis() - startTime) + "ms");

                synchronized (lookupLock) {
                    updatingLookup = false;
                    commonNamesLookup = lookup;
                }
            } catch (Exception e) {
            }
        }
    };

    /**
     * Permit disabling of cached species images
     */
    @Value("${autocomplete.commonnames.extra.enabled:true}")
    private Boolean enabled;

    public String translateCommonName(String untranslated, boolean canReturnNothing) {
        Map<String, String> m = getCommonNameMap();
        if (m == null) return null;

        String translated = m.get(untranslated);

        if (translated == null && !canReturnNothing) translated = untranslated;

        return translated;
    }

    public String lookupCommonName(String lsid) {
        Map<String, String> m = getCommonNameLookup();
        return m != null ? m.get(lsid) : null;
    }

    /**
     * init common name format
     */
    private Map<String, String> getCommonNameMap() {
        if (!enabled) return null;

        if (commonNamesTranslation == null) {
            synchronized (translationLock) {
                if (!updatingTranslation && commonNamesTranslation == null) {
                    updatingTranslation = true;
                    updateTranslationThread.run();
                }
            }
        }

        synchronized (translationLock) {
            return commonNamesTranslation;
        }
    }

    private Map<String, String> getCommonNameLookup() {
        if (!enabled) return null;

        if (commonNamesLookup == null) {
            synchronized (lookupLock) {
                if (!updatingLookup && commonNamesLookup == null) {
                    updatingLookup = true;
                    updateLookupThread.start();
                }
            }
        }

        synchronized (lookupLock) {
            return commonNamesLookup;
        }
    }

    public void resetCache() {

        synchronized (translationLock) {
            if (!updatingTranslation) {
                updatingTranslation = true;
                updateTranslationThread = new TranslationThread();
                updateLookupThread.start();
            }
        }

        synchronized (lookupLock) {
            if (!updatingLookup) {
                updatingLookup = true;
                updateLookupThread = new LookupThread();
                updateLookupThread.start();
            }
        }
    }

}
