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
package au.org.ala.biocache.dto;

import au.org.ala.biocache.service.RestartDataService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * This class provides thematic grouping of facets to aid rendering in UIs.
 *
 * @author Natasha Carter
 */
public class FacetThemes {
	
    private static String[] allFacets = new String[]{};
    private static String[] allFacetsLimited = new String[]{};
    private static java.util.List<FacetTheme> allThemes = RestartDataService.get(new FacetThemes(false), "allThemes", new TypeReference<java.util.ArrayList<FacetTheme>>(){}, java.util.ArrayList.class);
    private static LinkedHashMap<String, FacetDTO> facetsMap = new LinkedHashMap<String, FacetDTO>();
    
    private static Integer facetsMax = 4;
    private static Integer facetsDefaultMax = 0;
    private static Boolean facetDefault = true;

    //Used to wait for initialisation
    private static CountDownLatch initialised = new CountDownLatch(1);

    /**
     * Takes a file path to a configuration file in JSON and parses the file
     * into facets and facet themes.
     *
     * @param configFilePath
     * @throws IOException
     */
    public FacetThemes(String configFilePath, Set<IndexFieldDTO> indexedFields, int facetsMax, int facetsDefaultMax, boolean facetDefault) throws IOException {
        try {
            FacetThemes.facetsMax = facetsMax;
            FacetThemes.facetsDefaultMax = facetsDefaultMax;
            FacetThemes.facetDefault = facetDefault;
            
            if (configFilePath != null && new File(configFilePath).exists()){
                java.util.List<FacetTheme> newThemes = new ArrayList<>();
                ObjectMapper om = new ObjectMapper();
                List<Map<String,Object>> config = om.readValue(new File(configFilePath), List.class);
                for(Map<String, Object> facetGroup : config){
                    String title = (String) facetGroup.get("title");
                    List<Map<String,String>> facetsConfig = (List<Map<String,String>>) facetGroup.get("facets");
                    List<FacetDTO> facets = new ArrayList<FacetDTO>();
                    for(Map<String,String> facetsMap : facetsConfig){
                        String name = facetsMap.get("field");
                        String description = null;
                        String dwcTerm = null;
                        Boolean i18nValues = null;
                        if (indexedFields != null) {
                            for (IndexFieldDTO field : indexedFields) {
                                if (field.getName().equalsIgnoreCase(name)) {
                                    description = field.getDescription();
                                    dwcTerm = field.getDwcTerm();
                                    i18nValues = field.isI18nValues();
                                    
                                    //only add this facet if there is an associated SOLR field
                                    facets.add(new FacetDTO(name, facetsMap.get("sort"), description, dwcTerm, i18nValues));
                                    break;
                                }
                            }
                        }
                    }
                    newThemes.add(new FacetTheme(title, facets));
                }
                if (newThemes.size() > 0) {
                    FacetThemes.allThemes = newThemes;
                    initAllFacets();
                }
            } else {
                defaultInit();
            }
        } finally {
            initialised.countDown();
        }
    }

    /**
     * This method works around the static variable pattern used here, by ensuring that operations 
     * do not continue until after the static fields have been initialised.
     */
    private static void afterInitialisation() {
        try {
            if (initialised.getCount() > 0 && allThemes.size() > 0) {
                //data exists, do not wait
                initAllFacets();
                initialised.countDown();
            } else {
                initialised.await();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static String[] getAllFacetsLimited() {
        afterInitialisation();

        return allFacetsLimited;
    }

    public static Integer getFacetsMax() {
        afterInitialisation();

        return facetsMax;
    }

    public static LinkedHashMap<String, FacetDTO> getFacetsMap() {
        afterInitialisation();

        return facetsMap;
    }

    public static Boolean getFacetDefault() {
        afterInitialisation();

        return facetDefault;
    }

    public static List<FacetTheme> getAllThemes() {
        afterInitialisation();

        return allThemes;
    }

    synchronized private static void initAllFacets() {
        LinkedHashMap<String, FacetDTO> map = new LinkedHashMap<String, FacetDTO>();
        for (FacetTheme theme : allThemes) {
            for(FacetDTO f : theme.getFacets()) {
                map.put(f.getField(), f);
            }
            facetsMap = map;
            allFacets = facetsMap.keySet().toArray(new String[]{});
            allFacetsLimited = allFacets != null && allFacets.length > facetsDefaultMax ? Arrays.copyOfRange(allFacets, 0, facetsDefaultMax) : allFacets;
        }
    }

    public FacetThemes() {
        defaultInit();
        initialised.countDown();
    }

    public FacetThemes(boolean init) {
        if (init) {
            defaultInit();
            initialised.countDown();
        }
    }

    private void defaultInit() {
        allThemes.clear();
        allThemes.add(new FacetTheme("Taxonomic",
                new FacetDTO(OccurrenceIndex.TAXON_NAME, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.RAW_TAXON_NAME, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.COMMON_NAME, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.SUBSPECIES_NAME, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.SPECIES, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.GENUS, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.FAMILY, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.ORDER, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.CLASS, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.PHYLUM, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.KINGDOM, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.SPECIES_GROUP, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.TAXON_RANK, "count", null, null, null)));

        allThemes.add(new FacetTheme("Geospatial",
                new FacetDTO(OccurrenceIndex.COORDINATE_UNCERTAINTY, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.SENSITIVE, "count", null, null, null),
                new FacetDTO(OccurrenceIndex.STATE_CONSERVATION, "count", null, null, null),
                new FacetDTO(OccurrenceIndex.RAW_STATE_CONSERVATION, "count", null, null, null),
                new FacetDTO("cl966", "count", null, null, null),
                new FacetDTO("cl959", "count", null, null, null),
                new FacetDTO(OccurrenceIndex.STATE, "count", null, null, null),
                new FacetDTO(OccurrenceIndex.COUNTRY, "index", null, null, null),
                new FacetDTO("cl1918", "count", null, null, null),
                new FacetDTO("cl617", "count", null, null, null),
                new FacetDTO("cl620", "count", null, null, null)
        ));

        allThemes.add(new FacetTheme("Temporal",
                new FacetDTO(OccurrenceIndex.MONTH, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.YEAR, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.DECADE_FACET_NAME, "index", null, null, null))
        );

        allThemes.add(new FacetTheme("Record details",
                new FacetDTO(OccurrenceIndex.BASIS_OF_RECORD, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.TYPE_STATUS, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.MULTIMEDIA, "count", null, null, null),
                new FacetDTO(OccurrenceIndex.COLLECTOR, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.OCCURRENCE_STATUS, "index", null, null, null))
        );

        allThemes.add(new FacetTheme("Attribution",
                new FacetDTO(OccurrenceIndex.ALA_USER_ID, "count", null, null, null),
                new FacetDTO(OccurrenceIndex.DATA_PROVIDER_UID, "count", null, null, null),
                new FacetDTO(OccurrenceIndex.DATA_RESOURCE_UID, "count", null, null, null),
                new FacetDTO(OccurrenceIndex.INSTITUTION_UID, "count", null, null, null),
                new FacetDTO(OccurrenceIndex.COLLECTION_UID, "count", null, null, null),
                new FacetDTO(OccurrenceIndex.PROVENANCE, "count", null, null, null))
        );

        allThemes.add(new FacetTheme("Record assertions",
                new FacetDTO(OccurrenceIndex.ASSERTIONS, "count", null, null, null),
                new FacetDTO(OccurrenceIndex.ASSERTION_USER_ID, "index", null, null, null),
                new FacetDTO(OccurrenceIndex.OUTLIER_LAYER, "count", null, null, null),
                new FacetDTO(OccurrenceIndex.OUTLIER_LAYER_COUNT, "count", null, null, null),
                new FacetDTO(OccurrenceIndex.TAXONOMIC_ISSUE, "count", null, null, null),
                new FacetDTO(OccurrenceIndex.DUPLICATE_STATUS, "count", null, null, null)
        ));

        initAllFacets();
    }
}
