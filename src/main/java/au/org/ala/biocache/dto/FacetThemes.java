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
                new FacetDTO("taxon_name","index",null,null,null),
                new FacetDTO("raw_taxon_name","index",null,null,null),
                new FacetDTO("common_name","index",null,null,null),
                new FacetDTO("subspecies_name","index",null,null,null),
                new FacetDTO("species","index",null,null,null),
                new FacetDTO("genus","index",null,null,null),
                new FacetDTO("family","index",null,null,null),
                new FacetDTO("order","index",null,null,null),
                new FacetDTO("class","index",null,null,null),
                new FacetDTO("phylum","index",null,null,null),
                new FacetDTO("kingdom","index",null,null,null),
                new FacetDTO("species_group","index",null,null,null),
                new FacetDTO("rank","count",null,null,null),
                new FacetDTO("interaction","count",null,null,null),
                new FacetDTO("species_habitats","count",null,null,null)));

        allThemes.add(new FacetTheme("Geospatial",
                new FacetDTO("uncertainty","index",null,null,null),
                new FacetDTO("sensitive","count",null,null,null),
                new FacetDTO("state_conservation","count",null,null,null),
                new FacetDTO("raw_state_conservation","count",null,null,null),
                new FacetDTO("cl966","count",null,null,null),
                new FacetDTO("cl959","count",null,null,null),
                new FacetDTO("state","count",null,null,null),
                new FacetDTO("country","index",null,null,null),
                new FacetDTO("biogeographic_region","count",null,null,null),
                new FacetDTO("ibra","count",null,null,null),
                new FacetDTO("imcra", "count",null,null,null),
                new FacetDTO("cl1918","count",null,null,null),
                new FacetDTO("cl617", "count",null,null,null),
                new FacetDTO("cl620","count",null,null,null),
                new FacetDTO("geospatial_kosher","count",null,null,null)
        ));

        allThemes.add(new FacetTheme("Temporal",
                new FacetDTO("month","index",null,null,null),
                new FacetDTO("year","index",null,null,null),
                new FacetDTO("decade","index",null,null,null))
        );

        allThemes.add(new FacetTheme("Record details",
                new FacetDTO("basis_of_record","index",null,null,null),
                new FacetDTO("type_status","index",null,null,null),
                new FacetDTO("multimedia","count",null,null,null),
                new FacetDTO("collector","index",null,null,null),
                new FacetDTO("occurrence_status","index",null,null,null))
        );

        allThemes.add(new FacetTheme("Attribution",
                new FacetDTO("alau_user_id","count",null,null,null),
                new FacetDTO("data_provider_uid","count",null,null,null),
                new FacetDTO("data_resource_uid","count",null,null,null),
                new FacetDTO("institution_uid","count",null,null,null),
                new FacetDTO("collection_uid", "count",null,null,null),
                new FacetDTO("provenance", "count",null,null,null))
        );

        allThemes.add(new FacetTheme("Record assertions",
                new FacetDTO("assertions","count",null,null,null),
                new FacetDTO("assertion_user_id","index",null,null,null),
                new FacetDTO("outlier_layer","count",null,null,null),
                new FacetDTO("outlier_layer_count","count",null,null,null),
                new FacetDTO("taxonomic_issue","count",null,null,null),
                new FacetDTO("duplicate_status","count",null,null,null)
        ));

        initAllFacets();
    }
}
