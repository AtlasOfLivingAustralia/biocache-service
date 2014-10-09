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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This class provides thematic grouping of facets to aid rendering in UIs.
 *
 * @author Natasha Carter
 */
public class FacetThemes {
	
    public static String[] allFacets = new String[]{};
    public static java.util.List<FacetTheme> allThemes = new java.util.ArrayList<FacetTheme>();
    public static LinkedHashMap<String, Facet> facetsMap = new LinkedHashMap<String, Facet>();

    /**
     * Takes a file path to a configuration file in JSON and parses the file
     * into facets and facet themes.
     *
     * @param configFilePath
     * @throws IOException
     */
    public FacetThemes(String configFilePath) throws IOException {

        if(configFilePath != null && new File(configFilePath).exists()){
            allThemes.clear();
            ObjectMapper om = new ObjectMapper();
            List<Map<String,Object>> config = om.readValue(new File(configFilePath), List.class);
            for(Map<String, Object> facetGroup : config){
                String title = (String) facetGroup.get("title");
                List<Map<String,String>> facetsConfig = (List<Map<String,String>>) facetGroup.get("facets");
                List<Facet> facets = new ArrayList<Facet>();
                for(Map<String,String> facetsMap : facetsConfig){
                    facets.add(new Facet(facetsMap.get("field"), facetsMap.get("sort")));
                }
                allThemes.add(new FacetTheme(title, facets));
            }
            initAllFacets();
        } else {
            defaultInit();
        }
    }

    private void initAllFacets() {
        facetsMap.clear();
        for (FacetTheme theme : allThemes) {
            for(Facet f : theme.facets) {
                facetsMap.put(f.field, f);
            }
            allFacets = facetsMap.keySet().toArray(new String[]{});
        }
    }

    public FacetThemes() {
        defaultInit();
    }

    private void defaultInit() {
        allThemes.clear();
        allThemes.add(new FacetTheme("Taxonomic",
                new Facet("taxon_name","index"),
                new Facet("raw_taxon_name","index"),
                new Facet("common_name","index"),
                new Facet("subspecies_name","index"),
                new Facet("species","index"),
                new Facet("genus","index"),
                new Facet("family","index"),
                new Facet("order","index"),
                new Facet("class","index"),
                new Facet("phylum","index"),
                new Facet("kingdom","index"),
                new Facet("species_group","index"),
                new Facet("rank","count"),
                new Facet("interaction","count"),
                new Facet("species_habitats","count")));

        allThemes.add(new FacetTheme("Geospatial",
                new Facet("uncertainty","index"),
                new Facet("sensitive","count"),
                new Facet("state_conservation","count"),
                new Facet("raw_state_conservation","count"),
                new Facet("cl966","count"),
                new Facet("cl959","count"),
                new Facet("state","count"),
                new Facet("country","index"),
                new Facet("biogeographic_region","count"),
                new Facet("ibra","count"),
                new Facet("imcra", "count"),
                new Facet("cl1918","count"),
                new Facet("cl617", "count"),
                new Facet("cl620","count"),
                new Facet("geospatial_kosher","count")
        ));

        allThemes.add(new FacetTheme("Temporal",
                new Facet("month","index"),
                new Facet("year","index"),
                new Facet("decade","index"))
        );

        allThemes.add(new FacetTheme("Record details",
                new Facet("basis_of_record","index"),
                new Facet("type_status","index"),
                new Facet("multimedia","count"),
                new Facet("collector","index"),
                new Facet("occurrence_status_s","index"))
        );

        allThemes.add(new FacetTheme("Attribution",
                new Facet("alau_user_id","count"),
                new Facet("data_provider_uid","count"),
                new Facet("data_resource_uid","count"),
                new Facet("institution_uid","count"),
                new Facet("collection_uid", "count"),
                new Facet("provenance", "count"))
        );

        allThemes.add(new FacetTheme("Record assertions",
                new Facet("assertions","count"),
                new Facet("assertion_user_id","index"),
                new Facet("outlier_layer","count"),
                new Facet("outlier_layer_count","count"),
                new Facet("taxonomic_issue","count"),
                new Facet("duplicate_status","count")
        ));

        initAllFacets();
    }

    static public class Facet {

        private String field;
        private String sort;

        Facet(String title, String sort){
            this.field = title;
            this.sort = sort;
        }

        /**
         * @return the title
         */
        public String getField() {
            return field;
        }
        /**
         * @param field the field to set
         */
        public void setField(String field) {
            this.field = field;
        }
        /**
         * @return the defaultSort
         */
        public String getSort() {
            return sort;
        }
    }
 
    static class FacetTheme {

        private String title;
        private Facet[] facets;

        FacetTheme(String title, Facet... facets){
            this.title = title;
            this.facets = facets;
        }

        FacetTheme(String title, List<Facet> facets){
            this.title = title;
            this.facets = facets.toArray(new Facet[0]);
        }

        /**
         * @return the title
         */
        public String getTitle() {
            return title;
        }
        /**
         * @param title the title to set
         */
        public void setTitle(String title) {
            this.title = title;
        }
        /**
         * @return the facets
         */
        public Facet[] getFacets() {
            return facets;
        }
        /**
         * @param facets the facets to set
         */
        public void setFacets(Facet[] facets) {
            this.facets = facets;
        }
    }
}