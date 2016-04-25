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
import java.util.*;

/**
 * This class provides thematic grouping of facets to aid rendering in UIs.
 *
 * @author Natasha Carter
 */
public class FacetThemes {
	
    public static String[] allFacets = new String[]{};
    public static String[] allFacetsLimited = new String[]{};
    public static java.util.List<FacetTheme> allThemes = new java.util.ArrayList<FacetTheme>();
    public static LinkedHashMap<String, Facet> facetsMap = new LinkedHashMap<String, Facet>();
    
    public static Integer facetsMax = 4;
    public static Integer facetsDefaultMax = 0;
    public static Boolean facetDefault = true;
    
    /**
     * Takes a file path to a configuration file in JSON and parses the file
     * into facets and facet themes.
     *
     * @param configFilePath
     * @throws IOException
     */
    public FacetThemes(String configFilePath, Set<IndexFieldDTO> indexedFields, int facetsMax, int facetsDefaultMax, boolean facetDefault) throws IOException {
        this.facetsMax = facetsMax;
        this.facetsDefaultMax = facetsDefaultMax;
        this.facetDefault = facetDefault;
        
        if(configFilePath != null && new File(configFilePath).exists()){
            allThemes.clear();
            ObjectMapper om = new ObjectMapper();
            List<Map<String,Object>> config = om.readValue(new File(configFilePath), List.class);
            for(Map<String, Object> facetGroup : config){
                String title = (String) facetGroup.get("title");
                List<Map<String,String>> facetsConfig = (List<Map<String,String>>) facetGroup.get("facets");
                List<Facet> facets = new ArrayList<Facet>();
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
                                facets.add(new Facet(name, facetsMap.get("sort"), description, dwcTerm, i18nValues));
                                break;
                            }
                        }
                    }
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
            allFacetsLimited = allFacets != null && allFacets.length > facetsDefaultMax ? Arrays.copyOfRange(allFacets, 0, facetsDefaultMax) : allFacets;
        }
    }

    public FacetThemes() {
        defaultInit();
    }

    private void defaultInit() {
        allThemes.clear();
        allThemes.add(new FacetTheme("Taxonomic",
                new Facet("taxon_name","index",null,null,null),
                new Facet("raw_taxon_name","index",null,null,null),
                new Facet("common_name","index",null,null,null),
                new Facet("subspecies_name","index",null,null,null),
                new Facet("species","index",null,null,null),
                new Facet("genus","index",null,null,null),
                new Facet("family","index",null,null,null),
                new Facet("order","index",null,null,null),
                new Facet("class","index",null,null,null),
                new Facet("phylum","index",null,null,null),
                new Facet("kingdom","index",null,null,null),
                new Facet("species_group","index",null,null,null),
                new Facet("rank","count",null,null,null),
                new Facet("interaction","count",null,null,null),
                new Facet("species_habitats","count",null,null,null)));

        allThemes.add(new FacetTheme("Geospatial",
                new Facet("uncertainty","index",null,null,null),
                new Facet("sensitive","count",null,null,null),
                new Facet("state_conservation","count",null,null,null),
                new Facet("raw_state_conservation","count",null,null,null),
                new Facet("cl966","count",null,null,null),
                new Facet("cl959","count",null,null,null),
                new Facet("state","count",null,null,null),
                new Facet("country","index",null,null,null),
                new Facet("biogeographic_region","count",null,null,null),
                new Facet("ibra","count",null,null,null),
                new Facet("imcra", "count",null,null,null),
                new Facet("cl1918","count",null,null,null),
                new Facet("cl617", "count",null,null,null),
                new Facet("cl620","count",null,null,null),
                new Facet("geospatial_kosher","count",null,null,null)
        ));

        allThemes.add(new FacetTheme("Temporal",
                new Facet("month","index",null,null,null),
                new Facet("year","index",null,null,null),
                new Facet("decade","index",null,null,null))
        );

        allThemes.add(new FacetTheme("Record details",
                new Facet("basis_of_record","index",null,null,null),
                new Facet("type_status","index",null,null,null),
                new Facet("multimedia","count",null,null,null),
                new Facet("collector","index",null,null,null),
                new Facet("occurrence_status_s","index",null,null,null))
        );

        allThemes.add(new FacetTheme("Attribution",
                new Facet("alau_user_id","count",null,null,null),
                new Facet("data_provider_uid","count",null,null,null),
                new Facet("data_resource_uid","count",null,null,null),
                new Facet("institution_uid","count",null,null,null),
                new Facet("collection_uid", "count",null,null,null),
                new Facet("provenance", "count",null,null,null))
        );

        allThemes.add(new FacetTheme("Record assertions",
                new Facet("assertions","count",null,null,null),
                new Facet("assertion_user_id","index",null,null,null),
                new Facet("outlier_layer","count",null,null,null),
                new Facet("outlier_layer_count","count",null,null,null),
                new Facet("taxonomic_issue","count",null,null,null),
                new Facet("duplicate_status","count",null,null,null)
        ));

        initAllFacets();
    }

    static public class Facet {

        private String field;
        private String sort;
        private String description;
        private String dwcTerm;
        private Boolean i18nValues;

        Facet(String title, String sort, String description, String dwcTerm, Boolean i18nValues){
            this.field = title;
            this.sort = sort;
            this.description = description;
            this.dwcTerm = dwcTerm;
            this.i18nValues = i18nValues;
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
        /**
         * @param description the description to set
         */
        public void setDescription(String description) {
            this.description = description;
        }
        /**
         * @return the description
         */
        public String getDescription() {
            return description;
        }
        /**
         * @param dwcTerm the dwcTerm to set
         */
        public void setDwcTerm(String dwcTerm) {
            this.dwcTerm = dwcTerm;
        }
        /**
         * @return the dwcTerm
         */
        public String getDwcTerm() {
            return dwcTerm;
        }
        /**
         * @param i18nValues the i18nValues to set
         */
        public void setI18nValues(Boolean i18nValues) {
            this.i18nValues = i18nValues;
        }
        /**
         * @return the i18nValues
         */
        public Boolean isI18nValues() {
            return i18nValues;
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