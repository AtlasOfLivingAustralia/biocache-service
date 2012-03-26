package org.ala.biocache.dto;

import org.apache.commons.lang.ArrayUtils;

public class FacetThemes {
    public static String[] allFacets = new String[]{};
    public static java.util.List<FacetTheme> allThemes = new java.util.ArrayList<FacetTheme>();
    static {
        allThemes.add(new FacetTheme("Taxonomic", "taxon_name", "raw_taxon_name", "subspecies_name", "species_guid", "genus_guid","family","order","class", "phylum", "kingdom","species_group","rank","interaction"));
        allThemes.add(new FacetTheme("Geospatial","uncertainty","sensitive","state_conservation","raw_state_conservation","places","state","country","biogeographic_region","ibra","imcra","cl617","cl620","geospatial_kosher"));
        allThemes.add(new FacetTheme("Temporal","month","year","decade"));
        allThemes.add(new FacetTheme("Record Details", "basis_of_record","type_status","multimedia","collector"));
        allThemes.add(new FacetTheme("Attribution", "data_provider_uid","data_resource_uid","institution_uid","collection_uid","provenance"));
        allThemes.add(new FacetTheme("Record Assertions", "assertions", "outlier_layer", "outlier_layer_count"));
        for (FacetTheme theme : allThemes) {
            allFacets = (String[])ArrayUtils.addAll(allFacets, theme.facets);
        }
    }
       
 
    static class FacetTheme{
        private String title;
        private String[] facets;
        FacetTheme(String title, String... facets){
            this.title = title;
            this.facets = facets;
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
        public String[] getFacets() {
            return facets;
        }
        /**
         * @param facets the facets to set
         */
        public void setFacets(String[] facets) {
            this.facets = facets;
        }
    }
}
