package au.org.ala.biocache.service;

import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.IndexFieldDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.File;
import java.util.*;

/**
 * A service that supplies facet information.
 *
 * @author Doug Palmer &lt;Doug.Palmer@csiro.au&gt;
 * @copyright Copyright (c) 2016 CSIRO
 */
// Component declararation comes from AppConfig
public class FacetService {
    private static FacetService EMPTY = new FacetService();
    private static FacetService singleton = EMPTY;

    // If we're going to limit the number of facets, then choose important ones first
    private final static List<String> FACET_ORDER = Arrays.asList(
            "taxon_name",
            "common_name",
            "species_group",
            "multimedia",
            "genus",
            "family",
            "basis_of_record",
            "raw_taxon_name",
            "state",
            "biogeographic_region",
            "ibra",
            "imcra",
            "year",
            "decade",
            "taxonomic_issue",
            "duplicate_status",
            "month",
            "state_conservation",
            "sensitive",
            "data_provider_uid",
            "data_resource_uid",
            "institution_uid",
            "collection_uid",
            "collector",
            "interaction",
            "species_habitats",
            "species",
            "subspecies_name",
            "order",
            "class",
            "phylum",
            "kingdom",
            "rank",
            "uncertainty",
            "raw_state_conservation",
            "cl966",
            "cl959",
            "country",
            "cl1918",
            "cl617",
            "cl620",
            "geospatial_kosher",
            "type_status",
            "type_status",
            "occurrence_status_s",
            "alau_user_id",
            "provenance",
            "assertions",
            "assertion_user_id",
            "outlier_layer",
            "outlier_layer_count"
    );

    // Sort by facet order
    private final static Comparator<String> FACET_COMPARATOR = new Comparator<String>() {
        public int compare(String o1, String o2) {
            int i1 = FACET_ORDER.indexOf(o1);
            int i2 = FACET_ORDER.indexOf(o2);

            if (i1 < 0)
                i1 = FACET_ORDER.size() + 1;
            if (i2 < 0)
                i2 = FACET_ORDER.size() + 1;
            if (i1 == i2)
                return o1.compareTo(o2);
            else
                return i1 - i2;
        }
    };

    private final static Logger logger = Logger.getLogger(FacetService.class);

    private Integer facetsMax;
    private Boolean facetDefault;
    private String[] allFacets = null;
    private String[] allFacetsLimited = null;
    private List<FacetTheme> allThemes = null;
    private Map<String, Facet> facetsMap = null;

    /**
     * Get the singleton instance for initialising things that need to know about facets.
     *
     * @return The singleton
     */
    public static FacetService singleton() {
        return singleton;
    }

    public static void createSingleton(String facetConfig, Integer facetsMax, Boolean facetDefault) {
        if (singleton != EMPTY)
            logger.warn("Re-initialising singleton");
        singleton = new FacetService(facetConfig, facetsMax, facetDefault);
    }

    public FacetService() {
        this.facetDefault = false;
        this.facetsMax = 4;
        this.allFacets = new String[0];
        this.allFacetsLimited = new String[0];
    }

    public FacetService(String facetConfig, Integer facetsMax, Boolean facetDefault) {
        this.facetsMax = facetsMax;
        this.facetDefault = facetDefault;
        try {
            File configFile = new File(facetConfig);
            if (configFile.exists())
                this.loadConfig(configFile);
            else
                this.loadDefaults();
        } catch (Exception ex) {
            this.logger.error("Unable to load facet config from " + facetConfig + " ... using defaults", ex);
            this.loadDefaults();
        }
        this.initAllFacets();
    }

    public String[] getAllFacets() {
        return allFacets;
    }

    public String[] getAllFacetsLimited() {
        return allFacetsLimited;
    }

    public List<FacetTheme> getAllThemes() {
        return allThemes;
    }

    public Map<String, Facet> getFacetsMap() {
        return facetsMap;
    }

    public Integer getFacetsMax() {
        return facetsMax;
    }

    public Boolean getFacetDefault() {
        return facetDefault;
    }

    /**
     * Select the facets that we are going to display.
     * <p>
     * "Important" facets, as defined by {@link #FACET_ORDER} come first, so if we overrun the limit we get the good stuff first.
     * </p>
     *
     * @param facets The facet array
     * @param max The maximum number allowed
     * @return
     */
    public String[] selectFacets(String[] facets, int max) {
        if (facets == null || facets.length <= max)
            return facets;
        facets = Arrays.copyOf(facets, facets.length);
        Arrays.sort(facets, FACET_COMPARATOR);
        return Arrays.copyOf(facets, max);
    }

    private void loadConfig(File configFile) throws Exception {
        this.logger.info("Loading facet config file " + configFile);
        ObjectMapper om = new ObjectMapper();
        List<Map<String, Object>> config = om.readValue(configFile, List.class);

        this.allThemes = new ArrayList<FacetTheme>();
        for (Map<String, Object> facetGroup : config) {
            String title = (String) facetGroup.get("title");
            List<Map<String, Object>> facetsConfig = (List<Map<String, Object>>) facetGroup.get("facets");
            List<Facet> facets = new ArrayList<Facet>();
            for (Map<String, Object> facetsMap : facetsConfig) {
                String name = (String) facetsMap.get("field");
                String description = (String) facetsMap.get("description");
                String dwcTerm = (String) facetsMap.get("dwcTerm");
                Boolean i18nValues = (Boolean) facetsMap.get("i18nValues");
                facets.add(new Facet(name, (String) facetsMap.get("sort"), description, dwcTerm, i18nValues));
            }
            this.allThemes.add(new FacetTheme(title, facets));
        }
    }

    private void loadDefaults() {
        this.logger.info("Loading default facets");
        this.allThemes = new ArrayList<FacetTheme>();
        this.allThemes.add(new FacetTheme("Taxonomic",
                new Facet("taxon_name", "index", null, null, null),
                new Facet("raw_taxon_name", "index", null, null, null),
                new Facet("common_name", "index", null, null, null),
                new Facet("subspecies_name", "index", null, null, null),
                new Facet("species", "index", null, null, null),
                new Facet("genus", "index", null, null, null),
                new Facet("family", "index", null, null, null),
                new Facet("order", "index", null, null, null),
                new Facet("class", "index", null, null, null),
                new Facet("phylum", "index", null, null, null),
                new Facet("kingdom", "index", null, null, null),
                new Facet("species_group", "index", null, null, null),
                new Facet("rank", "count", null, null, null),
                new Facet("interaction", "count", null, null, null),
                new Facet("species_habitats", "count", null, null, null)));

        this.allThemes.add(new FacetTheme("Geospatial",
                new Facet("uncertainty", "index", null, null, null),
                new Facet("sensitive", "count", null, null, null),
                new Facet("state_conservation", "count", null, null, null),
                new Facet("raw_state_conservation", "count", null, null, null),
                new Facet("cl966", "count", null, null, null),
                new Facet("cl959", "count", null, null, null),
                new Facet("state", "count", null, null, null),
                new Facet("country", "index", null, null, null),
                new Facet("biogeographic_region", "count", null, null, null),
                new Facet("ibra", "count", null, null, null),
                new Facet("imcra", "count", null, null, null),
                new Facet("cl1918", "count", null, null, null),
                new Facet("cl617", "count", null, null, null),
                new Facet("cl620", "count", null, null, null),
                new Facet("geospatial_kosher", "count", null, null, null)
        ));

        this.allThemes.add(new FacetTheme("Temporal",
                new Facet("month", "index", null, null, null),
                new Facet("year", "index", null, null, null),
                new Facet("decade", "index", null, null, null))
        );

        this.allThemes.add(new FacetTheme("Record details",
                new Facet("basis_of_record", "index", null, null, null),
                new Facet("type_status", "index", null, null, null),
                new Facet("multimedia", "count", null, null, null),
                new Facet("collector", "index", null, null, null),
                new Facet("occurrence_status_s", "index", null, null, null))
        );

        this.allThemes.add(new FacetTheme("Attribution",
                new Facet("alau_user_id", "count", null, null, null),
                new Facet("data_provider_uid", "count", null, null, null),
                new Facet("data_resource_uid", "count", null, null, null),
                new Facet("institution_uid", "count", null, null, null),
                new Facet("collection_uid", "count", null, null, null),
                new Facet("provenance", "count", null, null, null))
        );

        this.allThemes.add(new FacetTheme("Record assertions",
                new Facet("assertions", "count", null, null, null),
                new Facet("assertion_user_id", "index", null, null, null),
                new Facet("outlier_layer", "count", null, null, null),
                new Facet("outlier_layer_count", "count", null, null, null),
                new Facet("taxonomic_issue", "count", null, null, null),
                new Facet("duplicate_status", "count", null, null, null)
        ));
    }

    private void initAllFacets() {
        this.facetsMap = new HashMap<String, Facet>();
        for (FacetTheme theme : this.allThemes) {
            for (Facet f : theme.facets) {
                this.facetsMap.put(f.field, f);
            }
        }
        List<String> keys = new ArrayList<String>(facetsMap.keySet());
        Collections.sort(keys, FACET_COMPARATOR);
        this.allFacets = keys.toArray(new String[0]);
        this.allFacetsLimited = allFacets != null && facetsMax != null && allFacets.length > facetsMax ? Arrays.copyOf(allFacets, facetsMax) : allFacets;
        this.logger.info("All facets = " + Arrays.toString(this.allFacets));
        this.logger.info("All facets limited = " + Arrays.toString(this.allFacetsLimited));
        this.logger.info("Facets max = " + this.facetsMax);
        this.logger.info("Facet default = " + this.facetDefault);
    }


    static public class Facet {

        private String field;
        private String sort;
        private String description;
        private String dwcTerm;
        private Boolean i18nValues;

        Facet(String title, String sort, String description, String dwcTerm, Boolean i18nValues) {
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

        FacetTheme(String title, Facet... facets) {
            this.title = title;
            this.facets = facets;
        }

        FacetTheme(String title, List<Facet> facets) {
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
