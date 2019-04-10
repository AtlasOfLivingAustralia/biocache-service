package au.org.ala.biocache.dto;

import au.org.ala.biocache.web.WMSUtils;

/**
 * This is a merge of the various query parameters used in WMSController. The resulting compatible
 * SpatialSearchRequestParams.q is retrieved with getFinalQuery()
 */
public class WmsQueryParams {
    /**
     * A valid SearchRequestParams.q
     */
    String layer;
    /**
     * A valid SearchRequestParams.q
     */
    String q;
    /**
     * When not "ALA:Occurrences", layers is transformed using WMSUtils.convertLayersParamToQ()
     */
    String query_layers;
    /**
     * When not "ALA:Occurrences", layers is transformed using WMSUtils.convertLayersParamToQ()
     */
    String layers;
    /**
     * A valid SearchRequestParams.q or a String matching 'qid:[0-9]{13}'
     */
    String cqlFilter;

    public String getCqlFilter() {
        return cqlFilter;
    }

    public void setCqlFilter(String cqlFilter) {
        this.cqlFilter = cqlFilter;
    }

    public String getLayers() {
        return layers;
    }

    public void setLayers(String layers) {
        this.layers = layers;
    }

    public String getQuery_layers() {
        return query_layers;
    }

    public void setQuery_layers(String query_layers) {
        this.query_layers = query_layers;
    }

    public String getLayer() {
        return layer;
    }

    public void setLayer(String layer) {
        this.layer = layer;
    }

    public String getQ() {
        return q;
    }

    public void setQ(String q) {
        this.q = q;
    }

    public String getFinalQuery() {
        String query = null;

        if (cqlFilter != null) {
            query = WMSUtils.getQ(cqlFilter);
        } else if (layer != null) {
            query = layer;
        } else if (layers != null && !"ALA:Occurrences".equalsIgnoreCase(layers)) {
            query = WMSUtils.convertLayersParamToQ(layers);
        } else if (query_layers != null && !"ALA:Occurrences".equalsIgnoreCase(query_layers)) {
            query = WMSUtils.convertLayersParamToQ(query_layers);
        } else if (q != null) {
            query = q;
        }

        return query;
    }
}

