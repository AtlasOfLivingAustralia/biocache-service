package au.org.ala.biocache.dto;

import org.apache.commons.lang.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DownloadHeaders {

    // fields for the request to SOLR
    public String[] originalIncluded = new String[0];

    // post-processing fields may be added to the SOLR request
    public String[] included = new String[0];

    // column header that aligns with the included fields
    public String[] labels = new String[0];

    // analysisIds for inclusion. intersections with these spatial-service fields are done during the download
    public String[] analysisIds = new String[0];

    // analysisId column headers
    public String[] analysisLabels = new String[0];

    // species list and the kvp value to include.
    public String[] speciesListIds = new String[0];

    // species list column headers
    public String[] speciesListLabels = new String[0];

    // qa (assertions) column headers
    public String[] qaLabels = new String[0];

    // qa (assertions) SOLR values
    public String[] qaIds = new String[0];

    // column headers of the miscellanious SOLR field (contains a JSON map). Populated during the download.
    public List<String> miscLabels = new ArrayList();

    public DownloadHeaders(String[] included, String[] labels, String[] analysisLabels, String[] analysisIds, String[] speciesListLabels, String[] speciesListIds) {
        this.originalIncluded = Arrays.stream(included).collect(Collectors.toList()).toArray(new String[0]);
        this.included = included;
        this.labels = labels;
        this.analysisLabels = analysisLabels;
        this.analysisIds = analysisIds;
        this.speciesListLabels = speciesListLabels;
        this.speciesListIds = speciesListIds;
    }

    public String[] joinedHeader() {
        return (String[]) ArrayUtils.addAll(labels, ArrayUtils.addAll(analysisLabels, ArrayUtils.addAll(speciesListLabels, qaLabels)));
    }

    public String[] joinOriginalIncluded() {
        return (String[]) ArrayUtils.addAll(originalIncluded, ArrayUtils.addAll(analysisIds, ArrayUtils.addAll(speciesListLabels, qaIds)));
    }
}
