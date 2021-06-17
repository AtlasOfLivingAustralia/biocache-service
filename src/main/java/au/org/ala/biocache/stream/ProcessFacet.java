package au.org.ala.biocache.stream;

import au.org.ala.biocache.dao.SearchDAOImpl;
import au.org.ala.biocache.dto.FacetResultDTO;
import au.org.ala.biocache.dto.FieldResultDTO;
import au.org.ala.biocache.dto.SearchResultDTO;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.io.Tuple;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static au.org.ala.biocache.dao.SearchDAOImpl.DECADE_PRE_1850_LABEL;

// Limitations:
// - Cannot process facetrange or facetquery.
// - Does not include a facetcount.
//
// Suitable for:
// - Facet cexpr on a single field.
// - Instances where flimit exceeds the number of facets.
public class ProcessFacet implements ProcessInterface {

    private final static Logger logger = Logger.getLogger(ProcessFacet.class);

    SearchResultDTO searchResult;
    SpatialSearchRequestParams requestParams;
    SearchDAOImpl searchDao;
    AbstractMessageSource messageSource;

    FacetResultDTO currentFacet;
    Queue<String> facetNames;
    String facetName = null;
    private List<FieldResultDTO> currentFieldList;

    public ProcessFacet(SearchResultDTO searchResult, SpatialSearchRequestParams requestParams,
                        SearchDAOImpl searchDao) {
        this.requestParams = requestParams;
        this.searchResult = searchResult;
        this.searchDao = searchDao;
        this.messageSource = searchDao.messageSource;

        this.facetNames = new LinkedList(CollectionUtils.arrayToList(requestParams.getFacets()));
        this.searchResult.setFacetResults(new ArrayList());
    }

    public boolean process(Tuple tuple) {
        initField();

        try {
            if (tuple != null && tuple.fieldNames.size() > 0) {
                long entryCount = tuple.getLong("count(*)");

                if (entryCount > 0) {
                    String countEntryName = tuple.getString("name");

                    if (StringUtils.isEmpty(countEntryName)) {
                        String label = "";
                        if (messageSource != null) {
                            label = messageSource.getMessage(facetName + ".novalue", null, "Not supplied", null);
                        }
                        currentFieldList.add(new FieldResultDTO(label, facetName + ".novalue", entryCount, "-" + facetName + ":*"));
                    } else {
                        if (countEntryName.equals(DECADE_PRE_1850_LABEL)) {
//                            currentFieldList.add(0, new FieldResultDTO(
//                                    searchDao.getFacetValueDisplayName(facetName, countEntryName),
//                                    facetName + "." + countEntryName,
//                                    entryCount,
//                                    searchDao.getFormattedFqQuery(facetName, countEntryName)
//                            ));
                        } else {
//                            currentFieldList.add(new FieldResultDTO(
//                                    searchDao.getFacetValueDisplayName(facetName, countEntryName),
//                                    facetName + "." + countEntryName,
//                                    entryCount,
//                                    searchDao.getFormattedFqQuery(facetName, countEntryName)
//                            ));
                        }
                    }

                }
            }

            return true;
        } catch (IllegalArgumentException e) {
            logger.warn("Failed to convert tuple to OccurrenceIndex: " + e.getMessage());
            return false;
        }
    }

    public boolean flush() {
        // indicate that the current facet is finished
        facetName = null;

        return true;
    }

    private void initField() {
        if (facetName == null) {
            // get next facet name
            facetName = facetNames.poll();

            currentFacet = new FacetResultDTO(facetName, new ArrayList());
            currentFieldList = currentFacet.getFieldResult();

            searchResult.getFacetResults().add(currentFacet);
        }
    }
}