package au.org.ala.biocache.stream;

import au.org.ala.biocache.dao.SearchDAOImpl;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.io.Tuple;
import org.springframework.context.support.AbstractMessageSource;

import java.util.Map;

import static au.org.ala.biocache.dao.SearchDAOImpl.DECADE_PRE_1850_LABEL;

public class ExploreFacet implements ProcessInterface {

    private final static Logger logger = Logger.getLogger(ExploreFacet.class);

    Map<String, Long> occurrenceCounts;
    SpatialSearchRequestParams requestParams;
    SearchDAOImpl searchDao;
    AbstractMessageSource messageSource;

    String facetName;

    public ExploreFacet(Map<String, Long> occurrenceCounts, SpatialSearchRequestParams requestParams, SearchDAOImpl searchDao) {
        this.requestParams = requestParams;
        this.occurrenceCounts = occurrenceCounts;
        this.searchDao = searchDao;
        this.messageSource = searchDao.messageSource;

        facetName = requestParams.getFacets()[0];
    }

    public boolean process(Tuple tuple) {
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
                        occurrenceCounts.put(label, entryCount);
                    } else {
                        if (countEntryName.equals(DECADE_PRE_1850_LABEL)) {
//                            occurrenceCounts.put(searchDao.getFacetValueDisplayName(facetName, countEntryName), entryCount);
                        } else {
//                            occurrenceCounts.put(searchDao.getFacetValueDisplayName(facetName, countEntryName), entryCount);
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
        return true;
    }
}