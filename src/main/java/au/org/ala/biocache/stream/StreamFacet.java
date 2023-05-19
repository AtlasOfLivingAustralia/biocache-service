package au.org.ala.biocache.stream;

import au.org.ala.biocache.dao.SearchDAOImpl;
import au.org.ala.biocache.dto.DownloadDetailsDTO;
import au.org.ala.biocache.dto.OccurrenceIndex;
import au.org.ala.biocache.dto.SpatialSearchRequestDTO;
import au.org.ala.biocache.writer.CSVRecordWriter;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.io.Tuple;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class StreamFacet implements ProcessInterface {

    private final static Logger logger = Logger.getLogger(StreamFacet.class);

    SearchDAOImpl searchDAO;
    DownloadDetailsDTO downloadDetails;
    SpatialSearchRequestDTO request;
    boolean includeCount;
    boolean includeSynonyms;
    boolean includeLists;
    OutputStream out;

    CSVRecordWriter writer;
    boolean shouldLookupAttribution;
    List<String> guids = null;
    List<Long> counts = null;

    long missingCount;

    public StreamFacet(SearchDAOImpl searchDAO, DownloadDetailsDTO downloadDetails, SpatialSearchRequestDTO request,
                       boolean lookupName, boolean includeCount, boolean includeSynonyms, boolean includeLists,
                       long missingCount, OutputStream out) {
        this.out = out;
        this.downloadDetails = downloadDetails;
        this.request = request;
        this.searchDAO = searchDAO;
        this.includeCount = includeCount;
        this.includeSynonyms = includeSynonyms;
        this.includeLists = includeLists;
        this.missingCount = missingCount;

        String facetName = request.getFacets()[0];

        // shouldLookup is valid for 1.0 and 2.0 SOLR schema
        boolean isGuid = request.getFacets()[0].contains("_guid") ||
                request.getFacets()[0].endsWith("ID");
        boolean isLsid = request.getFacets()[0].contains("_lsid") || request.getFacets()[0].contains(OccurrenceIndex.TAXON_CONCEPT_ID);
        boolean shouldLookupTaxon = lookupName && (isLsid || isGuid);
        boolean isUid = request.getFacets()[0].contains("_uid") || request.getFacets()[0].endsWith("Uid");
        this.shouldLookupAttribution = lookupName && isUid;

        String[] header = new String[]{facetName};
        if (shouldLookupTaxon) {
            header = searchDAO.speciesLookupService.getHeaderDetails(searchDAO.fieldMappingUtil.translateFieldName(facetName), includeCount, includeSynonyms);
        } else if (shouldLookupAttribution) {
            header = (String[]) ArrayUtils.addAll(header, new String[]{"name", "count"});
        } else if (includeCount) {
            header = (String[]) ArrayUtils.add(header, "count");
        }
        if (includeLists) {
            header = (String[]) ArrayUtils.addAll(header, searchDAO.listsService.getTypes().toArray(new String[]{}));
        }

        this.writer = new CSVRecordWriter(new CloseShieldOutputStream(out), header);
        this.writer.initialise();

        if (shouldLookupTaxon) {
            guids = new ArrayList<String>();
            counts = new ArrayList<Long>();
        }
    }

    public boolean process(Tuple tuple) {
        String name = null;
        Long count = 0L;
        for (Object value : tuple.getMap().values()) {
            if (value instanceof String) {
                name = (String) value;
            } else {
                count = (Long) value;
            }
        }

        try {
            //process the "species_guid_ facet by looking up the list of guids
            if (guids != null) {
                guids.add(name);
                if (includeCount) {
                    counts.add(count);
                }

                //Only want to send a sub set of the list so that the URI is not too long for BIE
                if (guids.size() == 30) {
                   try {
                       searchDAO.writeTaxonDetailsToStream(guids, counts, includeCount, includeSynonyms, includeLists, writer);
                   } catch (Exception e) {
                       logger.error("failed to write taxon details to stream: " + e.getMessage(), e);
                   }
                    guids.clear();
                    counts.clear();
                }
            } else {
                if (shouldLookupAttribution) {
                    writer.write(includeCount ? new String[]{name,
                            searchDAO.collectionCache.getNameForCode(name), Long.toString(count)} : new String[]{name});
                } else {
                    writer.write(includeCount ? new String[]{name, Long.toString(count)} : new String[]{name});
                }
            }

            if (downloadDetails != null) {
                downloadDetails.updateCounts(1);
            }
        } catch (Exception e) {
            logger.error(e);
        }

        return true;
    }

    public boolean flush() {
        // Finish guids batch and add missingCount
        if (guids != null) {
            if (missingCount > 0) {
                guids.add("");
                counts.add(missingCount);
            }
            try {
                //now write any guids that remain at the end of the looping
                searchDAO.writeTaxonDetailsToStream(guids, counts, includeCount, includeSynonyms, includeLists, writer);
            } catch (Exception e) {
                logger.error(e);
            }
        } else if (missingCount > 0) {
            writer.write(includeCount ? new String[]{"", Long.toString(missingCount)} : new String[]{""});
        }
        writer.finalise();

        return true;
    }
}
