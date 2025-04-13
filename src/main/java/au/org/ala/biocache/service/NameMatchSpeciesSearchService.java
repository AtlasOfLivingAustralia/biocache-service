package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.SpeciesCountDTO;
import au.org.ala.biocache.dto.SpeciesImageDTO;
import au.org.ala.biocache.util.OccurrenceUtils;
import au.org.ala.names.ws.client.ALANameUsageMatchServiceClient;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.util.ClientUtils;

import javax.inject.Inject;
import java.util.*;

/**
 * Index based lookup index serice
 */
public class NameMatchSpeciesSearchService implements SpeciesSearchService {
    /** Logger initialisation */
    private final static Logger logger = Logger.getLogger(NameMatchSpeciesSearchService.class);

    @Inject
    private ALANameUsageMatchServiceClient nameUsageMatchService = null;

    @Inject
    protected SpeciesCountsService speciesCountsService;

    @Inject
    protected LayersService layersService;

    @Inject
    protected SpeciesImageService speciesImageService;
    @Inject
    protected ImageMetadataService imageMetadataService;

    @Inject
    protected OccurrenceUtils occurrenceUtils;

    @Override
    public Map search(String query, String[] filterQuery, int max, boolean includeSynonyms, boolean includeAll, boolean includeCounts) {

        // TODO: better method of dealing with records with 0 occurrences being removed.
        int maxFind = includeAll ? max : max + 1000;

        List<Map> results = nameUsageMatchService.autocomplete(ClientUtils.escapeQueryChars(query), maxFind, includeSynonyms);

        List<Map> output = new ArrayList();

        SpeciesCountDTO countlist = includeCounts ? speciesCountsService.getCounts(filterQuery) : null;

        //sort by rank, then score, then name
        Collections.sort(results, new Comparator<Map>() {
            @Override
            public int compare(Map o1, Map o2) {
                //exact match is above everything, hopefully
                int sort = Double.valueOf(
                            (
                                (Double) o2.get("score") * (10000 - (Integer) o2.get("rankId"))
                            )
                        ).compareTo(
                                (Double) o1.get("score") * (10000 - (Integer) o1.get("rankId")
                        )
                );

                if (sort == 0)
                    return ((String) o1.get("name")).compareTo((String) o2.get("name"));
                else
                    return sort;
            }
        });

        int resultsTotal = 0;

        //add counter and filter output
        for (int i = 0; i < results.size(); i++) {
            Map nsr = results.get(i);
            try {
                long count = includeCounts ? speciesCountsService.getCount(countlist, Long.parseLong(nsr.get("left").toString()), Long.parseLong(nsr.get("right").toString())) : 0;

                if (speciesCountsService.isEnabled() && includeCounts) {
                    if(count > 0 || includeAll) {
                        if (includeCounts) {
                            nsr.put("count", count);
                            nsr.put("distributionsCount", layersService.getDistributionsCount(nsr.get("lsid").toString()));
                            nsr.put("checklistsCount", layersService.getChecklistsCount(nsr.get("lsid").toString()));
                            nsr.put("tracksCount", layersService.getTracksCount(nsr.get("lsid").toString()));
                        }
                        nsr.put("images", speciesImageService.get(Long.parseLong((String) nsr.get("left")), Long.parseLong((String) nsr.get("right"))));

                        if (output.size() <= max) {
                            output.add(nsr);
                        }

                        resultsTotal += 1;
                    }
                } else {
                    resultsTotal += 1;
                    output.add(nsr);
                }

            } catch (Exception e) {
                logger.error("Error thrown in autocomplete: " + e.getMessage(), e);
            }
        }

        //format output like BIE ws/search.json
        List<Map> formatted = new ArrayList();
        for (Map m : output) {
            formatted.add(format(m, query));
        }
        Map wrapper = new HashMap();
        wrapper.put("pageSize", max);
        wrapper.put("startIndex", 0);
        wrapper.put("totalRecords", resultsTotal);
        wrapper.put("sort", "score");
        wrapper.put("dir", "desc");
        wrapper.put("status", "OK");
        wrapper.put("query", query);
        wrapper.put("results", formatted);
        Map searchResults = new HashMap();
        searchResults.put("searchResults", wrapper);
        return searchResults;
    }

    /**
     * some formatting to better match autocomplete to bie
     *
     * @param m
     * @param searchTerm
     * @return
     */
    private Map format(Map m, String searchTerm) {
        Map formatted = new HashMap();

        String guid = (String) m.get("lsid");
        formatted.put("guid", guid);

        //all results resolve to accepted lsids
        formatted.put("linkIdentifier", guid);

        formatted.put("name", m.get("name"));
        formatted.put("idxType", "TAXON");
        formatted.put("score", m.get("score"));
        formatted.put("left", m.get("left"));
        formatted.put("right", m.get("right"));

        Map cl = (Map) m.get("cl");
        String parentUid = null;
        if (cl != null) {
            //TODO: get parent from names index
            if (guid.equals(cl.get("sid"))) parentUid = (String) cl.get("gid");
            if (guid.equals(cl.get("gid"))) parentUid = (String) cl.get("fid");
            if (guid.equals(cl.get("fid"))) parentUid = (String) cl.get("oid");
            if (guid.equals(cl.get("oid"))) parentUid = (String) cl.get("cid");
            if (guid.equals(cl.get("cid"))) parentUid = (String) cl.get("pid");
            if (guid.equals(cl.get("pid"))) parentUid = (String) cl.get("kid");
            formatted.put("parentGuid", parentUid);

            formatted.put("kingdom", cl.get("kingdom"));
            formatted.put("phylum", cl.get("phylum"));
            formatted.put("classs", cl.get("klass"));
            formatted.put("order", cl.get("order"));
            formatted.put("family", cl.get("family"));
            formatted.put("genus", cl.get("genus"));
            formatted.put("author", cl.get("authorship"));
        }

        //TODO: ?
        formatted.put("hasChildren", false);

        String rank = (String) m.get("rank");
        formatted.put("rank", (rank != null ? rank : "").toLowerCase());
        formatted.put("rankId", m.get("rankId"));
        formatted.put("rawRank", m.get("rank"));

        //TODO: get from names index
        formatted.put("isAustralian", "recorded");

        //from match type
        boolean scientificNameMatch = "scientificName".equals(m.get("match"));
        String highlight = scientificNameMatch ? (String) m.get("name") : (String) m.get("commonname");
        if (highlight == null) {
            for (Map syn : ((List<Map>) m.get("synonymMatch"))) {
                scientificNameMatch = "scientificName".equals(syn.get("match"));
                highlight = scientificNameMatch ? (String) syn.get("name") : (String) syn.get("commonname");
                if (highlight != null) {
                    break;
                }
            }
        }
        if (highlight != null) {
            int pos = highlight.toLowerCase().indexOf(searchTerm.toLowerCase());
            if (pos >= 0) {
                highlight = highlight.substring(0, pos) +
                        "<strong>" + highlight.substring(pos, pos + searchTerm.length()) + "</strong>" +
                        highlight.substring(pos + searchTerm.length(), highlight.length());
            }
        }
        formatted.put("highlight", highlight);

        if (m.get("commonname") == null) {
            Set<String> commonNames = nameUsageMatchService.getCommonNamesForLSID((String) m.get("lsid"), 1000);
            if (!commonNames.isEmpty()) {
                m.put("commonname", commonNames.iterator().next());
                m.put("commonnames", commonNames);
            }
        }
        if (m.get("commonname") != null) {
            formatted.put("commonName", m.get("commonnames"));
            formatted.put("commonNameSingle", m.get("commonname"));
        }

        formatted.put("nameComplete", m.get("name"));

        formatted.put("occCount", m.get("count"));
        formatted.put("distributionsCount", m.get("distributionsCount"));
        formatted.put("checklistsCount", m.get("checklistsCount"));
        formatted.put("tracksCount", m.get("tracksCount"));

        SpeciesImageDTO speciesImage = (SpeciesImageDTO) m.get("images");

        if (speciesImage != null && speciesImage.getImage() != null) {
            try {
                Map im = occurrenceUtils.getImageFormats(speciesImage.getImage());
                formatted.put("imageSource", speciesImage.getDataResourceUid());
                //number of occurrences with images
                formatted.put("imageCount", speciesImage.getCount());
                formatted.put("image", im.get("raw"));
                formatted.put("thumbnail", im.get("thumbnail"));
                formatted.put("imageUrl", im.get("raw"));
                formatted.put("smallImageUrl", im.get("small"));
                formatted.put("largeImageUrl", im.get("large"));
                formatted.put("thumbnailUrl", im.get("thumbnail"));

                formatted.put("imageMetadataUrl", imageMetadataService.getUrlFor(speciesImage.getImage()));
            } catch (Exception ex) {
                logger.warn("Unable to get image formats for " + speciesImage.getImage() + ": " + ex.getMessage());
                formatted.put("imageCount", 0);
            }
        } else {
            formatted.put("imageCount", 0);
        }

        //TODO: ?
        formatted.put("isExcluded", false);
        return formatted;
    }
}
