package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.SpeciesCountDTO;
import au.org.ala.biocache.dto.SpeciesImageDTO;
import au.org.ala.biocache.util.OccurrenceUtils;
import au.org.ala.names.ws.api.NameUsageMatch;
import au.org.ala.names.ws.client.ALANameUsageMatchServiceClient;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.springframework.context.support.AbstractMessageSource;

import javax.inject.Inject;
import java.util.*;

/**
 * Index based lookup index serice
 */
public class NameMatchSpeciesLookupService implements SpeciesLookupService {
    /** Logger initialisation */
    private final static Logger logger = Logger.getLogger(NameMatchSpeciesLookupService.class);

    private AbstractMessageSource messageSource; // use for i18n of the headers

    @Inject
    protected SpeciesCountsService speciesCountsService;

    @Inject
    protected SpeciesImageService speciesImageService;

    @Inject
    protected ImageMetadataService imageMetadataService;

    @Inject
    protected OccurrenceUtils occurrenceUtils;

    @Inject
    protected ListsService listsService;

    @Inject
    protected LayersService layersService;

    @Inject
    private ALANameUsageMatchServiceClient nameUsageMatchService = null;


//    @PreDestroy
//    public void destroy() {
//        try {
//            nameUsageMatchService.close();
//        } catch (Exception e) {
//            //log.error("Unable to close", e);
//            throw new RuntimeException("Unable to close");
//        }
//    }

    @Override
    public String getGuidForName(String name) {
        String lsid = null;
        try {
            lsid = nameUsageMatchService.searchForLSID(name);
        } catch (Exception e) {
            logger.debug("Error searching for name: " + name + " -  " + e.getMessage(), e);
        }

        return lsid;
    }

    @Override
    public String getAcceptedNameForGuid(String guid) {
        NameUsageMatch nsr = nameUsageMatchService.match(guid);
        if (nsr != null) {
            return nsr.getScientificName();
        } else {
            return null;
        }
    }

    @Override
    public List<String> getNamesForGuids(List<String> guids) {
        List<String> results = new ArrayList<String>(guids.size());
        int idx = 0;
        for(String guid: guids){
            results.add(idx, getAcceptedNameForGuid(guid));
            idx++;
        }
        return results;
    }

    @Override
    public List<String[]> getSpeciesDetails(List<String> guids, List<Long> counts, boolean includeCounts, boolean includeSynonyms, boolean includeLists) {
        List<String[]> results = new ArrayList<String[]>(guids.size());
        int idx = 0;
        for(String guid : guids){
            NameUsageMatch nsr = nameUsageMatchService.match(guid);
            if(nsr == null){
                String lsid = nameUsageMatchService.searchForLSID(guid);
                if(lsid != null){
                    nsr = nameUsageMatchService.match(lsid);
                } else if (guid != null && StringUtils.countMatches(guid, "|") == 4){
                    //is like names_and_lsid: sciName + "|" + taxonConceptId + "|" + vernacularName + "|" + kingdom + "|" + family
                    if (guid.startsWith("\"") && guid.endsWith("\"") && guid.length() > 2) guid = guid.substring(1, guid.length() - 1);
                    lsid = guid.split("\\|", 6)[1];
                    nsr = nameUsageMatchService.match(lsid);
                }
            }

            String[] result = null;
            List<String> lsids = new ArrayList<String>();
            if(nsr != null) {
                lsids.add(nsr.getGenusID());
                lsids.add(nsr.getFamilyID());
                lsids.add(nsr.getSpeciesID());
                result = new String[]{
                        nsr.getScientificName(),
                        nsr.getScientificNameAuthorship(),
                        nsr.getKingdom(),
                        nsr.getPhylum(),
                        nsr.getClasss(),
                        nsr.getOrder(),
                        nsr.getFamily(),
                        nsr.getGenus(),
                        nsr.getSpecies(),
                        nsr.getRankID() < 7000 /* TODO: replace 7000 with RANK.species */ ? nsr.getTaxonConceptID() : null
                };
            } else if (StringUtils.countMatches(guid, "|") == 4){
                //not matched and is like names_and_lsid: sciName + "|" + taxonConceptId + "|" + vernacularName + "|" + kingdom + "|" + family
                if (guid.startsWith("\"") && guid.endsWith("\"") && guid.length() > 2) guid = guid.substring(1, guid.length() - 1);
                String [] split = guid.split("\\|", 6);
                lsids.add(split[1]);
                result = new String[]{
                        split[0],
                        "",
                        split[3],
                        "",
                        "",
                        "",
                        split[4],
                        "",
                        "",
                        ""
                };
            } else {
                result = new String[]{
                        "unmatched",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        ""
                };
            }
            if(includeCounts) {
                result = (String[]) ArrayUtils.add(result, counts.get(idx).toString());
            }
            if (includeLists) {
                List types = listsService.getTypes();
                String[] row = new String[result.length + types.size()];
                System.arraycopy(result, 0, row, 0, result.length);
                Set<String> matches = new HashSet<String>();
                for (int j = 0; j < types.size(); j++) {
                    matches.clear();
                    for (String lsid : lsids) {
                        Set<String> found = listsService.get(types.get(j).toString(), lsid);
                        if (found != null) matches.addAll(found);
                    }
                    result[result.length - types.size() + j] = "";
                    for (String match : matches) {
                        if (result[result.length - types.size() + j].length() > 0) {
                            result[result.length - types.size() + j] += "|";
                        }
                        result[result.length - types.size() + j] += match;
                    }
                }
                result = row;
            }
            results.add(result);
            idx++;
        }
        return results;
    }

    @Override
    public String[] getHeaderDetails(String field, boolean includeCounts, boolean includeSynonyms) {
        String[] baseHeader =  new String[]{
            messageSource.getMessage("species.name", null,"Species Name", null),
            messageSource.getMessage("species.author", null,"Scientific Name Author", null),
            messageSource.getMessage("species.kingdom", null,"Kingdom", null),
            messageSource.getMessage("species.phylum", null,"Phylum", null),
            messageSource.getMessage("species.class", null,"Class", null),
            messageSource.getMessage("species.order", null,"Order", null),
            messageSource.getMessage("species.family", null,"Family", null),
            messageSource.getMessage("species.genus", null,"Genus", null),
            messageSource.getMessage("species.species", null,"Species", null),
            messageSource.getMessage("species.subspecies", null,"Subspecies", null)
        };
        if(includeCounts){
            return (String[]) ArrayUtils.add(baseHeader, messageSource.getMessage("species.count", null, "Number of Records", null));
        } else {
            return baseHeader;
        }
    }

    @Override
    public List<String> getGuidsForTaxa(List<String> taxaQueries) {
        return nameUsageMatchService.getGuidsForTaxa(taxaQueries);
    }

    public void setMessageSource(AbstractMessageSource messageSource) {
        this.messageSource = messageSource;
    }
    
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
                int sort = new Double(
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

        formatted.put("rank", m.get("rank").toString().toLowerCase());
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
