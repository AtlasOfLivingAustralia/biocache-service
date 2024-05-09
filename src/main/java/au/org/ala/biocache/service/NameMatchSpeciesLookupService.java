package au.org.ala.biocache.service;

import au.org.ala.names.ws.api.NameUsageMatch;
import au.org.ala.names.ws.client.ALANameUsageMatchServiceClient;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.support.AbstractMessageSource;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Index based lookup index serice
 */
public class NameMatchSpeciesLookupService implements SpeciesLookupService {
    /** Logger initialisation */
    private final static Logger logger = Logger.getLogger(NameMatchSpeciesLookupService.class);

    private AbstractMessageSource messageSource; // use for i18n of the headers

    @Inject
    protected ListsService listsService;

    @Inject
    protected LayersService layersService;

    @Inject
    private ALANameUsageMatchServiceClient nameUsageMatchService = null;

    private String[] baseHeader;
    private String[] countBaseHeader;
    private String[] synonymHeader;
    private String[] countSynonymHeader;

    @Cacheable("namematching")
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

    @Cacheable("namematching")
    @Override
    public String getAcceptedNameForGuid(String guid) {
        return nameUsageMatchService.getName(guid, true);
    }

    @Cacheable("namematching")
    @Override
    public List<String[]> getSpeciesDetails(List<String> guids, List<Long> counts, boolean includeCounts, boolean includeSynonyms, boolean includeLists) {
        List<String[]> results = new ArrayList<String[]>(guids.size());

        // handle guids that are from the names_and_lsid field
        List<String> guidsFiltered = guids.stream().map(g -> {
            //is like names_and_lsid: sciName + "|" + taxonConceptId + "|" + vernacularName + "|" + kingdom + "|" + family
            if(g != null && StringUtils.countMatches(g, '|') == 4) {
                g = g.split("\\|", 6)[1];
            }
            return g;
        }).collect(Collectors.toList());

        List<NameUsageMatch> matches = nameUsageMatchService.getAll(guidsFiltered, true);

        for (int i = 0; i < matches.size(); i++) {
            NameUsageMatch nsr = matches.get(i);
            String guid = guids.get(i);

            String[] result = null;
            List<String> lsids = new ArrayList<String>();
            if (nsr != null && nsr.isSuccess()) {
                lsids.add(nsr.getGenusID());
                lsids.add(nsr.getFamilyID());
                lsids.add(nsr.getSpeciesID());
                result = new String[]{
                        guid,
                        nsr.getScientificName(),
                        nsr.getScientificNameAuthorship(),
                        nsr.getRank(),
                        nsr.getKingdom(),
                        nsr.getPhylum(),
                        nsr.getClasss(),
                        nsr.getOrder(),
                        nsr.getFamily(),
                        nsr.getGenus(),
                        nsr.getVernacularName()
                };
            } else if (StringUtils.countMatches(guid, "|") == 4){
                //not matched and is like names_and_lsid: sciName + "|" + taxonConceptId + "|" + vernacularName + "|" + kingdom + "|" + family
                if (guid.startsWith("\"") && guid.endsWith("\"") && guid.length() > 2) guid = guid.substring(1, guid.length() - 1);
                String [] split = guid.split("\\|", 6);
                lsids.add(split[1]);
                result = new String[]{
                        guid,
                        split[0],
                        "",
                        "",
                        split[3],
                        "",
                        "",
                        "",
                        split[4],
                        "",
                        split[2]
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
                        "",
                        ""
                };
            }
            if (includeCounts) {
                result = (String[]) ArrayUtils.add(result, counts.get(i).toString());
            }
            if (includeLists) {
                List types = listsService.getTypes();
                String[] row = new String[result.length + types.size()];
                System.arraycopy(result, 0, row, 0, result.length);
                Set<String> listMatches = new HashSet<String>();
                for (int j = 0; j < types.size(); j++) {
                    listMatches.clear();
                    for (String lsid : lsids) {
                        Set<String> found = listsService.get(types.get(j).toString(), lsid);
                        if (found != null) {
                            listMatches.addAll(found);
                        }
                    }
                    row[result.length + j] = "";
                    for (String match : listMatches) {
                        // pipe separate multiple values
                        if (row[result.length  + j].length() > 0) {
                            row[result.length  + j] += " | ";
                        }
                        row[result.length + j] += match;
                    }
                }
                result = row;
            }
            results.add(result);
        }
        return results;
    }

    @Override
    public String[] getHeaderDetails(String field, boolean includeCounts, boolean includeSynonyms) {

        if (baseHeader == null) {
            //initialise all the headers
            initHeaders();
        }
        String[] startArray = baseHeader;
        if (includeCounts) {
            if (includeSynonyms) {
                startArray = countSynonymHeader;
            } else {
                startArray = countBaseHeader;
            }
        } else if (includeSynonyms) {
            startArray = synonymHeader;
        }
        return (String[]) ArrayUtils.add(startArray, 0, messageSource.getMessage("facet." + field, null, field, null));
    }

    private void initHeaders() {
        baseHeader = new String[]{messageSource.getMessage("species.name", null,"Species Name", null),
                messageSource.getMessage("species.author", null,"Scientific Name Author", null),
                messageSource.getMessage("species.rank", null,"Taxon Rank", null),
                messageSource.getMessage("species.kingdom", null,"Kingdom", null),
                messageSource.getMessage("species.phylum", null,"Phylum", null),
                messageSource.getMessage("species.class", null,"Class", null),
                messageSource.getMessage("species.order", null,"Order", null),
                messageSource.getMessage("species.family", null,"Family", null),
                messageSource.getMessage("species.genus", null,"Genus", null),
                messageSource.getMessage("species.common", null,"Vernacular Name", null)};
        countBaseHeader = (String[]) ArrayUtils.add(baseHeader,messageSource.getMessage("species.count", null,"Number of Records", null));
        synonymHeader = (String[]) ArrayUtils.add(baseHeader,messageSource.getMessage("species.synonyms", null,"Synonyms", null));
        countSynonymHeader = (String[]) ArrayUtils.add(synonymHeader,messageSource.getMessage("species.count", null,"Number of Records", null));
    }

    @Cacheable("namematching")
    @Override
    public List<String> getGuidsForTaxa(List<String> taxaQueries) {
        return nameUsageMatchService.getGuidsForTaxa(taxaQueries);
    }

    public void setMessageSource(AbstractMessageSource messageSource) {
        this.messageSource = messageSource;
    }
}
