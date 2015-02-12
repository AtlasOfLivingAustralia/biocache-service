package au.org.ala.biocache.service;


import au.org.ala.names.model.LinnaeanRankClassification;
import au.org.ala.names.model.NameSearchResult;
import au.org.ala.names.search.ALANameSearcher;
import com.mockrunner.util.common.StringUtil;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.context.support.AbstractMessageSource;

import java.util.ArrayList;
import java.util.List;

/**
 * Index based lookup index serice
 */
public class SpeciesLookupIndexService implements SpeciesLookupService {

    private AbstractMessageSource messageSource; // use for i18n of the headers

    protected String nameIndexLocation;

    private ALANameSearcher nameIndex = null;

    private ALANameSearcher getNameIndex() throws RuntimeException {
        if(nameIndex == null){
            try {
                nameIndex = new ALANameSearcher(nameIndexLocation);
            } catch (Exception e){
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return nameIndex;
    }

    @Override
    public String getGuidForName(String name) {
        try {
            return getNameIndex().searchForLSID(name);
        } catch(Exception e){
            return null;
        }
    }

    @Override
    public String getAcceptedNameForGuid(String guid) {
        NameSearchResult nsr = getNameIndex().searchForRecordByLsid(guid);
        if(nsr != null ){
            return nsr.getRankClassification() != null ? nsr.getRankClassification().getScientificName() : null;
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
    public List<String[]> getSpeciesDetails(List<String> guids, List<Long> counts, boolean includeCounts, boolean includeSynonyms) {
        List<String[]> results = new ArrayList<String[]>(guids.size());
        int idx = 0;
        for(String guid : guids){
            NameSearchResult nsr = getNameIndex().searchForRecordByLsid(guid);
            if(nsr == null){
                String lsid = getNameIndex().searchForLsidById(guid);
                if(lsid != null){
                    nsr = getNameIndex().searchForRecordByLsid(lsid);
                } else if (guid != null && StringUtil.countMatches(guid, "|") == 4){
                    //is like names_and_lsid: sciName + "|" + taxonConceptId + "|" + vernacularName + "|" + kingdom + "|" + family
                    if (guid.startsWith("\"") && guid.endsWith("\"") && guid.length() > 2) guid = guid.substring(1, guid.length() - 1);
                    lsid = guid.split("\\|", 6)[1];
                    nsr = getNameIndex().searchForRecordByLsid(lsid);
                }
            }

            String[] result = null;
            if(nsr != null) {
                LinnaeanRankClassification classification = nsr.getRankClassification();
                result = new String[]{
                        classification.getScientificName(),
                        classification.getAuthorship(),
                        classification.getKingdom(),
                        classification.getPhylum(),
                        classification.getKlass(),
                        classification.getOrder(),
                        classification.getFamily(),
                        classification.getGenus(),
                        classification.getSpecies(),
                        classification.getSubspecies()
                };
            } else if (StringUtil.countMatches(guid, "|") == 4){
                //not matched and is like names_and_lsid: sciName + "|" + taxonConceptId + "|" + vernacularName + "|" + kingdom + "|" + family
                if (guid.startsWith("\"") && guid.endsWith("\"") && guid.length() > 2) guid = guid.substring(1, guid.length() - 1);
                String [] split = guid.split("\\|", 6);
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
            if(includeCounts) {
                result = (String[]) ArrayUtils.add(result, counts.get(idx).toString());
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

    public void setMessageSource(AbstractMessageSource messageSource) {
        this.messageSource = messageSource;
    }

    public void setNameIndexLocation(String nameIndexLocation) {
        this.nameIndexLocation = nameIndexLocation;
    }
}
