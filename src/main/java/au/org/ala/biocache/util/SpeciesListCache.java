package au.org.ala.biocache.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import javax.inject.Inject;
import java.util.Map;

/**
 * Simple cache of species list names for query display purposes.
 */
@Component("speciesListCache")
public class SpeciesListCache {

    /** Log4J logger */
    private final static Logger logger = Logger.getLogger(SpeciesListCache.class);

    @Value("${list.tool.url:http://lists.ala.org.au}")
    protected String listToolUrl = null;

    /** Spring injected RestTemplate object */
    @Inject
    private RestOperations restTemplate;

    /**
     * Retrieve the name of a species list.
     * @param drUid
     * @return
     */
    public String getDisplayNameForList(String drUid){
        if(StringUtils.isEmpty(drUid)){
            return drUid;
        }

        try {
            String cleanedUid = drUid.replaceAll("\"", "");
            Map<String, String> entity = restTemplate.getForObject(listToolUrl + "/ws/speciesList/" + cleanedUid, Map.class);
            return entity.get("listName");
        } catch (Exception e){
            logger.warn("Unable to lookup species list name: " + e.getMessage());
            return drUid;
        }
    }
}
