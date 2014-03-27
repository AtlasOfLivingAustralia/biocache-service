package au.org.ala.biocache.util;

import au.org.ala.biocache.Config;
import au.org.ala.biocache.caches.TaxonProfileDAO;
import au.org.ala.biocache.model.TaxonProfile;
import au.org.ala.names.search.ALANameSearcher;
import au.org.ala.names.model.NameSearchResult;
import au.org.ala.names.model.RankType;
import au.org.ala.biocache.dto.Facet;
import au.org.ala.biocache.dto.OccurrenceSourceDTO;
import au.org.ala.biocache.dto.SearchRequestParams;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.service.AuthService;
import au.org.ala.biocache.service.SpeciesLookupService;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Component;
import scala.Option;

import javax.inject.Inject;
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class to provide utility methods used to populate search details.
 *
 * @author Natasha
 */
@Component("searchUtils")
public class SearchUtils {

    /** Logger initialisation */
    private final static Logger logger = Logger.getLogger(SearchUtils.class);

    @Value("${registry.url:\"http://collections.ala.org.au\"}")
    protected String registryUrl = "http://collections.ala.org.au";

    protected String bieBaseUrl = "http://bie.ala.org.au";

    @Inject
    private CollectionsCache collectionCache;

    @Inject
    private AuthService authService;
    //for i18n of display values for facets
    @Inject
    private AbstractMessageSource messageSource;
    @Inject
    private SpeciesLookupService speciesLookupService;
    @Value("${nameIndexLocation:/data/lucene/namematching_v13}")
    protected String nameIndexLocation;
    @Value("${taxon.profile.enabled:false}")
    protected boolean taxonProfileEnabled;

    ALANameSearcher nameIndex = null;

    protected static List<String> defaultParams = new ArrayList<String>();

    static {
        java.lang.reflect.Field[] fields = (java.lang.reflect.Field[])ArrayUtils.addAll(SpatialSearchRequestParams.class.getDeclaredFields(),SearchRequestParams.class.getDeclaredFields());
        for(java.lang.reflect.Field field:fields){
            defaultParams.add(field.getName());
        }
    }

    private  final List<String> ranks = (List<String>) org.springframework.util.CollectionUtils
      .arrayToList(new String[]{"kingdom", "phylum", "class", "order",
              "family", "genus", "species"});

    /**
    * Returns an array that contains the search string to use for a collection
    * search and display name for the results.
    *
    * @return true when UID could be located and query updated correctly
    */
    public boolean updateCollectionSearchString(SearchRequestParams searchParams, String uid) {
        try {
            // query the collectory for the institute and collection codes
            // needed to perform the search
            String[] uids = uid.split(",");
            searchParams.setQ(getUIDSearchString(uids));
            return true;
        } catch (Exception e) {
            logger.error("Problem contacting the collectory: " + e.getMessage(), e);
            return false;
        }
    }

    public String getUIDSearchString(String[] uids){
        StringBuilder sb = new StringBuilder();
        for(String uid : uids){
            if(sb.length()>0)
                sb.append(" OR ");
            sb.append(getUidSearchField(uid));
            sb.append(":");
            sb.append(uid);
        }
        return sb.toString();
    }

    public static String stripEscapedQuotes(String uid){
        if(uid == null) return null;
        if(uid.startsWith("\"") && uid.endsWith("\"") && uid.length()>2)
            return uid.substring(1,uid.length()-1);
        else if(uid.startsWith("\\\"") && uid.endsWith("\\\"") && uid.length()>4){
            return uid.substring(2,uid.length()-2);
        }
        return uid;
    }

    public String getUidDisplayString(String fieldName, String uid){
        return getUidDisplayString(fieldName, uid, true);
    }

    /**
     * Returns the display string for the supplied uid
     *
     * @param uid
     * @return a user friendly display name for this UID
     */
    public String getUidDisplayString(String fieldName, String uid, boolean includeField) {

        uid = stripEscapedQuotes(uid);

        //get the information from the collections cache
        if (uid.startsWith("in") && collectionCache.getInstitutions().containsKey(uid)) {
            if(includeField)
                return "Institution: " + collectionCache.getInstitutions().get(uid);
            else
                return collectionCache.getInstitutions().get(uid);
        } else if (uid.startsWith("co") && collectionCache.getCollections().containsKey(uid)) {
            if(includeField)
                return "Collection: " + collectionCache.getCollections().get(uid);
            else
                return collectionCache.getCollections().get(uid);
        } else if(uid.startsWith("dr") && collectionCache.getDataResources().containsKey(uid)){
            if(includeField)
                return "Data resource: " + collectionCache.getDataResources().get(uid);
            else
                return collectionCache.getDataResources().get(uid);
        } else if(uid.startsWith("drt")&& collectionCache.getDataResources().containsKey(uid)){
            if(includeField)
                return "Temporary Data resource: " + collectionCache.getDataResources().get(uid);
            else
                return collectionCache.getDataResources().get(uid);
        } else if(uid.startsWith("dp") && collectionCache.getDataProviders().containsKey(uid)){
            if(includeField)
                return "Data provider: " + collectionCache.getDataProviders().get(uid);
            else
                return collectionCache.getDataProviders().get(uid);
        } else if(uid.startsWith("dh") && collectionCache.getDataHubs().containsKey(uid)){
            if(includeField)
                return "Data hub: " + collectionCache.getDataHubs().get(uid);
            else
                return collectionCache.getDataHubs().get(uid);
        }
        return messageSource.getMessage(fieldName + "." + StringUtils.remove(uid, "\""), null, uid, null);
    }

    /**
     * Extracts the a rank and name from a query.
     *
     * E.g. genus:Macropus will return an a array of
     *
     * <code>
     * new String[]{"genus", "Macropus"};
     * </code>
     *
     * @param query
     */
    public String convertRankAndName(String query){

        Pattern rankAndName = Pattern.compile("([a-z]{1,})\\:([A-Za-z \\(\\)\\.]{1,})");
        int position = 0;
        Matcher m = rankAndName.matcher(query);
        if(m.find(position) && m.groupCount()==2){
            String rank = m.group(1);
            String scientificName = m.group(2);
            RankType rankType = RankType.getForName(rank.toLowerCase());
            if(rankType != null){
                try {
                    NameSearchResult r = Config.nameIndex().searchForRecord(scientificName, rankType);
                    if(r != null){
                        return "lft:[" + r.getLeft() + " TO " + r.getRight() + "]";
                    }
                } catch (Exception e) {
                    //fail silently if the parse failed
                }
            }
        }
        return query;
    }

    /**
     * Returns an array where the first value is the search string and the
     * second is a display string.
     *
     * @param lsid
     * @return
     */
    public String[] getTaxonSearch(String lsid) {

        String[] result = new String[0];
        if(taxonProfileEnabled){
            // Get the taxon profile from the biocache cache - this could be replaced with a webservice call if necessary
            Option<TaxonProfile> opt = TaxonProfileDAO.getByGuid(lsid);

            if (!opt.isEmpty()) {
                TaxonProfile tc = opt.get();
                StringBuffer dispSB = new StringBuffer(tc.getRankString()
                        + ": " + tc.getScientificName());
                if (tc.getCommonName() != null) {
                    dispSB.append(" : ");
                    dispSB.append(tc.getCommonName());
                }
                //return the lft and rgt range if they exist otherwise return the scientific name
                if (tc.getLeft() == null || tc.getRight() == null){
                    result = new String[]{"taxon_name:\"" + tc.getScientificName() + "\" OR taxon_concept_lsid:" + ClientUtils.escapeQueryChars(lsid), dispSB.toString()};
                } else {
                    StringBuilder sb = new StringBuilder("lft:[");
                    sb.append(tc.getLeft()).append(" TO ").append(tc.getRight()).append("]");
                    result = new String[]{sb.toString(), dispSB.toString()};
                }
            } else {
                //If the lsid for the taxon concept can not be found just return the original string
                result = new String[]{"taxon_concept_lsid:" + ClientUtils.escapeQueryChars(lsid), "taxon_concept_lsid:" + lsid};
            }
        } else {
            //use the name matching index
            try {
                if(nameIndex == null){
                    nameIndex = new ALANameSearcher(nameIndexLocation);
                }
                NameSearchResult nsr = nameIndex.searchForRecordByLsid(lsid);
                if(nsr != null ){
                    String rank = nsr.getRank() != null ? nsr.getRank().toString() : "Unknown Rank";
                    String scientificName = nsr.getRankClassification() != null ? nsr.getRankClassification().getScientificName():null;
                    StringBuffer dispSB = new StringBuffer(rank + ": " + scientificName);
                    StringBuilder sb = new StringBuilder("lft:[");
                    String lft = nsr.getLeft() != null ? nsr.getLeft():"0";
                    String rgt = nsr.getRight() != null ? nsr.getRight():"0";
                    sb.append(lft).append(" TO ").append(rgt).append("]");
                    return new String[]{sb.toString(), dispSB.toString()};
                } else {
                    return new String[]{"taxon_concept_lsid:" + ClientUtils.escapeQueryChars(lsid), "taxon_concept_lsid:" + lsid};
                }
            } catch(Exception e){
                logger.error(e.getMessage(), e);
            }
        }

        return result;
    }

    /**
     * Formats the query string before
     *
     * @param query
     * @return
     */
    public static String formatSearchQuery(String query) {
        // set the query
        StringBuilder queryString = new StringBuilder();
        if (query.equals("*:*") || query.contains(" AND ") || query.contains(" OR ") || query.startsWith("(")
                || query.endsWith("*") || query.startsWith("{")) {
            queryString.append(query);
        } else if (query.contains(":") && !query.startsWith("urn")) {
            // search with a field name specified (other than an LSID guid)
            queryString.append(formatGuid(query));
        } else {
            // regular search
            queryString.append(ClientUtils.escapeQueryChars(query));
        }
        return queryString.toString();
    }

    public static String formatGuid(String guid) {
        String[] bits = StringUtils.split(guid, ":", 2);
        StringBuffer queryString = new StringBuffer();
        queryString.append(ClientUtils.escapeQueryChars(bits[0]));
        queryString.append(":");
        queryString.append(ClientUtils.escapeQueryChars(bits[1]));
        return queryString.toString();
    }

    /**
     * returns the solr field that should be used to search for a particular uid
     *
     * @param uid
     * @return
     */
    public String getUidSearchField(String uid) {
        if (uid.startsWith("co"))
            return "collection_uid";
        if (uid.startsWith("in"))
            return "institution_uid";
        if (uid.startsWith("dr"))
            return "data_resource_uid";
        if (uid.startsWith("dp"))
            return "data_provider_uid";
        if (uid.startsWith("dh"))
            return "data_hub_uid";
        return null;
    }

    /**
     * Returns an ordered list of the next ranks after the supplied rank.
     *
     * @param rank
     * @return
     */
    public List<String> getNextRanks(String rank, boolean includeSuppliedRank) {
        int start = includeSuppliedRank ? ranks.indexOf(rank) : ranks.indexOf(rank) + 1;
        if (start > 0)
            return ranks.subList(start, ranks.size());
        return ranks;
    }

    public List<String> getRanks() {
        return ranks;
    }

    /**
     * Returns the information for the supplied source keys
     * <p/>
     * TODO: There may be a better location for this method.
     *
     * @param sources
     * @return
     */
    public List<OccurrenceSourceDTO> getSourceInformation(Map<String, Integer> sources) {

        Set<String> keys = sources.keySet();
        logger.debug("Listing the source information for : " + keys);
        List<OccurrenceSourceDTO> lsources = new ArrayList<OccurrenceSourceDTO>();
        try {
            for (String key : keys) {
                String name = key;
                if (key.startsWith("co"))
                    name = collectionCache.getCollections().get(key);
                else if (key.startsWith("in"))
                    name = collectionCache.getInstitutions().get(key);
                else if (key.startsWith("dr"))
                    name = collectionCache.getDataResources().get(key);
                lsources.add(new OccurrenceSourceDTO(name, key, sources.get(key)));
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        // sort the sources based on count
        java.util.Collections.sort(lsources,
                new java.util.Comparator<OccurrenceSourceDTO>() {
                    @Override
                    public int compare(OccurrenceSourceDTO o1,
                                       OccurrenceSourceDTO o2) {
                        //sort the counts in reverse order so that max count appears at the top of the list
                        return o2.getCount() - o1.getCount();
                    }
                });
        return lsources;
    }

    /**
     * Provide default values for parameters if they have any null or "empty" values
     *
     * @param requestParams
     */
    public static void setDefaultParams(SearchRequestParams requestParams) {
        SearchRequestParams blankRequestParams = new SearchRequestParams(); // use for default values
        logger.debug("requestParams = " + requestParams);

        if (requestParams.getStart() == null) {
            requestParams.setStart(blankRequestParams.getStart());
        }

        if (requestParams.getPageSize() == null) {
            requestParams.setPageSize(blankRequestParams.getPageSize());
        }

        if (requestParams.getSort() == null || requestParams.getSort().isEmpty()) {
            requestParams.setSort(blankRequestParams.getSort());
        }

        if (requestParams.getDir() == null || requestParams.getDir().isEmpty()) {
            requestParams.setDir(blankRequestParams.getDir());
        }
    }
    
    public static Map<String, String[]> getExtraParams(Map map){
        Map<String, String[]> extraParams = new java.util.HashMap<String, String[]>(map);
        for(String field : defaultParams)
            extraParams.remove(field);
        return extraParams;
    }

    /**
     * Create a HashMap for the filter queries, using the first SOLR field as the key and subsequent
     * query string as the value.
     *
     * Refactor: now returns a Map<String, ActiveFacet> with an additional field "label" that is used to
     * provide a human readable version of the filter query.
     * 
     * NC 2013-01-11: This method has been moved from hubs-webapp so that all the processing is performedby the service rather than the client. 
     * This means that the authService will perform the lookup here.
     *
     * @param filterQuery
     * @return
     */
    public Map<String, Facet> addFacetMap(String[] filterQuery,Set<String> authIndexFields) {
        Map<String, Facet> afs = new HashMap<String, Facet>();
        //Map<String, String> userNamesByIds = authService.getMapOfAllUserNamesById(); // cached by Eh Cache

        if (filterQuery != null && filterQuery.length > 0) {
            // iterate over the fq params
            for (String fq : filterQuery) {
                if (fq != null && !fq.isEmpty()) {
                    Boolean isExcludeFilter = false;
                    // remove Boolean braces if present
                    if (fq.startsWith("(") && fq.endsWith(")")){
                        fq = StringUtils.remove(fq, "(");
                        fq = StringUtils.removeEnd(fq, ")");
                    } else if (fq.startsWith("-(") && fq.endsWith(")")) {
                        fq = StringUtils.remove(fq, "-(");
                        fq = StringUtils.removeEnd(fq, ")");
                        //fq = "-" + fq;
                        isExcludeFilter = true;
                    }

                    String[] fqBits = StringUtils.split(fq, ":", 2);
                    // extract key for map
                    if (fqBits.length  == 2) {
                        String key = fqBits[0];
                        String value = fqBits[1];
                        
                        if ("data_hub_uid".equals(key)) {
                            // exclude these...
                            continue;
                        }
                        
                        Facet f = new Facet();
                        f.setName(key);
                        f.setValue(value);
                        logger.debug("1. fq = " + key + " => " + value);
                        // if there are internal Boolean operators, iterate over sub queries
                        String patternStr = "[ ]+(OR)[ ]+";
                        String[] tokens = fq.split(patternStr, -1);
                        List<String> labels = new ArrayList<String>(); // store sub-queries in this list

                        for (String token : tokens) {
                            logger.debug("token: " + token);
                            String[] tokenBits = StringUtils.split(token, ":", 2);
                            if (tokenBits.length == 2) {
                                String fn = tokenBits[0];
                                String fv = tokenBits[1];
                                String i18n = null;
                                if(fn.endsWith("_s")){
                                    //hack for dynamic facets
                                    i18n = fn.replaceAll("_s", "");
                                } else {
                                    i18n = messageSource.getMessage("facet."+fn, null, fn, null);
                                }

                                if (StringUtils.equals(fn, "species_guid") || StringUtils.equals(fn, "genus_guid")) {
                                    fv = substituteLsidsForNames(fv.replaceAll("\"",""));
                                } else if (StringUtils.equals(fn, "occurrence_year")) {
                                    fv = substituteYearsForDates(fv);
                                } else if (StringUtils.equals(fn, "month")) {
                                    fv = substituteMonthNamesForNums(fv);
                                } else if (authIndexFields.contains(fn)) {
                                    if (authService.getMapOfAllUserNamesById().containsKey(StringUtils.remove(fv, "\""))) 
                                        fv = authService.getMapOfAllUserNamesById().get(StringUtils.remove(fv, "\""));
                                    else if (authService.getMapOfAllUserNamesByNumericId().containsKey(StringUtils.remove(fv, "\"")))
                                        fv = authService.getMapOfAllUserNamesByNumericId().get(StringUtils.remove(fv, "\""));
                                  
                                } else if (StringUtils.contains(fv, "@")) {
                                    //fv = StringUtils.substringBefore(fv, "@"); // hide email addresses
                                    if (authService.getMapOfAllUserNamesById().containsKey(StringUtils.remove(fv, "\""))) {
                                        fv = authService.getMapOfAllUserNamesById().get(StringUtils.remove(fv, "\""));
                                    } else {
                                        fv = fv.replaceAll("\\@\\w+", "@.."); // hide email addresses
                                    }

                                } else {
                                    fv = getUidDisplayString(fn, fv,false);
                                }

                                labels.add(i18n + ":" + fv);
                            }
                        }

                        String label = StringUtils.join(labels, " OR "); // join sub-queries back together
                        if (isExcludeFilter) {
                            label = "-" + label;
                        }
                        logger.debug("label = " + label);
                        f.setDisplayName(label);

                        afs.put(key, f); // add to map
                    }
                }
            }
        }
        return afs;
    }

    /**
     * Lookup a taxon name for a GUID
     *
     * @param fieldValue
     * @return
     */
    private String substituteLsidsForNames(String fieldValue) {
        String name = fieldValue;
        List<String> guids = new ArrayList<String>();
        guids.add(fieldValue);
        List<String> names = speciesLookupService.getNamesForGuids(guids);
        
        if (names != null && names.size() >= 1) {
            name = names.get(0);
        }

        return name;
    }

    /**
     * Convert month number to its name. E.g. 12 -> December.
     * Silently fails if the conversion cant happen.
     *
     * @param fv
     * @return monthStr
     */
    private String substituteMonthNamesForNums(String fv) {
        String monthStr = new String(fv);
        try {
            int m = Integer.parseInt(monthStr);
            Month month = Month.get(m - 1); // 1 index months
            monthStr = month.name();
        } catch (Exception e) {
            // ignore
        }
        return monthStr;
    }

    /**
     * Turn SOLR date range into year range.
     * E.g. [1940-01-01T00:00:00Z TO 1949-12-31T00:00:00Z]
     * to
     * 1940-1949
     * 
     * @param fieldValue
     * @return
     */
    private String substituteYearsForDates(String fieldValue) {
        String dateRange = URLDecoder.decode(fieldValue);
        String formattedDate = StringUtils.replaceChars(dateRange, "[]", "");
        String[] dates =  formattedDate.split(" TO ");
        
        if (dates != null && dates.length > 1) {
            // grab just the year portions
            dateRange = StringUtils.substring(dates[0], 0, 4) + "-" + StringUtils.substring(dates[1], 0, 4);
        }

        return dateRange;
    }
    
    /**
     * Enum for months lookup
     */
    protected enum Month {
        January, February, March, April, May, June, July, August, September, October, November, December;
        public static Month get(int i){
            return values()[i];
        }
    }

    /**
     * @param registryUrl the registryUrl to set
     */
    public void setRegistryUrl(String registryUrl) {
        this.registryUrl = registryUrl;
    }

    /**
     * @param bieBaseUrl the bieBaseUrl to set
     */
    public void setBieBaseUrl(String bieBaseUrl) {
        this.bieBaseUrl = bieBaseUrl;
    }
}