package au.org.ala.biocache.util;

import au.org.ala.biocache.Config;
import au.org.ala.biocache.dto.OccurrenceSourceDTO;
import au.org.ala.biocache.dto.SearchRequestParams;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.service.SpeciesLookupService;
import au.org.ala.names.model.NameSearchResult;
import au.org.ala.names.model.RankType;
import au.org.ala.names.search.ALANameSearcher;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.MessageSource;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.CollectionUtils;
import org.springframework.web.servlet.HandlerMapping;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
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
    private static final Pattern DUD_URL_PATTERN = Pattern.compile("([a-z]+:/)([^/].*)");

    /** Logger initialisation */
    private final static Logger logger = Logger.getLogger(SearchUtils.class);
    @Inject
    private CollectionsCache collectionCache;

    /**
     * Comma separated list of solr fields that need to have the authService substitute values if they are used in a facet.
     */
    @Value("${auth.substitution.fields:}")
    protected String authServiceFields = "";

    public Set<String> authIndexFields = new HashSet<String>();

    //for i18n of display values for facets
    @Inject
    private MessageSource messageSource;
    @Inject
    private SpeciesLookupService speciesLookupService;
    @Value("${name.index.dir:/data/lucene/namematching}")
    protected String nameIndexLocation;

    ALANameSearcher nameIndex = null;

    protected static List<String> defaultParams = new ArrayList<String>();

    static {
        java.lang.reflect.Field[] fields = (java.lang.reflect.Field[]) ArrayUtils.addAll(SpatialSearchRequestParams.class.getDeclaredFields(),SearchRequestParams.class.getDeclaredFields());
        for(java.lang.reflect.Field field:fields){
            defaultParams.add(field.getName());
        }
    }

    private  final List<String> ranks = (List<String>) org.springframework.util.CollectionUtils
      .arrayToList(new String[]{"kingdom", "phylum", "class", "order",
              "family", "genus", "species"});

    /**
     * Util to filter SimpleOrderedMap output from a SOLR json.facet query and return the result of
     * SimpleOrderedMap.getVal.
     *
     * @param input   SimpleOrderedMap or List<SimpleOrderedMap> that is the output from a SOLR json.facet query.
     * @param filters List of traversal instructions. A String for Map, Integer for List, ending with Integer for
     *                use in getVal or ending with String for use in get.
     * @return
     */
    public static Object getVal(Object input, Object... filters) {
        Object item = getItem(input, ArrayUtils.subarray(filters, 0, filters.length - 1));
        Object lastFilter = filters[filters.length - 1];
        if (item == null) {
            return item;
        } else if (lastFilter instanceof Integer) {
            return ((SimpleOrderedMap) item).getVal((Integer) lastFilter);
        } else {
            return ((SimpleOrderedMap) item).get((String) lastFilter);
        }
    }

    /**
     * Util to filter SimpleOrderedMap output from a SOLR json.facet query and return the result of
     * SimpleOrderedMap.getVal.
     *
     * @param input   SimpleOrderedMap or List<SimpleOrderedMap> that is the output from a SOLR json.facet query.
     * @param filters List of traversal instructions. A String for Map, Integer for List. The last filtered item must
     *                be of the type List<SimpleOrderedMap>
     * @return
     */
    public static List<SimpleOrderedMap> getList(Object input, Object... filters) {
        Object list = getItem(input, filters);
        if (list == null) {
            list = new ArrayList();
        }
        return (List<SimpleOrderedMap>) list;
    }

    /**
     * Util to filter SimpleOrderedMap output from a SOLR json.facet query and return the result of
     * SimpleOrderedMap.getVal.
     *
     * @param input   SimpleOrderedMap or List<SimpleOrderedMap> that is the output from a SOLR json.facet query.
     * @param filters List of traversal instructions. A String for Map, Integer for List. The last filtered item must
     *                be of the type SimpleOrderedMap
     * @return
     */
    public static SimpleOrderedMap getMap(Object input, Object... filters) {
        return (SimpleOrderedMap) getItem(input, filters);
    }

    /**
     * Util to filter SimpleOrderedMap output from a SOLR json.facet query and return the result of
     * SimpleOrderedMap.getVal.
     *
     * @param input   SimpleOrderedMap or List<SimpleOrderedMap> that is the output from a SOLR json.facet query.
     * @param filters List of traversal instructions. A String for Map, Integer for List.
     * @return
     */
    public static Object getItem(Object input, Object... filters) {
        Object item = input;
        for (int i = 0; item != null && i < filters.length; i++) {
            Object filter = filters[i];
            if (filter instanceof String) {
                item = ((SimpleOrderedMap) item).get((String) filter);
            } else if (filter instanceof Number) {
                item = ((List<SimpleOrderedMap>) item).get((Integer) filter);
            }
        }
        return item;
    }

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

        if(uid == null) return null;

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
        } else if(uid.startsWith("drt")&& collectionCache.getTempDataResources().containsKey(uid)){
            if(includeField)
                return "Temporary Data resource: " + collectionCache.getTempDataResources().get(uid);
            else
                return collectionCache.getTempDataResources().get(uid);
        } else if(uid.startsWith("dr") && collectionCache.getDataResources().containsKey(uid)){
            if(includeField)
                return "Data resource: " + collectionCache.getDataResources().get(uid);
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
        String value = StringUtils.remove(uid, "\"");

        return messageSource.getMessage(fieldName + "." + value, null, uid, null);
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
                return new String[]{"taxon_concept_lsid:\"" + ClientUtils.escapeQueryChars(lsid) + "\"", "taxon_concept_lsid:\"" + lsid + "\""};
            }
        } catch(Exception e){
            logger.error(e.getMessage(), e);
        }

        return result;
    }

    /**
     * Get a GUID from a path.
     * <p>
     *     This is complicated by the spring framework reducing http://xxx.yyy to http:/xxx.yyy so we have to put it back.
     * </p>
     * @param request The request
     *
     * @return The guid
     */
    public String getGuidFromPath(HttpServletRequest request) {
        String guid = new AntPathMatcher().extractPathWithinPattern(
                (String) request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE),
                request.getServletPath());

        if (guid.endsWith(".json"))
            guid = guid.substring(0, guid.length() - 5);
        Matcher duds = DUD_URL_PATTERN.matcher(guid);
        if (duds.matches())
            guid = duds.group(1) + "/" + duds.group(2);
        return guid;
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
                else if (key.startsWith("drt"))
                    name = collectionCache.getTempDataResources().get(key);
                else if (key.startsWith("dr"))
                    name = collectionCache.getDataResources().get(key);
                else if (key.startsWith("dp"))
                    name = collectionCache.getDataProviders().get(key);
                else if (key.startsWith("dh"))
                    name = collectionCache.getDataHubs().get(key);
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
        if (requestParams.getFacet() == null)
            requestParams.setFacet(blankRequestParams.getFacet());

        if (requestParams.getFacets() == null)
            requestParams.setFacets(blankRequestParams.getFacets());
    }
    
    public static Map<String, String[]> getExtraParams(Map map){
        Map<String, String[]> extraParams = new java.util.HashMap<String, String[]>(map);
        for(String field : defaultParams)
            extraParams.remove(field);
        return extraParams;
    }

    private boolean isDynamicField(String fieldName){
        return fieldName.endsWith("_s") || fieldName.endsWith("_i") || fieldName.endsWith("_d");
    }

    public static String formatDynamicFieldName(String fieldName){
        if(fieldName.length()>2){
            return StringUtils.capitalize(fieldName.substring(0, fieldName.length() - 2).replace("_", " "));
        }
        return fieldName;
    }

    /**
     * Lookup a taxon name for a GUID
     *
     * @param fieldValue
     * @return
     */
    public String substituteLsidsForNames(String fieldValue) {
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
    public String substituteMonthNamesForNums(String fv) {
        try {
            String monthStr = new String(fv);
            //strip quotes and match
            int m = Integer.parseInt(monthStr.replaceAll("\"",""));
            Month month = Month.get(m - 1); // 1 index months
            return month.name();
        } catch (Exception e) {
            // ignore
        }
        return fv;
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
    public String substituteYearsForDates(String fieldValue) {
        String dateRange = URLDecoder.decode(fieldValue);
        String formattedDate = StringUtils.replaceChars(dateRange, "[]\\", "");
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

    public Set<String> getAuthIndexFields() {
        if (authIndexFields.size() == 0) {
            //set up the hash set of the fields that need to have the authentication service substitute
            if (logger.isDebugEnabled()) {
                logger.debug("Auth substitution fields to use: " + authServiceFields);
            }
            Set set = new java.util.HashSet<String>();
            CollectionUtils.mergeArrayIntoCollection(authServiceFields.split(","), set);
            authIndexFields = set;
        }
        return authIndexFields;
    }

    public static String formatValue(Object value) {
        if (value instanceof Date) {
            return value == null ? "" : org.apache.commons.lang.time.DateFormatUtils.format((Date) value, "yyyy-MM-dd");
        } else {
            return value == null ? "" : value.toString();
        }
    }
}