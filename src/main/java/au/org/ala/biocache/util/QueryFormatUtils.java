package au.org.ala.biocache.util;

import au.org.ala.biocache.dao.QidCacheDAO;
import au.org.ala.biocache.dto.Facet;
import au.org.ala.biocache.dto.SearchRequestParams;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.model.Qid;
import au.org.ala.biocache.service.AuthService;
import au.org.ala.biocache.service.LayersService;
import au.org.ala.biocache.service.ListsService;
import au.org.ala.biocache.service.SpeciesLookupService;
import com.googlecode.ehcache.annotations.Cacheable;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component("queryFormatUtils")
public class QueryFormatUtils {

    private static final Logger logger = Logger.getLogger(QueryFormatUtils.class);

    @Inject
    protected SearchUtils searchUtils;

    @Inject
    protected AbstractMessageSource messageSource;

    @Inject
    protected SpeciesLookupService speciesLookupService;

    @Inject
    protected LayersService layersService;

    @Inject
    protected QidCacheDAO qidCacheDao;

    @Inject
    protected RangeBasedFacets rangeBasedFacets;

    @Inject
    protected ListsService listsService;

    @Inject
    private AuthService authService;

    protected static final String QUOTE = "\"";
    protected static final char[] CHARS = {' ', ':'};

    private String spatialField = "geohash";

    //Patterns that are used to prepare a SOLR query for execution
    protected Pattern lsidPattern = Pattern.compile("(^|\\s|\"|\\(|\\[|')lsid:\"?([a-zA-Z0-9/\\.:\\-_]*)\"?");
    protected Pattern speciesListPattern = Pattern.compile("(^|\\s|\"|\\(|\\[|')species_list:\"?(dr[0-9]*)\"?");
    protected Pattern urnPattern = Pattern.compile("\\burn:[a-zA-Z0-9\\.:-]*");
    protected Pattern httpPattern = Pattern.compile("http:[a-zA-Z0-9/\\.:\\-_]*");
    protected Pattern spacesPattern = Pattern.compile("[^\\s\"\\(\\)\\[\\]{}']+|\"[^\"]*\"|'[^']*'");
    protected Pattern uidPattern = Pattern.compile("(?:[\"]*)?([a-z_]*_uid:)([a-z0-9]*)(?:[\"]*)?");
    protected Pattern spatialPattern = Pattern.compile(spatialField + ":\"Intersects\\([a-zA-Z=\\-\\s0-9\\.\\,():]*\\)\\\"");
    protected Pattern qidPattern = QidCacheDAO.qidPattern;//Pattern.compile("qid:[0-9]*");
    protected Pattern termPattern = Pattern.compile("([a-zA-z_]+?):((\".*?\")|(\\\\ |[^: \\)\\(])+)"); // matches foo:bar, foo:"bar bash" & foo:bar\ bash
    protected Pattern indexFieldPatternMatcher = java.util.regex.Pattern.compile("<span.*?</span>|\\b[a-z_0-9]{1,}:");
    protected Pattern layersPattern = Pattern.compile("(^|\\b)(el|cl)[0-9abc]+:");
    protected Pattern taxaPattern = Pattern.compile("(^|\\s|\"|\\(|\\[|')taxa:\"?([a-zA-Z0-9\\s\\(\\)\\.:\\-_]*)\"?");

    private int maxBooleanClauses = 1024;

    public int getMaxBooleanClauses() {
        return maxBooleanClauses;
    }

    public void setMaxBooleanClauses(int maxBooleanClauses) {
        this.maxBooleanClauses = maxBooleanClauses;
    }

    public Map<String, Facet> formatSearchQuery(SpatialSearchRequestParams searchParams) {
        return formatSearchQuery(searchParams, false);
    }

    @Cacheable(cacheName = "formatSearchQuery")
    public Map<String, Facet> formatSearchQuery(SpatialSearchRequestParams searchParams, boolean forceQueryFormat) {
        Map<String, Facet> activeFacetMap = new HashMap();
        //Only format the query if it doesn't already supply a formattedQuery.
        if (forceQueryFormat || StringUtils.isEmpty(searchParams.getFormattedQuery())) {
            // set the query
            String query = searchParams.getQ();

            int requestedFqCount = searchParams.getFq() == null ? 0 : searchParams.getFq().length;

            formatQueryTerm(query, searchParams, false);

            //format fqs for facets that need ranges substituted
            for (int i = 0; i < searchParams.getFq().length; i++) {
                String fq = searchParams.getFq()[i];

                if (fq != null && fq.length() > 0) {
                    String [] formatted = formatQueryTerm(fq, searchParams, true);

                    addFormattedFq(new String[]{formatted[1]}, searchParams);

                    //add to activeFacetMap fqs that are not inserted by a qid, and the q of qids in fqs.
                    //do not add spatial fields
                    if (i < requestedFqCount && !formatted[1].contains(spatialField + ":")) {
                        Facet facet = new Facet();
                        facet.setDisplayName(formatted[0]);
                        String [] fv = fq.split(":");
                        if (fv.length >= 2) {
                            facet.setName(fv[0]);
                            facet.setValue(fq.substring(fv[0].length() + 1));
                        }
                        activeFacetMap.put(fq, facet);
                    }
                }
            }

            //add spatial query term for wkt or lat/lon/radius parameters
            String spatialQuery = buildSpatialQueryString(searchParams);
            if (spatialQuery != null) {
                addFormattedFq(new String[] { spatialQuery }, searchParams);
            }
        }

        updateQueryContext(searchParams);

        return activeFacetMap;
    }

    public void addFqs(String [] fqs, SpatialSearchRequestParams searchParams) {
        if (fqs != null && searchParams != null) {
            String[] currentFqs = searchParams.getFq();
            if (currentFqs == null || currentFqs.length == 0 || (currentFqs.length == 1 && currentFqs[0].length() == 0)) {
                searchParams.setFq(fqs);
            } else {
                //we need to add the current Fqs together
                searchParams.setFq((String[]) ArrayUtils.addAll(currentFqs, fqs));
            }
        }
    }
    private void addFormattedFq(String [] fqs, SearchRequestParams searchParams) {
        if (fqs != null && searchParams != null) {
            String[] currentFqs = searchParams.getFormattedFq();
            if (currentFqs == null || currentFqs.length == 0 || (currentFqs.length == 1 && currentFqs[0].length() == 0)) {
                searchParams.setFormattedFq(fqs);
            } else {
                //we need to add the current Fqs together
                searchParams.setFormattedFq((String[]) ArrayUtils.addAll(currentFqs, fqs));
            }
        }
    }

    /**
     * Replace query qid value with actual query.
     *
     * When !isFq the searchParams q, formattedQuery and displayString may be updated with the qid values.
     *
     * @param query
     * @param searchParams required when !isFq
     * @param isFq
     * @return
     */
    private String [] formatQid(String query, SpatialSearchRequestParams searchParams, boolean isFq) {
        String q = query;
        String displayString = query;
        if (query.contains("qid:")) {
            Matcher matcher = qidPattern.matcher(query);
            while (matcher.find()) {
                String value = matcher.group();
                try {
                    String qidValue = SearchUtils.stripEscapedQuotes(value.substring(4));
                    Qid qid = qidCacheDao.get(qidValue);
                    if (qid != null) {
                        q = qid.getQ();
                        if (!isFq) {
                            searchParams.setQ(qid.getQ());
                        }

                        //add the fqs from the params cache
                        addFqs(qid.getFqs(), searchParams);

                        //add wkt
                        if (searchParams != null) {
                            if (StringUtils.isEmpty(searchParams.getWkt()) && StringUtils.isNotEmpty(qid.getWkt())) {
                                searchParams.setWkt(qid.getWkt());
                            } else if (StringUtils.isNotEmpty(searchParams.getWkt()) && StringUtils.isNotEmpty(qid.getWkt())) {
                                //Add the qid.wkt search term to searchParams.fq instead of wkt -> Geometry -> intersection -> wkt
                                addFqs(new String[]{SpatialUtils.getWKTQuery(spatialField, qid.getWkt(), false)}, searchParams);
                            }
                        }
                    }
                } catch (NumberFormatException e) {
                } catch (QidMissingException e) {
                }
            }
        }
        return new String[] {q, displayString};
    }

    /**
     * Substitute matched_name_children and matched_name with the correct formattedQuery and displayString.
     *
     * @param current String [] { displayString, formattedQuery } to update.
     */
    private String [] formatTerms(String [] current) {
        // look for field:term sub queries and catch fields: matched_name & matched_name_children
        if (current[1].contains(":")) {
            StringBuffer queryString = new StringBuffer();

            // will match foo:bar, foo:"bar bash" & foo:bar\ bash
            Matcher matcher = termPattern.matcher(current[1]);
            queryString.setLength(0);

            while (matcher.find()) {
                String value = matcher.group();
                if (logger.isDebugEnabled()) {
                    logger.debug("term query: " + value);
                    logger.debug("groups: " + matcher.group(1) + "|" + matcher.group(2));
                }

                if ("matched_name".equals(matcher.group(1))) {
                    // name -> accepted taxon name (taxon_name:)
                    String field = matcher.group(1);
                    String queryText = matcher.group(2);

                    if (queryText != null && !queryText.isEmpty()) {
                        String guid = speciesLookupService.getGuidForName(queryText.replaceAll("\"", "")); // strip any quotes
                        if (logger.isInfoEnabled()) {
                            logger.info("GUID for " + queryText + " = " + guid);
                        }

                        if (guid != null && !guid.isEmpty()) {
                            String acceptedName = speciesLookupService.getAcceptedNameForGuid(guid); // strip any quotes
                            if (logger.isInfoEnabled()) {
                                logger.info("acceptedName for " + queryText + " = " + acceptedName);
                            }

                            if (acceptedName != null && !acceptedName.isEmpty()) {
                                field = "taxon_name";
                                queryText = acceptedName;
                            }
                        } else {
                            field = "taxon_name";
                        }

                        // also change the display query
                        current[0] = current[0].replaceAll("matched_name", "taxon_name");
                    }

                    if (StringUtils.containsAny(queryText, CHARS) && !queryText.startsWith("[") && !queryText.startsWith("\"")) {
                        // quote any text that has spaces or colons but not range queries or if already quoted
                        queryText = QUOTE + queryText + QUOTE;
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("queryText: " + queryText);
                    }

                    matcher.appendReplacement(queryString, matcher.quoteReplacement(field + ":" + queryText));

                } else if ("matched_name_children".equals(matcher.group(1))) {
                    String field = matcher.group(1);
                    String queryText = matcher.group(2);

                    if (queryText != null && !queryText.isEmpty()) {
                        String guid = speciesLookupService.getGuidForName(queryText.replaceAll("\"", "")); // strip any quotes
                        if (logger.isInfoEnabled()) {
                            logger.info("GUID for " + queryText + " = " + guid);
                        }

                        if (guid != null && !guid.isEmpty()) {
                            field = "lsid";
                            queryText = guid;
                        } else {
                            field = "taxon_name";
                        }
                    }

                    if (StringUtils.containsAny(queryText, CHARS) && !queryText.startsWith("[") && !queryText.startsWith("\"")) {
                        // quote any text that has spaces or colons but not range queries and not already quoted
                        queryText = QUOTE + queryText + QUOTE;
                    }

                    matcher.appendReplacement(queryString, Matcher.quoteReplacement(field + ":" + queryText));
                } else {
                    matcher.appendReplacement(queryString, Matcher.quoteReplacement(value));
                }
            }
            matcher.appendTail(queryString);

            current[1] = queryString.length() > 0 ? queryString.toString() : current[1];
            current[0] = current[1];
        }
        return current;
    }

    /**
     * Substitute taxa with the correct formattedQuery and displayString.
     *
     * @param current String [] { displayString, formattedQuery } to update.
     */
    private String [] formatTaxa(String [] current) {
        // replace taxa queries with lsid: and text:
        if (current[1].contains("taxa:")) {
            StringBuffer queryString = new StringBuffer();
            Matcher matcher = taxaPattern.matcher(current[1]);
            queryString.setLength(0);
            while (matcher.find()) {
                String value = matcher.group();
                String taxa = matcher.group(2);

                if (logger.isDebugEnabled()) {
                    logger.debug("found taxa " + taxa);
                }

                List<String> taxaQueries = new ArrayList<>();
                taxaQueries.add(taxa);
                List<String> guidsForTaxa = speciesLookupService.getGuidsForTaxa(taxaQueries);
                String q = createQueryWithTaxaParam(taxaQueries, guidsForTaxa);

                matcher.appendReplacement(queryString, q);
            }

            matcher.appendTail(queryString);

            current[1] = queryString.toString();
            current[0] = current[1];
        }
        return current;
    }

    /**
     * Substitute lft ranges for species lists in queries for formattedQuery and displayString.
     *
     * @param current String [] { displayString, formattedQuery } to update.
     */
    private void formatSpeciesList(String [] current) {
        //if the query string contains species_list: replace with the equivalent (lsid: OR lsid: ...etc) before lsid: is parsed
        if (current[1].contains("species_list:")) {
            StringBuffer queryString = new StringBuffer();
            StringBuffer displaySb = new StringBuffer();
            int last = 0;
            queryString.setLength(0);
            Matcher matcher = speciesListPattern.matcher(current[1]);
            while (matcher.find()) {
                String speciesList = matcher.group(2);

                if (logger.isDebugEnabled()) {
                    logger.debug("found speciesList " + speciesList);
                }

                try {
                    List<String> lsids = listsService.getListItems(speciesList);

                    //fetch lsid info
                    StringBuilder sbgroup = new StringBuilder();
                    StringBuilder sb = new StringBuilder();
                    sb.append(matcher.group(1));

                    //build species_list around maxBooleanClauses limit
                    int max = getMaxBooleanClauses();
                    int count = 0;
                    for (String lsid : lsids) {
                        String [] taxon = searchUtils.getTaxonSearch(lsid);
                        if (taxon.length > 1) {
                            if (sbgroup.length() + sb.length() > matcher.group(1).length()) {
                                sb.append(" OR ");
                            }
                            sb.append(taxon[0]);
                            count++;
                            if (count % (max-10) == 0) {
                                if (sbgroup.length() > 0) sbgroup.append(" OR ");
                                sbgroup.append("(").append(sb.toString()).append(")");
                                sb = new StringBuilder();
                            }
                        }
                    }

                    matcher.appendReplacement(queryString, matcher.group(1) + "(" + sbgroup.toString() + ")");

                    displaySb.append(current[1].substring(last, matcher.start()));
                    Map<String, String> list = listsService.getListInfo(speciesList);
                    String name = list == null ? "Species list" : list.get("listName");
                    displaySb.append(matcher.group(1)).append("<span class='species_list' id='").append(speciesList).append("'>").append(name).append("</span>");
                } catch (Exception e) {
                    logger.error("failed to get species list: " + speciesList);
                }
                last = matcher.end();
            }

            matcher.appendTail(queryString);

            current[0] = displaySb.toString();
            current[1] = queryString.toString();
        }
    }

    /**
     * Substitute lft ranges for lsids in queries for formattedQuery and displayString.
     *
     * @param current String [] { displayString, formattedQuery } to update.
     */
    private void formatLsid(String [] current) {
        //if the query string contains lsid: we will need to replace it with the corresponding lft range
        if (current[1].contains("lsid:")) {
            StringBuffer queryString = new StringBuffer();
            StringBuffer displaySb = new StringBuffer();
            int last = 0;

            Matcher matcher = lsidPattern.matcher(current[1]);
            queryString.setLength(0);
            while (matcher.find()) {
                //only want to process the "lsid" if it does not represent taxon_concept_lsid etc...
                if ((matcher.start() > 0 && current[1].charAt(matcher.start() - 1) != '_') || matcher.start() == 0) {
                    String value = matcher.group();
                    if (logger.isDebugEnabled()) {
                        logger.debug("pre-processing " + value);
                    }
                    String lsid = matcher.group(2);
                    if (lsid.contains("\"")) {
                        //remove surrounding quotes, if present
                        lsid = lsid.replaceAll("\"", "");
                    }
                    if (lsid.contains("\\")) {
                        //remove internal \ chars, if present
                        //noinspection MalformedRegex
                        lsid = lsid.replaceAll("\\\\", "");
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("lsid = " + lsid);
                    }
                    String[] values = searchUtils.getTaxonSearch(lsid);
                    String lsidHeader = matcher.groupCount() > 1 && matcher.group(1).length() > 0 ? matcher.group(1) : "";
                    matcher.appendReplacement(queryString, lsidHeader + values[0]);
                    displaySb.append(current[0].substring(last, matcher.start()));
                    if (!values[1].startsWith("taxon_concept_lsid:")) {
                        displaySb.append(lsidHeader).append("<span class='lsid' id='").append(lsid).append("'>").append(values[1]).append("</span>");
                    } else {
                        displaySb.append(lsidHeader).append(values[1]);
                    }
                    last = matcher.end();
                }
            }

            if (last > 0) {
                matcher.appendTail(queryString);
                displaySb.append(current[1].substring(last, current[1].length()));

                current[0] = displaySb.toString();
                current[1] = queryString.toString();
            }
        }
    }

    /**
     * Format Urn in queries for formattedQuery.
     *
     * @param current String [] { displayString, formattedQuery } to update.
     */
    private void formatUrn(String [] current) {
        StringBuffer queryString = new StringBuffer();
        if (current[1].contains("urn")) {
            //escape the URN strings before escaping the rest this avoids the issue with attempting to search on a urn field
            Matcher matcher = urnPattern.matcher(current[1]);
            queryString.setLength(0);
            while (matcher.find()) {
                String value = matcher.group();
                if (logger.isDebugEnabled()) {
                    logger.debug("escaping lsid urns  " + value);
                }
                matcher.appendReplacement(queryString, prepareSolrStringForReplacement(value));
                String name = searchUtils.substituteLsidsForNames(value);
                if (name != null) {
                    current[0] = current[0].replace(value, name);
                }
            }
            matcher.appendTail(queryString);
            current[1] = queryString.toString();
        }
    }

    /**
     * Format http in queries for formattedQuery.
     *
     * @param current String [] { displayString, formattedQuery } to update.
     */
    private void formatHttp(String [] current) {
        StringBuffer queryString = new StringBuffer();
        if (current[1].contains("http")) {
            //escape the HTTP strings before escaping the rest this avoids the issue with attempting to search on a urn field
            Matcher matcher = httpPattern.matcher(current[1]);
            queryString.setLength(0);
            while (matcher.find()) {
                String value = matcher.group();

                if (logger.isDebugEnabled()) {
                    logger.debug("escaping lsid http uris  " + value);
                }
                matcher.appendReplacement(queryString, prepareSolrStringForReplacement(value));

                String name = searchUtils.substituteLsidsForNames(value);
                if (name != null) {
                    current[0].replace(value, name);
                }
            }
            matcher.appendTail(queryString);
            current[1] = queryString.toString();
        }
    }

    /**
     * Spatial query formatting for formattedQuery and displayString.
     *
     * Fixes query escaping.
     * Update displayString with *_uid values.
     * Format displayString for *:* and lat,lng,radius queries.
     *
     * @param current String [] { displayString, formattedQuery } to update.
     * @return true iff this is a spatial query.
     */
    private boolean formatSpatial(String [] current) {
        if (current[1].contains("Intersects")) {
            StringBuffer queryString = new StringBuffer();
            StringBuilder displaySb = new StringBuilder();

            Matcher matcher = spatialPattern.matcher(current[1]);
            if (matcher.find()) {
                String spatial = matcher.group();
                SpatialSearchRequestParams subQuery = new SpatialSearchRequestParams();
                if (logger.isDebugEnabled()) {
                    logger.debug("region Start : " + matcher.regionStart() + " start :  " + matcher.start() + " spatial length " + spatial.length() + " query length " + current[1].length());
                }
                //format the search query of the remaining text only
                subQuery.setQ(current[1].substring(matcher.start() + spatial.length(), current[1].length()));
                //format the remaining query
                formatSearchQuery(subQuery, false);

                //now append Q's together
                queryString.setLength(0);
                //need to include the prefix
                queryString.append(current[1].substring(0, matcher.start()));
                queryString.append(spatial);
                queryString.append(subQuery.getFormattedQuery());
                //add the spatial information to the display string
                if (spatial.contains("circles")) {
                    String[] values = spatial.substring(spatial.indexOf("=") + 1, spatial.indexOf("}")).split(",");
                    if (values.length == 3) {
                        displaySb.setLength(0);
                        displaySb.append(subQuery.getDisplayString());
                        displaySb.append(" - within ").append(values[2]).append(" km of point(")
                                .append(values[0]).append(",").append(values[1]).append(")");
                    }
                } else {
                    displaySb.append(subQuery.getDisplayString() + " - within supplied region");
                }
            }
            if (queryString.length() > 0) {
                current[0] = displaySb.toString();
                current[1] = queryString.toString();
            }
            return true;
        }
        return false;
    }

    /**
     * General formatting for formattedQuery and displayString.
     *
     * Fixes query escaping.
     * Update displayString with *_uid values.
     * Format displayString for *:* and lat,lng,radius queries.
     *
     * Not suitable for formatting queries containing Intersect, AND, OR
     *
     * @param current String [] { displayString, formattedQuery } to update.
     * @param searchParams The search parameters
     */
    private void formatGeneral(String [] current, SpatialSearchRequestParams searchParams) {
        StringBuffer queryString = new StringBuffer();

        //escape reserved characters unless the colon represents a field name colon
        Matcher matcher = spacesPattern.matcher(current[1]);
        if (!current[1].contains(" OR ") && !current[1].contains(" AND ")) {
            while (matcher.find()) {

                String value = matcher.group();

                //special cases to ignore from character escaping
                //if the value is a single - or * it means that we don't want to escape it as it is likely
                // to have occurred in the following situation -(occurrence_date:[* TO *]) or *:*
                if (!value.equals("-") && /*!value.equals("*")  && !value.equals("*:*") && */ !value.endsWith("*")) {

                    //split on the colon
                    String[] bits = StringUtils.split(value, ":", 2);
                    if (bits.length == 2) {
                        //urn and http are already encoded for SOLR
                        //range queries are not to be escaped
                        //bracketed queries are not to be escaped
                        if (!(bits[0].contains("urn") || !bits[1].contains("urn\\") || !bits[0].contains("http") || !bits[1].contains("http\\") ||
                                (bits[1].contains("[") && bits[1].contains(" TO ") && bits[1].contains("]")) ||
                                bits[0].startsWith("-(") || bits[0].startsWith("("))) {
                            matcher.appendReplacement(queryString, bits[0] + ":" + prepareSolrStringForReplacement(bits[1]));
                        } else {
                            matcher.appendReplacement(queryString, prepareSolrStringForReplacement(bits[0]) + ":" + prepareSolrStringForReplacement(bits[1]));
                        }


                    } else if (!value.endsWith(":")) {
                        //need to ignore field names where the : is at the end because the pattern matching will
                        // return field_name: as a match when it has a double quoted value
                        //default behaviour is to escape all
                        matcher.appendReplacement(queryString, prepareSolrStringForReplacement(value));
                    }
                }
            }
        }
        matcher.appendTail(queryString);

        StringBuffer displaySb = new StringBuffer();
        //substitute better display strings for collection/inst etc searches
        if (current[0].contains("_uid")) {
            displaySb.setLength(0);
            String normalised = current[0].replaceAll("\"", "");
            matcher = uidPattern.matcher(normalised);
            while (matcher.find()) {
                String newVal = "<span>" + searchUtils.getUidDisplayString(matcher.group(1), matcher.group(2)) + "</span>";
                matcher.appendReplacement(displaySb, newVal);
            }
            matcher.appendTail(displaySb);
            current[0] = displaySb.toString();
        }
        if (current[1].equals("*:*")) {
            current[0] = "[all records]";
        }
        if (searchParams != null && searchParams.getLat() != null && searchParams.getLon() != null && searchParams.getRadius() != null) {
            displaySb.setLength(0);
            displaySb.append(current[0]);
            displaySb.append(" - within ").append(searchParams.getRadius()).append(" km of point(")
                    .append(searchParams.getLat()).append(",").append(searchParams.getLon()).append(")");
            current[0] = displaySb.toString();

        }
        current[1] = queryString.length() > 0 ? queryString.toString() : current[1];
    }

    /**
     * Reverse rangeBasedFacet display strings to valid query values.
     *
     * @param current String [] { displayString, formattedQuery } to update.
     * @param isFq true iff the query is an fq. else false.
     */
    private void formatTitleMap(String [] current, boolean isFq) {
        if (isFq) {
            String[] parts = current[1].split(":", 2);
            //check to see if the first part is a range based query and update if necessary
            Map<String, String> titleMap = rangeBasedFacets.getTitleMap(parts[0]);
            if (titleMap != null) {
                current[1] = titleMap.get(parts[1]);
            }
        }
    }

    /**
     * Produce a formattedQuery and displayString for a query.
     *
     * When it is not an fq, the searchParams formattedQuery and displayString are updated.
     *
     * Additional fqs may be added to searchParams.
     *
     * @param query The query or fq
     * @param searchParams The search parameters. Can be null when isFq == true
     * @param isFq true iff the query is an fq. else false.
     * @return String [] { displayString, formattedQuery }
     */
    public String [] formatQueryTerm(String query, SpatialSearchRequestParams searchParams, boolean isFq) {
        String [] formatted = formatQid(query, searchParams, isFq);

        formatTerms(formatted);
        formatTaxa(formatted);
        formatSpeciesList(formatted);

        formatLsid(formatted);
        formatUrn(formatted);
        formatHttp(formatted);
        formatTitleMap(formatted, isFq);

        if (!formatSpatial(formatted)) {
            formatGeneral(formatted, searchParams);
        }

        formatted[0] = formatDisplayStringWithI18n(formatted[0]);

        if (!isFq) {
            searchParams.setDisplayString(formatted[0]);
            searchParams.setFormattedQuery(formatted[1]);
        }

        return formatted;
    }

    /**
     * Substitute displayText with i18n properties
     *
     * @param displayText
     * @return
     */
    public String formatDisplayStringWithI18n(String displayText) {
        if (StringUtils.trimToNull(displayText) == null) return displayText;
        try {
            String formatted = "";
            String gap;

            Matcher m = indexFieldPatternMatcher.matcher(displayText);
            int currentPos = 0;
            while (m.find(currentPos)) {
                formatted += displayText.substring(currentPos, m.start());

                String matchedIndexTerm = m.group(0);
                if (matchedIndexTerm.startsWith("<span")) {
                    formatted += matchedIndexTerm;
                    currentPos = m.end();
                } else {
                    MatchResult mr = m.toMatchResult();

                    //format facet name
                    Matcher lm = layersPattern.matcher(matchedIndexTerm);
                    matchedIndexTerm = matchedIndexTerm.replaceAll(":", "");
                    String i18n = null;
                    if (lm.matches()) {
                        i18n = layersService.getName(matchedIndexTerm);
                    }
                    if (i18n == null) {
                        i18n = messageSource.getMessage("facet." + matchedIndexTerm, null, matchedIndexTerm, null);
                    }

                    //format display value
                    //values that contain indexFieldPatternMatcher matches, e.g. urn: http:, are already replaced.
                    String extractedValue = displayText.substring(mr.end());
                    int end = 0;
                    //remove wrapping '(', '"', and check for termination with ' ' if it is not wrapped
                    if (extractedValue.startsWith("(")) {
                        extractedValue = extractedValue.substring(1, extractedValue.indexOf(')') > 1 ? extractedValue.indexOf(')') : extractedValue.length());
                        end += 2;
                    }
                    if (extractedValue.startsWith("\"")) {
                        extractedValue = extractedValue.substring(1, extractedValue.indexOf('\"', 1) > 1 ? extractedValue.indexOf('\"', 1) : extractedValue.length());
                        end += 2;
                    }
                    if (extractedValue.startsWith("[")) {
                        extractedValue = extractedValue.substring(1, extractedValue.indexOf(']') > 1 ? extractedValue.indexOf(']') : extractedValue.length());
                        end += 2;
                    }
                    if (extractedValue.contains(" ") && end == 0) {
                        extractedValue = extractedValue.substring(0, extractedValue.indexOf(' ') > 1 ? extractedValue.indexOf(' ') : extractedValue.length());
                    }

                    String formattedExtractedValue = formatValue(matchedIndexTerm, extractedValue);

                    String i18nForValue = messageSource.getMessage(matchedIndexTerm + "." + formattedExtractedValue, null, "", null);
                    if (i18nForValue.length() == 0)
                        i18nForValue = messageSource.getMessage(formattedExtractedValue, null, formattedExtractedValue, null);

                    formatted += i18n + ":" + i18nForValue;

                    currentPos = mr.end() + extractedValue.length() + end;
                }
            }
            formatted += displayText.substring(currentPos, displayText.length());

            return formatted;

        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(e.getMessage(), e);
            }
            return displayText;
        }
    }

    private String formatValue(String fn, String fv) {
        fv = SearchUtils.stripEscapedQuotes(fv);

        if (StringUtils.equals(fn, "species_guid") || StringUtils.equals(fn, "genus_guid")) {
            fv = searchUtils.substituteLsidsForNames(fv.replaceAll("\"",""));
        } else if (StringUtils.equals(fn, "occurrence_year")) {
            fv = searchUtils.substituteYearsForDates(fv);
        } else if (StringUtils.equals(fn, "month")) {
            fv = searchUtils.substituteMonthNamesForNums(fv);
        } else if (searchUtils.getAuthIndexFields().contains(fn)) {
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
            fv = searchUtils.getUidDisplayString(fn, fv, false);
        }

        return fv;
    }

    /**
     * Creates a SOLR escaped string the can be used in a StringBuffer.appendReplacement
     * The appendReplacement needs an extra delimiting on the backslashes when quoted.
     *
     * @param value
     * @return
     */
    private String prepareSolrStringForReplacement(String value) {
        if (value.equals("*")) {
            return value;
        }

        //if starts and ends with quotes just escape the inside
        boolean quoted = false;

        StringBuffer sb = new StringBuffer();
        if (value.startsWith("-")) {
            sb.append("-");
            value = value.substring(1);
        }
        if (value.startsWith("\"") && value.endsWith("\"")) {
            quoted = true;
            value = value.substring(1, value.length() - 1);
            sb.append("\"");
            sb.append(ClientUtils.escapeQueryChars(value).replaceAll("\\\\", "\\\\\\\\"));
        } else {
            sb.append(ClientUtils.escapeQueryChars(value));
        }

        if (quoted) sb.append("\"");
        return sb.toString();
    }

    protected void updateQueryContext(SearchRequestParams searchParams) {
        //TODO better method of getting the mappings between qc on solr fields names
        String qc = searchParams.getQc();
        if (StringUtils.isNotEmpty(qc)) {
            //add the query context to the filter query
            addFormattedFq(getQueryContextAsArray(qc), searchParams);
        }
    }

    public String[] getQueryContextAsArray(String queryContext) {
        if (StringUtils.isNotEmpty(queryContext)) {
            String[] values = queryContext.split(",");
            for (int i = 0; i < values.length; i++) {
                String field = values[i];
                values[i] = field.replace("hub:", "data_hub_uid:");
            }
            //add the query context to the filter query
            return values;
        }
        return new String[]{};
    }

    /**
     * Generate SOLR query from a taxa[] query
     *
     * @param taxaQueries
     * @param guidsForTaxa
     * @return
     */
    String createQueryWithTaxaParam(List taxaQueries, List guidsForTaxa) {
        StringBuilder query = new StringBuilder();

        if (taxaQueries.size() != guidsForTaxa.size()) {
            // Both Lists must the same size
            throw new IllegalArgumentException("Arguments (List) are not the same size: taxaQueries.size() (${taxaQueries.size()}) != guidsForTaxa.size() (${guidsForTaxa.size()})");
        }

        if (taxaQueries.size() > 1) {
            // multiple taxa params (array)
            query.append("(");
            for (int i = 0; i < guidsForTaxa.size(); i++) {
                String guid = (String) guidsForTaxa.get(i);
                if (i > 0) query.append(" OR ");
                if (guid != null && !guid.isEmpty()) {
                    query.append("lsid:").append(guid);
                } else {
                    query.append("text:").append(taxaQueries.get(i));
                }
            }
            query.append(")");
        } else if (guidsForTaxa.size() > 0) {
            // single taxa param
            String taxa = (String) taxaQueries.get(0);
            String guid = (String) guidsForTaxa.get(0);
            if (guid != null && !guid.isEmpty()) {
                query.append("lsid:").append(guid);
            } else if (taxa != null && !taxa.isEmpty()) {
                query.append("text:").append(taxa);
            }
        }

        return query.toString();
    }

    protected String buildSpatialQueryString(SpatialSearchRequestParams searchParams) {
        if (searchParams != null) {
            StringBuilder sb = new StringBuilder();
            if (searchParams.getLat() != null) {
                sb.append(latLonPart(searchParams));
            } else if (!StringUtils.isEmpty(searchParams.getWkt())) {
                //format the wkt
                sb.append(SpatialUtils.getWKTQuery(spatialField, searchParams.getWkt(), false));
            }
            return sb.toString();
        }
        return null;
    }

    public String latLonPart(SpatialSearchRequestParams searchParams) {
        StringBuilder sb = new StringBuilder();

        if (searchParams.getLat() != null) {
            sb.append(spatialField).append(":\"Intersects(Circle(");
            sb.append(searchParams.getLon().toString()).append(" ").append(searchParams.getLat().toString());
            sb.append(" d=").append(SpatialUtils.convertToDegrees(searchParams.getRadius()).toString());
            sb.append("))\"");
        }

        return sb.toString();
    }
}