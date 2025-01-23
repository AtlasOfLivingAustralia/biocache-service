package au.org.ala.biocache.util;

import au.org.ala.biocache.dao.QidCacheDAO;
import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.service.*;
import au.org.ala.biocache.service.ListsService.SpeciesListSearchDTO;
import au.org.ala.biocache.util.solr.FieldMappingUtil;
import au.org.ala.ws.security.profile.AlaUserProfile;
import com.google.common.collect.Iterables;
import com.google.common.html.HtmlEscapers;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.springframework.beans.InvalidPropertyException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static au.org.ala.biocache.dto.OccurrenceIndex.CONTAINS_SENSITIVE_PATTERN;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

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
    protected AuthService authService;

    @Inject
    protected DataQualityService dataQualityService;

    @Inject
    protected FieldMappingUtil fieldMappingUtil;

    protected static final String QUOTE = "\"";
    protected static final char[] CHARS = {' ', ':'};

    private String spatialField = "geohash";

    //Patterns that are used to prepare a SOLR query for execution
    protected Pattern lsidPattern = Pattern.compile("(^|\\s|\"|\\(|\\[|'|-)taxonConceptID:\"?([a-zA-Z0-9/\\.:\\-_]*)\"?");
    protected Pattern speciesListPattern = Pattern.compile("(^|\\s|\"|\\(|\\[|'|-)species_list:\"?(dr[0-9]*)\"?");

    protected Pattern spatialObjectPattern = Pattern.compile("(^|\\s|\"|\\(|\\[|'|-)spatialObject:\"?([0-9]*)\"?");
    protected Pattern urnPattern = Pattern.compile("\\burn:[a-zA-Z0-9\\.:-]*");
    protected Pattern httpPattern = Pattern.compile("https?:[a-zA-Z0-9/\\.:\\-_]*");
    protected Pattern uidPattern = Pattern.compile("(?:[\"]*)?((?:[a-z_]*_uid:)|(?:[a-zA-Z]*Uid:))(\\w*)(?:[\"]*)?");
    protected Pattern spatialPattern = Pattern.compile(spatialField + ":\"Intersects\\([a-zA-Z=\\-\\s0-9\\.\\,():]*\\)\\\"");
    protected Pattern qidPattern = QidCacheDAO.qidPattern;//Pattern.compile("qid:[0-9]*");
    protected Pattern termPattern = Pattern.compile("([a-zA-z_]+?):((\".*?\")|(\\\\ |[^: \\)\\(])+)"); // matches foo:bar, foo:"bar bash" & foo:bar\ bash
    protected Pattern indexFieldPatternMatcher = java.util.regex.Pattern.compile("<span.*?</span>|(\\b|-)[\\w*\\(]{1,}:");
    protected Pattern layersPattern = Pattern.compile("(^|\\b)(el|cl)[0-9abc]+:");
    protected Pattern taxaPattern = Pattern.compile("(^|\\s|\"|\\(|\\[|')taxa:\"?([^\"]+)\"?");

    private int maxBooleanClauses = 1024;

    @Value("${solr.circle.segments:18}")
    int solrCircleSegments = 18;

    /**
     * When facet tagging/excluding is enabled, this flag determines whether to combine
     * multiple excluded facet fields into each facet {!ex} tag.
     * @see au.org.ala.biocache.util.QueryFormatUtils#applyFilterTagging(au.org.ala.biocache.dto.SpatialSearchRequestDTO)
     */
    @Value("${facets.combineExcludedFields:false}")
    boolean combineExcludedFacetFields = false;

    /**
     * This is appended to the query displayString when SpatialSearchRequestParams.wkt is used.
     */
    @Value("${wkt.display.string: - within user defined polygon}")
    protected String wktDisplayString;

    /**
     * This is appended to the query displayString when SpatialSearchRequestParams.lat, lon, radius are used.
     */
    @Value("${circle.display.string: - within {0} km of point({1}, {2})}")
    protected String circleDisplayString;

    public int getMaxBooleanClauses() {
        return maxBooleanClauses;
    }

    public void setMaxBooleanClauses(int maxBooleanClauses) {
        this.maxBooleanClauses = maxBooleanClauses;
    }

    public Map[] formatSearchQuery(SpatialSearchRequestDTO searchParams) throws QidMissingException {
        return formatSearchQuery(searchParams, false);
    }


    /**
     * Format the search query. Note: Cacheable annotation is deliberately removed as this introduced a bug
     * for Facet Count queries.
     *
     * @param searchParams
     * @param forceQueryFormat
     * @return
     */
//    @Cacheable(cacheName = "formatSearchQuery")
    public Map[] formatSearchQuery(SpatialSearchRequestDTO searchParams, boolean forceQueryFormat) throws QidMissingException{
        Map<String, Facet> activeFacetMap = new HashMap();
        Map<String, List<Facet>> activeFacetObj = new HashMap<>();
        Map[] fqMaps = {activeFacetMap, activeFacetObj};
        Boolean isIncludeUnfilteredFacetValues = searchParams.getIncludeUnfilteredFacetValues();

        //Only format the query if it doesn't already supply a formattedQuery.
        if (forceQueryFormat || StringUtils.isEmpty(searchParams.getFormattedQuery())) {
            String[] originalFqs = searchParams.getFq();

            String [] formatted = formatQueryTerm(searchParams.getQ(), searchParams);
            searchParams.setDisplayString(formatted[0]);
            searchParams.setFormattedQuery(formatted[1]);

            //reset formattedFq in case of searchParams reuse
            searchParams.setFormattedFq(null);

            // Apply filter tagging and excluding filters if flag is set
            if (isIncludeUnfilteredFacetValues) {
                applyFilterTagging(searchParams);
            }

            //format fqs for facets that need ranges substituted
            if (searchParams.getFq() != null) {
                for (int i = 0; i < searchParams.getFq().length; i++) {
                    String fq = searchParams.getFq()[i];
                    String fqOriginal = originalFqs[i]; // not altered by `applyFilterTagging()`

                    if (fq != null && !fq.isEmpty()) {
                        formatted = formatQueryTerm(fq, searchParams);
                        String[] formattedOriginal = formatQueryTerm(fqOriginal, searchParams);

                        if (StringUtils.isNotEmpty(formatted[1])) {
                            addFormattedFq(new String[]{formatted[1]}, searchParams);
                        }

                        // add to activeFacetMap fqs that are not inserted by a qid, and the q of qids in fqs.
                        // do not add spatial fields
                        if (originalFqs != null && originalFqs.length > i && !formatted[1].contains(spatialField + ":")) {
                            Facet facet = new Facet();
                            facet.setDisplayName(isIncludeUnfilteredFacetValues ? formattedOriginal[0] : formatted[0]);
                            String[] fv = fqOriginal.split(":");
                            if (fv.length >= 2) {
                                facet.setName(fv[0]);
                                facet.setValue(fqOriginal.substring(fv[0].length() + 1));
                            }
                            activeFacetMap.put(facet.getName(), facet);

                            // activeFacetMap is based on the assumption that each fq is on different filter so its a [StringKey: Facet] structure
                            // but actually different fqs can use same filter key for example &fq=-month:'11'&fq=-month='12' so we added a new map
                            // activeFacetObj which is [StringKey: List<Facet>]
                            String fqKey = parseFQ(fqOriginal);
                            if (fqKey != null) {
                                String formattedFacet = (isIncludeUnfilteredFacetValues) ? formattedOriginal[0] : formatted[0];
                                Facet fct = new Facet(fqKey, formattedFacet); // display name is the formatted name, for example '11' to 'November'
                                fct.setValue(fqOriginal); // value in activeFacetMap is the part with key replaced by '', but here is the original fq because front end will need it
                                List<Facet> valList = activeFacetObj.getOrDefault(fqKey, new ArrayList<>());
                                valList.add(fct);
                                activeFacetObj.put(fqKey, valList);
                            }
                        }
                    }
                }
            }

            //remove any fqs that were added
            searchParams.setFq(originalFqs);

            //add spatial query term for wkt or lat/lon/radius parameters. DisplayString is already added by formatGeneral
            String spatialQuery = buildSpatialQueryString(searchParams);

            if (StringUtils.isNotEmpty(spatialQuery)) {
                addFormattedFq(new String[] { spatialQuery }, searchParams);
            }

            updateQualityProfileContext(searchParams);
        }

        updateQueryContext(searchParams);
        return fqMaps;
    }

    /**
     * Apply facet tagging and filter exclusions to the search request.
     *
     * Note: due to bug/feature in SOLRJ, the excluded facets are added to
     * the facet pivot list instead of the facet list, otherwise SOLRJ will
     * ignore the facets with counts greater than totalRecords count, when generating
     * the facetResults.
     *
     * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
     * @date 2025-01-09
     *
     * @param searchParams
     */
    private void applyFilterTagging(SpatialSearchRequestDTO searchParams) {
        List<String> facetList = new ArrayList<String>();
        List<String> facetPivotList = new ArrayList<String>();
        List<String> fqList = new ArrayList<String>();

        // Get a list of excluded fields
        List<String> excludedFields = Arrays.stream(searchParams.getFacets())
                .filter(f -> searchParams.getFq() != null && Arrays.stream(searchParams.getFq()).anyMatch(fq -> fq.contains(f)))
                .collect(Collectors.toList());

        // Add the excluded fields to the facetPivotList || facetList
        for (String f : searchParams.getFacets()) {
            if (excludedFields.contains(f)) {
                String prefix = combineExcludedFacetFields ? "{!ex=" + String.join(",", excludedFields) + "}" : "{!ex=" + f + "}";
                facetPivotList.add(prefix + f);
            } else {
                facetList.add(f);
            }
        }

        // Add the tag syntax to the fqList, if the fq is a facet
        if (searchParams.getFq() != null) {
            for (String fq : searchParams.getFq()) {
                String fqField = org.apache.commons.lang3.StringUtils.substringBefore(fq, ":");
                if (Arrays.asList(searchParams.getFacets()).contains(fqField)) {
                    String prefix = "{!tag=" + fqField + "}";
                    fqList.add(prefix + fq);
                } else {
                    fqList.add(fq);
                }
            }
        }

        // Update the searchParams with the new facetPivotList, facetList, and fqList
        searchParams.setPivotFacets(facetPivotList.toArray(new String[0]));
        searchParams.setFacets(facetList.toArray(new String[0]));
        searchParams.setFq(fqList.toArray(new String[0]));
    }

    /**
     * To retrieve the key from a fq
     *
     * This method assumes fq is in one of below format
     * fq = key:val or fq = -key:val
     * fq = (key:val) or fq = (-key:val)
     * fq = -(key:val) or fq = -(-key:val)
     * fq = key:val1 OR key:val2 or fq = -key:val1 OR -key:val2
     * fq = (key:val1 OR key:val2) or fq = (-key:val1 OR -key:val2)
     * fq = -(key:val1 OR key:val2) or fq = -(-key:val1 OR -key:val2)
     *
     * a new fq format added 18/09/2020
     * fq = license:"CC BY NC" license:"CC BY NC 4.0 (Int)"
     *
     * inclusive and exclusive keys can't co-exist in one fq
     */
    private String parseFQ(String fq) {
        fq = StringUtils.trimToEmpty(fq);
        if ((fq.startsWith("-(") || fq.startsWith("(")) && !fq.endsWith(")")) {
            return null;
        }

        boolean globalInclusive = true;
        if (fq.startsWith("-(") && fq.endsWith(")")) {
            fq = fq.substring(2, fq.length() - 1);
            globalInclusive = false;
        } else if (fq.startsWith("(") && fq.endsWith(")")) {
            fq = fq.substring(1, fq.length() - 1);
        }

        if (StringUtils.isNotEmpty(fq)) {
            String[] fv = fq.split(":");
            if (fv.length >= 2 && StringUtils.isNotBlank(fv[0]) && StringUtils.isNotBlank(fv[1])) {
                boolean localInclusive = fv[0].charAt(0) != '-';
                String key = localInclusive ? fv[0] : fv[0].substring(1);

                return globalInclusive ^ localInclusive ? "-" + key : key;
            }
        }

        return null;
    }

    public void addFqs(String [] fqs, SpatialSearchRequestDTO searchParams) {
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

    private void addFormattedFq(String [] fqs, SearchRequestDTO searchParams) {
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
     * @param searchParams
     * @return
     */
    private String [] formatQid(String query, SpatialSearchRequestDTO searchParams) throws QidMissingException{
        String q = query;
        String displayString = query;
        if (query != null && query.contains("qid:")) {
            Matcher matcher = qidPattern.matcher(query);
            int count = 0;
            while (matcher.find()) {
                String value = matcher.group();
                try {
                    String qidValue = SearchUtils.stripEscapedQuotes(value.substring(4));
                    Qid qid = qidCacheDao.get(qidValue);
                    if (qid != null) {
                        if (count > 0) {
                            //add qid to fq when >1 qid is already found
                            addFqs(new String[] { qid.getQ() }, searchParams);
                        } else if (qid.getQ().contains("qid:")) {
                            String [] interior = formatQid(qid.getQ(), searchParams);
                            displayString = interior[0];
                            q = interior[1];
                        } else {
                            q = qid.getQ();
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

                        count = count + 1;
                    } else {
                        throw new QidMissingException("Unrecognised QID: " + searchParams.getQ()    );
                    }
                } catch (NumberFormatException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return new String[] {displayString, q};
    }

    /**
     * Substitute matched_name_children and matched_name with the correct formattedQuery and displayString.
     *
     * @param current String [] { displayString, formattedQuery } to update.
     */
    private String[] formatTerms(String[] current) {
        // look for field:term sub queries and catch fields: matched_name & matched_name_children
        if (current != null && current.length >= 2 && current[1] != null && current[1].contains(":")) {
            String taxonName = OccurrenceIndex.TAXON_NAME;

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
                                field = taxonName;
                                queryText = acceptedName;
                            }
                        } else {
                            field = taxonName;
                        }

                        // also change the display query
                        current[0] = current[0].replaceAll("matched_name", taxonName);
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
                            field = taxonName;
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
        if (current != null && current.length >=2 && current[1] != null && current[1].contains("taxa:")) {
            StringBuffer queryString = new StringBuffer();
            Matcher matcher = taxaPattern.matcher(current[1]);
            queryString.setLength(0);
            while (matcher.find()) {
                String field = matcher.group(1);
                String taxa = matcher.group(2);

                if (logger.isDebugEnabled()) {
                    logger.debug("found taxa " + taxa);
                }

                List<String> taxaQueries = new ArrayList<>();
                taxaQueries.add(taxa);
                List<String> guidsForTaxa = speciesLookupService.getGuidsForTaxa(taxaQueries);
                String q = createQueryWithTaxaParam(taxaQueries, guidsForTaxa);

                matcher.appendReplacement(queryString, field + q);
            }

            matcher.appendTail(queryString);

            current[1] = queryString.toString();
            current[0] = current[1];
        } else if(StringUtils.isNotBlank(current[1]) && !current[1].contains(":")) {
            //attempt to just match the string to a taxon
            List<String> taxaQueries = new ArrayList<>();
            taxaQueries.add(current[1]);
            List<String> guidsForTaxa = speciesLookupService.getGuidsForTaxa(taxaQueries);
            if(guidsForTaxa.size() == 1){
                String q = createQueryWithTaxaParam(taxaQueries, guidsForTaxa);
                current[1] = q;
            }
        }
        return current;
    }

    /**
     * Substitute lft ranges for species lists in queries for formattedQuery and displayString.
     *
     * @param current String [] { displayString, formattedQuery } to update.
     */
    private void formatSpeciesList(String[] current) {
        if (current == null || current.length < 2 || current[1] == null) {
            return;
        }

        //if the query string contains species_list: replace with the equivalent (lsid: OR lsid: ...etc) before lsid: is parsed
        StringBuffer sb = new StringBuffer();
        Matcher m = speciesListPattern.matcher(current[1]);
        int max = getMaxBooleanClauses();
        HashSet<String> failedLists = new HashSet<>();
        while (m.find()) {
            String speciesList = m.group(2);
            String prefix = m.group(1);
            try {
                List<ListsService.SpeciesListItemDTO> lsids = listsService.getListItems(speciesList, false);

                List<String> strings;

                strings = lsids.stream().map(t -> t.lsid)
                        .map(searchUtils::getTaxonSearch)
                        .filter(t -> t.length > 1)
                        .map(t -> t[0])
                        .collect(toList());

                Iterable<List<String>> partition = Iterables.partition(strings, max - 10);
                String q = stream(partition.spliterator(), false)
                        .map(part -> part.stream()
                                .collect(joining(" OR ", "(", ")")))
                        .collect(joining(" OR "));
                if (q.length() > 1) {
                    q = "(" + q + ")";
                }
                m.appendReplacement(sb, prefix + q);
            } catch (Exception e) {
                logger.error("failed to get species list: " + speciesList);
                if (logger.isDebugEnabled()){
                    logger.debug("Failed to get species list: " + speciesList, e);
                }
                m.appendReplacement(sb, prefix + "(NOT *:*)");
                failedLists.add(speciesList);
            }
        }
        m.appendTail(sb);
        current[1] = sb.toString();

        sb = new StringBuffer();
        m = speciesListPattern.matcher(current[0]);
        while (m.find()) {
            String speciesList = m.group(2);
            String prefix = m.group(1);
            if (failedLists.contains(speciesList)) {
                m.appendReplacement(sb, prefix + "<span class=\"species_list failed\" id='" + HtmlEscapers.htmlEscaper().escape(speciesList) + "'>" + HtmlEscapers.htmlEscaper().escape(speciesList) + " (FAILED)</span>");
            } else {
                try {
                    SpeciesListSearchDTO.SpeciesListDTO dto = listsService.getListInfo(speciesList);
                    String name = dto.listName;
                    m.appendReplacement(sb, prefix + "<span class='species_list' id='" + HtmlEscapers.htmlEscaper().escape(speciesList) + "'>" + HtmlEscapers.htmlEscaper().escape(name) + "</span>");
                } catch (Exception e) {
                    logger.error("Couldn't get species list name for " + speciesList, e);
                    m.appendReplacement(sb, prefix + "<span class='species_list' id='" + HtmlEscapers.htmlEscaper().escape(speciesList) + "'>Species list</span>");
                }
            }
        }
        m.appendTail(sb);
        current[0] = sb.toString();
    }

    /**
     * Substitute lft ranges for lsids in queries for formattedQuery and displayString.
     *
     * @param current String [] { displayString, formattedQuery } to update.
     */
    private void formatLsid(String[] current) {
        if (current == null || current.length < 2 || current[1] == null) {
            return;
        }

        // TODO: remove the translation.
        // translation has been performed prior to formatting lsid (in formatQueryTerm)
        // other formatting functions called prior should not use not inject query fields that need translation.
        String translatedQuery = fieldMappingUtil.translateQueryFields(current[1]);

        //if the query string contains lsid: we will need to replace it with the corresponding lft range
        if (translatedQuery.contains("taxonConceptID:")) {
            StringBuffer queryString = new StringBuffer();
            StringBuffer displaySb = new StringBuffer();
            int last = 0;

            Matcher matcher = lsidPattern.matcher(translatedQuery);
            queryString.setLength(0);
            while (matcher.find()) {
                //only want to process the "lsid" if it does not represent taxon_concept_lsid etc...
                if ((matcher.start() > 0 && translatedQuery.charAt(matcher.start() - 1) != '_') || matcher.start() == 0) {
                    String value = matcher.group();
                    if (logger.isDebugEnabled()) {
                        logger.debug("pre-processing " + value);
                    }
                    String lsidHeader = matcher.group(1);
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

                    String taxonConceptId = OccurrenceIndex.TAXON_CONCEPT_ID;

                    if (value != null && values.length > 0) {

                        matcher.appendReplacement(queryString, lsidHeader + values[0]);

                        displaySb.append(translatedQuery.substring(last, matcher.start()));
                        if (!values[1].startsWith(taxonConceptId + ":")) {
                            displaySb.append(lsidHeader).append("<span class='lsid' id='").append(lsid).append("'>").append(values[1]).append("</span>");
                        } else {
                            displaySb.append(lsidHeader).append(values[1]);
                        }
                        last = matcher.end();
                    } else {
                        logger.error("Unable to match LSID for query expansion : " + lsid);
                    }
                }
            }

            if (last > 0) {
                matcher.appendTail(queryString);
                displaySb.append(translatedQuery.substring(last));

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
        if(current == null || current.length < 2 || current[1] == null){
            return;
        }

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
                matcher.appendReplacement(queryString, prepareSolrStringForReplacement(value, true));
                //this lsid->name replacement is too slow
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
        if(current == null || current.length < 2 || current[1] == null){
            return;
        }

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
                matcher.appendReplacement(queryString, prepareSolrStringForReplacement(value, true));

                //this lsid->name replacement is too slow
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
    private boolean formatSpatial(String [] current) throws QidMissingException {
        if(current == null || current.length < 2 || current[1] == null){
            return false;
        }

        if (current[1].contains("Intersects")) {
            StringBuffer queryString = new StringBuffer();
            StringBuilder displaySb = new StringBuilder();

            Matcher matcher = spatialPattern.matcher(current[1]);
            if (matcher.find()) {
                String spatial = matcher.group();
                SpatialSearchRequestDTO subQuery = new SpatialSearchRequestDTO();
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
     * Insert spatialObject WKT.
     *
     * If the query string contains spatialObject: replace with the equivalent geohash:Intersects(WKT)
     *
     * @param current
     * @return
     * @throws QidMissingException
     */
    private void formatSpatialObject(String [] current) {
        if (current == null || current.length < 2 || current[1] == null) {
            return;
        }

        //if the query string contains spatialObject: replace with the equivalent geohash:Intersects(WKT)
        StringBuffer sb = new StringBuffer();
        Matcher m = spatialObjectPattern.matcher(current[1]);
        int max = getMaxBooleanClauses();
        HashSet<String> failedObjects = new HashSet<>();
        while (m.find()) {
            String spatialObjectId = m.group(2);
            String prefix = m.group(1);
            try {
                String wkt = layersService.getObjectWkt(spatialObjectId);

                if (wkt == null) {
                    throw new Exception("invalid object id");
                }
                String q = prefix + spatialField + ":\"Intersects(" + wkt + ")\"";

                m.appendReplacement(sb, q);
            } catch (Exception e) {
                logger.error("failed to get WKT for object: " + spatialObjectId);
                m.appendReplacement(sb, prefix + "(NOT *:*)");
                failedObjects.add(spatialObjectId);
            }
        }
        m.appendTail(sb);
        current[1] = sb.toString();

        sb = new StringBuffer();
        m = spatialObjectPattern.matcher(current[0]);
        while (m.find()) {
            String spatialObjectId = m.group(2);
            String prefix = m.group(1);
            if (failedObjects.contains(spatialObjectId)) {
                m.appendReplacement(sb, prefix + "<span class=\"spatialObject failed\" id='" + HtmlEscapers.htmlEscaper().escape(spatialObjectId) + "'>" + HtmlEscapers.htmlEscaper().escape(spatialObjectId) + " (FAILED)</span>");
            } else {
                try {
                    SpatialObjectDTO obj = layersService.getObject(spatialObjectId);
                    String name = obj.getName();
                    m.appendReplacement(sb, prefix + "<span class='spatialObject' id='" + HtmlEscapers.htmlEscaper().escape(spatialObjectId) + "'>" + HtmlEscapers.htmlEscaper().escape(name) + "</span>");
                } catch (Exception e) {
                    logger.error("Couldn't get spatial object name for " + spatialObjectId, e);
                    m.appendReplacement(sb, prefix + "<span class='spatialObject' id='" + HtmlEscapers.htmlEscaper().escape(spatialObjectId) + "'>Species list</span>");
                }
            }
        }
        m.appendTail(sb);
        current[0] = sb.toString();
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
    private void formatGeneral(String [] current, SpatialSearchRequestDTO searchParams) {
        if(current == null || current.length < 2 || current[1] == null){
            return;
        }

        current[1] = formatString(current[1], true);

        Matcher matcher;
        StringBuffer displaySb = new StringBuffer();
        //substitute better display strings for collection/inst etc searches
        if (current[0].contains("_uid") || current[0].contains("Uid")) {
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
        if (searchParams != null) {
            if (searchParams.getLat() != null && searchParams.getLon() != null && searchParams.getRadius() != null) {
                current[0] += MessageFormat.format(circleDisplayString, searchParams.getRadius(), searchParams.getLat(),
                        searchParams.getLon());
            } else if(StringUtils.isNotEmpty(searchParams.getWkt())) {
                current[0] += wktDisplayString;
            }
        }
    }

    /**
     * Reverse rangeBasedFacet display strings to valid query values.
     *
     * @param current String [] { displayString, formattedQuery } to update.
     */
    private void formatTitleMap(String [] current) {
        if(current == null || current.length < 2 || current[1] == null){
            return;
        }

        String[] parts = current[1].split(":", 2);

        if(rangeBasedFacets != null && parts != null && parts.length > 0) {
            //check to see if the first part is a range based query and update if necessary
            Map<String, String> titleMap = rangeBasedFacets.getTitleMap(parts[0]);
            if (titleMap != null && titleMap.size() > 0) {
                String q = titleMap.get(parts[1]);
                if (StringUtils.isNotEmpty(q)) {
                    current[1] = q;
                }
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
     * @param searchParams The search parameters.
     * @return String[] { displayString, formattedQuery }
     */
    public String[] formatQueryTerm(String query, SpatialSearchRequestDTO searchParams) throws QidMissingException {

        String tQuery = fieldMappingUtil.translateQueryFields(query);
        String [] formatted = formatQid(tQuery, searchParams);

        formatTerms(formatted);
        formatTaxa(formatted);
        formatSpeciesList(formatted);

        formatLsid(formatted);
        formatUrn(formatted);
        formatHttp(formatted);
        formatTitleMap(formatted);

        formatSpatialObject(formatted);

        if (!formatSpatial(formatted)) {
            formatGeneral(formatted, searchParams);
        }

        formatted[0] = formatString(formatted[0], false);

        return formatted;
    }

    /**
     * Substitute text with i18n properties or escape for SOLR.
     *
     * @param text    String to format
     * @param isQuery
     * @return
     */
    public String formatString(String text, boolean isQuery) {
        if (StringUtils.trimToNull(text) == null) return text;

        // Queries containing OR, AND or Intersects( must already be correctly escaped for SOLR
        // Note: if escaping is required, extract expressions from nested () [] "" for escaping with formatString.
        if (isQuery && text.contains(" OR ") || text.contains(" AND ") || text.contains("Intersects(")) return text;

        try {
            String formatted = "";

            Matcher m = indexFieldPatternMatcher.matcher(text);
            int currentPos = 0;
            while (m.find(currentPos)) {
                formatted += text.substring(currentPos, m.start());

                String matchedIndexTerm = m.group(0);
                if (matchedIndexTerm.startsWith("<span")) {
                    formatted += matchedIndexTerm;
                    currentPos = m.end();
                } else {
                    MatchResult mr = m.toMatchResult();

                    if (matchedIndexTerm.startsWith("-(")) {
                        matchedIndexTerm = matchedIndexTerm.substring(2);
                        formatted += "-(";
                    } else if (matchedIndexTerm.startsWith("-")) {
                        matchedIndexTerm = matchedIndexTerm.substring(1);
                        formatted += "-";
                    }

                    //format facet name
                    String i18n = null;
                    if (isQuery) {
                        i18n = matchedIndexTerm;
                    } else {
                        Matcher lm = layersPattern.matcher(matchedIndexTerm);
                        matchedIndexTerm = matchedIndexTerm.replaceAll(":", "");
                        if (lm.matches()) {
                            i18n = layersService.getName(matchedIndexTerm);
                        }
                        if (i18n == null) {
                            i18n = messageSource.getMessage("facet." + fieldMappingUtil.translateFieldName(matchedIndexTerm), null, matchedIndexTerm, null);
                        }
                        i18n += ":";
                    }

                    //format display value
                    //values that contain indexFieldPatternMatcher matches, e.g. urn: http:, are already replaced.
                    String extractedValue = text.substring(mr.end());

                    int end = 0;
                    //remove wrapping '(', '"', and check for termination with ' ' if it is not wrapped
                    if (extractedValue.startsWith("(")) {
                        extractedValue = extractedValue.substring(1, extractedValue.indexOf(')') > 1 ? extractedValue.indexOf(')') : extractedValue.length());
                        end += 2;
                    }
                    if (extractedValue.startsWith("\"")) {
                        // if it contains OR, isQuery == false
                        if (extractedValue.contains(" OR ")) {
                            extractedValue = extractedValue.substring(0, extractedValue.indexOf(" OR "));
                        }

                        // search for term in the extractedValue and clip
                        // NOTE: the if the quoted term value contains content that looks like a term "name" then it will be
                        //       treated as a new term.
                        Matcher termMatcher = termPattern.matcher(extractedValue);
                        if (termMatcher.find()) {
                            extractedValue = extractedValue.substring(0, termMatcher.start());
                        }

                        // below code fragment extracts the filter value and try to format for solr query or get display value
                        // &fq = taxon_name:""Cyclophora"+lechriostropha"
                        // the old implementation yields this fq to be sent to solr: taxon_name:"\"Cyclophora",
                        // instead of start from left, we need to search from right for the first unescaped "
                        // so the correct fq taxon_name:"\"Cyclophora\"+lechriostropha" is sent to solr
                        //
                        // if the query has an 'OR', for example month:"03" OR month:"04", we can't directly start from right
                        // sine it will extract "03" OR month:"04" as a value. we need to split it with ' OR '

                        //unescape \\ and \"
                        extractedValue = extractedValue.replace("\\\\", "\\").replace("\\\"", "\"");

                        int pos = extractedValue.length() - 1;
                        // find last unescaped "
                        while ((pos = extractedValue.lastIndexOf('\"', pos)) >= 0) {
                            if ((pos == 0) || (extractedValue.charAt(pos - 1) != '\\')) break;
                            pos--;
                        }

                        // there are 2 \"
                        if (pos >= 1) {
                            extractedValue = extractedValue.substring(1, pos);
                        } else { // there's only 1 \"
                            extractedValue = extractedValue.substring(1, extractedValue.length());
                        }
                        end += 2;
                    }
                    boolean skipEncoding = false;
                    if (extractedValue.startsWith("[")) {
                        skipEncoding = true;
                        extractedValue = extractedValue.substring(1, extractedValue.indexOf(']') > 1 ? extractedValue.indexOf(']') : extractedValue.length());
                        end += 2;
                    }

                    if (extractedValue.endsWith(")") && end == 0) {
                        extractedValue = extractedValue.substring(0, extractedValue.length() - 1);
                        end += 1;
                    } else if (extractedValue.contains(" ") && end == 0) {
                        extractedValue = extractedValue.substring(0, extractedValue.indexOf(' ') >= 1 ? extractedValue.indexOf(' ') : extractedValue.length());
                    }

                    String i18nForValue;
                    if (skipEncoding && isQuery) {
                        i18nForValue = extractedValue;
                    } else if (isQuery) {
                        // some values are already encoded
                        if (!extractedValue.contains("http\\") && !extractedValue.contains("urn\\")) {
                            i18nForValue = prepareSolrStringForReplacement(extractedValue, false);
                        } else {
                            i18nForValue = extractedValue;
                        }
                    } else {
                        String formattedExtractedValue = formatValue(matchedIndexTerm, extractedValue);
                        i18nForValue = messageSource.getMessage(fieldMappingUtil.translateFieldName(matchedIndexTerm) + "." + formattedExtractedValue, null, formattedExtractedValue, null);
                    }

                    formatted += i18n + text.substring(mr.end(), mr.end() + extractedValue.length() + end).replace(extractedValue, i18nForValue);

                    currentPos = mr.end() + extractedValue.length() + end;
                }
            }

            formatted += text.substring(currentPos, text.length());

            return formatted;

        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(e.getMessage(), e);
            }
            return text;
        }
    }

    private String formatValue(String fn, String fv) {
        fv = SearchUtils.stripEscapedQuotes(fv);

        String speciesGuid = OccurrenceIndex.SPECIESID;
        String genusGuid = OccurrenceIndex.GENUSID;
        String occurrenceYear = OccurrenceIndex.OCCURRENCE_YEAR_INDEX_FIELD;
        String month = OccurrenceIndex.MONTH;

        String tfn = fieldMappingUtil.translateFieldName(fn);

        if (StringUtils.equals(tfn, speciesGuid) || StringUtils.equals(tfn, genusGuid)) {
            fv = searchUtils.substituteLsidsForNames(fv.replaceAll("\"", ""));
        } else if (StringUtils.equals(tfn, occurrenceYear)) {
            fv = searchUtils.substituteYearsForDates(fv);
        } else if (StringUtils.equals(tfn, month)) {
            fv = searchUtils.substituteMonthNamesForNums(fv);
        } else if (searchUtils.getAuthIndexFields().contains(tfn)) {
            String cfv = StringUtils.remove(fv, "\"");
            Optional<AlaUserProfile> profile = authService.lookupAuthUser(cfv, false);
            if (profile.isPresent()) {
                fv = profile.get().getName();
            }
        } else if (StringUtils.contains(fv, "@")) {
            String cfv = StringUtils.remove(fv, "\"");
            Optional<AlaUserProfile> profile = authService.lookupAuthUser(cfv, false);
            if (profile.isPresent()) {
                fv = profile.get().getName();
            } else {
                fv = fv.replaceAll("\\@\\w+", "@.."); // hide email addresses
            }
        } else {
            fv = searchUtils.getUidDisplayString(tfn, fv, false);
        }

        return fv;
    }

    /**
     * Creates a SOLR escaped string the can be used in a StringBuffer.appendReplacement
     *
     * @param value
     * @return
     */
    private String prepareSolrStringForReplacement(String value, boolean forMatcher) {
        if (value.equals("*")) {
            return value;
        }
        StringBuffer sb = new StringBuffer();
        if (forMatcher) {
            sb.append(ClientUtils.escapeQueryChars(value).replaceAll("\\\\", "\\\\\\\\"));
        } else {
            sb.append(ClientUtils.escapeQueryChars(value));
        }

        // unescape the trailing * character
        if (sb.length() > 1 && "\\*".equals(sb.substring(sb.length() -2))) {
            sb.replace(sb.length() -2, sb.length(), "*");
        }

        return sb.toString();
    }

    protected void updateQueryContext(SearchRequestDTO searchParams) {
        //TODO better method of getting the mappings between qc on solr fields names
        String qc = searchParams.getQc();
        if (StringUtils.isNotEmpty(qc)) {
            //add the query context to the filter query
            addFormattedFq(getQueryContextAsArray(qc), searchParams);
        }
    }

    public String[] getQueryContextAsArray(String queryContext) {
        String dataHubUid = OccurrenceIndex.DATA_HUB_UID;
        if (StringUtils.isNotEmpty(queryContext)) {
            String[] values = queryContext.split(",");
            for (int i = 0; i < values.length; i++) {
                String field = values[i];
                values[i] = field.replace("hub:", dataHubUid + ":");
            }
            //add the query context to the filter query
            return values;
        }
        return new String[]{};
    }

    protected void updateQualityProfileContext(SearchRequestDTO searchParams) {
        Map<String, String> enabledFiltersByLabel = dataQualityService.getEnabledFiltersByLabel(searchParams);
        String[] enabledFilters = enabledFiltersByLabel.values().toArray(new String[0]);
        addFormattedFq(enabledFilters, searchParams);
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
            throw new IllegalArgumentException("Arguments (List) are not the same size: taxaQueries.size() (" + taxaQueries.size() +") != guidsForTaxa.size() (" + guidsForTaxa.size() +")");
        }

        if (taxaQueries.size() > 1) {
            // multiple taxa params (array)
            query.append("(");
            for (int i = 0; i < guidsForTaxa.size(); i++) {
                String guid = (String) guidsForTaxa.get(i);
                if (i > 0) query.append(" OR ");
                if (guid != null && !guid.isEmpty()) {
                    query.append(OccurrenceIndex.TAXON_CONCEPT_ID + ":").append(guid);
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
                query.append(OccurrenceIndex.TAXON_CONCEPT_ID + ":").append(guid);
            } else if (taxa != null && !taxa.isEmpty()) {
                query.append("text:").append(taxa);
            }
        }

        return query.toString();
    }

    public String buildSpatialQueryString(SpatialSearchRequestDTO searchParams) {
        if (searchParams != null) {
            StringBuilder sb = new StringBuilder();
            if (searchParams.getLat() != null) {
                String wkt = createCircleWkt(searchParams.getLon(), searchParams.getLat(), searchParams.getRadius());
                sb.append(spatialField).append(":\"Intersects(").append(wkt).append(")\"");
            } else if (!StringUtils.isEmpty(searchParams.getWkt())) {
                //format the wkt
                sb.append(SpatialUtils.getWKTQuery(spatialField, searchParams.getWkt(), false));
            }
            return sb.toString();
        }
        return null;
    }

    /**
     * Create circle WKT
     *
     * @param longitude decimal degrees
     * @param latitude decimal degrees
     * @param radius km
     */
    private String createCircleWkt(double longitude, double latitude, double radius) {
        //radius to m
        radius *= 1000;

        boolean belowMinus180 = false;
        int step = 360 / solrCircleSegments;
        double[][] points = new double[360/step][];
        for (int i = 0; i < 360; i+=step) {
            points[i/step] = computeOffset(latitude, 0, radius, i);
            if (points[i/step][0] + longitude < -180) {
                belowMinus180 = true;
            }
        }

        //longitude translation
        double dist = ((belowMinus180) ? 360 : 0) + longitude;

        StringBuilder s = new StringBuilder();
        s.append("POLYGON((");
        for (int i = 0; i < 360; i+=step) {
            s.append(points[i/step][0] + dist).append(" ").append(points[i/step][1]).append(",");
        }
        // append the first point to close the circle
        s.append(points[0][0] + dist).append(" ").append(points[0][1]);
        s.append("))");

        return s.toString();
    }

    private double[] computeOffset(double lat, double lng, double radius, int angle) {
        double b = radius / 6378137.0;
        double c = angle * (Math.PI / 180.0);
        double e = lat * (Math.PI / 180.0);
        double d = Math.cos(b);
        b = Math.sin(b);
        double f = Math.sin(e);
        e = Math.cos(e);
        double g = d * f + b * e * Math.cos(c);

        double x = (lng * (Math.PI / 180.0) + Math.atan2(b * e * Math.sin(c), d - f * g)) / (Math.PI / 180.0);
        double y = Math.asin(g) / (Math.PI / 180.0);

        return new double[]{x, y};
    }

    public static void assertNoSensitiveValues(Class c, String property, String input) {
        if (input != null && input.matches(CONTAINS_SENSITIVE_PATTERN)) {
            InvalidPropertyException e = new InvalidPropertyException(c, property, "Input cannot contain any of: " + org.apache.commons.lang3.StringUtils.join(OccurrenceIndex.sensitiveSOLRHdr, ", "));
            logger.error("Input matches a sensitive field", e);
            throw e;
        }

    }

    public static void assertNoSensitiveValues(Class c, String property, String[] inputs) {
        if (inputs != null) {
            for (String input : inputs) {
                assertNoSensitiveValues(c, property, input);
            }
        }
    }
}
