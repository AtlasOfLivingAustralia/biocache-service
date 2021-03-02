/**************************************************************************
 * Copyright (C) 2010 Atlas of Living Australia
 * All Rights Reserved.
 * <p>
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 * <p>
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.biocache.web;

import au.org.ala.biocache.dao.IndexDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import net.sf.ehcache.CacheManager;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.common.util.NamedList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.inject.Inject;
import javax.servlet.ServletContext;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 */
@Controller
@Api(basePath = "/", value = "/", description = "Home Controller")
public class ChartController extends AbstractSecureController implements Serializable {
    /**
     * Logger initialisation
     */
    private final static Logger logger = Logger.getLogger(ChartController.class);
    /**
     * Fulltext search DAO
     */
    @Inject
    protected SearchDAO searchDAO;
    @Inject
    protected IndexDAO indexDao;


    @Autowired
    private ServletContext servletContext;

    @Resource(name = "cacheManager")
    private CacheManager cacheManager;

    @Value("${charts.series.max:5}")
    private Integer maxSeriesFacets;

    @Value("${charts.facets.string.max:50}")
    private Integer maxStringFacets;

    @Value("${charts.facets.number.max:50}")
    private Integer maxNumberFacets;


    /**
     * Supports various chart types
     * 1. occurrence bar/pie/line chart of field
     * x=field
     * 2. mean/max/min/quartile of field2, bar/pie/line chart of field1
     * x=field1
     * stats=field2
     * 3. occurrence bar/pie chart of numeric field with predefined ranges (min to <= value1; >value1 <= value2; > value2 <= max)
     * x=field
     * xranges=min,value1,value2,max
     * 4. mean/max/min/quartile of field2, occurrence bar/pie chart of numeric field1 with predefined ranges (min to <= value1; >value1 <= value2; > value2 <= max)
     * x=field1
     * xranges=min,value1,value2,max
     * stats=field2
     * <p>
     * stats are only available to numeric fields
     *
     * additional chart series can be generated with
     * series=field
     * seriesranges=min,value1,value2,max
     *
     * @param searchParams
     * @param x
     * @param xranges
     * @param stats
     * @param response
     * @return
     * @throws Exception
     */
    @ApiOperation(value = "Standard charting",
            notes = "Generate data for a standard chart",
            response = FieldStatsItem.class, responseContainer = "List")
    @RequestMapping(value = "/chart", method = RequestMethod.GET)
    public
    @ResponseBody
    Map chart(SpatialSearchRequestParams searchParams,
              @RequestParam(value = "x", required = false) String x,
              @RequestParam(value = "xranges", required = false) String xranges,
              @RequestParam(value = "stats", required = false) String stats,
              // default stats value is only for backwards compatability
              @RequestParam(value = "statType", required = false, defaultValue = "min,max,mean,missing,stddev,count,sum") String statType,
              @RequestParam(value = "series", required = false) String series,
              @RequestParam(value = "seriesranges", required = false) String seriesranges,
              @RequestParam(value = "seriesother", required = false, defaultValue = "false") Boolean seriesother,
              @RequestParam(value = "xother", required = false, defaultValue = "true") Boolean xother,
              @RequestParam(value = "seriesmissing", required = false, defaultValue = "false") Boolean seriesmissing,
              @RequestParam(value = "xmissing", required = false, defaultValue = "true") Boolean xmissing,
              @RequestParam(value = "fsort", required = false, defaultValue = "index") String fsort) throws Exception {

        List<String> statTypes = Arrays.asList(statType.split(","));
        //construct series subqueries
        List<Map> seriesFqs = produceSeriesFqs(searchParams, x, series, seriesranges, seriesother, seriesmissing);

        //limit facets returned with an fq or defined xranges
        StringBuilder inverseXranges = new StringBuilder();
        StringBuilder xRanges = new StringBuilder();
        xranges = produceLimitingXRanges(searchParams, x, xranges, xmissing, xRanges, inverseXranges);
        if (!xother) {
            xRanges = new StringBuilder();
            inverseXranges = new StringBuilder();
        }

        boolean date = isDate(x);

        for (Map seriesq : seriesFqs) {
            //update fq
            String[] fqBackup = searchParams.getFq();

            if (seriesq.containsKey("fq")) {
                appendFq(searchParams, seriesq.get("fq").toString());
            }

            List data = new ArrayList();

            if (xranges == null && stats == null) {
                //1. occurrence bar/pie/line chart of field
                searchParams.setFacet(true);
                searchParams.setFlimit(maxStringFacets);
                searchParams.setFsort(fsort);
                searchParams.setFacets(new String[]{x});

                if (xRanges.length() > 0) appendFq(searchParams, xRanges.toString());

                Collection<FacetResultDTO> l = searchDAO.findByFulltextSpatialQuery(searchParams, null).getFacetResults();
                if (l.size() > 0) {
                    data = l.iterator().next().getFieldResult();
                    if (!xmissing) {
                        for (int i = data.size() - 1; i >= 0; i--) {
                            if (StringUtils.isEmpty(((FieldResultDTO) data.get(i)).getLabel())) data.remove(i);
                        }
                    }
                }

                if (inverseXranges.length() > 0) {
                    searchParams.setFq(fqBackup);
                    if (seriesq.containsKey("fq")) appendFq(searchParams, seriesq.get("fq").toString());

                    searchParams.setFacet(false);
                    appendFq(searchParams, inverseXranges.toString());
                    SearchResultDTO sr = searchDAO.findByFulltextSpatialQuery(searchParams, null);
                    if (sr != null) {
                        data.add(new FieldResultDTO("Other", "Other", sr.getTotalRecords()));
                    }
                }
            } else if (xranges == null && stats != null) {
                //2. mean/max/min/quartile of field2, bar/pie/line chart of field1
                if (xRanges.length() > 0) appendFq(searchParams, xRanges.toString());
                data = searchDAO.searchStat(searchParams, stats, x, statTypes);
                if (!xmissing) {
                    for (int i = data.size() - 1; i >= 0; i--) {
                        if (StringUtils.isEmpty(((FieldStatsItem) data.get(i)).getLabel())) data.remove(i);
                    }
                }

                if (inverseXranges.length() > 0) {
                    searchParams.setFq(fqBackup);
                    if (seriesq.containsKey("fq")) appendFq(searchParams, seriesq.get("fq").toString());

                    searchParams.setFacet(false);
                    appendFq(searchParams, inverseXranges.toString());
                    List d = searchDAO.searchStat(searchParams, stats, null, statTypes);
                    if (d != null && d.size() > 0) {
                        ((FieldStatsItem) d.get(0)).setLabel("Other");
                        data.add(d.get(0));
                    }
                }
            } else if (xranges != null && stats == null) {
                //3. occurrence bar/pie chart of numeric field with predefined ranges (min to <= value1; >value1 <= value2; > value2 <= max)

                //use separate queries for each fq in xranges
                searchParams.setFacet(false);
                String[] xrangessplit = xranges.split(",");
                String[] fqBackup2 = searchParams.getFq();
                String[] xrangesFqs = new String[searchParams.getFq().length + 1];
                if (searchParams.getFq().length > 0)
                    System.arraycopy(searchParams.getFq(), 0, xrangesFqs, 0, searchParams.getFq().length);
                List output = new ArrayList();
                for (int i = 0; i < xrangessplit.length - 1; i++) {
                    Map m = makeRangeMap(i == 0, x, xrangessplit[i], xrangessplit[i + 1], date);

                    xrangesFqs[fqBackup2.length] = m.get("fq").toString();
                    searchParams.setFq(xrangesFqs);

                    SearchResultDTO l = searchDAO.findByFulltextSpatialQuery(searchParams, null);
                    if (l != null) {
                        String label = m.get("label").toString();
                        FieldResultDTO fr = new FieldResultDTO(label, label, l.getTotalRecords(), m.get("fq").toString());

                        output.add(fr);
                    }
                }
                searchParams.setFq(fqBackup2);

                data = output;
            } else if (xranges != null && stats != null) {
                //4. mean/max/min/quartile of field2, occurrence bar/pie chart of numeric field1 with predefined ranges (min to <= value1; >value1 <= value2; > value2 <= max)

                String[] fqs = new String[searchParams.getFq().length + 1];
                if (searchParams.getFq().length > 0)
                    System.arraycopy(searchParams.getFq(), 0, fqs, 0, searchParams.getFq().length);

                String[] r = xranges.split(",");
                List output = new ArrayList<>(r.length - 1);
                for (int i = 0; i < r.length; i++) {
                    if (i < r.length - 1) {
                        Map m = makeRangeMap(i == 0, x, r[i], r[i + 1], date);
                        fqs[fqs.length - 1] = m.get("fq").toString();
                        searchParams.setFq(fqs);

                        List result = searchDAO.searchStat(searchParams, stats, null, statTypes);
                        if (result.size() > 0) {
                            ((FieldStatsItem) result.iterator().next()).setFq(fqs[fqs.length - 1]);
                            ((FieldStatsItem) result.iterator().next()).setLabel(m.get("label").toString());
                            output.addAll(result);
                        }
                    }
                }

                data = output;
            }

            seriesq.put("data", data);

            searchParams.setFq(fqBackup);
        }

        //insert zeros
        insertZeros(seriesFqs);

        //format output data
        Map m = new HashMap();
        m.put("data", seriesFqs);
        m.put("x", x);
        m.put("series", series);
        m.put("value", stats);
        m.put("xLabel", getFieldDescription(x));
        m.put("seriesLabel", getFieldDescription(series));
        m.put("valueLabel", getFieldDescription(stats));

        return m;
    }

    private void insertZeros(List<Map> seriesFqs) {
        if (seriesFqs.size() > 1) {
            //build list
            List all = new ArrayList();
            for (Map s : seriesFqs) {
                List data = (List) s.get("data");
                for (Object o : data) {
                    if (o instanceof FieldStatsItem) {
                        all.add(((FieldStatsItem) o).getLabel());
                    } else if (o instanceof FieldResultDTO) {
                        all.add(((FieldResultDTO) o).getLabel());
                    }
                }
            }

            //insert zeros
            Map m = new HashMap();
            m.put("count", 0L);
            for (Map s : seriesFqs) {
                Set<String> current = new HashSet(all);
                List data = (List) s.get("data");
                boolean fieldStatsItem = false;
                for (Object o : data) {
                    if (o instanceof FieldStatsItem) {
                        fieldStatsItem = true;
                        current.remove(((FieldStatsItem) o).getLabel());
                    } else if (o instanceof FieldResultDTO) {
                        current.remove(((FieldResultDTO) o).getLabel());
                    }
                }

                for (String c : current) {
                    if (fieldStatsItem) {
                        data.add(new FieldStatsItem(new FieldStatsInfo(new NamedList<>(m), c)));
                    } else {
                        data.add(new FieldResultDTO(c, c, 0));
                    }
                }
                //sort
                Collections.sort(data, new Comparator() {
                    @Override
                    public int compare(Object o1, Object o2) {
                        String label1 = "", label2 = "";
                        if (o1 instanceof FieldStatsItem) {
                            label1 = ((FieldStatsItem) o1).getLabel();
                            label2 = ((FieldStatsItem) o2).getLabel();
                        } else if (o1 instanceof FieldResultDTO) {
                            label1 = ((FieldResultDTO) o1).getLabel();
                            label2 = ((FieldResultDTO) o2).getLabel();
                        }
                        if (label1 == null) label1 = "";
                        if (label2 == null) label2 = "";
                        return label1.compareTo(label2);
                    }
                });

                s.put("data", data);
            }
        }
    }

    private String produceLimitingXRanges(SpatialSearchRequestParams searchParams, String x, String xranges, Boolean xmissing, StringBuilder query, StringBuilder inverse) throws Exception {
        if (xranges == null && x != null) {
            List fqs = makeSeriesFacets(x, searchParams, null, xmissing);

            boolean date = isDate(x);
            if (fqs.size() > 0 && (isNumber(x) || date)) {
                //build xranges
                String newXRanges = "";
                for (int i = 0; i < fqs.size(); i++) {
                    if (i > 0) {
                        newXRanges += ",";
                    }

                    newXRanges += ((Map) fqs.get(i)).get("label").toString().split(" - ")[0];

                    if (date) {
                        newXRanges += "T00:00:00Z";
                    }
                }

                String[] lastElement = ((Map) fqs.get(fqs.size() - 1)).get("label").toString().split(" - ");

                if(lastElement !=null && lastElement.length > 1){
                    newXRanges += "," + ((Map) fqs.get(fqs.size() - 1)).get("label").toString().split(" - ")[1];
                }

                if (date) {
                    newXRanges += "T00:00:00Z";
                }

                //use fqs as xranges
                xranges = newXRanges;
            } else if (fqs.size() > 0) {
                //this will include any facets excluded due to maxStringFacets
                inverse.append(fqFromSeriesFacets(fqs, false));
                query.append(fqFromSeriesFacets(fqs, true));
            }
        }

        return xranges;
    }

    private List<Map> produceSeriesFqs(SpatialSearchRequestParams searchParams, String x, String series, String seriesranges, Boolean includeMissing, Boolean other) throws Exception {
        List seriesFqs = new ArrayList();

        boolean date = isDate(series);

        if (series != null && seriesranges != null) {
            String[] sr = seriesranges.split(",");
            for (int i = 0; i < sr.length - 1; i++) {
                seriesFqs.add(makeRangeMap(i == 0, series, sr[i], sr[i + 1], date));
            }
        } else if (series != null) {
            seriesFqs.addAll(makeSeriesFacets(series, searchParams, maxSeriesFacets, includeMissing));

            //limit subsequent queries to any limits that may be imposed by the series definition
            if (!isNumber(series) && !date && other) {
                Map m = new HashMap();
                m.put("fq", fqFromSeriesFacets(seriesFqs, true));
                m.put("label", "Other");
                seriesFqs.add(m);
            }
        } else {
            Map sm = new HashMap();
            sm.put("label", x);
            seriesFqs.add(sm);
        }

        return seriesFqs;
    }

    private boolean isNumber(String field) throws Exception {
        String[] numberType = new String[]{"int", "tint", "double", "tdouble", "long", "tlong", "float", "tfloat"};
        for (IndexFieldDTO f : indexDao.getIndexedFields()) {
            if (f.getName().equalsIgnoreCase(field) && ArrayUtils.contains(numberType, f.getDataType())) return true;
        }
        return false;
    }

    private boolean isDecimal(String field) throws Exception {
        String[] numberType = new String[]{"double", "tdouble", "float", "tfloat"};
        for (IndexFieldDTO f : indexDao.getIndexedFields()) {
            if (f.getName().equalsIgnoreCase(field) && ArrayUtils.contains(numberType, f.getDataType())) {
                return true;
            }
        }
        return false;
    }

    private boolean isDate(String field) throws Exception {
        for (IndexFieldDTO f : indexDao.getIndexedFields()) {
            if (f.getName().equalsIgnoreCase(field) && (f.getDataType().equalsIgnoreCase("tdate") || f.getDataType().equalsIgnoreCase("date")))
                return true;
        }
        return false;
    }

    private String getFieldDescription(String field) throws Exception {
        for (IndexFieldDTO f : indexDao.getIndexedFields()) {
            if (f.getName().equalsIgnoreCase(field) && f.getDescription() != null) return f.getDescription();
        }
        return field;
    }

    private List getSeriesFacets(String series, SpatialSearchRequestParams searchParams, Integer _maxFacets, Boolean includeMissing) throws Exception {
        List seriesFqs = new ArrayList();

        searchParams.setFacet(true);
        searchParams.setFlimit(_maxFacets);
        searchParams.setFacets(new String[]{series});

        if (!isDate(series) && !isNumber(series)) searchParams.setFsort("count");

        Collection<FacetResultDTO> l = searchDAO.findByFulltextSpatialQuery(searchParams, null).getFacetResults();
        if (l.size() > 0) {
            for (FieldResultDTO f : l.iterator().next().getFieldResult()) {
                if (!includeMissing && StringUtils.isEmpty(f.getLabel())) continue;

                Map sm = new HashMap();
                sm.put("fq", f.getFq());
                sm.put("label", f.getLabel());
                seriesFqs.add(sm);
            }
        }

        return seriesFqs;
    }

    private List makeSeriesFacets(String series, SpatialSearchRequestParams searchParams, Integer newMax, Boolean includeMissing) throws Exception {
        List seriesFqs = new ArrayList();

        //for numeric series with > maxFacets, generate buckets
        boolean date = isDate(series);
        if (isNumber(series) || date) {
            int maxSeries = newMax != null ? newMax : maxNumberFacets;
            List list = getSeriesFacets(series, searchParams, maxSeries + 1, includeMissing);
            if (list.size() > maxSeries + (includeMissing ? 1 : 0)) {
                //get min/max
                List minMax = (List) chart(searchParams, null, null, series, "max,min", null, null, false, false, false, false, "count").get("data");
                if (date) {
                    Long min = ((Date) ((FieldStatsItem) ((List) ((Map) minMax.get(0)).get("data")).get(0)).getMin()).getTime();
                    Long max = ((Date) ((FieldStatsItem) ((List) ((Map) minMax.get(0)).get("data")).get(0)).getMax()).getTime();
                    Long step = (long) (Math.ceil(((max - min) / maxSeries) / (1000 * 60 * 60 * 24)) * (1000 * 60 * 60 * 24));
                    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                    //made 'Z' valid
                    df.setTimeZone(TimeZone.getTimeZone("UTC"));
                    //add fqs
                    long i;
                    for (i = 0; i < maxSeries - 1 && min + step * (i + 1.5) < max; i++) {
                        seriesFqs.add(makeRangeMap(i == 0, series, df.format(new Date(min + step * i)), df.format(new Date(min + step * (i + 1))), date));
                    }
                    //add last fq
                    seriesFqs.add(makeRangeMap(false, series, df.format(new Date(min + step * i)), df.format(new Date(max)), date));
                } else {
                    if (isDecimal(series)) {
                        Double min = ((Number) ((FieldStatsItem) ((List) ((Map) minMax.get(0)).get("data")).get(0)).getMin()).doubleValue();
                        Double max = ((Number) ((FieldStatsItem) ((List) ((Map) minMax.get(0)).get("data")).get(0)).getMax()).doubleValue();
                        Double step = (max - min) / (double) maxSeries;
                        //add fqs
                        int i;
                        for (i = 0; i < maxSeries - 1 && min + step * (i + 1.5) < max; i++) {
                            seriesFqs.add(makeRangeMap(i == 0, series, String.valueOf(min + step * i), String.valueOf(min + step * (i + 1)), date));
                        }
                        //add last fq
                        seriesFqs.add(makeRangeMap(false, series, String.valueOf(min + step * i), String.valueOf(max), date));
                    } else {
                        Long min = ((Number) ((FieldStatsItem) ((List) ((Map) minMax.get(0)).get("data")).get(0)).getMin()).longValue();
                        Long max = ((Number) ((FieldStatsItem) ((List) ((Map) minMax.get(0)).get("data")).get(0)).getMax()).longValue();
                        Long step = (long) Math.ceil((max - min) / (double) maxSeries);
                        //add fqs
                        int i;
                        for (i = 0; i < maxSeries - 1 && min + step * (i + 1.5) < max; i++) {
                            seriesFqs.add(makeRangeMap(i == 0, series, String.valueOf(min + step * i), String.valueOf(min + step * (i + 1)), date));
                        }
                        //add last fq
                        seriesFqs.add(makeRangeMap(false, series, String.valueOf(min + step * i), String.valueOf(max), date));
                    }
                }
            } else {
                seriesFqs.addAll(list);
            }
        } else {
            int maxSeries = newMax != null ? newMax : maxStringFacets;
            List list = getSeriesFacets(series, searchParams, maxSeries + 1, includeMissing);
            if (list.size() > maxSeries + (includeMissing ? 1 : 0)) {
                seriesFqs.addAll(list);
            }
        }

        return seriesFqs;
    }

    private Map makeRangeMap(boolean first, String field, String start, String end, boolean date) {
        Map sm = new HashMap();
        String separator = start.contains(":") ? "\"" : "";
        sm.put("fq", field + ":[" + start + " TO " + end + "]" + (first ? "" : " AND -(" + field + ":" + separator + start + separator + ")"));
        if (date) {
            sm.put("label", start.substring(0, 10) + " - " + end.substring(0, 10));
        } else {
            sm.put("label", start + " - " + end);
        }
        return sm;
    }

    private String fqFromSeriesFacets(List fqs, boolean invert) {
        //use fqs as an OR
        String fq = "";
        for (int i = 0; i < fqs.size(); i++) {
            if (!((Map) fqs.get(i)).get("fq").toString().startsWith("-")) {
                if (fq.length() > 0) fq += " OR ";
                fq += ((Map) fqs.get(i)).get("fq");
            }
        }

        //invert
        if (invert) {
            return "-(" + fq.substring(0, fq.indexOf(':')) + ":* AND -" + fq.replace(" OR ", " AND -") + ");";
        } else {
            return fq;
        }
    }

    private void appendFq(SpatialSearchRequestParams searchParams, String fq) {
        String[] seriesqFqs = new String[searchParams.getFq().length + 1];
        if (searchParams.getFq().length > 0)
            System.arraycopy(searchParams.getFq(), 0, seriesqFqs, 0, searchParams.getFq().length);
        seriesqFqs[searchParams.getFq().length] = fq;
        searchParams.setFq(seriesqFqs);
    }
}