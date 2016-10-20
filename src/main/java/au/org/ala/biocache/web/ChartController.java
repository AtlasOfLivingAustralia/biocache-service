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

import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.FacetResultDTO;
import au.org.ala.biocache.dto.FieldResultDTO;
import au.org.ala.biocache.dto.FieldStatsItem;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import net.sf.ehcache.CacheManager;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.common.util.NamedList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;
import java.io.Serializable;
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

    @Autowired
    private ServletContext servletContext;

    @Resource(name = "cacheManager")
    private CacheManager cacheManager;


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
    List chart(SpatialSearchRequestParams searchParams,
               @RequestParam(value = "x", required = false) String x,
               @RequestParam(value = "xranges", required = false) String xranges,
               @RequestParam(value = "stats", required = false) String stats,
               @RequestParam(value = "series", required = false) String series,
               @RequestParam(value = "seriesranges", required = false) String seriesranges,
               HttpServletResponse response) throws Exception {

        //construct series subqueries
        List<Map> seriesFqs = new ArrayList();

        if (series != null && seriesranges != null) {
            String[] sr = seriesranges.split(",");
            for (int i = 0; i < sr.length; i++) {
                if (i < sr.length - 1) {
                    Map sm = new HashMap();
                    sm.put("fq", x + ":[" + sr[i] + " TO " + sr[i + 1] + "]" + (i > 0 ? " AND -" + x + ":" + sr[i] : ""));
                    sm.put("label", sr[i] + " - " + sr[i + 1]);
                    seriesFqs.add(sm);
                }
            }
        } else if (series != null) {
            searchParams.setFacet(true);
            searchParams.setFlimit(-1);
            searchParams.setFacets(new String[]{series});

            Collection<FacetResultDTO> l = searchDAO.findByFulltextSpatialQuery(searchParams, null).getFacetResults();
            if (l.size() > 0) {
                for (FieldResultDTO f : l.iterator().next().getFieldResult()) {
                    Map sm = new HashMap();
                    sm.put("fq", f.getFq());
                    sm.put("label", f.getLabel());
                    seriesFqs.add(sm);
                }
            }
        } else {
            Map sm = new HashMap();
            sm.put("label", x);
            seriesFqs.add(sm);
        }

        for (Map seriesq : seriesFqs) {

            //update fq
            String[] fqBackup = searchParams.getFq();

            if (seriesq.containsKey("fq")) {
                String[] seriesqFqs = new String[searchParams.getFq().length + 1];
                if (searchParams.getFq().length > 0)
                    System.arraycopy(searchParams.getFq(), 0, seriesqFqs, 0, searchParams.getFq().length);
                seriesqFqs[searchParams.getFq().length] = seriesq.get("fq").toString();
                searchParams.setFq(seriesqFqs);
            }


            List data = new ArrayList();

            if (xranges == null && stats == null) {
                //1. occurrence bar/pie/line chart of field
                searchParams.setFacet(true);
                searchParams.setFlimit(-1);
                searchParams.setFacets(new String[]{x});

                Collection<FacetResultDTO> l = searchDAO.findByFulltextSpatialQuery(searchParams, null).getFacetResults();
                if (l.size() > 0) {
                    data = l.iterator().next().getFieldResult();
                }
            } else if (xranges == null && stats != null) {
                //2. mean/max/min/quartile of field2, bar/pie/line chart of field1
                data = searchDAO.searchStat(searchParams, stats, x);
            } else if (xranges != null && stats == null) {
                //3. occurrence bar/pie chart of numeric field with predefined ranges (min to <= value1; >value1 <= value2; > value2 <= max)
                searchParams.setFacet(true);
                searchParams.setFlimit(-1);
                searchParams.setFacets(new String[]{x});

                Collection<FacetResultDTO> l = searchDAO.findByFulltextSpatialQuery(searchParams, null).getFacetResults();
                if (l.size() > 0) {
                    FacetResultDTO result = l.iterator().next();

                    List ranges = new ArrayList<Double>();
                    String[] r = xranges.split(",");
                    List<FieldResultDTO> output = new ArrayList<FieldResultDTO>(r.length - 1);
                    for (int i = 0; i < r.length; i++) {
                        ranges.add(Double.parseDouble(r[i]));

                        if (i < r.length - 1) {
                            String fq = x + ":[" + r[i] + " TO " + r[i + 1] + "]" + (i > 0 ? " AND -" + x + ":" + r[i] : "");
                            FieldResultDTO fr = new FieldResultDTO(r[i] + " - " + r[i + 1], 0, fq);
                            fr.setLabel(r[i] + " - " + r[i + 1]);
                            output.add(fr);
                        }
                    }

                    for (FieldResultDTO f : result.getFieldResult()) {
                        if (StringUtils.isNotEmpty(f.getFieldValue())) {
                            int idx = Collections.binarySearch(ranges, Double.parseDouble(f.getFieldValue()));

                            if (idx < 0) idx = (idx + 1) * -1;
                            if (idx > 0) idx--;
                            if (idx >= 0 && idx < output.size())
                                output.get(idx).setCount(output.get(idx).getCount() + f.getCount());
                        }
                    }

                    data = output;
                }
            } else if (xranges != null && stats != null) {
                //4. mean/max/min/quartile of field2, occurrence bar/pie chart of numeric field1 with predefined ranges (min to <= value1; >value1 <= value2; > value2 <= max)

                String[] fqs = new String[searchParams.getFq().length + 1];
                if (searchParams.getFq().length > 0)
                    System.arraycopy(searchParams.getFq(), 0, fqs, 0, searchParams.getFq().length);

                String[] r = xranges.split(",");
                List output = new ArrayList<>(r.length - 1);
                for (int i = 0; i < r.length; i++) {
                    if (i < r.length - 1) {
                        fqs[fqs.length - 1] = x + ":[" + r[i] + " TO " + r[i + 1] + "]" + (i > 0 ? " AND -" + x + ":" + r[i] : "");
                        searchParams.setFq(fqs);

                        List result = searchDAO.searchStat(searchParams, stats, null);
                        if (result.size() > 0) {
                            ((FieldStatsItem) result.iterator().next()).setFq(fqs[fqs.length - 1]);
                            ((FieldStatsItem) result.iterator().next()).setLabel(r[i] + " - " + r[i + 1]);
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
                        data.add(new FieldStatsItem(new FieldStatsInfo(new NamedList<Object>(), c)));
                    } else {
                        data.add(new FieldResultDTO(c, 0));
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
            }
        }

        return seriesFqs;
    }
}