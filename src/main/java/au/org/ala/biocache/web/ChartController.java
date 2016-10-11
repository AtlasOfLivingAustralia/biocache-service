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

import au.org.ala.biocache.dao.FieldStatsItem;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.FacetResultDTO;
import au.org.ala.biocache.dto.FieldResultDTO;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import net.sf.ehcache.CacheManager;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
    Object chart(SpatialSearchRequestParams searchParams,
                 @RequestParam(value = "x", required = true) String x,
                 @RequestParam(value = "xranges", required = false) String xranges,
                 @RequestParam(value = "stats", required = false) String stats,
                 HttpServletResponse response) throws Exception {

        if (xranges == null && stats == null) {
            //1. occurrence bar/pie/line chart of field
            searchParams.setFacet(true);
            searchParams.setFlimit(-1);
            searchParams.setFacets(new String[]{x});

            Collection<FacetResultDTO> l = searchDAO.findByFulltextSpatialQuery(searchParams, null).getFacetResults();
            return l.iterator().next().getFieldResult();
        } else if (xranges == null && stats != null) {
            //2. mean/max/min/quartile of field2, bar/pie/line chart of field1
            return searchDAO.searchStat(searchParams, stats, x);
        } else if (xranges != null && stats == null) {
            //3. occurrence bar/pie chart of numeric field with predefined ranges (min to <= value1; >value1 <= value2; > value2 <= max)
            searchParams.setFacet(true);
            searchParams.setFlimit(-1);
            searchParams.setFacets(new String[]{x});

            FacetResultDTO result = searchDAO.findByFulltextSpatialQuery(searchParams, null).getFacetResults().iterator().next();

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

            return output;
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

            return output;
        }

        return null;
    }
}