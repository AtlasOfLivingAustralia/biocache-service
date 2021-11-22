package au.org.ala.biocache.web;

import au.org.ala.biocache.dao.IndexDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.IndexFieldDTO;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.stream.ScatterplotSearch;
import io.swagger.annotations.Api;
import io.swagger.v3.oas.annotations.Operation;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartRenderingInfo;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.encoders.EncoderUtil;
import org.jfree.chart.encoders.ImageFormat;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.DefaultXYDataset;
import org.jfree.ui.RectangleEdge;
import org.locationtech.jts.math.Vector2D;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.annotation.JsonInclude;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import java.awt.*;
import java.awt.geom.Ellipse2D;
import java.awt.image.BufferedImage;
import java.util.List;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This controller is responsible for providing basic scatterplot services.
 * <p>
 * Basic scatterplot is
 * - occurrences, standard biocache query
 * - x, numerical stored value field
 * - y, numerical stored value field
 * - height, integer default 256
 * - width, integer default 256
 * - title, string default query-display-name
 * - pointcolour, colour as RGB string like FF0000 for red, default 0000FF
 * - pointradius, double default 3
 */
@Controller
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ScatterplotController {

    final private static Logger logger = Logger.getLogger(ScatterplotController.class);

    private final static String DEFAULT_SCATTERPLOT_TITLE = " ";
    private final static String DEFAULT_SCATTERPLOT_HEIGHT = "256";
    private final static String DEFAULT_SCATTERPLOT_WIDTH = "256";
    private final static String DEFAULT_SCATTERPLOT_POINTCOLOUR = "0000FF";
    private final static String DEFAULT_SCATTERPLOT_POINTRADIUS = "3";
    private final static List<String> VALID_DATATYPES = Arrays.asList("float", "double", "int", "long", "tfloat", "tdouble", "tint", "tlong");

    @Inject
    protected SearchDAO searchDAO;
    @Inject
    protected IndexDAO indexDao;

    @Operation(summary = "Generate a scatterplot", tags = "Scatterplots")
    @RequestMapping(value = {"/scatterplot"}, method = RequestMethod.GET)
    public void scatterplot(SpatialSearchRequestParams requestParams,
                            @RequestParam(value = "x", required = true) String x,
                            @RequestParam(value = "y", required = true) String y,
                            @RequestParam(value = "height", required = false, defaultValue = DEFAULT_SCATTERPLOT_HEIGHT) Integer height,
                            @RequestParam(value = "width", required = false, defaultValue = DEFAULT_SCATTERPLOT_WIDTH) Integer width,
                            @RequestParam(value = "title", required = false, defaultValue = DEFAULT_SCATTERPLOT_TITLE) String title,
                            @RequestParam(value = "pointcolour", required = false, defaultValue = DEFAULT_SCATTERPLOT_POINTCOLOUR) String pointcolour,
                            @RequestParam(value = "pointradius", required = false, defaultValue = DEFAULT_SCATTERPLOT_POINTRADIUS) Double pointradius,
                            HttpServletResponse response) throws Exception {

        JFreeChart jChart = makeScatterplot(requestParams, x, y, title, pointcolour, pointradius);

        //produce image
        ChartRenderingInfo chartRenderingInfo = new ChartRenderingInfo();
        BufferedImage bi = jChart.createBufferedImage(width, height, BufferedImage.TRANSLUCENT, chartRenderingInfo);
        byte[] bytes = EncoderUtil.encode(bi, ImageFormat.PNG, true);

        //output image
        response.setContentType("image/png");

        try {
            response.getOutputStream().write(bytes);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Operation(summary = "Get details of a point on a plot", tags = "Scatterplots")
    @RequestMapping(value = {"/scatterplot/point", "/scatterplot/point.json" }, method = RequestMethod.GET)
    @ResponseBody
    public Map scatterplotPointInfo(SpatialSearchRequestParams requestParams,
                                    @RequestParam(value = "x", required = true) String x,
                                    @RequestParam(value = "y", required = true) String y,
                                    @RequestParam(value = "height", required = false, defaultValue = DEFAULT_SCATTERPLOT_HEIGHT) Integer height,
                                    @RequestParam(value = "width", required = false, defaultValue = DEFAULT_SCATTERPLOT_WIDTH) Integer width,
                                    @RequestParam(value = "title", required = false, defaultValue = DEFAULT_SCATTERPLOT_TITLE) String title,
                                    @RequestParam(value = "pointx1", required = true) Integer pointx1,
                                    @RequestParam(value = "pointy1", required = true) Integer pointy1,
                                    @RequestParam(value = "pointx2", required = true) Integer pointx2,
                                    @RequestParam(value = "pointy2", required = true) Integer pointy2) throws Exception {

        JFreeChart jChart = makeScatterplot(requestParams, x, y, title, "000000", 1.0);

        //produce image
        ChartRenderingInfo chartRenderingInfo = new ChartRenderingInfo();
        BufferedImage bi = jChart.createBufferedImage(width, height, BufferedImage.TRANSLUCENT, chartRenderingInfo);

        XYPlot plot = (XYPlot) jChart.getPlot();

        //identify point range across x and y
        double tx1 = plot.getRangeAxis().java2DToValue(pointx1, chartRenderingInfo.getPlotInfo().getDataArea(), RectangleEdge.BOTTOM);
        double tx2 = plot.getRangeAxis().java2DToValue(pointx2, chartRenderingInfo.getPlotInfo().getDataArea(), RectangleEdge.BOTTOM);
        double ty1 = plot.getDomainAxis().java2DToValue(pointy1, chartRenderingInfo.getPlotInfo().getDataArea(), RectangleEdge.LEFT);
        double ty2 = plot.getDomainAxis().java2DToValue(pointy2, chartRenderingInfo.getPlotInfo().getDataArea(), RectangleEdge.LEFT);
        double x1 = Math.min(tx1, tx2);
        double x2 = Math.max(tx1, tx2);
        double y1 = Math.min(ty1, ty2);
        double y2 = Math.max(ty1, ty2);

        Map map = new HashMap();
        map.put("xaxis_pixel_selection", new int[]{pointx1, pointx2});
        map.put("yaxis_pixel_selection", new int[]{pointy1, pointy2});
        map.put("xaxis", x);
        map.put("yaxis", y);
        map.put("xaxis_range", new double[]{x1, x2});
        map.put("yaxis_range", new double[]{y1, y2});

        return map;
    }

    JFreeChart makeScatterplot(SpatialSearchRequestParams requestParams, String x, String y
            , String title, String pointcolour, Double pointradius) throws Exception {
        //verify x and y are numerical and stored
        String displayNameX = null;
        String displayNameY = null;
        Set<IndexFieldDTO> indexedFields = indexDao.getIndexedFields();

        displayNameX = getFieldDescription(x, indexedFields);
        displayNameY = getFieldDescription(y, indexedFields);

        // format query
        requestParams.setFlimit(-1);
        SolrQuery query = searchDAO.initSolrQuery(requestParams, false, null);

        // set parameters for streaming a facet query
        query.set("facet.field", x + "," + y);
        query.setFacetSort(x + " asc," + y + " asc");

        Set<Vector2D> vectors = new HashSet<>();
        AtomicInteger count = new AtomicInteger();
        ScatterplotSearch proc = new ScatterplotSearch(vectors, x, y, count);

        try {
            indexDao.streamingQuery(query, null, proc, null);
        } catch (Exception e) {
            logger.error("scatterplot failed", e);
        }

        if (count.get() == 0) {
            throw new Exception("No valid records found for these input parameters");
        }

        // copy vectors to array
        double[][] data = new double[2][vectors.size()];
        int i = 0;
        for (Vector2D v : vectors) {
            data[0][i] = v.getX();
            data[1][i] = v.getY();
            i++;
        }

        //create dataset
        DefaultXYDataset xyDataset = new DefaultXYDataset();
        xyDataset.addSeries("series", data);

        //create chart
        JFreeChart jChart = ChartFactory.createScatterPlot(
                title.equals(" ") ? requestParams.getDisplayString() : title //chart display name
                , displayNameX //x-axis display name
                , displayNameY //y-axis display name
                , xyDataset
                , PlotOrientation.HORIZONTAL, false, false, false);
        jChart.setBackgroundPaint(Color.white);

        //styling
        XYPlot plot = (XYPlot) jChart.getPlot();
        Font axisfont = new Font("Arial", Font.PLAIN, 10);
        Font titlefont = new Font("Arial", Font.BOLD, 11);
        plot.getDomainAxis().setLabelFont(axisfont);
        plot.getDomainAxis().setTickLabelFont(axisfont);
        plot.getRangeAxis().setLabelFont(axisfont);
        plot.getRangeAxis().setTickLabelFont(axisfont);
        plot.setBackgroundPaint(new Color(220, 220, 220));
        jChart.getTitle().setFont(titlefont);

        //point shape and colour
        Color c = new Color(Integer.parseInt(pointcolour, 16));
        plot.getRenderer().setSeriesPaint(0, c);
        plot.getRenderer().setSeriesShape(0, new Ellipse2D.Double(-pointradius, -pointradius, pointradius * 2, pointradius * 2));

        return jChart;
    }

    /**
     * Get the description of the fieldName provided or throw an Exception.
     *
     * @param fieldName     Field name to search for.
     * @param indexedFields Set of all indexed fields.
     * @return
     * @throws Exception
     */
    public String getFieldDescription(String fieldName, Set<IndexFieldDTO> indexedFields) throws Exception {
        for (IndexFieldDTO field : indexedFields) {
            if (field.getName().equals(fieldName)) {
                if (!VALID_DATATYPES.contains(field.getDataType())) {
                    throw new Exception("'" + fieldName + "' with data type '" + field.getDataType() + "' is not a valid data type (" + StringUtils.join(VALID_DATATYPES, ", ") + ")");
                } else if (!field.isStored() && !field.isDocvalue()) {
                    throw new Exception("Cannot use '" + fieldName + "'.  It is not a stored or docvalue field.");
                } else {
                    return field.getDescription();
                }
            }
        }

        throw new Exception("'" + fieldName + "'is not a vaild field.");
    }
}
