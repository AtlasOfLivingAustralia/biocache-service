package au.org.ala.biocache.web;

import java.io.InputStream;
import java.net.URL;

import javax.inject.Inject;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import net.sf.json.JSONObject;

import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.SearchResultDTO;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.w3c.dom.Document;

/**
 * Geospatial controller for the biocache services.
 *
 * @author "Natasha Carter <Natasha.Carter@csiro.au>"
 */
@Controller
public class GeospatialController {

    /** Logger initialisation */
    private final static Logger logger = Logger.getLogger(GeospatialController.class);
    /** Fulltext search DAO */
    @Inject
    protected SearchDAO searchDAO;

    /**
     * Performs an occurrence search based on wkt. Use the &facets parameter to
     * limit the facets to the ones required. eg &facets=species_guid
     * @param requestParams
     * @param url
     * @param model
     * @return
     * @throws Exception
     */
    @RequestMapping(value = {"/occurrences/spatial*"}, method = RequestMethod.GET)
    public @ResponseBody SearchResultDTO listWktOccurrences(SpatialSearchRequestParams requestParams,
                                    @RequestParam(value = "url", required = false) String url,
                                    Model model) throws Exception {
        
        requestParams.setQ("*:*");       
        //don't limit the factes
        requestParams.setFlimit(-1);
        //check to see if a url has been provided
        if(url != null && url.length() > 0){
            logger.info("Loading the WKT from " + url);
            //get the info from the url it should be XML data
            URL xmlUrl = new URL(url);
            InputStream in = xmlUrl.openStream();
            Document doc = parse(in);
            try {
                String geoJson =doc.getElementsByTagName("entry").item(0).getChildNodes().item(0).getNodeValue();
                requestParams.setWkt(wktFromJSON(geoJson));
            } catch(Exception e) {
                logger.warn("Unable to parse the supplied URL for a WKT", e);
            }
        }
        return searchDAO.findByFulltextSpatialQuery(requestParams,null);
    }

    private static String wktFromJSON(String json) {
        try {
            logger.info("The json:" + json);
            JSONObject obj = JSONObject.fromObject(json);

            //String coords = obj.getJSONArray("geometries").getJSONObject(0).getString("coordinates");
            String coords = obj.getString("coordinates");

            if (obj.getString("type").equalsIgnoreCase("multipolygon")) {
                return coords.replace("]]],[[[", "))*((")
                		.replace("]],[[", "))*((")
                		.replace("],[", "*")
                		.replace(",", " ")
                		.replace("*", ",")
                		.replace("[[[[", "MULTIPOLYGON(((")
                		.replace("]]]]", ")))");
            } else {
                return coords.replace("],[", "*")
                		.replace(",", " ")
                		.replace("*", ",")
                		.replace("[[[[", "POLYGON((")
                		.replace("]]]]", "))")
                		.replace("],[", "),(");
            }
        } catch (Exception e) {
            logger.warn("Unable to get JSON", e);
            return "none";
        }
    }

    /**
     * Constructs a Document object by reading from an input stream.
     */
    public static Document parse (InputStream is) {
        Document ret = null;
        DocumentBuilderFactory domFactory;
        DocumentBuilder builder;

        try {
            domFactory = DocumentBuilderFactory.newInstance();
            domFactory.setValidating(false);
            domFactory.setNamespaceAware(false);
            builder = domFactory.newDocumentBuilder();
            ret = builder.parse(is);
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }
        return ret;
    }
}
