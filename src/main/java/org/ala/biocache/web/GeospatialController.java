package org.ala.biocache.web;

import javax.inject.Inject;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.ala.biocache.*;
import org.ala.biocache.dao.SearchDAO;

import org.ala.biocache.dao.TaxonDAO;
import org.ala.biocache.dto.SearchResultDTO;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import org.ala.biocache.dto.SpatialSearchRequestParams;
import org.springframework.web.bind.annotation.ResponseBody;
import org.w3c.dom.Document;

import net.sf.json.JSONObject;
import org.ala.biocache.dto.OccurrencePoint;
import org.ala.biocache.dto.PointType;
import org.apache.commons.lang.StringUtils;

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
//    @Inject
//    protected TaxonDAO taxonDAO;
//
//    @RequestMapping(value = {"/ogc/owsXXXXX"}, method = RequestMethod.GET)
//    public void getCapabilities(HttpServletRequest request,
//                                HttpServletResponse response,
//                                @RequestParam(value="q", required = false, defaultValue = "*:*") String query,
//                                @RequestParam(value="fq", required = false) String[] filterQueries)
//        throws Exception {
//        response.setContentType("text/xml");
//        response.setHeader("Content-Description", "File Transfer");
//        response.setHeader("Content-Disposition", "attachment; filename=GetCapabilities.xml");
//        response.setHeader("Content-Transfer-Encoding","binary");
//        try {
//            //webservicesRoot
//            String biocacheServerUrl = request.getSession().getServletContext().getInitParameter("webservicesRoot");
//            PrintWriter writer = response.getWriter();
//            writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
//                   "<!DOCTYPE WMT_MS_Capabilities SYSTEM \"http://spatial.ala.org.au/geoserver/schemas/wms/1.1.1/WMS_MS_Capabilities.dtd\">\n" +
//                   "<WMT_MS_Capabilities version=\"1.1.1\" updateSequence=\"28862\">\n" +
//                   "  <Service>\n" +
//                   "    <Name>OGC:WMS</Name>\n" +
//                   "    <Title>Atlas of Living Australia (WMS) - Species occurrences</Title>\n" +
//                   "    <Abstract>WMS services for species occurrences.</Abstract>\n" +
//                   "    <KeywordList>\n" +
//                   "      <Keyword>WMS</Keyword>\n" +
//                   "      <Keyword>GEOSERVER</Keyword>\n" +
//                   "      <Keyword>Species occurrence data</Keyword>\n" +
//                   "      <Keyword>ALA</Keyword>\n" +
//                   "      <Keyword>NCRIS</Keyword>\n" +
//                   "    </KeywordList>\n" +
//                   "    <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\"" +biocacheServerUrl + "/ogc/wms\"/>\n" +
//                   "    <ContactInformation>\n" +
//                   "      <ContactPersonPrimary>\n" +
//                   "        <ContactPerson>ALA Support</ContactPerson>\n" +
//                   "        <ContactOrganization>ALA (CSIRO)</ContactOrganization>\n" +
//                   "      </ContactPersonPrimary>\n" +
//                   "      <ContactPosition>Support Manager</ContactPosition>\n" +
//                   "      <ContactAddress>\n" +
//                   "        <AddressType></AddressType>\n" +
//                   "        <Address/>\n" +
//                   "        <City>Canberra</City>\n" +
//                   "        <StateOrProvince>ACT</StateOrProvince>\n" +
//                   "        <PostCode>2601</PostCode>\n" +
//                   "        <Country>Australia</Country>\n" +
//                   "      </ContactAddress>\n" +
//                   "      <ContactVoiceTelephone>+61 2 6246 4400</ContactVoiceTelephone>\n" +
//                   "      <ContactFacsimileTelephone>+61 2 6246 4400</ContactFacsimileTelephone>\n" +
//                   "      <ContactElectronicMailAddress>support@ala.org.au</ContactElectronicMailAddress>\n" +
//                   "    </ContactInformation>\n" +
//                   "    <Fees>NONE</Fees>\n" +
//                   "    <AccessConstraints>NONE</AccessConstraints>\n" +
//                   "  </Service>\n" +
//                   "  <Capability>\n" +
//                   "    <Request>\n" +
//                   "      <GetCapabilities>\n" +
//                   "        <Format>application/vnd.ogc.wms_xml</Format>\n" +
//                   "        <DCPType>\n" +
//                   "          <HTTP>\n" +
//                   "            <Get>\n" +
//                   "              <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\"" +biocacheServerUrl + "/ogc/ows?SERVICE=WMS&amp;\"/>\n" +
//                   "            </Get>\n" +
//                   "            <Post>\n" +
//                   "              <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\""+biocacheServerUrl + "/ogc/ows?SERVICE=WMS&amp;\"/>\n" +
//                   "            </Post>\n" +
//                   "          </HTTP>\n" +
//                   "        </DCPType>\n" +
//                   "      </GetCapabilities>\n" +
//                   "      <GetMap>\n" +
//                   "        <Format>image/png</Format>\n" +
//                   "        <DCPType>\n" +
//                   "          <HTTP>\n" +
//                   "            <Get>\n" +
//                   "              <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\""+ biocacheServerUrl + "/ogc/wms/reflect?SERVICE=WMS&amp;OUTLINE=TRUE&amp;\"/>\n" +
//                   "            </Get>\n" +
//                   "          </HTTP>\n" +
//                   "        </DCPType>\n" +
//                   "      </GetMap>\n" +
//                   "      <GetFeatureInfo>\n" +
//                   "        <Format>text/html</Format>\n" +
//                   "        <DCPType>\n" +
//                   "          <HTTP>\n" +
//                   "            <Get>\n" +
//                   "              <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\""+biocacheServerUrl+"/ogc/getFeatureInfo\"/>\n" +
//                   "            </Get>\n" +
//                   "            <Post>\n" +
//                   "              <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\""+biocacheServerUrl+"/ogc/getFeatureInfo\"/>\n" +
//                   "            </Post>\n" +
//                   "          </HTTP>\n" +
//                   "        </DCPType>\n" +
//                   "      </GetFeatureInfo>\n" +
//                   "      <GetLegendGraphic>\n" +
//                   "        <Format>image/png</Format>\n" +
//                   "        <Format>image/jpeg</Format>\n" +
//                   "        <Format>image/gif</Format>\n" +
//                   "        <DCPType>\n" +
//                   "          <HTTP>\n" +
//                   "            <Get>\n" +
//                   "              <OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\" xlink:href=\""+biocacheServerUrl+"/ogc/legendGraphic\"/>\n" +
//                   "            </Get>\n" +
//                   "          </HTTP>\n" +
//                   "        </DCPType>\n" +
//                   "      </GetLegendGraphic>\n" +
//                   "    </Request>\n" +
//                   "    <Exception>\n" +
//                   "      <Format>application/vnd.ogc.se_xml</Format>\n" +
//                   "      <Format>application/vnd.ogc.se_inimage</Format>\n" +
//                   "    </Exception>\n" +
////                   "    <UserDefinedSymbolization SupportSLD=\"1\" UserLayer=\"1\" UserStyle=\"1\" RemoteWFS=\"1\"/>\n" +
//                   "    <Layer>\n" +
//                   "      <Title>Atlas of Living Australia - Species occurrence layers</Title>\n" +
//                   "      <Abstract>Custom WMS services for ALA species occurrences</Abstract>\n" +
//                   "      <SRS>EPSG:900913</SRS>\n" +
//                   "      <SRS>EPSG:4326</SRS>\n" +
//                   "     <LatLonBoundingBox minx=\"-179.9\" miny=\"-89.9\" maxx=\"179.9\" maxy=\"89.9\"/>\n"
//            );
//
//            writer.write(generateStylesForPoints());
//
//            filterQueries = ArrayUtils.add(filterQueries, "geospatial_kosher:true");
//
//            taxonDAO.extractHierarchy(query, filterQueries, writer);
//
//            writer.write("</Layer></Capability></WMT_MS_Capabilities>\n");
//        } catch (Exception e){
//            e.printStackTrace();
//        }
//    }
//
//    public String generateStylesForPoints(){
//        //need a better listings of colours
//        String[] colorsNames = new String[]{
//                "DarkRed","IndianRed","DarkSalmon","SaddleBrown", "Chocolate", "SandyBrown","Orange","DarkGreen","Green", "Lime", "LightGreen", "MidnightBlue", "Blue",
//                "SteelBlue","CadetBlue","Aqua","PowderBlue", "DarkOliveGreen", "DarkKhaki", "Yellow","Moccasin","Indigo","Purple", "Fuchsia", "Plum", "Black", "White"
//        };
//        String[] colorsCodes = new String[]{
//                "8b0000","FF0000","CD5C5C","E9967A", "8B4513", "D2691E", "F4A460","FFA500","006400","008000", "00FF00", "90EE90", "191970", "0000FF",
//                "4682B4","5F9EA0","00FFFF","B0E0E6", "556B2F", "BDB76B", "FFFF00","FFE4B5","4B0082","800080", "FF00FF", "DDA0DD", "000000",  "FFFFFF"
//        };
//        String[] sizes = new String[]{"5","10","2"};
//        String[] sizesNames = new String[]{"medium","large","small"};
//        String[] opacities = new String[]{"0.5","1", "0.2"};
//        String[] opacitiesNames = new String[]{"medium","opaque", "transparency"};
//        StringBuffer sb = new StringBuffer();
//        int colorIdx = 0;
//        int sizeIdx = 0;
//        int opIdx = 0;
//        for(String color: colorsNames){
//            for(String size: sizes){
//                for(String opacity : opacities){
//                    sb.append("<Style>\n" +
//                            "<Name>"+colorsCodes[colorIdx]+";opacity="+opacity+";size="+size+"</Name> \n" +
//                            "<Title>"+color+";opacity="+opacitiesNames[opIdx]+";size="+sizesNames[sizeIdx]+"</Title> \n" +
//                            "</Style>\n");
//                    opIdx++;
//                }
//                opIdx = 0;
//                sizeIdx++;
//            }
//            sizeIdx = 0;
//            colorIdx++;
//        }
//        return sb.toString();
//    }

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
        if(url != null && url.length()>0){
            logger.info("Loading the WKT from " + url);
            //get the info from the url it should be XML data
            URL xmlUrl = new URL(url);
            InputStream in = xmlUrl.openStream();
            Document doc = parse(in);
            try {
                String geoJson =doc.getElementsByTagName("entry").item(0).getChildNodes().item(0).getNodeValue();
                requestParams.setWkt(wktFromJSON(geoJson));
            }
            catch(Exception e){
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
                return coords.replace("]]],[[[", "))*((").replace("]],[[", "))*((").replace("],[", "*").replace(",", " ").replace("*", ",").replace("[[[[", "MULTIPOLYGON(((").replace("]]]]", ")))");
            } else {
                return coords.replace("],[", "*").replace(",", " ").replace("*", ",").replace("[[[[", "POLYGON((").replace("]]]]", "))").replace("],[", "),(");
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
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        return ret;
    }

    public static void main(String[] args) throws Exception{
//        String url = "http://spatial.ala.org.au/gazetteer/ibra/Australian_Alps.xml";
//        URL xmlUrl = new URL(url);
//            InputStream in = xmlUrl.openStream();
//            Document doc = parse(in);
//            System.out.println(doc.getElementsByTagName("entry").getLength());
//            System.out.println(doc.getElementsByTagName("entry").item(0).getChildNodes().item(0).getNodeValue());
        String json = "{\"type\":\"MultiPolygon\",\"coordinates\":[[[[145.648936693066,-37.661942598869],[145.650608044561,-37.6620222269492],[145.652055599241,-37.661858444561]]]]}";
        System.out.println(wktFromJSON(json));
    }
//
//    public void setTaxonDAO(TaxonDAO taxonDAO) {
//        this.taxonDAO = taxonDAO;
//    }
}
