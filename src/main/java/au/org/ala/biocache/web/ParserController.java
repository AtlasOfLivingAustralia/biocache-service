package au.org.ala.biocache.web;

import au.org.ala.biocache.parser.ParsedRecord;
import au.org.ala.biocache.parser.AdHocParser;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.HttpURLConnection;
import java.util.*;

/**
 * This controller adhoc data parsing webservices.
 */
@Controller
public class ParserController {

    private final static Logger logger = Logger.getLogger(UploadController.class);
    /**
     * Expects a request body in JSON
     *
     * @param request
     * @return
     */
    @RequestMapping(value="/parser/areDwcTerms", method = RequestMethod.POST)
    public @ResponseBody
    boolean areDwcTerms(HttpServletRequest request, HttpServletResponse response) throws Exception {
        ObjectMapper om = new ObjectMapper();
        try {
            InputStream input = request.getInputStream();
            List<String> terms = om.readValue(input, new TypeReference<List<String>>() {});
            input.close();
            String[] termArray = terms.toArray(new String[]{});
            return AdHocParser.areColumnHeaders(termArray);
        } catch(Exception e) {
            logger.error(e.getMessage(),e);
            response.sendError(HttpURLConnection.HTTP_BAD_REQUEST);
            return false;
        }
    }

    @RequestMapping(value="/parser/matchTerms", method = RequestMethod.POST)
    public @ResponseBody String[] guessFieldTypes(HttpServletRequest request, HttpServletResponse response) throws Exception {
        ObjectMapper om = new ObjectMapper();
        try {
            InputStream input = request.getInputStream();
            List<String> terms = om.readValue(input, new TypeReference<List<String>>() {});
            input.close();
            String[] termArray = terms.toArray(new String[]{});
            return AdHocParser.guessColumnHeadersArray(termArray);
        } catch(Exception e) {
            logger.error(e.getMessage(),e);
            response.sendError(HttpURLConnection.HTTP_BAD_REQUEST);
            return null;
        }
    }

    @RequestMapping(value="/parser/mapTerms", method = RequestMethod.POST)
    public @ResponseBody Map<String,String> guessFieldTypesWithOriginal(HttpServletRequest request, HttpServletResponse response) throws Exception {
        ObjectMapper om = new ObjectMapper();
        try {
            InputStream input = request.getInputStream();
            List<String> terms = om.readValue(input, new TypeReference<List<String>>() {});
            input.close();
            String[] termArray = terms.toArray(new String[]{});
            String[] matchedTerms = AdHocParser.mapColumnHeadersArray(termArray);
            //create a map and return this
            Map<String,String> map = new LinkedHashMap<String,String>();
            for(int i = 0; i < termArray.length; i++){
              map.put(termArray[i], matchedTerms[i]);
            }
            return map;
        } catch(Exception e) {
            logger.error(e.getMessage(),e);
            response.sendError(HttpURLConnection.HTTP_BAD_REQUEST);
            return null;
        }
    }

    @RequestMapping(value="/process/adhoc", method = RequestMethod.POST)
    public @ResponseBody ParsedRecord processRecord(HttpServletRequest request, HttpServletResponse response) throws Exception {
        ObjectMapper om = new ObjectMapper();
        try {
            InputStream input = request.getInputStream();
            String json = IOUtils.toString(input);
            logger.debug(json);
            String utf8String = new String(json.getBytes(), "UTF-8");
            LinkedHashMap<String,String> record = om.readValue(utf8String, new TypeReference<LinkedHashMap<String,String>>() {});
            input.close();
            logger.debug("Mapping column headers...");
            String[] headers = AdHocParser.mapOrReturnColumnHeadersArray(record.keySet().toArray(new String[]{}));
            logger.debug("Processing line...");
            return AdHocParser.processLineArrays(headers, record.values().toArray(new String[]{}));
        } catch(Exception e) {
            logger.error(e.getMessage(),e);
            response.sendError(HttpURLConnection.HTTP_BAD_REQUEST);
            return null;
        }
    }
}