package org.ala.biocache.web;

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.biocache.*;
import au.org.ala.util.ParsedRecord;
import au.org.ala.util.AdHocParser;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.ServletRequestUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Controller
public class UploadController {

    private final static Logger logger = Logger.getLogger(UploadController.class);

    protected String uploadStatusDir = "/data/biocache-upload";

    /**
     * Upload a dataset using a POST, returning a UID for this data
     *
     * @return an identifier for this temporary dataset
     */
    @RequestMapping("/upload")
    public String uploadHomePage(){
        return "upload/home";
    }

    /**
     * Expects a request body in JSON
     *
     * @param request
     * @return
     */
    @RequestMapping(value="/parser/areDwcTerms", method = RequestMethod.POST)
    public @ResponseBody boolean areDwcTerms(HttpServletRequest request, HttpServletResponse response) throws Exception {
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
            return AdHocParser.guessColumnHeaders(termArray);
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
            String[] matchedTerms = AdHocParser.mapColumnHeaders(termArray);
            //create a map and return this
            Map<String,String> map = new LinkedHashMap<String,String>();
            for(int i=0; i<termArray.length; i++){
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
            String utf8String = new String(json.getBytes(), "UTF-8");
            LinkedHashMap<String,String> record = om.readValue(utf8String, new TypeReference<LinkedHashMap<String,String>>() {});
            input.close();
            String[] headers = AdHocParser.mapColumnHeaders(record.keySet().toArray(new String[]{}));
            return AdHocParser.processLine(headers, record.values().toArray(new String[]{}));
        } catch(Exception e) {
            logger.error(e.getMessage(),e);
            response.sendError(HttpURLConnection.HTTP_BAD_REQUEST);
            return null;
        }
    }

    /**
     * Upload a dataset using a POST, returning a UID for this data
     *
     * @return an identifier for this temporary dataset
     */
    @RequestMapping(value="/upload/status/{tempDataResourceUid}.json", method = RequestMethod.GET)
    public @ResponseBody Map<String,String> uploadStatus(@PathVariable String tempDataResourceUid, HttpServletResponse response) throws Exception {
       response.setContentType("application/json");
       File file = new File(uploadStatusDir + File.separator + tempDataResourceUid);
       if(file.exists()){
         String value = FileUtils.readFileToString(file);
         ObjectMapper om = new ObjectMapper();
         return om.readValue(value, Map.class);
       } else {
         response.sendError(404);
         return null;
       }
    }

    /**
     * Upload a dataset using a POST, returning a UID for this data
     *
     * @return an identifier for this temporary dataset
     */
    @RequestMapping(value="/upload/post", method = RequestMethod.POST)
    public @ResponseBody Map<String,String> uploadOccurrenceData(HttpServletRequest request) throws Exception {

        //check the request
        String headers = request.getParameter("headers");
        String csvData = request.getParameter("csvData");
        String firstLineIsDataAsString = request.getParameter("firstLineIsData");
        boolean firstLineIsData = false;
        if(firstLineIsDataAsString != null){
            firstLineIsData = Boolean.parseBoolean(firstLineIsDataAsString);
        }

        //get a record count
        int lineCount = 0;
        BufferedReader reader = new BufferedReader(new StringReader(csvData));
        while(reader.readLine() != null){
            lineCount++;
        }

        String datasetName = request.getParameter("datasetName");
        String separator = request.getParameter("separator");
        char separatorChar = ',';
        if(separator != null && "TAB".equalsIgnoreCase(separator)){
           separatorChar =  '\t';
        }

        logger.debug("######### Retrieved - separator (original): '" + separator + "'");
        logger.debug("######### Retrieved - separatorChar: '" + separatorChar + "'");

        PostMethod post = new PostMethod("http://collections.ala.org.au/ws/tempDataResource");
        ObjectMapper mapper = new ObjectMapper();

        UserUpload uu = new UserUpload();
        uu.setNumberOfRecords(lineCount);
        uu.setName(datasetName);

        String json = mapper.writeValueAsString(uu);
        System.out.println("Sending: " + json);

        post.setRequestBody(json);
        HttpClient httpClient = new HttpClient();
        httpClient.executeMethod(post);
        logger.debug("######### Retrieved: " + post.getResponseBodyAsString());
        logger.debug("######### Retrieved: " + post.getResponseHeader("location").getValue());
        logger.debug("######### Data uploaded....");
        String collectoryUrl = post.getResponseHeader("location").getValue();

        String tempUid = collectoryUrl.substring(collectoryUrl.lastIndexOf('/') + 1);

        //do the upload asynchronously
        UploaderThread ut = new UploaderThread();
        ut.headers = headers;
        ut.firstLineIsData = firstLineIsData;
        ut.csvData = csvData;
        ut.separatorChar = separatorChar;
        ut.uploadStatusDir = uploadStatusDir;
        ut.recordsToLoad = lineCount;
        ut.tempUid = tempUid;

        Thread t = new Thread(ut);
        t.start();

        logger.debug("######### Temporary UID being returned...." + tempUid);
        Map<String,String> details = new HashMap<String,String>();
        details.put("uid", tempUid);
        return details;
    }
}


class UploadStatus {
    protected String status;
    protected Integer totalRecords;
    protected Integer completed;
    protected Integer percentage;

    UploadStatus(){}

    UploadStatus(String status, Integer completed, Integer percentage, Integer totalRecords) {
        this.status = status;
        this.completed = completed;
        this.percentage = percentage;
        this.totalRecords = totalRecords;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getCompleted() {
        return completed;
    }

    public void setCompleted(Integer completed) {
        this.completed = completed;
    }

    public Integer getPercentage() {
        return percentage;
    }

    public void setPercentage(Integer percentage) {
        this.percentage = percentage;
    }

    public Integer getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(Integer totalRecords) {
        this.totalRecords = totalRecords;
    }
}

class UploaderThread implements Runnable {

    private final static Logger logger = Logger.getLogger(UploaderThread.class);
    public Integer loadComplete = 0;
    public String status = "LOADING";
    protected String headers;
    protected String csvData;
    protected boolean firstLineIsData;
    protected Character separatorChar;
    protected String tempUid;
    protected String uploadStatusDir;
    protected Integer recordsToLoad = 0;

    @Override
    public void run(){
        File statusDir = null;
        File statusFile = null;
        ObjectMapper om = new ObjectMapper();

        try {
            statusDir = new File(uploadStatusDir);
            if(!statusDir.exists()){
                FileUtils.forceMkdir(statusDir);
            }
            statusFile = new File(uploadStatusDir+File.separator+tempUid);
            statusFile.createNewFile();
        } catch (Exception e1){
            logger.error(e1.getMessage(), e1);
            throw new RuntimeException(e1);
        }

        try {

            FileUtils.writeStringToFile(statusFile, om.writeValueAsString(new UploadStatus("LOADING",0,0,recordsToLoad)));

            Integer counter = 0;
            CSVReader csvReader = new CSVReader(new StringReader(csvData), separatorChar);
            try {
                String[] currentLine = csvReader.readNext();
                //boolean firstListAreHeaders = AdHocParser.areColumnHeaders(currentLine);

//                String[] unnormalised = headers.split(",");
//                String[] headerArray = new String[unnormalised.length];
//                for(int i=0; i<headerArray.length; i++){
//                    headerArray[i] = unnormalised[i].trim();
//                }

                String[] headerUnmatched = headers.split(",");
                String[] headerArray = AdHocParser.mapColumnHeaders(headerUnmatched);

                //if the first line is data, add a record, else discard
                if(firstLineIsData){
                    addRecord(tempUid, currentLine, headerArray);
                }

                //write the data
                Integer percentComplete  = 0;
                while((currentLine = csvReader.readNext())!=null){
                    counter++;
                    //System.out.println("Processing record: " + counter);
                    loadComplete = (int) ((counter.floatValue() / recordsToLoad.floatValue()) * 100);
                    if(percentComplete.equals(loadComplete)){
                        percentComplete = loadComplete;
                        FileUtils.writeStringToFile(statusFile, om.writeValueAsString(new UploadStatus("LOADING",counter,loadComplete,recordsToLoad)));
                    }
                    addRecord(tempUid, currentLine, headerArray);
                    //currentLine = csvReader.readNext();
                }
            } catch(Exception e) {
                logger.error(e.getMessage(),e);
            } finally {
                csvReader.close();
            }

            status = "INDEXING";
            FileUtils.writeStringToFile(statusFile, om.writeValueAsString(new UploadStatus("INDEXING",0,0, recordsToLoad)));
            au.org.ala.biocache.Store.index(tempUid);
            status = "COMPLETE";
            FileUtils.writeStringToFile(statusFile, om.writeValueAsString(new UploadStatus("COMPLETE",counter,100,recordsToLoad)));
        } catch(Exception ex){
          try {
            status = "FAILED";
            FileUtils.writeStringToFile(statusFile,status);
          } catch (IOException ioe){
            logger.error("Loading failed and failed to update the status: " + ex.getMessage(), ex);
          }
          logger.error("Loading failed: " + ex.getMessage(), ex);
        }
    }

    private void addRecord(String tempUid, String[] currentLine, String[] headers) {
        Map<String,String> map = new HashMap<String, String>();
        for(int i=0; i< headers.length && i< currentLine.length; i++){
            if(currentLine[i] !=null && currentLine[i].trim().length() >0 ){
                map.put(headers[i], currentLine[i].trim());
            }
        }
        if(!map.isEmpty()){
            au.org.ala.biocache.Store.insertRecord(tempUid, map, false);
        }
    }
}

