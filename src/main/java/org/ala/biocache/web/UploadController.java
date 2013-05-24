package org.ala.biocache.web;

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.biocache.*;
import au.org.ala.util.ParsedRecord;
import au.org.ala.util.AdHocParser;
import org.ala.biocache.dto.Facet;
import org.ala.biocache.dto.SpatialSearchRequestParams;
import org.ala.layers.dao.IntersectCallback;
import org.ala.layers.dto.IntersectionFile;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Controller that supports the upload of CSV data to be indexed and processed in the biocache.
 * This controller also includes code for adhoc data parsing webservices.
 */
@Controller
public class UploadController {

    private final static Logger logger = Logger.getLogger(UploadController.class);

    protected String uploadStatusDir = "/data/biocache-upload";

    private Pattern dataResourceUidP = Pattern.compile("data_resource_uid:([\\\"]{0,1}[a-z]{2,3}[0-9]{1,}[\\\"]{0,1})");

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

    /**
     * Upload a dataset using a POST, returning a UID for this data
     *
     * @return an identifier for this temporary dataset
     */
    @RequestMapping(value="/upload/status/{tempDataResourceUid}.json", method = RequestMethod.GET)
    public @ResponseBody Map<String,String> uploadStatus(@PathVariable String tempDataResourceUid, HttpServletResponse response) throws Exception {
       response.setContentType("application/json");
       File file = new File(uploadStatusDir + File.separator + tempDataResourceUid);
       int retries = 5;
       while(file.exists() && retries>0){
         try {
             String value = FileUtils.readFileToString(file);
             ObjectMapper om = new ObjectMapper();
             return om.readValue(value, Map.class);
         } catch (Exception e){
             Thread.sleep(50);
             retries--;
         }
       }
       response.sendError(404);
       return null;
    }

    /**
     * Upload a dataset using a POST, returning a UID for this data
     *
     * @return an identifier for this temporary dataset
     */
    @RequestMapping(value="/upload/customIndexes/{tempDataResourceUid}.json", method = RequestMethod.GET)
    public @ResponseBody String[] customIndexes(@PathVariable String tempDataResourceUid, HttpServletResponse response) throws Exception {
        response.setContentType("application/json");
        return au.org.ala.biocache.Store.retrieveCustomIndexFields(tempDataResourceUid);
    }

    /**
     * Retrieve a list of data resource UIDs
     * @param queryExpression
     * @return
     */
    private List<String> getDrs(String queryExpression){
        List<String> drs = new ArrayList<String>();
        if(queryExpression != null){
            Matcher m = dataResourceUidP.matcher(queryExpression);
            while(m.find()){
                for(int x =0; x<m.groupCount(); x++){
                    drs.add(m.group(x).replaceAll("data_resource_uid:", "").replaceAll("\\\"",""));
                }
            }
        }
        return drs;
    }

    /**
     * Upload a dataset using a POST, returning a UID for this data
     *
     * @return an identifier for this temporary dataset
     */
    @RequestMapping(value="/upload/dynamicFacets", method = RequestMethod.GET)
    public @ResponseBody List<Facet> dynamicFacets(
            SpatialSearchRequestParams requestParams,
            HttpServletResponse response) throws Exception {

        List<String> drs = new ArrayList<String>();
        drs.addAll(getDrs(requestParams.getQ()));
        drs.addAll(getDrs(requestParams.getQc()));
        for(String fq : requestParams.getFq())
            drs.addAll(getDrs(fq));

        response.setContentType("application/json");
        List<Facet> fs = new ArrayList<Facet>();
        for(String dr: drs){
            String[] facetsRaw = au.org.ala.biocache.Store.retrieveCustomIndexFields(dr);
            for (String f: facetsRaw){
                String displayName = f;
                boolean isRange=false;
                if(displayName.endsWith("_s")) {
                    displayName = displayName.substring(0, displayName.length()-2);
                }
                else if(displayName.endsWith("_i") || displayName.endsWith("_d")){
                    displayName = displayName.substring(0, displayName.length()-2);
                    isRange=true;
                }
                displayName = displayName.replaceAll("_", " ");
                fs.add(new Facet(f, StringUtils.capitalize(displayName)));
                //when the custom field is an _i or _d automatically include the range as an available facet
                if(isRange)
                  fs.add(new Facet(f + "_RNG", StringUtils.capitalize(displayName)));
            }
        }
        return fs;
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
        String customIndexedFieldsString = StringUtils.trimToNull(request.getParameter("customIndexedFields"));
        String[] customIndexFields = null;
        if(customIndexedFieldsString != null){
            logger.debug("######### Retrieved - headers: '" + headers + "'");
            customIndexFields = customIndexedFieldsString.trim().split(",");
            logger.debug("######### Retrieved - custom index fields requested: '" + customIndexedFieldsString + "'");
        }

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
        //System.out.println("Sending: " + json);

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
        ut.customIndexFields = customIndexFields;

        Thread t = new Thread(ut);
        t.start();

        logger.debug("######### Temporary UID being returned...." + tempUid);
        Map<String,String> details = new HashMap<String,String>();
        details.put("uid", tempUid);
        return details;
    }
}

final class UploadStatus {

    final String status;
    final String description;
    final Integer percentage;

    public UploadStatus(String status, String description, Integer percentage) {
        this.status = status;
        this.description = description;
        this.percentage = percentage;
    }

    public String getStatus() {
        return status;
    }

    public String getDescription() {
        return description;
    }

    public Integer getPercentage() {
        return percentage;
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
    protected Integer recordsToLoad = null;
    protected String[] customIndexFields = null;    

    String[] cleanUpHeaders(String[] headers){
        int i = 0;
        for(String hdr: headers){
            headers[i] = hdr.replaceAll("[^a-zA-Z0-9]+","_");
            i++;
        }
        return headers;
    }

    @Override
    public void run(){

        File statusDir = null;
        File statusFile = null;
        ObjectMapper om = new ObjectMapper();
        List<String> intList = new ArrayList<String>();        
        List<String> floatList = new ArrayList<String>();
        List<String> stringList = new ArrayList<String>();

        try {
            statusDir = new File(uploadStatusDir);
            if(!statusDir.exists()){
                FileUtils.forceMkdir(statusDir);
            }
            statusFile = new File(uploadStatusDir + File.separator + tempUid);
            statusFile.createNewFile();
        } catch (Exception e1){
            logger.error(e1.getMessage(), e1);
            throw new RuntimeException(e1);
        }

        try {

            //count the lines
            FileUtils.writeStringToFile(statusFile, om.writeValueAsString(new UploadStatus("LOADING","Starting...",0)));
            Integer recordCount = 0;
            BufferedReader reader = new BufferedReader(new StringReader(csvData));
            while(reader.readLine()!=null)
               recordCount++;
            if(!firstLineIsData)
                recordCount--;

            Integer counter = 0;
            CSVReader csvReader = new CSVReader(new StringReader(csvData), separatorChar);
            try {
                String[] currentLine = csvReader.readNext();
                String[] headerUnmatched = cleanUpHeaders(headers.split(","));
                String[] headerArray = AdHocParser.mapOrReturnColumnHeadersArray(headerUnmatched);

                if(customIndexFields == null){
                    List<String> filteredHeaders = filterByMaxColumnLengths(headerArray, new CSVReader(new StringReader(csvData), separatorChar), 50);
                    //derive a list of custom index field
                    filteredHeaders = filterCustomIndexFields(filteredHeaders);
                    customIndexFields = filteredHeaders.toArray(new String[0]);
                }
                
                //default data type for the "customIndexFields" is a string                
                CollectionUtils.addAll(intList, customIndexFields);

                //if the first line is data, add a record, else discard
                if(firstLineIsData){
                    addRecord(tempUid, currentLine, headerArray,intList,floatList,stringList);
                }

                //write the data to DB
                Integer percentComplete  = 0;
                while((currentLine = csvReader.readNext())!=null){
                    counter++;
                    addRecord(tempUid, currentLine, headerArray, intList, floatList, stringList);
                    if(counter % 100 == 0){
                        Integer percentageComplete = 0;
                        if(counter != 0){
                            percentageComplete = (int) ((float) (counter + 1) / (float) recordCount * 25);
                        }
                        FileUtils.writeStringToFile(statusFile, om.writeValueAsString(new UploadStatus("LOADING",String.format("%d of %d records loaded.", counter, recordCount),percentageComplete)));
                    }
                }
            } catch(Exception e) {
                logger.error(e.getMessage(),e);
                throw e;
            } finally {
                csvReader.close();
            }

            //update the custom index fields so that the are appended with the data type _i for int and _d for float/double
            List<String> tmpCustIndexFields = new ArrayList<String>();
            for(String f : customIndexFields){
                if(intList.contains(f))
                   tmpCustIndexFields.add(f +"_i");
                else if(floatList.contains(f))
                   tmpCustIndexFields.add(f+"_d");
                else
                    tmpCustIndexFields.add(f); //default is a string
            }
            
            status = "SAMPLING";
            UploadIntersectCallback u = new UploadIntersectCallback(statusFile);
            FileUtils.writeStringToFile(statusFile, om.writeValueAsString(new UploadStatus("SAMPLING","Starting",25)));
            au.org.ala.biocache.Store.sample(tempUid, u);

            status = "PROCESSING";
            logger.debug("Processing " + tempUid);
            DefaultObserverCallback processingCallback = new DefaultObserverCallback("PROCESSING", recordCount, statusFile, 50, "processed");
            au.org.ala.biocache.Store.process(tempUid, 4, processingCallback);

            status = "INDEXING";
            logger.debug("Indexing " + tempUid + " " + tmpCustIndexFields);
            DefaultObserverCallback indexingCallback = new DefaultObserverCallback("INDEXING", recordCount, statusFile, 75, "indexed");
            au.org.ala.biocache.Store.index(tempUid, tmpCustIndexFields.toArray(new String[0]), indexingCallback);

            status = "COMPLETE";
            FileUtils.writeStringToFile(statusFile, om.writeValueAsString(new UploadStatus(status,"Loading complete",100)));

        } catch (Exception ex) {
          try {
            status = "FAILED";
            FileUtils.writeStringToFile(statusFile, om.writeValueAsString(new UploadStatus(status,"The system was unable to load this data.",0)));
          } catch (IOException ioe) {
            logger.error("Loading failed and failed to update the status: " + ex.getMessage(), ex);
          }
          logger.error("Loading failed: " + ex.getMessage(), ex);
        }
    }

    //TODO move to config
    protected static List<String> alreadyIndexedFields = Arrays.asList(new String[]{
        "eventDate",
        "scientificName",
        "commonName",
        "isoCountryCode",
        "country",
        "kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "species",
        "stateProvince",
        "imcra",
        "ibra",
        "places",
        "decimalLatitude",
        "decimalLongitude",
        "year",
        "month",
        "basisOfRecord",
        "typeStatus",
        "collector",
        "establishmentMeans"});

    public List<String> filterCustomIndexFields(List<String> suppliedHeaders){
        List<String> customIndexFields = new ArrayList<String>();
        for(String hdr: suppliedHeaders){
            if(!alreadyIndexedFields.contains(hdr)){
                customIndexFields.add(hdr);
            }
        }
        return customIndexFields;
    }

    public List<String> filterByMaxColumnLengths(String[] headers, CSVReader csvReader, int maxColumnLength) throws Exception {
        int[] columnLengths = new int[headers.length];
        for(int i=0; i<columnLengths.length; i++) columnLengths[i] = 0; //initialise - needed ?
        String[] fields = csvReader.readNext();
        while(fields != null){
            for(int j=0; j<columnLengths.length;j++){
                if(fields.length> j && columnLengths[j] < fields[j].length()){
                    columnLengths[j] = fields[j].length();
                }
            }
            fields = csvReader.readNext();
        }
        List<String> filterList = new ArrayList<String>();
        for(int k=0; k< columnLengths.length; k++){
            logger.debug("Column length: " + headers[k] + " = " + columnLengths[k]);
            if(columnLengths[k] <= maxColumnLength ){
                filterList.add(headers[k]);
            }
        }
        return filterList;
    }

    private void addRecord(String tempUid, String[] currentLine, String[] headers, List<String> intList, List<String> floatList, List<String> stringList) {
        Map<String,String> map = new HashMap<String, String>();
        for(int i = 0; i < headers.length && i < currentLine.length; i++){
            if(currentLine[i] != null && currentLine[i].trim().length() > 0 ){
                map.put(headers[i], currentLine[i].trim());
                //now test of the header value is part of the custom index fields and perform a data check
                if(intList.contains(headers[i])){
                    try{
                        Integer.parseInt(currentLine[i].trim());
                    }
                    catch(Exception e){
                        //this custom index column could not possible be an integer
                        intList.remove(headers[i]);
                        floatList.add(headers[i]);
                    }
                }
                if(floatList.contains(headers[i])){
                    try {
                        Float.parseFloat(currentLine[i].trim());
                    } catch(Exception e) {
                        //this custom index column can only be a string
                      floatList.remove(headers[i]);
                      stringList.add(headers[i]);
                    }
                }
            }
        }
        if(!map.isEmpty()){
            au.org.ala.biocache.Store.loadRecord(tempUid, map, false);
        }
    }

    public class DefaultObserverCallback implements ObserverCallback {

        private String processName = "";
        private Integer total = -1;
        private File fileToWriteTo;
        private Integer startingPercentage = 0;
        private String verb = ""; // "processing" or "indexing"

        public DefaultObserverCallback(String processName, Integer total, File fileToWriteTo, Integer startingPercentage, String verb){
            this.total = total;
            this.processName = processName;
            this.fileToWriteTo = fileToWriteTo;
            this.startingPercentage = startingPercentage;
            this.verb = verb;
        }

        @Override
        public void progressMessage(int recordCount) {
            try {
                ObjectMapper om = new ObjectMapper();
                Integer percentageComplete = 0;
                if(recordCount>-1){
                    percentageComplete =  (int) ((float) (recordCount + 1) / (float) total * 25);
                    FileUtils.writeStringToFile(fileToWriteTo, om.writeValueAsString(
                            new UploadStatus(processName,
                                    String.format("%d of %d records %s.", recordCount, total, verb),
                                    startingPercentage + percentageComplete)));
                }
            } catch(Exception e){
                logger.debug(e.getMessage(),e);
            }
        }
    }

    public class UploadIntersectCallback implements IntersectCallback {

        File theFile = null;
        IntersectionFile[] intersectionFiles;
        IntersectionFile intersectionFile;
        Integer currentLayerIdx = -1;
        String message = "";

        public UploadIntersectCallback(File fileToWriteTo){
            this.theFile = fileToWriteTo;
        }

        @Override
        public void setLayersToSample(IntersectionFile[] intersectionFiles) {
            this.intersectionFiles = intersectionFiles;
        }

        @Override
        public void setCurrentLayer(IntersectionFile intersectionFile) {
            this.intersectionFile = intersectionFile;
        }

        @Override
        public void setCurrentLayerIdx(Integer currentLayerIdx) {
            synchronized (this){
                if(currentLayerIdx > this.currentLayerIdx){
                    this.currentLayerIdx = currentLayerIdx;
                }
            }
        }

        @Override
        public void progressMessage(String message) {
            this.message = message;
            try {
                ObjectMapper om = new ObjectMapper();
                Integer percentageComplete = 0;
                if(currentLayerIdx>-1){
                    percentageComplete =  (int) ((float) (currentLayerIdx + 1) / (float) intersectionFiles.length * 25);
                }
                if(intersectionFile !=null && currentLayerIdx>0){
                    FileUtils.writeStringToFile(theFile, om.writeValueAsString(new UploadStatus("SAMPLING",
                        String.format("%d of %d layers sampled. Currently sampling %s.",
                                currentLayerIdx+1, intersectionFiles.length,
                                intersectionFile.getLayerName()), 25 + percentageComplete)));
                }
            } catch(Exception e){
                logger.debug(e.getMessage(),e);
            }
        }
    }
}