package au.org.ala.biocache.web;

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.biocache.ObserverCallback;
import au.org.ala.biocache.dto.Facet;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.parser.AdHocParser;
import org.ala.layers.dao.IntersectCallback;
import org.ala.layers.dto.IntersectionFile;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.ServletRequestUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Controller that supports the upload of CSV data to be indexed and processed in the biocache.
 */
@Controller
public class UploadController {

    private final static Logger logger = Logger.getLogger(UploadController.class);

    @Value("${registry.url:http://collections.ala.org.au/ws}")
    protected String registryUrl;

    @Value("${upload.status:/data/biocache-upload/status}")
    protected String uploadStatusDir;

    @Value("${upload.temp:/data/biocache-upload/temp}")
    protected String uploadTempDir;

    private Pattern dataResourceUidP = Pattern.compile("data_resource_uid:([\\\"]{0,1}[a-z]{2,3}[0-9]{1,}[\\\"]{0,1})");
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
        "establishmentMeans",
        "coordinateUncertaintyInMeters",
        "decimalLatitude",
        "decimalLongitude"
    });

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
                for(int x = 0; x < m.groupCount(); x++){
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
        for(String fq : requestParams.getFq()){
            drs.addAll(getDrs(fq));
        }

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
        for(int i = 0; i < columnLengths.length; i++) columnLengths[i] = 0; //initialise - needed ?
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
        for(int k = 0; k < columnLengths.length; k++){
            logger.debug("Column length: " + headers[k] + " = " + columnLengths[k]);
            if(columnLengths[k] <= maxColumnLength ){
                filterList.add(headers[k]);
            }
        }
        return filterList;
    }

    private void mkWorkingDirs() throws Exception {
        File uploadStatusDirF = new File(uploadStatusDir);
        if(!uploadStatusDirF.exists()){
            FileUtils.forceMkdir(uploadStatusDirF);
        }

        File uploadTempDirF = new File(uploadTempDir);
        if(!uploadTempDirF.exists()){
            FileUtils.forceMkdir(uploadTempDirF);
        }
    }

    public String[] getHeaders(HttpServletRequest request){
        String headers = request.getParameter("headers");
        String[] headerUnmatched = cleanUpHeaders(headers.split(","));
        return AdHocParser.mapOrReturnColumnHeadersArray(headerUnmatched);
    }

    String[] cleanUpHeaders(String[] headers){
        int i = 0;
        for(String hdr: headers){
            headers[i] = hdr.replaceAll("[^a-zA-Z0-9]+","_");
            i++;
        }
        return headers;
    }

    /**
     * Upload a dataset using a POST, returning a UID for this data
     *
     * @return an identifier for this temporary dataset
     */
    @RequestMapping(value="/upload/post", method = RequestMethod.POST)
    public @ResponseBody Map<String,String> uploadOccurrenceData(HttpServletRequest request, HttpServletResponse response) throws Exception {

        final String urlToZippedData = request.getParameter("csvZippedUrl");
        final String csvDataAsString = request.getParameter("csvData");
        final String datasetName = request.getParameter("datasetName");
        if(StringUtils.isEmpty(urlToZippedData) && StringUtils.isEmpty(csvDataAsString)){
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Must supply 'csvZippedUrl' or 'csvData'");
            return null;
        }

        if(StringUtils.isEmpty(datasetName)){
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Must supply 'datasetName'");
            return null;
        }

        try {
            mkWorkingDirs();

            //check the request
            String[] headers = getHeaders(request);
            boolean firstLineIsData = ServletRequestUtils.getBooleanParameter(request,"firstLineIsData");
            String[] customIndexFields = null;

            //get a record count
            int lineCount = -1;

            CSVReader csvData = null;

            if(urlToZippedData != null) {

                //download to local directory....
                File csvFile = downloadCSV(urlToZippedData);

                //do a line count
                lineCount = doLineCount(csvFile);
                logger.debug("Line count: " + lineCount);

                //derive a list of custom index field
                CSVReader readerForCol = new CSVReader(new FileReader(csvFile), ',', '"');
                List<String> filteredHeaders = filterByMaxColumnLengths(headers, readerForCol, 50);
                filteredHeaders = filterCustomIndexFields(filteredHeaders);
                customIndexFields = filteredHeaders.toArray(new String[0]);
                readerForCol.close();

                //initialise the reader
                csvData = new CSVReader(new FileReader(csvFile), ',', '"');

            } else {

                final char separatorChar = getSeparatorChar(request);

                //do a line count
                lineCount = doLineCount(csvDataAsString);

                //derive a list of custom index field
                CSVReader readerForCol = new CSVReader(new StringReader(csvDataAsString), separatorChar, '"');
                List<String> filteredHeaders = filterByMaxColumnLengths(headers, readerForCol, 50);
                filteredHeaders = filterCustomIndexFields(filteredHeaders);
                customIndexFields = filteredHeaders.toArray(new String[0]);
                readerForCol.close();

                //initialise the reader
                csvData = new CSVReader(new StringReader(csvDataAsString), separatorChar, '"');
            }

            String tempUid = createTempResource(request, datasetName, lineCount);

            //do the upload asynchronously
            UploaderThread ut = new UploaderThread();
            ut.headers = headers;
            ut.datasetName = datasetName;
            ut.firstLineIsData = firstLineIsData;
            ut.csvData = csvData;
            ut.lineCount = lineCount;
            ut.uploadStatusDir = uploadStatusDir;
            ut.recordsToLoad = lineCount;
            ut.tempUid = tempUid;
            ut.customIndexFields = customIndexFields;
            Thread t = new Thread(ut);
            t.start();

            logger.debug("Temporary UID being returned...." + tempUid);
            Map<String,String> details = new HashMap<String,String>();
            details.put("uid", tempUid);
            return details;

        } catch (Exception e){
            logger.error(e.getMessage(),e);
        }
        return null;
    }

    private int doLineCount(String csvDataAsString) throws IOException {
        int lineCount = 0;
        BufferedReader reader = new BufferedReader(new StringReader(csvDataAsString));
        while(reader.readLine() != null){
            lineCount++;
        }
        reader.close();
        return lineCount;
    }

    private char getSeparatorChar(HttpServletRequest request) {
        char separatorChar = ',';
        String separator = request.getParameter("separator");
        if(separator != null && "TAB".equalsIgnoreCase(separator)){
           separatorChar =  '\t';
        }
        return separatorChar;
    }

    private int doLineCount(File csvFile) throws IOException {
        int lineCount = 0;
        BufferedReader reader = new BufferedReader(new FileReader(csvFile));
        while(reader.readLine() != null){
            lineCount++;
        }
        reader.close();
        return lineCount;
    }

    private File downloadCSV(String urlToZippedData) throws IOException {
        long fileId = System.currentTimeMillis();
        String zipFilePath = uploadTempDir + File.separator + fileId + ".zip";
        String unzippedFilePath = uploadTempDir + File.separator + fileId + ".csv";
        InputStream input = new URL(urlToZippedData).openStream();
        OutputStream output = new FileOutputStream(zipFilePath);
        IOUtils.copyLarge(input, output);

        //extract zip
        ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFilePath));
        ZipEntry ze = zis.getNextEntry();
        byte[] buffer = new byte[10240];
        FileOutputStream fos = new FileOutputStream(unzippedFilePath);
        int len;
        while ((len = zis.read(buffer))>0){
            fos.write(buffer, 0, len);
        }
        fos.flush();
        fos.close();
        return new File(unzippedFilePath);
    }

    private String createTempResource(HttpServletRequest request, String datasetName, int lineCount) throws IOException {

        ObjectMapper mapper = new ObjectMapper();

        UserUpload uu = new UserUpload();
        uu.setNumberOfRecords(lineCount);
        uu.setName(datasetName);

        String json = mapper.writeValueAsString(uu);
        PostMethod post = new PostMethod(registryUrl + "/ws/tempDataResource");
        post.setRequestBody(json);
        HttpClient httpClient = new HttpClient();
        httpClient.executeMethod(post);

        logger.debug("######### Retrieved: " + post.getResponseHeader("location").getValue());
        logger.debug("######### Data uploaded....");
        String collectoryUrl = post.getResponseHeader("location").getValue();
        return collectoryUrl.substring(collectoryUrl.lastIndexOf('/') + 1);
    }
    
    public void setRegistryUrl(String registryUrl) {
		this.registryUrl = registryUrl;
	}

	public void setUploadStatusDir(String uploadStatusDir) {
		this.uploadStatusDir = uploadStatusDir;
	}

	public void setUploadTempDir(String uploadTempDir) {
		this.uploadTempDir = uploadTempDir;
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
    public String status = "LOADING";
    protected String[] headers;
    protected String datasetName = "";
    protected CSVReader csvData;
    protected int lineCount = 0;
    protected boolean firstLineIsData;
    protected String tempUid;
    protected String uploadStatusDir;
    protected Integer recordsToLoad = null;
    protected String[] customIndexFields = null;

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
            FileUtils.writeStringToFile(statusFile, om.writeValueAsString(new UploadStatus("LOADING", "Starting...", 0)));
            Integer recordCount = lineCount;
            if(!firstLineIsData){
                recordCount--;
            }

            Integer counter = 0;

            try {
                String[] currentLine = csvData.readNext();

                //default data type for the "customIndexFields" is a string
                CollectionUtils.addAll(intList, customIndexFields);

                //if the first line is data, add a record, else discard
                if(firstLineIsData){
                    addRecord(tempUid, datasetName, currentLine, headers, intList, floatList, stringList);
                }

                //write the data to DB
                Integer percentComplete  = 0;
                while((currentLine = csvData.readNext()) != null){
                    //System.out.println("######## loading line: " + counter);
                    counter++;
                    addRecord(tempUid, datasetName, currentLine, headers, intList, floatList, stringList);
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
                csvData.close();
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
            FileUtils.writeStringToFile(statusFile, om.writeValueAsString(new UploadStatus("PROCESSING","Starting",50)));
            DefaultObserverCallback processingCallback = new DefaultObserverCallback("PROCESSING", recordCount, statusFile, 50, "processed");
            au.org.ala.biocache.Store.process(tempUid, 4, processingCallback);

            status = "INDEXING";
            logger.debug("Indexing " + tempUid + " " + tmpCustIndexFields);
            FileUtils.writeStringToFile(statusFile, om.writeValueAsString(new UploadStatus("INDEXING","Starting",75)));
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

    private void addRecord(String tempUid, String datasetName, String[] currentLine, String[] headers, List<String> intList, List<String> floatList, List<String> stringList) {
        Map<String,String> map = new HashMap<String, String>();
        for(int i = 0; i < headers.length && i < currentLine.length; i++){
            if(currentLine[i] != null && currentLine[i].trim().length() > 0 ){
                map.put(headers[i], currentLine[i].trim());
                //now test of the header value is part of the custom index fields and perform a data check
                if(intList.contains(headers[i])){
                    try {
                        Integer.parseInt(currentLine[i].trim());
                    } catch(Exception e){
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
        map.put("datasetName", datasetName);
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