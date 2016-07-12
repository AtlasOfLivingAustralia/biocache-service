/**************************************************************************
 *  Copyright (C) 2013 Atlas of Living Australia
 *  All Rights Reserved.
 * 
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 * 
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.biocache.service;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import au.org.ala.biocache.stream.OptionalZipOutputStream;
import au.org.ala.biocache.dao.PersistentQueueDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.DownloadDetailsDTO;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import au.org.ala.biocache.dto.DownloadRequestParams;
import au.org.ala.biocache.dto.IndexFieldDTO;
import au.org.ala.biocache.util.AlaFileUtils;
import org.ala.client.appender.RestLevel;
import org.ala.client.model.LogEventVO;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;
import sun.security.pkcs.EncodingException;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URLEncoder;
import java.util.*;

/**
 * Services to perform the downloads.
 * 
 * Can configure the number of off-line download processors
 * @author Natasha Carter (natasha.carter@csiro.au)
 */
@Component("downloadService")
public class DownloadService {

    private static final Logger logger = Logger.getLogger(DownloadService.class);
    /** Number of threads to perform to offline downloads on can be configured. */
    @Value("${concurrent.downloads:1}")
    protected int concurrentDownloads = 1;
    /** Additional download threads for matching subsets of offline downloads.
     * default:
     * 4 threads for SOLR downloads for <50000 occurrences
     * 1 thread for SOLR downloads with any number of occurrences
     * 2 threads for CASSANDA downloads for <50000 occurrences
     */
    @Value("${concurrent.downloads.extra:[{\"threads\": 4, \"maxRecords\": 50000, \"type\": \"index\"}, {\"threads\": 1, \"maxRecords\": 100000000, \"type\": \"index\"}, {\"threads\": 1, \"maxRecords\": 50000, \"type\": \"db\"}]}")
    protected String concurrentDownloadsExtra;
    @Inject
    protected PersistentQueueDAO persistentQueueDAO;
    @Inject 
    SearchDAO searchDAO;
    @Inject
    private RestOperations restTemplate;
    @Inject
    private org.codehaus.jackson.map.ObjectMapper objectMapper;
    @Inject
    private EmailService emailService;
    @Inject
    private AbstractMessageSource messageSource;

    //default value is supplied for the property below
    @Value("${webservices.root:http://localhost:8080/biocache-service}")
    protected String webservicesRoot;

    //NC 20131018: Allow citations to be disabled via config (enabled by default)
    @Value("${citations.enabled:true}")
    protected Boolean citationsEnabled;

    //Allow headings information to be disabled via config (enabled by default)
    @Value("${headings.enabled:true}")
    protected Boolean headingsEnabled;

    /** Stores the current list of downloads that are being performed. */
    private List<DownloadDetailsDTO> currentDownloads = Collections.synchronizedList(new ArrayList<DownloadDetailsDTO>());

    @Value("${data.description.url:https://docs.google.com/spreadsheet/ccc?key=0AjNtzhUIIHeNdHhtcFVSM09qZ3c3N3ItUnBBc09TbHc}")
    private String dataFieldDescriptionURL;

    @Value("${registry.url:http://collections.ala.org.au/ws}")
    protected String registryUrl;

    @Value("${citations.url:http://collections.ala.org.au/ws/citations}")
    protected String citationServiceUrl;

    @Value("${download.url:http://biocache.ala.org.au/biocache-download}")
    protected String biocacheDownloadUrl;

    @Value("${download.dir:/data/biocache-download}")
    protected String biocacheDownloadDir;

    @Value("${download.email.subject:ALA Occurrence Download Complete - [filename]}")
    protected String biocacheDownloadEmailSubject;

    @Value("${download.email.body:The download file has been generated on [date] via the search: [searchUrl]. Please download your file from [url]}")
    protected String biocacheDownloadEmailBody;

    @Value("${download.email.subject:Occurrence Download Failed - [filename]}")
    protected String biocacheDownloadEmailSubjectError;

    @Value("${download.email.body.error:The download has failed.}")
    protected String biocacheDownloadEmailBodyError;

    @Value("${download.readme.content:When using this download please use the following citation:<br><br><cite>Atlas of Living Australia occurrence download at <a href='[url]'>biocache</a> accessed on [date].</cite><br><br>Data contributed by the following providers:<br><br>[dataProviders]<br><br>More information can be found at <a href='http://www.ala.org.au/about-the-atlas/terms-of-use/citing-the-atlas/'>citing the ALA</a>.}")
    protected String biocacheDownloadReadme;

    @Value("${biocache.ui.url:http://biocache.ala.org.au}")
    protected String biocacheUiUrl;

    @PostConstruct    
    public void init(){
        //init on thread so as to not hold up other PostConstruct that this may depend on
        new Thread() {
            @Override
            public void run() {
                //create the threads that will be used to perform the downloads
                int i = 0;
                while (i < concurrentDownloads) {
                    new Thread(new DownloadThread(null, null)).start();
                    i++;
                }

                //create additional threads to operate on subsets of downloads
                try {
                    JSONParser jp = new JSONParser();
                    JSONArray concurrentDownloadsExtraJson = (JSONArray) jp.parse(concurrentDownloadsExtra);
                    for (Object o : concurrentDownloadsExtraJson) {
                        JSONObject jo = (JSONObject) o;
                        int threads = ((Long) jo.get("threads")).intValue();
                        while (threads > 0) {
                            Integer maxRecords = jo.containsKey("maxRecords") ? ((Long) jo.get("maxRecords")).intValue() : null;
                            String type = jo.containsKey("type") ? jo.get("type").toString() : null;
                            DownloadType dt = null;
                            if (type != null) {
                                dt = "index".equals(type) ? DownloadType.RECORDS_INDEX : DownloadType.RECORDS_DB;
                            }
                            new Thread(new DownloadThread(maxRecords, dt)).start();
                            threads--;
                        }
                    }
                } catch (Exception e) {
                    logger.error("failed to create all extra offline download threads for concurrent.downloads.extra=" + concurrentDownloadsExtra, e);
                }
            }
        }.start();
    }

    /**
     * Registers a new active download
     * @param requestParams
     * @param ip
     * @param type
     * @return
     */
    public DownloadDetailsDTO registerDownload(DownloadRequestParams requestParams, String ip, DownloadDetailsDTO.DownloadType type){
        DownloadDetailsDTO dd = new DownloadDetailsDTO(requestParams.toString(), ip, type);
        currentDownloads.add(dd);
        return dd;
    }

    /**
     * Removes a completed download from active list.
     * @param dd
     */
    public void unregisterDownload(DownloadDetailsDTO dd){
        //remove it from the list
        currentDownloads.remove(dd);
    }

    /**
     * Returns a list of current downloads
     * @return
     */
    public List<DownloadDetailsDTO> getCurrentDownloads(){
        return currentDownloads;
    }

    private void writeQueryToStream(DownloadDetailsDTO dd,DownloadRequestParams requestParams, String ip, OutputStream out, boolean includeSensitive, boolean fromIndex) throws Exception {
        writeQueryToStream(dd, requestParams, ip, out, includeSensitive, fromIndex, true, true);
    }
    
    /**
     * Writes the supplied download to the supplied output stream. It will include all the appropriate citations etc.
     * 
     * @param dd
     * @param requestParams
     * @param ip
     * @param out
     * @param includeSensitive
     * @param fromIndex
     * @throws Exception
     */
    public void writeQueryToStream(DownloadDetailsDTO dd,DownloadRequestParams requestParams, String ip, OutputStream out, boolean includeSensitive, boolean fromIndex, boolean limit, boolean zip) throws Exception {

        String filename = requestParams.getFile();
        String originalParams = requestParams.toString();
        //Use a zip output stream to include the data and citation together in the download
        OptionalZipOutputStream sp = new OptionalZipOutputStream(zip ? OptionalZipOutputStream.Type.zipped : OptionalZipOutputStream.Type.unzipped, out);
        String suffix = requestParams.getFileType().equals("shp") ? "zip" : requestParams.getFileType();
        sp.putNextEntry(filename + "." +suffix);
        //put the facets
        if("all".equals(requestParams.getQa())){
            requestParams.setFacets(new String[]{"assertions", "data_resource_uid"});
        } else {
            requestParams.setFacets(new String[]{"data_resource_uid"});
        }
        Map<String, Integer> uidStats = null;
        try {
            if(fromIndex)
                uidStats = searchDAO.writeResultsFromIndexToStream(requestParams, sp, includeSensitive, dd, limit);
            else
                uidStats = searchDAO.writeResultsToStream(requestParams, sp, 100, includeSensitive ,dd, limit);
        } catch (Exception e) {
            logger.error(e.getMessage(),e);
        } finally {
            unregisterDownload(dd);
        }
        sp.closeEntry();

        //add the readme for the Shape file header mappings if necessary
        if(dd.getHeaderMap() != null){
            sp.putNextEntry("Shape-README.html");
            sp.write(("The name of features is limited to 10 characters. Listed below are the mappings of feature name to download field:").getBytes());
            sp.write(("<table><td><b>Feature</b></td><td><b>Download Field<b></td>").getBytes());
            for(String key:dd.getHeaderMap().keySet()){
                sp.write(("<tr><td>"+key+"</td><td>"+dd.getHeaderMap().get(key)+"</td></tr>").getBytes());
            }
            sp.write(("</table>").getBytes());
        }
        
        //Add the data citation to the download
        List<String> citationsForReadme = new ArrayList<String>();
        if (uidStats != null &&!uidStats.isEmpty() && citationsEnabled) {
            //add the citations for the supplied uids
            sp.putNextEntry("citation.csv");
            try {
                getCitations(uidStats, sp, requestParams.getSep(), requestParams.getEsc(), citationsForReadme);
            } catch (Exception e) {
                logger.error(e.getMessage(),e);
            }
            sp.closeEntry();
        } else {
            logger.debug("Not adding citation. Enabled: " + citationsEnabled + " uids: " +uidStats);
        }

        //add the Readme for the data field descriptions
        sp.putNextEntry("README.html");
        String dataProviders = "<ul><li>" + StringUtils.join(citationsForReadme, "</li><li>") + "</li></ul>";

        //online downloads will not have a file location or request params set in dd.
        if (dd.getRequestParams() == null) {
            dd.setRequestParams(requestParams);
        }
        if (dd.getFileLocation() == null) {
            dd.setFileLocation(generateSearchUrl(dd.getRequestParams()));
        }

        String fileLocation = dd.getFileLocation().replace(biocacheDownloadDir, biocacheDownloadUrl);
        String readmeContent = biocacheDownloadReadme.replace("[url]", fileLocation).replace("[date]",
                dd.getStartDateString()).replace("[searchUrl]",generateSearchUrl(dd.getRequestParams())).replace("[dataProviders]", dataProviders);
        logger.debug(readmeContent);
        sp.write((readmeContent).getBytes());
        sp.write(("For more information about the fields that are being downloaded please consult <a href='" + dataFieldDescriptionURL + "'>Download Fields</a>.").getBytes());
        sp.closeEntry();

        //Add headings file, listing information about the headings
        if (headingsEnabled) {
            //add the citations for the supplied uids
            sp.putNextEntry("headings.csv");
            try {
                getHeadings(uidStats, sp, requestParams, dd.getMiscFields());
            } catch (Exception e) {
                logger.error(e.getMessage(),e);
            }
            sp.closeEntry();
        } else {
            logger.debug("Not adding header. Enabled: " + headingsEnabled + " uids: " +uidStats);
        }
        
        sp.flush();
        sp.close();
        
        //now construct the sourceUrl for the log event
        String sourceUrl = originalParams.contains("qid:")? webservicesRoot + "?"+ requestParams.toString(): webservicesRoot +"?"+ originalParams;

        //logger.debug("UID stats : " + uidStats);
        //log the stats to ala logger        
        LogEventVO vo = new LogEventVO(1002,requestParams.getReasonTypeId(), requestParams.getSourceTypeId(), requestParams.getEmail(), requestParams.getReason(), ip,null, uidStats, sourceUrl);        
        logger.log(RestLevel.REMOTE, vo);
    }
    
    public void writeQueryToStream(DownloadRequestParams requestParams, HttpServletResponse response, String ip, ServletOutputStream out, boolean includeSensitive, boolean fromIndex, boolean zip) throws Exception {
        String filename = requestParams.getFile();

        response.setHeader("Cache-Control", "must-revalidate");
        response.setHeader("Pragma", "must-revalidate");

        if (zip) {
            response.setHeader("Content-Disposition", "attachment;filename=" + filename + ".zip");
            response.setContentType("application/zip");
        } else {
            response.setHeader("Content-Disposition", "attachment;filename=" + filename + ".txt");
            response.setContentType("text/plain");
        }
        
        DownloadDetailsDTO.DownloadType type= fromIndex ? DownloadType.RECORDS_INDEX:DownloadType.RECORDS_DB;
        DownloadDetailsDTO dd = registerDownload(requestParams, ip, type);
        writeQueryToStream(dd, requestParams, ip, out, includeSensitive, fromIndex, true, zip);
    }
    
    /**
     * get citation info from citation web service and write it into citation.txt file.
     * 
     * @param uidStats
     * @param out
     * @throws HttpException
     * @throws IOException
     */
    public void getCitations(Map<String, Integer> uidStats, OutputStream out, char sep, char esc, List readmeCitations) throws IOException{
        if(citationsEnabled){
            if(uidStats == null || uidStats.isEmpty() || out == null){
                //throw new NullPointerException("keys and/or out is null!!");
                logger.error("Unable to generate citations: keys and/or out is null!!");
                return;
            }

            CSVWriter writer = new CSVWriter(new OutputStreamWriter(out), sep, '"',esc);
            //Object[] citations = restfulClient.restPost(citationServiceUrl, "text/json", uidStats.keySet());
            List<LinkedHashMap<String, Object>> entities = restTemplate.postForObject(citationServiceUrl, uidStats.keySet(), List.class);
            if(entities.size()>0){
                //i18n of the citation header
                writer.writeNext(new String[]{messageSource.getMessage("citation.uid", null, "UID", null),
                    messageSource.getMessage("citation.name", null,"Name", null),
                    messageSource.getMessage("citation.citation", null,"Citation", null),
                    messageSource.getMessage("citation.rights", null,"Rights", null),
                    messageSource.getMessage("citation.link", null,"More Information", null),
                    messageSource.getMessage("citation.dataGeneralizations", null,"Data generalisations", null),
                    messageSource.getMessage("citation.informationWithheld", null,"Information withheld", null),
                    messageSource.getMessage("citation.downloadLimit", null,"Download limit", null),
                    messageSource.getMessage("citation.count", null,"Number of Records in Download", null)});

                for(Map<String,Object> record : entities){
                    StringBuilder sb = new StringBuilder();
                    //ensure that the record is not null to prevent NPE on the "get"s
                    if(record != null){
                        String count = uidStats.get(record.get("uid")).toString();
                        String[] row = new String[]{getOrElse(record,"uid",""),getOrElse(record, "name",""), getOrElse(record, "citation",""),
                                getOrElse(record,"rights", ""), getOrElse(record, "link",""),getOrElse(record,"dataGeneralizations",""),
                                getOrElse(record, "informationWithheld",""), getOrElse(record, "downloadLimit", ""), count};
                        writer.writeNext(row);

                        if (readmeCitations != null) {
                            readmeCitations.add(row[2] + " (" + row[3] + "). " + row[4]); // used in README.txt
                        }

                    } else {
                        logger.warn("A null record was returned from the collectory citation service: " + entities);
                    }
                }
            }
            writer.flush();
        }
    }

    /**
     * get headings info from index/fields web service and write it into headings.csv file.
     * 
     * output columns:
     *  column name
     *  field requested
     *  dwc 
     *  description
     *  info
     *  field
     *
     * @param out
     * @throws HttpException
     * @throws IOException
     */
    public void getHeadings(Map<String, Integer> uidStats, OutputStream out, DownloadRequestParams params, String [] miscHeaders) throws Exception {
        if (headingsEnabled) {
            if (out == null) {
                //throw new NullPointerException("keys and/or out is null!!");
                logger.error("Unable to generate headings info: out is null!!");
                return;
            }

            CSVWriter writer = new CSVWriter(new OutputStreamWriter(out), params.getSep(), '"', params.getEsc());
            //Object[] citations = restfulClient.restPost(citationServiceUrl, "text/json", uidStats.keySet());
            Set<IndexFieldDTO> indexedFields = searchDAO.getIndexedFields();

            //header
            writer.writeNext(new String[]{"Column name", "Requested field", "DwC Name", "Field name", "Field description", "Download field name", "Download field description", "More information"});

            String[] fieldsRequested = null;
            String[] headerOutput = null;
            for (Map.Entry<String, Integer> e : uidStats.entrySet()) {
                if (e.getValue() == -1) {
                    //String fields requested
                    fieldsRequested = e.getKey().split(",");

                } else if (e.getValue() == -2) {
                    headerOutput = e.getKey().split(",");
                }
            }

            if (fieldsRequested != null && headerOutput != null) {
                //ignore first fieldsRequested and headerOutput record
                for (int i = 1; i < fieldsRequested.length; i++) {

                    //find indexedField by download name
                    IndexFieldDTO ifdto = null;
                    for (IndexFieldDTO f : indexedFields) {
                        //find a matching field
                        if (fieldsRequested[i].equalsIgnoreCase(f.getDownloadName())) {
                            ifdto = f;
                            break;
                        }
                    }
                    //find indexedField by field name
                    if (ifdto == null) {
                        for (IndexFieldDTO f : indexedFields) {
                            //find a matching field
                            if (fieldsRequested[i].equalsIgnoreCase(f.getName())) {
                                ifdto = f;
                                break;
                            }
                        }
                    }

                    if (ifdto != null) {
                        writer.writeNext(new String[]{headerOutput[i], fieldsRequested[i],
                                ifdto.getDwcTerm() != null ? ifdto.getDwcTerm() : "",
                                ifdto.getName() != null ? ifdto.getName() : "",
                                ifdto.getDescription() != null ? ifdto.getDescription() : "",
                                ifdto.getDownloadName() != null ? ifdto.getDownloadName() : "",
                                ifdto.getDownloadDescription() != null ? ifdto.getDownloadDescription() : "",
                                ifdto.getInfo() != null ? ifdto.getInfo() : ""
                        });
                    } else {
                        //others, e.g. assertions
                        String info = messageSource.getMessage("description." + fieldsRequested[i], null, "", null);
                        writer.writeNext(new String[]{headerOutput[i], fieldsRequested[i],
                                "",
                                "",
                                "",
                                "",
                                "",
                                info != null ? info : ""
                        });
                    }
                }
            }

            //misc headers
            if (miscHeaders != null) {
                for (int i = 0; i < miscHeaders.length; i++) {
                    writer.writeNext(new String[]{miscHeaders[i],
                            "",
                            "",
                            "",
                            "",
                            "",
                            "Raw header from data provider."
                    });
                }
            }

            writer.flush();
        }
    }

    private String getOrElse(Map map, String key, String value){
        if(map.containsKey(key)){
            return map.get(key).toString();
        } else{
            return value;
        }
    }
    
    /**
     * A thread responsible for creating a records dump offline.
     * 
     * @author Natasha Carter (natasha.carter@csiro.au)
     */
    private class DownloadThread implements Runnable {
        
        private DownloadDetailsDTO currentDownload = null;

        private Integer maxRecords = null;
        private DownloadType downloadType = null;

        public DownloadThread(Integer maxRecords, DownloadType downloadType) {
            this.maxRecords = maxRecords;
            this.downloadType = downloadType;
        }

        @Override
        public void run() {
            while(true){
                if(persistentQueueDAO.getTotalDownloads()==0){
                    try {
                        Thread.currentThread().sleep(10000);
                    } catch(InterruptedException e){
                        //I don't care that I have been interrupted.
                    }
                }
                currentDownload = persistentQueueDAO.getNextDownload(maxRecords, downloadType);
                if(currentDownload != null){
                    logger.info("Starting to download the offline request: " + currentDownload);
                    //we are now ready to start the download
                    //we need to create an output stream to the file system

                    try{
                        FileOutputStream fos = FileUtils.openOutputStream(new File(currentDownload.getFileLocation()));
                        //register the download
                        currentDownloads.add(currentDownload);
                        //cannot include misc columns if shp
                        if (!currentDownload.getRequestParams().getFileType().equals("csv") && currentDownload.getRequestParams().getIncludeMisc()) {
                            currentDownload.getRequestParams().setIncludeMisc(false);
                        }
                        writeQueryToStream(currentDownload, currentDownload.getRequestParams(),
                                currentDownload.getIpAddress(), fos, currentDownload.getIncludeSensitive(), 
                                currentDownload.getDownloadType() == DownloadType.RECORDS_INDEX, false, true);
                        //now that the download is complete email a link to the recipient.
                        String subject = messageSource.getMessage("offlineEmailSubject",null,
                                biocacheDownloadEmailSubject.replace("[filename]", currentDownload.getRequestParams().getFile()),null);

                        if(currentDownload!=null && currentDownload.getFileLocation() != null){
                            insertMiscHeader(currentDownload);

                            String fileLocation = currentDownload.getFileLocation().replace(biocacheDownloadDir, biocacheDownloadUrl);
                            String searchUrl = generateSearchUrl(currentDownload.getRequestParams());
                            String emailBodyHtml = biocacheDownloadEmailBody.replace("[url]", fileLocation).replace("[date]",
                                    currentDownload.getStartDateString()).replace("[searchUrl]",searchUrl);
                            String body = messageSource.getMessage("offlineEmailBody", new Object[]{fileLocation, searchUrl, currentDownload.getStartDateString()}, emailBodyHtml, null);

                            //save the statistics to the download directory
                            FileOutputStream statsStream = FileUtils.openOutputStream(new File(new File(currentDownload.getFileLocation()).getParent()+File.separator+"downloadStats.json"));
                            String json = objectMapper.writeValueAsString(currentDownload);
                            statsStream.write(json.getBytes() );
                            statsStream.flush();
                            statsStream.close();

                            emailService.sendEmail(currentDownload.getEmail(), subject, body);
                        }

                    } catch(Exception e){
                        logger.error("Error in offline download, sending email. download path: " + currentDownload.getFileLocation(), e);

                        try {
                            String subject = messageSource.getMessage("offlineEmailSubjectError", null,
                                    biocacheDownloadEmailSubjectError.replace("[filename]", currentDownload.getRequestParams().getFile()), null);

                            String fileLocation = currentDownload.getFileLocation().replace(biocacheDownloadDir, biocacheDownloadUrl);
                            String body = messageSource.getMessage("offlineEmailBodyError", new Object[]{fileLocation},
                                    biocacheDownloadEmailBodyError.replace("[url]", fileLocation), null);

                            //user email
                            emailService.sendEmail(currentDownload.getEmail(), subject,
                                    body + "\r\n\r\nuniqueId:" + currentDownload.getUniqueId() + " path:" + currentDownload.getFileLocation().replace(biocacheDownloadDir, ""));
                        } catch (Exception ex) {
                            logger.error("Error sending error message to download email. " + currentDownload.getFileLocation(), ex);
                        }
                    } finally {
                        //incase of server up/down, only remove from queue after emails are sent
                        persistentQueueDAO.removeDownloadFromQueue(currentDownload);
                    }
                }
            }
        }
    }

    /**
     * Generate a search URL the user can use to regenerate the same download (assumes they came via biocache UI)
     *
     * @param params
     * @return url
     */
    private String generateSearchUrl(DownloadRequestParams params) {
        StringBuilder sb = new StringBuilder();
        sb.append(biocacheUiUrl + "/occurrences/search?");

        if (params.getQId() != null) {
            try {
                sb.append("qid=").append(URLEncoder.encode("" + params.getQId(), "UTF-8"));
            } catch (UnsupportedEncodingException e) {

            }
        }
        if (params.getQ() != null) {
            try {
                sb.append("&q=").append(URLEncoder.encode(params.getQ(), "UTF-8"));
            } catch (UnsupportedEncodingException e) {

            }
        }

        if (params.getFq().length > 0 ) {
            for (String fq : params.getFq()) {
                if (StringUtils.isNotEmpty(fq)) {
                    try {
                        sb.append("&fq=").append(URLEncoder.encode(fq, "UTF-8"));
                    } catch (UnsupportedEncodingException e) {

                    }
                }
            }
        }

        if (StringUtils.isNotEmpty(params.getQc())) {
            try {
                sb.append("&qc=").append(URLEncoder.encode(params.getQc(), "UTF-8"));
            } catch (UnsupportedEncodingException e) {

            }
        }

        if (StringUtils.isNotEmpty(params.getWkt())) {
            try {
                sb.append("&wkt=").append(URLEncoder.encode(params.getWkt(), "UTF-8"));
            } catch (UnsupportedEncodingException e) {

            }
        }

        if (params.getLat() != null && params.getLon() != null && params.getRadius() != null) {
            sb.append("&lat=").append(params.getLat());
            sb.append("&lon=").append(params.getLon());
            sb.append("&radius=").append(params.getRadius());
        }

        return sb.toString();
    }

    private void insertMiscHeader(DownloadDetailsDTO download) {
        if (download.getMiscFields() != null && download.getMiscFields().length > 0 &&
                download.getRequestParams() != null ) {
            try {
                //unpack zip
                File unzipDir = new File(download.getFileLocation() + ".dir" + File.separator);
                unzipDir.mkdirs();
                AlaFileUtils.unzip(unzipDir.getPath(), download.getFileLocation());

                //insert header
                for (File f : unzipDir.listFiles()) {
                    if ((f.getName().endsWith(".csv") || f.getName().endsWith(".tsv")) && !"headings.csv".equals(f.getName())) {
                        //make new file
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(f));
                        File fnew = new File(f.getPath() + ".new");
                        FileWriter fw = new FileWriter(fnew);
                        String line;
                        int row = 0;
                        while ((line = bufferedReader.readLine()) != null) {
                            if (row == 0) {
                                String miscHeader[] = download.getMiscFields();

                                if ("csv".equals(download.getRequestParams().getFileType())) {
                                    //retain csv settings
                                    CSVReader reader = new CSVReader(new StringReader(line));
                                    String header[] = reader.readNext();
                                    reader.close();

                                    String newHeader[] = new String[header.length + miscHeader.length];
                                    if (header.length > 0) System.arraycopy(header, 0, newHeader, 0, header.length);
                                    if (miscHeader.length > 0)
                                        System.arraycopy(miscHeader, 0, newHeader, header.length, miscHeader.length);

                                    StringWriter sw = new StringWriter();
                                    CSVWriter writer = new CSVWriter(sw, download.getRequestParams().getSep(), '"', download.getRequestParams().getEsc());
                                    writer.writeNext(newHeader);

                                    line = sw.toString();
                                } else {
                                    for (int i = 0; i < miscHeader.length; i++) {
                                        line += '\t';
                                        line += miscHeader[i].replace("\r", "").replace("\n", "").replace("\t", "");
                                    }
                                    line += '\n';
                                }
                            } else {
                                fw.write("\n");
                            }
                            fw.write(line);
                            row++;
                        }
                        //replace original file
                        FileUtils.copyFile(fnew, f);
                        fnew.delete();
                    }
                }

                //rezip and cleanup
                FileUtils.deleteQuietly(new File(download.getFileLocation()));
                AlaFileUtils.createZip(unzipDir.getPath(), download.getFileLocation());
                FileUtils.deleteDirectory(unzipDir);
            } catch (Exception e) {
                logger.error("failed to append misc header", e);
            }
        }
    }
}
