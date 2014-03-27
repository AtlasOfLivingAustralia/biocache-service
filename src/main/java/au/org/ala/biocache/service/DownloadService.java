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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipOutputStream;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import au.org.ala.biocache.dao.PersistentQueueDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.DownloadDetailsDTO;
import au.org.ala.biocache.dto.DownloadRequestParams;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import org.ala.client.appender.RestLevel;
import org.ala.client.model.LogEventVO;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

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
    private int concurrentDownloads = 1;
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

    /** Stores the current list of downloads that are being performed. */
    private List<DownloadDetailsDTO> currentDownloads = Collections.synchronizedList(new ArrayList<DownloadDetailsDTO>());

    @Value("${data.description.url:https://docs.google.com/spreadsheet/ccc?key=0AjNtzhUIIHeNdHhtcFVSM09qZ3c3N3ItUnBBc09TbHc}")
    private String dataFieldDescriptionURL;

    @Value("${registry.url:http://collections.ala.org.au/ws}")
    protected String registryUrl;

    @Value("${citations.url:http://collections.ala.org.au/ws/citations}")
    protected String citationServiceUrl;

    @Value("${media.url:http://biocache.ala.org.au/biocache-media/}")
    public static String biocacheMediaUrl;

    @Value("${media.dir:/data/biocache-media/}")
    public static String biocacheMediaDir;

    @PostConstruct    
    public void init(){
        //create the threads that will be used to perform the downloads
        int i = 0;
        while(i < concurrentDownloads){
            new Thread(new DownloadThread()).start();
            i++;
        }
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
        writeQueryToStream(dd, requestParams, ip, out, includeSensitive, fromIndex, true);
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
    private void writeQueryToStream(DownloadDetailsDTO dd,DownloadRequestParams requestParams, String ip, OutputStream out, boolean includeSensitive, boolean fromIndex, boolean limit) throws Exception {
        String filename = requestParams.getFile();
        String originalParams = requestParams.toString();
        //Use a zip output stream to include the data and citation together in the download
        ZipOutputStream zop = new ZipOutputStream(out);
        String suffix = requestParams.getFileType().equals("shp")?"zip":requestParams.getFileType(); 
        zop.putNextEntry(new java.util.zip.ZipEntry(filename + "." +suffix));
        //put the facets
        if("all".equals(requestParams.getQa())){
            requestParams.setFacets(new String[]{"assertions", "data_resource_uid"});
        } else{
            requestParams.setFacets(new String[]{"data_resource_uid"});
        }
        Map<String, Integer> uidStats = null;
        try {
            if(fromIndex)
                uidStats = searchDAO.writeResultsFromIndexToStream(requestParams, zop, includeSensitive, dd, limit);
            else
                uidStats = searchDAO.writeResultsToStream(requestParams, zop, 100, includeSensitive ,dd);
        } catch (Exception e) {
            logger.error(e.getMessage(),e);
        } finally {
            unregisterDownload(dd);
        }
        zop.closeEntry();
        
        //add the Readme for the data field descriptions
        zop.putNextEntry(new java.util.zip.ZipEntry("README.html"));
        zop.write(("For more information about the fields that are being downloaded please consult <a href='" + dataFieldDescriptionURL + "'>Download Fields</a>.").getBytes());
        
        //add the readme for the Shape file header mappings if necessary
        if(dd.getHeaderMap() != null){
            zop.putNextEntry(new java.util.zip.ZipEntry("Shape-README.html"));
            zop.write(("The name of features is limited to 10 characters. Listed below are the mappings of feature name to download field:").getBytes());
            zop.write(("<table><td><b>Feature</b></td><td><b>Download Field<b></td>").getBytes());
            for(String key:dd.getHeaderMap().keySet()){
                zop.write(("<tr><td>"+key+"</td><td>"+dd.getHeaderMap().get(key)+"</td></tr>").getBytes());
            }
            zop.write(("</table>").getBytes());
            //logger.debug("JSON::: " + objectMapper.writeValueAsString(dd));
        }
        
        //Add the data citation to the download
        if (uidStats != null &&!uidStats.isEmpty() && citationsEnabled) {
            //add the citations for the supplied uids
            zop.putNextEntry(new java.util.zip.ZipEntry("citation.csv"));
            try {
                getCitations(uidStats, zop);
            } catch (Exception e) {
                logger.error(e.getMessage(),e);
            }
            zop.closeEntry();
        } else {
            logger.debug("Not adding citation. Enabled: " + citationsEnabled + " uids: " +uidStats);
        }
        zop.flush();
        zop.close();
        
        //now construct the sourceUrl for the log event
        String sourceUrl = originalParams.contains("qid:")? webservicesRoot + "?"+ requestParams.toString(): webservicesRoot +"?"+ originalParams;

        //logger.debug("UID stats : " + uidStats);
        //log the stats to ala logger        
        LogEventVO vo = new LogEventVO(1002,requestParams.getReasonTypeId(), requestParams.getSourceTypeId(), requestParams.getEmail(), requestParams.getReason(), ip,null, uidStats, sourceUrl);        
        logger.log(RestLevel.REMOTE, vo);
    }
    
    public void writeQueryToStream(DownloadRequestParams requestParams, HttpServletResponse response, String ip, ServletOutputStream out, boolean includeSensitive, boolean fromIndex) throws Exception {
        String filename = requestParams.getFile();

        response.setHeader("Cache-Control", "must-revalidate");
        response.setHeader("Pragma", "must-revalidate");
        response.setHeader("Content-Disposition", "attachment;filename=" + filename +".zip");
        response.setContentType("application/zip");
        
        DownloadDetailsDTO.DownloadType type= fromIndex ? DownloadType.RECORDS_INDEX:DownloadType.RECORDS_DB;
        DownloadDetailsDTO dd = registerDownload(requestParams, ip, type);
        writeQueryToStream(dd, requestParams, ip, out, includeSensitive, fromIndex);
    }
    
    /**
     * get citation info from citation web service and write it into citation.txt file.
     * 
     * @param uidStats
     * @param out
     * @throws HttpException
     * @throws IOException
     */
    public void getCitations(Map<String, Integer> uidStats, OutputStream out) throws IOException{
        if(citationsEnabled){
            if(uidStats == null || uidStats.isEmpty() || out == null){
                //throw new NullPointerException("keys and/or out is null!!");
                logger.error("Unable to generate citations: keys and/or out is null!!");
            }
            
            //Object[] citations = restfulClient.restPost(citationServiceUrl, "text/json", uidStats.keySet());
            List<LinkedHashMap<String, Object>> entities = restTemplate.postForObject(citationServiceUrl, uidStats.keySet(), List.class);
            if(entities.size()>0){
                out.write("\"Data resource ID\",\"Data resource\",\"Citation\",\"Rights\",\"More information\",\"Data generalizations\",\"Information withheld\",\"Download limit\",\"Number of Records in Download\"\n".getBytes());
                for(Map<String,Object> record : entities){
                    StringBuilder sb = new StringBuilder();
                    //ensure that the record is not null to prevent NPE on the "get"s
                    if(record != null){
                        sb.append("\"").append(record.get("uid")).append("\",");
                        sb.append("\"").append(record.get("name")).append("\",");
                        sb.append("\"").append(record.get("citation")).append("\",");
                        sb.append("\"").append(record.get("rights")).append("\",");
                        sb.append("\"").append(record.get("link")).append("\",");
                        sb.append("\"").append(record.get("dataGeneralizations")).append("\",");
                        sb.append("\"").append(record.get("informationWithheld")).append("\",");
                        sb.append("\"").append(record.get("downloadLimit")).append("\",");
                        String count = uidStats.get(record.get("uid")).toString();
                        sb.append("\"").append(count).append("\"");
                        sb.append("\n");                    
                        out.write(sb.toString().getBytes());
                    } else{
                        logger.warn("A null record was returned from the collectory citation service: " + entities);
                    }
                }
            }
        }
    }
    
    /**
     * @return the concurrentDownloads
     */
    public int getConcurrentDownloads() {
        return concurrentDownloads;
    }

    /**
     * @param concurrentDownloads the concurrentDownloads to set
     */
    public void setConcurrentDownloads(int concurrentDownloads) {
        this.concurrentDownloads = concurrentDownloads;
    }
    
    /**
     * @param registryUrl the registryUrl to use
     */
    public void setRegistryUrl(String registryUrl) {
        this.registryUrl = registryUrl;
    }

    public void setCitationServiceUrl(String citationServiceUrl) {
        this.citationServiceUrl = citationServiceUrl;
    }


    public void setDataFieldDescriptionURL(String dataFieldDescriptionURL) {
        this.dataFieldDescriptionURL = dataFieldDescriptionURL;
    }
    
    /**
     * A thread responsible for creating a records dump offline.
     * 
     * @author Natasha Carter (natasha.carter@csiro.au)
     */
    private class DownloadThread implements Runnable{
        
        private DownloadDetailsDTO currentDownload =null;       

        @Override
        public void run() {
            while(true){
                if(persistentQueueDAO.getTotalDownloads()==0){
                    try {
                        Thread.currentThread().sleep(10000);
                    } catch(InterruptedException e){
                        //I don't care thatI have been interrupted.
                    }
                }
                currentDownload = persistentQueueDAO.getNextDownload();
                if(currentDownload != null){
                    logger.info("Starting to download the offline request: " + currentDownload);
                    //we are now ready to start the download
                    //we need to create an outputstream to the file syste,
                    try{
                        FileOutputStream fos = FileUtils.openOutputStream(new File(currentDownload.getFileLocation()));
                        //register the download
                        currentDownloads.add(currentDownload);
                        writeQueryToStream(currentDownload, currentDownload.getRequestParams(),
                                currentDownload.getIpAddress(), fos, currentDownload.getIncludeSensitive(), 
                                currentDownload.getDownloadType() == DownloadType.RECORDS_INDEX, false);
                      //now that the download is complete email a link to the recipient.
                        String subject = messageSource.getMessage("offlineEmailSubject",null,"Occurrence Download Complete - "+currentDownload.getRequestParams().getFile(),null);
                        String fileLocation = currentDownload.getFileLocation().replace(biocacheMediaDir, biocacheMediaUrl);
                        String body = messageSource.getMessage("offlineEmailBody",new Object[]{fileLocation}, "The file has been generated. Please download you file from " +fileLocation , null);
                        emailService.sendEmail(currentDownload.getEmail(), 
                                subject, 
                                body);
                        //now take the job off the list
                        persistentQueueDAO.removeDownloadFromQueue(currentDownload);
                        
                        //save the statistics to the download directory                        
                        FileOutputStream statsStream = FileUtils.openOutputStream(new File(new File(currentDownload.getFileLocation()).getParent()+File.separator+"downloadStats.json"));
                        String json = objectMapper.writeValueAsString(currentDownload);
                        statsStream.write(json.getBytes() );
                        statsStream.flush();
                        statsStream.close();
                    } catch(Exception e){
                        logger.error("Error in offline download", e);
                        //TODO maybe send an email to support saying that the offline email failed??
                    }
                }
            }
        }
    }
}
