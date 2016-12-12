/**************************************************************************
 * Copyright (C) 2013 Atlas of Living Australia
 * All Rights Reserved.
 * <p/>
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 * <p/>
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.biocache.web;

import au.org.ala.biocache.dao.PersistentQueueDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.DownloadDetailsDTO;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import au.org.ala.biocache.dto.DownloadRequestParams;
import au.org.ala.biocache.dto.IndexFieldDTO;
import au.org.ala.biocache.service.AuthService;
import au.org.ala.cas.util.AuthenticationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocumentList;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A Controller for downloading records based on queries.  This controller
 * will provide methods for offline asynchronous downloads of large result sets.
 * <ul> 
 * <li> persistent queue to contain the offline downloads. - written to filesystem before emailing to supplied user </li>
 * <li> administering the queue - changing order, removing items from queue </li>
 * </ul> 
 * @author Natasha Carter (natasha.carter@csiro.au)
 */
@Controller
public class DownloadController extends AbstractSecureController {

    private final static Logger logger = Logger.getLogger(DownloadController.class);

    /** Fulltext search DAO */
    @Inject
    protected SearchDAO searchDAO;

    @Inject
    protected PersistentQueueDAO persistentQueueDAO;

    @Inject
    protected AuthService authService;

    @Value("${webservices.root:http://localhost:8080/biocache-service}")
    protected String webservicesRoot;

    @Value("${download.url:http://biocache.ala.org.au/biocache-download}")
    protected String biocacheDownloadUrl;

    @Value("${download.dir:/data/biocache-download}")
    protected String biocacheDownloadDir;

    @Value("${download.auth.sensitive:false}")
    protected Boolean downloadAuthSensitive;

    //TODO: this should be retrieved from SDS
    @Value("${sensitiveAccessRoles:{\n" +
            "\n" +
            "\"ROLE_SDS_ACT\" : \"sensitive:\\\"generalised\\\" AND (cl927:\\\"Australian Captial Territory\\\" OR cl927:\\\"Jervis Bay Territory\\\") AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\"\n" +
            "\"ROLE_SDS_NSW\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"New South Wales (including Coastal Waters)\\\" AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_NZ\" : \"sensitive:\\\"generalised\\\" AND (data_resource_uid:dr2707 OR data_resource_uid:dr812 OR data_resource_uid:dr814 OR data_resource_uid:dr808 OR data_resource_uid:dr806 OR data_resource_uid:dr815 OR data_resource_uid:dr802 OR data_resource_uid:dr805 OR data_resource_uid:dr813) AND -cl927:* AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_NT\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Northern Territory (including Coastal Waters)\\\" AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_QLD\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Queensland (including Coastal Waters)\\\" AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_SA\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"South Australia (including Coastal Waters)\\\" AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_TAS\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Tasmania (including Coastal Waters)\\\" AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_VIC\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Victoria (including Coastal Waters)\\\" AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_WA\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Western Australia (including Coastal Waters)\\\" AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_BIRDLIFE\" : \"sensitive:\\\"generalised\\\" AND (data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\"\n" +
            "\n" +
            "}}")
    protected String sensitiveAccessRoles;

    /**
     * Retrieves all the downloads that are on the queue
     * @return
     */
    @RequestMapping("occurrences/offline/download/stats")
    public @ResponseBody List<DownloadDetailsDTO> getCurrentDownloads(
            HttpServletResponse response,
            @RequestParam(value = "apiKey", required = true) String apiKey) throws Exception {
        if (apiKey != null) {
            if (shouldPerformOperation(apiKey, response, false)) {
                return persistentQueueDAO.getAllDownloads();
            }
        }
        return null;
    }

    /**
     * Add a download to the offline queue
     * @param requestParams
     * @param ip
     * @param apiKey
     * @param type
     * @param response
     * @param request
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "occurrences/offline/{type}/download*", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody Object occurrenceDownload(
            DownloadRequestParams requestParams,
            @RequestParam(value = "ip", required = false) String ip,
            @RequestParam(value = "apiKey", required = false) String apiKey,
            @PathVariable("type") String type,
            HttpServletResponse response,
            HttpServletRequest request) throws Exception {

        DownloadType downloadType = "index".equals(type.toLowerCase()) ? DownloadType.RECORDS_INDEX : DownloadType.RECORDS_DB;

        return download(requestParams, ip, apiKey, response, request, downloadType);
    }

    /**
     * Add a download to the offline queue
     * @param requestParams
     * @param ip
     * @param apiKey
     * @param response
     * @param request
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "occurrences/offline/download*", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody Object occurrenceDownload(
            DownloadRequestParams requestParams,
            @RequestParam(value = "ip", required = false) String ip,
            @RequestParam(value = "apiKey", required = false) String apiKey,
            HttpServletResponse response,
            HttpServletRequest request) throws Exception {

        //download from index when there are no CASSANDRA fields requested
        boolean hasDBColumn = requestParams.getIncludeMisc();
        String fields = requestParams.getFields() + "," + requestParams.getExtra();
        if (fields.length() > 1) {
            for (String column : fields.split(",")) {
                for (IndexFieldDTO field : searchDAO.getIndexedFields()) {
                    if (!field.isStored() && field.getDownloadName() != null && field.getDownloadName().equals(column)) {
                        hasDBColumn = true;
                        break;
                    }
                }
                if (hasDBColumn) break;
            }
        }
        DownloadType downloadType = hasDBColumn ? DownloadType.RECORDS_DB : DownloadType.RECORDS_INDEX;

        return download(requestParams, ip, apiKey, response, request, downloadType);
    }

    private Object download(DownloadRequestParams requestParams, String ip, String apiKey,
                            HttpServletResponse response, HttpServletRequest request,
                            DownloadType downloadType) throws Exception {

        boolean sensitive = false;
        if (apiKey != null) {
            if (shouldPerformOperation(apiKey, response, false)) {
                sensitive = true;
            }
        } else if (StringUtils.isEmpty(requestParams.getEmail())) {
            response.sendError(HttpServletResponse.SC_PRECONDITION_FAILED, "Unable to perform an offline download without an email address");
        }

        //get the fq that includes only the sensitive data that the userId ROLES permits
        String sensitiveFq = null;
        if (!sensitive) {
            sensitiveFq = getSensitiveFq(request);
        }

        ip = ip == null ? request.getRemoteAddr() : ip;

        //create a new task
        DownloadDetailsDTO dd = new DownloadDetailsDTO(requestParams, ip, downloadType);
        dd.setIncludeSensitive(sensitive);
        dd.setSensitiveFq(sensitiveFq);

        //get query (max) count for queue priority
        requestParams.setPageSize(0);
        SolrDocumentList result = searchDAO.findByFulltext(requestParams);
        dd.setTotalRecords(result.getNumFound());

        Map<String, Object> status = new LinkedHashMap<>();
        DownloadDetailsDTO d = persistentQueueDAO.isInQueue(dd);
        if (d != null) {
            dd = d;
            status.put("message", "Already in queue.");
        } else {
            persistentQueueDAO.addDownloadToQueue(dd);
        }

        status.put("status", "inQueue");
        status.put("statusUrl", webservicesRoot + "/occurrences/offline/status/" + dd.getUniqueId());
        status.put("queueSize", persistentQueueDAO.getTotalDownloads());
        return status;
    }

    @RequestMapping(value = "occurrences/offline/status/{id}", method = RequestMethod.GET)
    public @ResponseBody Object occurrenceDownloadStatus(@PathVariable("id") String id) throws Exception {

        Map<String, Object> status = new LinkedHashMap<>();

        //is it in the queue?
        List<DownloadDetailsDTO> downloads = persistentQueueDAO.getAllDownloads();
        for (DownloadDetailsDTO dd : downloads) {
            if (id.equals(dd.getUniqueId())) {
                if (dd.getFileLocation() == null) {
                    status.put("status", "inQueue");
                } else {
                    status.put("status", "running");
                }
                status.put("statusUrl", webservicesRoot + "/occurrences/offline/status/" + id);
                break;
            }
        }

        //is it finished?
        if (!status.containsKey("status")) {
            int sep = id.lastIndexOf('-');
            File dir = new File(biocacheDownloadDir + File.separator + id.substring(0, sep) + File.separator + id.substring(sep + 1));
            if (dir.isDirectory() && dir.exists()) {
                for (File file : dir.listFiles()) {
                    if (file.isFile() && file.getPath().endsWith(".zip") && file.length() > 0) {
                        status.put("status", "finished");
                        status.put("downloadUrl", file.getPath().replace(biocacheDownloadDir, biocacheDownloadUrl));
                    }
                }
                if (!status.containsKey("status")) {
                    status.put("status", "failed");
                }
            }
        }

        if (!status.containsKey("status")) {
            status.put("status", "invalidId");
        }

        return status;
    }

    /**
     * Cancel queued download. Does not cancel a download in progress.
     *
     * @param id
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "occurrences/offline/cancel/{id}", method = RequestMethod.GET)
    public @ResponseBody Object occurrenceDownloadCancel(
            @PathVariable("id") String id,
            HttpServletResponse response,
            @RequestParam(value = "apiKey", required = true) String apiKey) throws Exception {

        if (apiKey == null || !shouldPerformOperation(apiKey, response, false)) {
            return null;
        }

        Map<String, Object> status = new LinkedHashMap<>();

        //is it in the queue?
        List<DownloadDetailsDTO> downloads = persistentQueueDAO.getAllDownloads();
        for (DownloadDetailsDTO dd : downloads) {
            if (id.equals(dd.getUniqueId())) {
                if (dd.getFileLocation() == null) {
                    persistentQueueDAO.removeDownloadFromQueue(dd);
                    status.put("cancelled", "true");
                    status.put("status", "notInQueue");
                } else {
                    status.put("cancelled", "false");
                    status.put("status", "running");
                }
                break;
            }
        }

        if (!status.containsKey("status")) {
            status.put("cancelled", "false");
            status.put("status", "notInQueue");
        }

        return status;
    }

    private String getSensitiveFq(HttpServletRequest request) throws ParseException {

        if (!isValidKey(request.getHeader("apiKey")) || !downloadAuthSensitive) {
            return null;
        }

        String sensitiveFq = "";
        JSONParser jp = new JSONParser();
        JSONObject jo = (JSONObject) jp.parse(sensitiveAccessRoles);
        List roles = authService.getUserRoles(request.getHeader("X-ALA-userId"));
        for (Object role : jo.keySet()) {
            if (roles.contains(role)) {
                if (sensitiveFq.length() > 0) {
                    sensitiveFq += " OR ";
                }
                sensitiveFq += "(" + jo.get(role) + ")";
            }
        }

        if (sensitiveFq.length() == 0) {
            return null;
        }

        logger.debug("sensitiveOnly download requested for user: " + AuthenticationUtils.getUserId(request) +
                ", using fq: " + sensitiveFq);

        return sensitiveFq;
    }
}
