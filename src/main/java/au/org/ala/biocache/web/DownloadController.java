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
import au.org.ala.biocache.dto.DownloadRequestDTO;
import au.org.ala.biocache.dto.DownloadRequestParams;
import au.org.ala.biocache.service.AuthService;
import au.org.ala.biocache.service.DownloadService;
import au.org.ala.ws.security.AlaUser;
import io.swagger.annotations.ApiParam;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import lombok.extern.slf4j.Slf4j;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrDocumentList;
import org.springdoc.api.annotations.ParameterObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.springframework.web.server.ResponseStatusException;

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
@JsonInclude(JsonInclude.Include.NON_NULL)
@Slf4j
public class DownloadController extends AbstractSecureController {

    /** Fulltext search DAO */
    @Inject
    protected SearchDAO searchDAO;

    @Inject
    protected PersistentQueueDAO persistentQueueDAO;

    @Inject
    protected AuthService authService;

    @Inject
    protected DownloadService downloadService;

    @Value("${download.auth.bypass:false}")
    boolean authBypass = false;

    @Value("${download.auth.role:ROLE_USER}")
    String downloadRole;

    /**
     * Retrieves all the downloads that are on the queue
     * @return
     */
    @SecurityRequirement(name="JWT")
    @Secured({"ROLE_ADMIN"})
    @Operation(summary = "Retrieves all the downloads that are on the queue", tags = "Monitoring")
    @RequestMapping(value = {"occurrences/offline/download/stats"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody List getCurrentDownloads() {

        JsonConfig config = new JsonConfig();
        config.setJsonPropertyFilter((source, name, value) -> value == null);

        JSONArray ja = JSONArray.fromObject(persistentQueueDAO.getAllDownloads(), config);
        for (Object jo : ja) {
            String id = (String) ((net.sf.json.JSONObject) jo).get("uniqueId");
            // TODO  how can we construct these urls
            ((net.sf.json.JSONObject) jo).put("cancelURL", downloadService.webservicesRoot + "/occurrences/offline/cancel/" + id + "?apiKey=TO_BE_ADDED");
//            ((net.sf.json.JSONObject) jo).put("cancelURL", downloadService.webservicesRoot + "/occurrences/offline/cancel/" + id + "?apiKey=" + apiKey);

        }
        return ja;
    }

    /**
     * Add a download to the offline queue
     * @param requestParams
     * @param ip
     * @param response
     * @param request
     * @return
     * @throws Exception
     */
    @Operation(summary = "Asynchronous occurrence download", tags = "Download")
    @RequestMapping(value = { "occurrences/offline/download"}, method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody Object occurrenceDownload(
            @Valid @ParameterObject DownloadRequestParams requestParams,
            @Parameter(description = "Original IP making the request") @RequestParam(value = "ip", required = false) String ip,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {

        DownloadRequestDTO downloadRequestDTO = DownloadRequestDTO.create(requestParams, request);
        Optional<AlaUser> downloadUser = authService.getDownloadUser(downloadRequestDTO, request);

        if (!downloadUser.isPresent()){
            response.sendError(400, "No valid email");
            return null;
        }

        return download(
                downloadRequestDTO,
                downloadUser.get(),
                ip,
                request.getHeader("user-agent"),
                request,
                response,
                DownloadDetailsDTO.DownloadType.RECORDS_INDEX);
    }

    private Map<String, Object> download(DownloadRequestDTO requestParams,
                                         AlaUser alaUser,
                                         String ip,
                                         String userAgent,
                                         HttpServletRequest request,
                                         HttpServletResponse response,
                                         DownloadDetailsDTO.DownloadType downloadType) throws Exception {

        // check the email is supplied and a matching user account exists with the required privileges
        if (alaUser == null || StringUtils.isEmpty(alaUser.getEmail())) {
            response.sendError(HttpServletResponse.SC_PRECONDITION_FAILED, "Unable to perform an offline download without an email address");
            return null;
        }

        ip = ip == null ? request.getRemoteAddr() : ip;

        //create a new task
        DownloadDetailsDTO dd = new DownloadDetailsDTO(requestParams, alaUser, ip, userAgent, downloadType);

        //get query (max) count for queue priority
        requestParams.setPageSize(0);
        requestParams.setFacet(false);
        SolrDocumentList result = searchDAO.findByFulltext(requestParams);
        dd.setTotalRecords(result.getNumFound());

        Map<String, Object> status = new LinkedHashMap<>();
        DownloadDetailsDTO d = persistentQueueDAO.isInQueue(dd);

        if (d != null) {
            status.put("message", "Already in queue.");
            status.put("status", "inQueue");
            status.put("queueSize", persistentQueueDAO.getTotalDownloads());
            status.put("statusUrl", downloadService.webservicesRoot + "/occurrences/offline/status/" + d.getUniqueId());
        } else if (dd.getTotalRecords() > downloadService.dowloadOfflineMaxSize) {
            //identify this download as too large
            File file = new File(downloadService.biocacheDownloadDir + File.separator + UUID.nameUUIDFromBytes(dd.getRequestParams().getEmail().getBytes(StandardCharsets.UTF_8)) + File.separator + dd.getStartTime() + File.separator + "tooLarge");
            FileUtils.forceMkdir(file.getParentFile());
            FileUtils.writeStringToFile(file, "", "UTF-8");
            status.put("downloadUrl", downloadService.biocacheDownloadUrl);
            status.put("status", "skipped");
            status.put("message", downloadService.downloadOfflineMsg);
            status.put("error", "Requested to many records (" + dd.getTotalRecords() + "). The maximum is (" + downloadService.dowloadOfflineMaxSize + ")");
        } else {
            persistentQueueDAO.addDownloadToQueue(dd);
            status.put("status", "inQueue");
            status.put("queueSize", persistentQueueDAO.getTotalDownloads());
            status.put("statusUrl", downloadService.webservicesRoot + "/occurrences/offline/status/" + dd.getUniqueId());
        }

        status.put("searchUrl", downloadService.generateSearchUrl(dd.getRequestParams()));
        writeStatusFile(dd.getUniqueId(), status);

        return status;
    }

    @SecurityRequirement(name="JWT")
    @Secured({"ROLE_ADMIN"})
    @Operation(summary = "List all occurrence downloads", tags = "Monitoring")
    @RequestMapping(value = {"occurrences/offline/status"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Object allOccurrenceDownloadStatus() throws Exception {

        List<Map<String, Object>> allStatus = new ArrayList<Map<String, Object>>();

        Map<String,String> userIdLookup = authService.getMapOfEmailToId();
        //is it in the queue?
        List<DownloadDetailsDTO> downloads = persistentQueueDAO.getAllDownloads();
        for (DownloadDetailsDTO dd : downloads) {
            Map<String, Object> status = new LinkedHashMap<>();
            String id = dd.getUniqueId();
            if (dd.getFileLocation() == null) {
                status.put("status", "inQueue");
            } else {
                status.put("status", "running");
                status.put("records", dd.getRecordsDownloaded());
            }
            status.put("id", id);
            status.put("totalRecords", dd.getTotalRecords());
            status.put("downloadParams", dd.getRequestParams());
            status.put("startDate", dd.getStartDateString());
            status.put("thread", dd.getProcessingThreadName());
            if (userIdLookup != null) {
                status.put("userId", authService.getMapOfEmailToId().get(dd.getRequestParams().getEmail()));
            }
            status.put("statusUrl", downloadService.webservicesRoot + "/occurrences/offline/status/" + id);

            setStatusIfEmpty(id, status);

            allStatus.add(status);
        }

        return allStatus;
    }

    private void setStatusIfEmpty(String id, Map<String, Object> status) throws UnsupportedEncodingException {
        //is it finished?
        if (!status.containsKey("status")) {
            int sep = id.lastIndexOf('-');
            File dir = new File(downloadService.biocacheDownloadDir + File.separator + id.substring(0, sep) + File.separator + id.substring(sep + 1));
            if (dir.isDirectory() && dir.exists()) {
                for (File file : dir.listFiles()) {
                    if (file.isFile() && file.getPath().endsWith(".zip") && file.length() > 0) {
                        status.put("status", "finished");
                        status.put("downloadUrl", downloadService.biocacheDownloadUrl + File.separator + URLEncoder.encode(file.getPath().replace(downloadService.biocacheDownloadDir + "/", ""), "UTF-8").replace("%2F", "/").replace("+", "%20"));
                    }
                    if (file.isFile() && "tooLarge".equals(file.getName())) {
                        status.put("status", "skipped");
                        status.put("message", downloadService.downloadOfflineMsg);
                        status.put("downloadUrl", downloadService.dowloadOfflineMaxUrl);
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
    }

    private void writeStatusFile(String id, Map status) throws IOException {
        File statusDir = new File(downloadService.biocacheDownloadDir + "/" + id.replaceAll("-([0-9]*)$", "/$1"));
        statusDir.mkdirs();
        String json = net.sf.json.JSONObject.fromObject(status).toString();
        FileUtils.writeStringToFile(new File(statusDir.getPath() + "/status.json"), json, "UTF-8");
    }

    @Operation(summary = "Get the status of download", tags = "Download")
    @RequestMapping(value = { "occurrences/offline/status/{id}"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiParam(value = "id", required = true)
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
                    status.put("records", dd.getRecordsDownloaded());
                }
                status.put("totalRecords", dd.getTotalRecords());
                status.put("statusUrl", downloadService.webservicesRoot + "/occurrences/offline/status/" + id);
                if (authService.getMapOfEmailToId() != null) {
                    status.put("userId", authService.getMapOfEmailToId().get(dd.getRequestParams().getEmail()));
                }
                break;
            }
        }

        //is it finished?
        String cleanId = id.replaceAll("[^a-z\\-0-9]", "");
        cleanId = cleanId.replaceAll("-([0-9]*)$", "/$1");
        if (!status.containsKey("status")) {
            File dir = new File(downloadService.biocacheDownloadDir + File.separator + cleanId);
            if (dir.isDirectory() && dir.exists()) {
                for (File file : dir.listFiles()) {
                    if (file.isFile() && file.getPath().endsWith(".zip") && file.length() > 0) {
                        status.put("status", "finished");
                        status.put("downloadUrl", downloadService.biocacheDownloadUrl + File.separator + URLEncoder.encode(file.getPath().replace(downloadService.biocacheDownloadDir + "/", ""), "UTF-8").replace("%2F", "/").replace("+", "%20"));
                    }
                    if (file.isFile() && "tooLarge".equals(file.getName())) {
                        status.put("status", "skipped");
                        status.put("message", downloadService.downloadOfflineMsg);
                        status.put("downloadUrl", downloadService.dowloadOfflineMaxUrl);
                        status.put("error", "requested to many records. The upper limit is (" + downloadService.dowloadOfflineMaxSize + ")");
                    }
                }
                if (!status.containsKey("status")) {
                    status.put("status", "failed");
                }
            }

            // write final status to a file
            if (status.containsKey("status")) {
                writeStatusFile(cleanId, status);
            }
        }

        if (!status.containsKey("status")) {
            //check downloads directory for a status file
            File file = new File(downloadService.biocacheDownloadDir + File.separator + cleanId + "/status.json");
            if (file.exists()) {
                status.putAll(JSONObject.fromObject(FileUtils.readFileToString(file, "UTF-8")));

                // the status.json is only used when a download request is 'lost'. Use an appropriate status.
                status.put("status", "unavailable");
                status.put("message", "This download is unavailable.");
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
    @SecurityRequirement(name="JWT")
    @Secured({"ROLE_ADMIN"})
    @Operation(summary = "Cancel an offline download", tags = "Monitoring")
    @RequestMapping(value = {"occurrences/offline/cancel/{id}"}, method = RequestMethod.GET)
    @ApiParam(value = "id", required = true)
    public @ResponseBody Object occurrenceDownloadCancel(
            @PathVariable("id") String id,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {

        if (!shouldPerformOperation(request, response)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Insufficient authentication credentials provided.");
        }

        Map<String, Object> status = new LinkedHashMap<>();

        //is it in the queue?
        List<DownloadDetailsDTO> downloads = persistentQueueDAO.getAllDownloads();
        for (DownloadDetailsDTO dd : downloads) {
            if (id.equals(dd.getUniqueId())) {
                persistentQueueDAO.removeDownloadFromQueue(dd);
                status.put("cancelled", "true");
                status.put("status", "notInQueue");
                break;
            }
        }

        if (!status.containsKey("status")) {
            status.put("cancelled", "false");
            status.put("status", "notInQueue");
        }

        return status;
    }
}
