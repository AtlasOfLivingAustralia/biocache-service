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
import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.service.AuthService;
import au.org.ala.biocache.service.DownloadService;
import au.org.ala.ws.security.profile.AlaUserProfile;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.swagger.annotations.ApiParam;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocumentList;
import org.springdoc.api.annotations.ParameterObject;
import org.springframework.beans.factory.annotation.Value;
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

import static java.util.stream.Collectors.*;

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

    final private static Logger logger = Logger.getLogger(ScatterplotController.class);

    /** Fulltext search DAO */
    @Inject
    protected SearchDAO searchDAO;

    @Inject
    protected PersistentQueueDAO persistentQueueDAO;

    @Inject
    protected AuthService authService;

    @Inject
    protected DownloadService downloadService;

    @Value("${auth.legacy.emailonly.downloads.enabled:true}")
    protected Boolean emailOnlyEnabled = true;

    /**
     * Retrieves all the downloads that are on the queue
     * @return
     */
    @Deprecated
    @SecurityRequirement(name="JWT")
    @Secured({"ROLE_ADMIN"})
    @Operation(summary = "Retrieves all the downloads that are on the queue", tags = "Monitoring")
    @RequestMapping(value = {"occurrences/offline/download/stats"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Map<String, List<DownloadStatusDTO>> getCurrentDownloads() throws Exception {
        return allOccurrenceDownloadStatus();
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
    @Parameters(value = {
            // DownloadRequestParams
            @Parameter(name="email", description = "The email address to sent the download email once complete.", in = ParameterIn.QUERY, required = true),
            @Parameter(name="reason", description = "Reason for download.", in = ParameterIn.QUERY),
            @Parameter(name="file", description = "Download File name.", in = ParameterIn.QUERY),
            @Parameter(name="fields", description = "Fields to download.", in = ParameterIn.QUERY, required = true),
            @Parameter(name="extra", description = "CSV list of extra fields to be added to the download.", in = ParameterIn.QUERY),
            @Parameter(name="qa", description = "the CSV list of issue types to include in the download, defaults to 'all'. Also supports 'none'", in = ParameterIn.QUERY),
            @Parameter(name="sep", description = "Field delimiter for fileType='csv', defaults to ','", in = ParameterIn.QUERY, schema = @Schema(type = "char", allowableValues = {",", "\t"})),
            @Parameter(name="esc", description = "Field escape for fileType='csv', defaults to '\"'", schema = @Schema(type = "char", defaultValue = "\""), in = ParameterIn.QUERY),
            @Parameter(name="dwcHeaders", description = "Use darwin core headers, defaults to false",  schema = @Schema(type = "boolean", defaultValue = "false"), in = ParameterIn.QUERY),
            @Parameter(name="includeMisc", description = "Include miscellaneous properties, defaults to false",  schema = @Schema(type = "boolean", defaultValue = "false"), in = ParameterIn.QUERY),
            @Parameter(name="reasonTypeId", description = "Logger reason ID See https://logger.ala.org.au/service/logger/reasons",  required = true, schema = @Schema(type = "string", defaultValue = "10"), in = ParameterIn.QUERY),
            @Parameter(name="sourceTypeId", description = "Source ID See https://logger.ala.org.au/service/logger/sources",  schema = @Schema(type = "string", defaultValue = "0"), in = ParameterIn.QUERY),
            @Parameter(name="fileType", description = "File type. CSV or TSV. Defaults to CSV", schema = @Schema(type = "string", allowableValues = {"csv", "tsv"}), in = ParameterIn.QUERY),
            @Parameter(name="customHeader", description = "Override header names with a CSV with 'requested field','header' pairs", in = ParameterIn.QUERY),
            @Parameter(name="mintDoi", description = "Request to generate a DOI for the download or not. Default false", schema = @Schema(type = "boolean", defaultValue = "false"), in = ParameterIn.QUERY),
            @Parameter(name="emailNotify", description = "Send notification email. Default true", schema = @Schema(type = "boolean", defaultValue = "true"), in = ParameterIn.QUERY),

            // SpatialSearchRequestParams (download specific only)
            @Parameter(name="q", description = "Main search query. Examples 'q=Kangaroo' or 'q=vernacularName:red'", in = ParameterIn.QUERY),
            @Parameter(name="fq", description = "Filter queries. Examples 'fq=state:Victoria&fq=state:Queensland", array = @ArraySchema(schema = @Schema(type = "string")), in = ParameterIn.QUERY),
            @Parameter(name="qId", description = "Query ID for persisted queries", in = ParameterIn.QUERY),
            @Parameter(name="pageSize", description = "The prefix to limit returned values for previewing results. Use a value < 10, e.g. 5", in = ParameterIn.QUERY),
            @Parameter(name="qc", description = "The query context to be used for the search. This will be used to generate extra query filters.", in = ParameterIn.QUERY),

            @Parameter(name="qualityProfile", description = "The quality profile to use, null for default", in = ParameterIn.QUERY),
            @Parameter(name="disableAllQualityFilters", description = "Disable all default filters", in = ParameterIn.QUERY),
            @Parameter(name="disableQualityFilter", description = "Default filters to disable (currently can only disable on category, so it's a list of disabled category name)", array = @ArraySchema(schema = @Schema(type = "string")), in = ParameterIn.QUERY),

            @Parameter(name="radius", description = "Radius for a spatial search. Use together with lat and lon.", schema = @Schema(type = "float"), in = ParameterIn.QUERY),
            @Parameter(name="lat", description = "Decimal latitude for the spatial search. Use together with radius and lon.", schema = @Schema(type = "float"), in = ParameterIn.QUERY),
            @Parameter(name="lon", description = "Decimal longitude for the spatial search. Use together with radius and lat.", schema = @Schema(type = "float"), in = ParameterIn.QUERY),

            @Parameter(name="wkt", description = "Well Known Text for the spatial search. Large WKT will be simplified.", in = ParameterIn.QUERY)
    } )
    @Tag(name ="Download", description = "Services for downloading occurrences and specimen data")
    @RequestMapping(value = { "occurrences/offline/download"}, method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody DownloadStatusDTO occurrenceDownload(
            @Valid @Parameter(hidden = true) DownloadRequestParams requestParams,
            @Parameter(description = "Original IP making the request") @RequestParam(value = "ip", required = false) String ip,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {

        DownloadRequestDTO downloadRequestDTO = DownloadRequestDTO.create(requestParams, request);
        Optional<AlaUserProfile> downloadUser = authService.getDownloadUser(downloadRequestDTO, request);

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

    private DownloadStatusDTO download(DownloadRequestDTO requestParams,
                                         AlaUserProfile alaUser,
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

        DownloadStatusDTO status = new DownloadStatusDTO();
        DownloadDetailsDTO d = persistentQueueDAO.isInQueue(dd);

        status = getQueueStatus(d);

        if (d != null) {
            status.setMessage("Already in queue.");
            status.setStatus(DownloadStatusDTO.DownloadStatus.IN_QUEUE);
        } else if (dd.getTotalRecords() > downloadService.dowloadOfflineMaxSize) {
            //identify this download as too large
            File file = new File(downloadService.biocacheDownloadDir + File.separator + UUID.nameUUIDFromBytes(dd.getRequestParams().getEmail().getBytes(StandardCharsets.UTF_8)) + File.separator + dd.getStartTime() + File.separator + "tooLarge");
            FileUtils.forceMkdir(file.getParentFile());
            FileUtils.writeStringToFile(file, "", "UTF-8");
            status.setDownloadUrl(downloadService.biocacheDownloadUrl);
            status.setStatus(DownloadStatusDTO.DownloadStatus.TOO_LARGE);
            status.setMessage(downloadService.downloadOfflineMsg);
            status.setError("Requested to many records (" + dd.getTotalRecords() + "). The maximum is (" + downloadService.dowloadOfflineMaxSize + ")");

            writeStatusFile(dd.getUniqueId(), status);
        } else {
            downloadService.add(dd);
            status = getQueueStatus(dd);

            writeStatusFile(dd.getUniqueId(), status);
        }

        return status;
    }

    @SecurityRequirement(name="JWT")
    @Secured({"ROLE_ADMIN"})
    @Operation(summary = "List all occurrence downloads", tags = "Monitoring")
    @RequestMapping(value = {"occurrences/offline/status/all"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Map<String, List<DownloadStatusDTO>> allOccurrenceDownloadStatus() {

        //return each queue
        Map<String, List<DownloadStatusDTO>> downloads = persistentQueueDAO.getAllDownloads().stream()
                .collect(groupingBy(dd -> dd.getAlaUser().getEmail(), mapping(this::getQueueStatusAdmin, toList())));

        return downloads;
    }

    @SecurityRequirement(name="JWT")
    @Secured({"ROLE_USER"})
    @Operation(summary = "List all occurrence downloads by the current user", tags = "Monitoring")
    @RequestMapping(value = {"occurrences/offline/status"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Map<String, List<DownloadStatusDTO>> allOccurrenceDownloadStatusForUser(
            HttpServletRequest request) throws Exception {

        Optional<AlaUserProfile> alaUserProfile = authService.getRecordViewUser(request);

        //return each queue
        Map<String, List<DownloadStatusDTO>> downloads = persistentQueueDAO.getAllDownloads().stream()
                .filter(dd -> dd.getAlaUser().getUserId().equals(alaUserProfile.get().getUserId()))
                .collect(groupingBy(dd -> dd.getAlaUser().getEmail(), mapping(this::getQueueStatus, toList())));

        return downloads;
    }

    private void writeStatusFile(String id, DownloadStatusDTO status) throws IOException {
        File statusDir = new File(downloadService.biocacheDownloadDir + "/" + id.replaceAll("-([0-9]*)$", "/$1"));
        statusDir.mkdirs();
        ObjectWriter ow = new ObjectMapper().writer();
        String json = ow.writeValueAsString(status);
        FileUtils.writeStringToFile(new File(statusDir.getPath() + "/status.json"), json, "UTF-8");
    }

    @Operation(summary = "Get the status of download", tags = "Download")
    @RequestMapping(value = { "occurrences/offline/status/{id}"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiParam(value = "id", required = true)
    public @ResponseBody DownloadStatusDTO occurrenceDownloadStatus(@PathVariable("id") String id) {
        //is it in the queue?
        List<DownloadDetailsDTO> downloads = persistentQueueDAO.getAllDownloads();
        for (DownloadDetailsDTO dd : downloads) {
            if (id.equals(dd.getUniqueId())) {
                return getQueueStatus(dd);
            }
        }

        String cleanId = id.replaceAll("[^a-z\\-0-9]", "");

        return getOtherStatus(cleanId);
    }

    private DownloadStatusDTO getQueueStatusAdmin(DownloadDetailsDTO dd) {
        return getQueueStatus(dd, true);
    }

    private DownloadStatusDTO getQueueStatus(DownloadDetailsDTO dd) {
        return getQueueStatus(dd, false);
    }

    private DownloadStatusDTO getQueueStatus(DownloadDetailsDTO dd, boolean isAdmin) {
        DownloadStatusDTO status = new DownloadStatusDTO();

        if (dd != null) {
            String id = dd.getUniqueId();
            if (dd.getRecordsDownloaded().get() == 0) {
                status.setStatus(DownloadStatusDTO.DownloadStatus.IN_QUEUE);
                status.setQueueSize(downloadService.getDownloadsForUserId(dd.getAlaUser().getUserId()).size() );
            } else {
                status.setStatus(DownloadStatusDTO.DownloadStatus.RUNNING);
                status.setRecords(dd.getRecordsDownloaded().longValue());
            }
            status.setTotalRecords(dd.getTotalRecords());
            status.setStatusUrl(downloadService.webservicesRoot + "/occurrences/offline/status/" + id);
            if (isAdmin && authService.getMapOfEmailToId() != null) {
                status.setUserId(authService.getMapOfEmailToId().get(dd.getRequestParams().getEmail()));
            }
            status.setSearchUrl(downloadService.generateSearchUrl(dd.getRequestParams()));
            status.setCancelUrl(downloadService.webservicesRoot + "/occurrences/offline/cancel/" + dd.getUniqueId());
        }

        return status;
    }

    private DownloadStatusDTO getOtherStatus(String id) {
        DownloadStatusDTO status = new DownloadStatusDTO();

        File statusFile = new File(downloadService.biocacheDownloadDir + File.separator + id.replaceAll("-([0-9]*)$", "/$1") + "/status.json");
        if (status.getStatus() == null) {
            //check downloads directory for a status file
            if (statusFile.exists()) {
                ObjectMapper om = new ObjectMapper();
                try {
                    status = om.readValue(statusFile, DownloadStatusDTO.class);
                } catch (IOException e) {
                    logger.error("failed to read file: " + statusFile.getPath() + ", " + e.getMessage());
                }
            }
        }

        // look for output files
        if (status.getStatus() == null
                || status.getStatus() == DownloadStatusDTO.DownloadStatus.RUNNING
                || status.getStatus() == DownloadStatusDTO.DownloadStatus.IN_QUEUE) {
            File dir = new File(downloadService.biocacheDownloadDir + File.separator + id.replaceAll("-([0-9]*)$", "/$1"));
            if (dir.isDirectory() && dir.exists()) {
                for (File file : dir.listFiles()) {

                    // output zip exists
                    if (file.isFile() && file.getPath().endsWith(".zip") && file.length() > 0) {
                        status.setStatus(DownloadStatusDTO.DownloadStatus.FINISHED);
                        try {
                            status.setDownloadUrl(downloadService.biocacheDownloadUrl + File.separator + URLEncoder.encode(file.getPath().replace(downloadService.biocacheDownloadDir + "/", ""), "UTF-8").replace("%2F", "/").replace("+", "%20"));
                        } catch (UnsupportedEncodingException e) {
                            logger.error("Failed to URLEncode for id:" + id + ", " + e.getMessage());
                        }
                    }

                    // notification for large download
                    if (file.isFile() && "tooLarge".equals(file.getName())) {
                        status.setStatus(DownloadStatusDTO.DownloadStatus.TOO_LARGE);
                        status.setMessage(downloadService.downloadOfflineMsg);
                        status.setDownloadUrl(downloadService.dowloadOfflineMaxUrl);
                        status.setError("requested to many records. The upper limit is (" + downloadService.dowloadOfflineMaxSize + ")");
                    }
                }
            }
        }

        if (status.getStatus() != null) {
            try {
                writeStatusFile(id, status);
            } catch (IOException e) {
                logger.error("failed to write status file for id=" + id + ", " + e.getMessage());
            }
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
    @Secured({"ROLE_USER"})
    @Operation(summary = "Cancel an offline download", tags = "Monitoring")
    @RequestMapping(value = {"occurrences/offline/cancel/{id}"}, method = RequestMethod.GET)
    @ApiParam(value = "id", required = true)
    public @ResponseBody DownloadStatusDTO occurrenceDownloadCancel(
            @PathVariable("id") String id,
            HttpServletRequest request) throws Exception {

        //is it in the queue?
        List<DownloadDetailsDTO> downloads = persistentQueueDAO.getAllDownloads();
        for (DownloadDetailsDTO dd : downloads) {
            if (id.equals(dd.getUniqueId())) {
                AlaUserProfile alaUserProfile = authService.getRecordViewUser(request).get();

                if (dd.getAlaUser().getUserId().equals(alaUserProfile.getUserId()) || alaUserProfile.getRoles().contains("ROLE_ADMIN")) {
                    // get current status
                    DownloadStatusDTO status = getQueueStatus(dd);

                    // cancel download
                    downloadService.cancel(dd);

                    // update status
                    status.setStatus(DownloadStatusDTO.DownloadStatus.CANCELLED);
                    writeStatusFile(dd.getUniqueId(), status);

                    return status;
                }
            }
        }

        return null;
    }
}
