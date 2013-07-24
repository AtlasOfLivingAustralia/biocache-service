package org.ala.biocache.web;

import java.util.List;

import javax.inject.Inject;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.ala.biocache.dao.PersistentQueueDAO;
import org.ala.biocache.dao.SearchDAO;
import org.ala.biocache.dto.DownloadDetailsDTO;
import org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import org.ala.biocache.dto.DownloadRequestParams;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;


/**
 * 
 * A Controller for downloading records based on queries.  This controller
 * will provide methods for offline asynchronous downloads of large result sets.
 * 
 * - persistent queue to contain the offline downloads. - written to filesystem before emailing to supplied user
 * - administering the queue - changing order, removing items from queue
 * - 
 * @author Natasha Carter (natasha.carter@csiro.au)
 *
 */
@Controller
public class DownloadController extends AbstractSecureController {

    /** Fulltext search DAO */
    @Inject
    protected SearchDAO searchDAO;
    
    @Inject
    protected PersistentQueueDAO persistentQueueDAO;
    
    /** Stores whether or not additional offline downloads can be requested    */
    private boolean isOfflineAvailable;
    
    /**
     * Retrieves all the downloads that are on the queue
     * @return
     */
    @RequestMapping("/offline/download/stats")
    public @ResponseBody List<DownloadDetailsDTO> getCurrentDownloads(){
        return persistentQueueDAO.getAllDownloads();
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
    @RequestMapping(value = "occurrences/offline/{type}/download*", method = RequestMethod.GET)
    public String occurrenceDownload(
            DownloadRequestParams requestParams,
            @RequestParam(value="ip", required=false) String ip,
            @RequestParam(value="apiKey", required=false) String apiKey,
            @PathVariable("type") String type,
            HttpServletResponse response,
            HttpServletRequest request) throws Exception {
        
        boolean sensitive = false;
        if(apiKey != null){
            if(shouldPerformOperation(apiKey, response, false)){
                sensitive = true;
            }
        } else if (StringUtils.isEmpty(requestParams.getEmail())){
            response.sendError(HttpServletResponse.SC_PRECONDITION_FAILED, "Unable to perform an offline download without an email address");
        }
        
        ip = ip == null?request.getRemoteAddr():ip;
        DownloadType downloadType = "index".equals(type.toLowerCase())?DownloadType.RECORDS_INDEX:DownloadType.RECORDS_DB;
        //create a new task
        DownloadDetailsDTO dd = new DownloadDetailsDTO(requestParams, ip, downloadType);
        dd.setIncludeSensitive(sensitive);
        
        persistentQueueDAO.addDownloadToQueue(dd);
        return null;
    }

}
