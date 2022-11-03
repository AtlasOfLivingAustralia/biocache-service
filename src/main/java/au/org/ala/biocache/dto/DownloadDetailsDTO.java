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
package au.org.ala.biocache.dto;

import au.org.ala.ws.security.profile.AlaUserProfile;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stores the details of a logged download.  Will allow for monitoring of downloads.
 * 
 * @author Natasha Carter
 */
@Schema(name = "DownloadDetails")
public class DownloadDetailsDTO {

    private DownloadRequestDTO requestParams;
    private DownloadType downloadType;

    private String ipAddress;
    private String userAgent;
    private AlaUserProfile alaUser;

    // processing fields
    private String fileLocation;
    private Map<String,String> headerMap = null;
    private String [] miscFields = null;

    private Date startDate;
    private Date lastUpdate;
    private long totalRecords = 0;
    private final AtomicLong recordsDownloaded = new AtomicLong(0);
    private AtomicBoolean interrupt = new AtomicBoolean(false);
    private String processingThreadName = null;

    /**
     * Default constructor necessary for Jackson to create an object from the JSON. 
     */
    public DownloadDetailsDTO(){}

    public DownloadDetailsDTO(@NotNull DownloadRequestDTO params, AlaUserProfile alaUser, String ipAddress, String userAgent, DownloadType type){
        this.requestParams = params;
        this.alaUser = alaUser;
        this.ipAddress = ipAddress;
        this.userAgent = userAgent;
        this.downloadType = type;
        this.startDate = new Date();
        this.lastUpdate = new Date();
    }

    public Date getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(Date lastUpdate) {
        this.lastUpdate = lastUpdate;
    }
    
    @JsonIgnore
    public long getStartTime(){
        return startDate.getTime();
    }
    
    public String getStartDateString(){
        return startDate.toString();
    }

    public String getStartDateString(String format){
        return new SimpleDateFormat(format).format(startDate);
    }

    public Date getStartDate(){
        return this.startDate;
    }
    
    public void setStartDate(Date startDate){
        this.startDate = startDate;
    }
    
    public AtomicLong getRecordsDownloaded(){
        return recordsDownloaded;
    }

    public String getIpAddress(){
        return ipAddress;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public DownloadType getDownloadType(){
        return downloadType;
    }
    
    /**
     * @param downloadType the downloadType to set
     */
    public void setDownloadType(DownloadType downloadType) {
        this.downloadType = downloadType;
    }
    
    public void updateCounts(int number){
        recordsDownloaded.addAndGet(number);
        lastUpdate = new Date();
    }
    
    public void setTotalRecords(long total){
        this.totalRecords = total;
    }
    public long getTotalRecords(){
        return totalRecords;
    }

    /**
     * @return the fileLocation
     */
    public String getFileLocation() {
        return fileLocation;
    }

    /**
     * @param fileLocation the fileLocation to set
     */
    public void setFileLocation(String fileLocation) {
        this.fileLocation = fileLocation;
    }

    /**
     * @return the requestParams
     */
    public DownloadRequestDTO getRequestParams() {
        return requestParams;
    }

    /**
     * @param requestParams the requestParams to set
     */
    public void setRequestParams(DownloadRequestDTO requestParams) {
        this.requestParams = requestParams;
    }

    /**
     * @param miscFields the miscFields to set
     */
    public void setMiscFields(String[] miscFields) {
        this.miscFields = miscFields;
    }

    /**
     * @return the miscFields
     */
    public String [] getMiscFields() {
        return miscFields;
    }

    /**
     * @param ipAddress the ipAddress to set
     */
    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    /**
     * @return the headerMap
     */
    @JsonIgnore //don't want to display this in the status...
    public Map<String, String> getHeaderMap() {
        return headerMap;
    }

    /**
     * @return unique id constructed from email and start time
     */
    public String getUniqueId() {
        return UUID.nameUUIDFromBytes(requestParams.getEmail().getBytes(StandardCharsets.UTF_8)) + "-" + getStartTime();
    }

    /**
     * @param headerMap the headerMap to set
     */
    public void setHeaderMap(Map<String, String> headerMap) {
        this.headerMap = headerMap;
    }

    public void setInterrupt(AtomicBoolean interrupt) {
        this.interrupt = interrupt;
    }

    public AtomicBoolean getInterrupt() {
        return interrupt;
    }

    public String getProcessingThreadName() {
        return processingThreadName;
    }

    public void setProcessingThreadName(String processingThreadName) {
        this.processingThreadName = processingThreadName;
    }

    public AlaUserProfile getAlaUser() {
        return alaUser;
    }

    public void setAlaUser(AlaUserProfile alaUser) {
        this.alaUser = alaUser;
    }

    public void resetCounts() {
        recordsDownloaded.set(0);
        lastUpdate = new Date();
    }

    /**
     * Encompasses the different types of downloads that can be performed.
     */ 
    public enum DownloadType{
        FACET,
        RECORDS_INDEX
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DownloadDetailsDTO [downloadType=")
                .append(downloadType).append(", startDate=").append(startDate)
                .append(", lastUpdate=").append(lastUpdate)
                .append(", totalRecords=").append(totalRecords)
                .append(", recordsDownloaded=").append(recordsDownloaded)
                .append(", downloadParams=").append(requestParams.toString())
                .append(", ipAddress=").append(ipAddress).append(", email=")
                .append(requestParams.getEmail()).append(", requestParams=").append(requestParams)
                .append("]");
        return builder.toString();
    }
}
