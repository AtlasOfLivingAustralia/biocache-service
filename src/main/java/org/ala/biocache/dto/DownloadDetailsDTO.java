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
package org.ala.biocache.dto;

import java.util.Date;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * Stores the details of a download.  Will allow for monitoring of downloads
 * 
 * @author Natasha Carter
 */
public class DownloadDetailsDTO {

    private DownloadType downloadType;
    private Date startDate;
    private Date lastUpdate;
    private long totalRecords = 0;
    private long recordsDownloaded = 0;
    private String downloadParams;
    private String ipAddress;
    private String email;
    private DownloadRequestParams requestParams;
    private String fileLocation;
    private boolean includeSensitive = false;
    private Map<String,String> headerMap = null;
    
    /**
     * Default constructor necessary for Jackson to create an object from the JSON. 
     */
    public DownloadDetailsDTO(){}
    
    public DownloadDetailsDTO(String params, String ipAddress, DownloadType type){
        this.downloadParams = params;
        this.ipAddress = ipAddress;
        this.downloadType = type;
        this.startDate = new Date();
        this.lastUpdate = new Date();
    }
    
    public DownloadDetailsDTO(DownloadRequestParams params, String ipAddress, DownloadType type){
        this(params.getUrlParams(), ipAddress, type);
        requestParams = params;
        email = requestParams.getEmail();
    }
    
    public String getLastUpdate(){
        return lastUpdate == null ? null:lastUpdate.toString();
    }
    
    @JsonIgnore
    public long getStartTime(){
        return startDate.getTime();
    }
    
    public String getStartDateString(){
        return startDate.toString();
    }
    
    public Date getStartDate(){
        return this.startDate;
    }
    
    public void setStartDate(Date startDate){
        this.startDate = startDate;
    }
    
    public long getRecordsDownloaded(){
        return recordsDownloaded;
    }
    
    public String getDownloadParams(){
        return downloadParams;
    }
    
    /**
     * @param downloadParams the downloadParams to set
     */
    public void setDownloadParams(String downloadParams) {
        this.downloadParams = downloadParams;
    }
    public String getIpAddress(){
        return ipAddress;
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
        recordsDownloaded +=number;
        lastUpdate = new Date();
    }
    
    public void setTotalRecords(long total){
        this.totalRecords = total;
    }
    public long getTotalRecords(){
        return totalRecords;
    }
  
    /**
     * @return the email
     */
    public String getEmail() {
        return email;
    }

    /**
     * @param email the email to set
     */
    public void setEmail(String email) {
        this.email = email;
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
    public DownloadRequestParams getRequestParams() {
        return requestParams;
    }

    /**
     * @param requestParams the requestParams to set
     */
    public void setRequestParams(DownloadRequestParams requestParams) {
        this.requestParams = requestParams;
    }

    /**
     * @return the includeSensitive
     */
    public boolean getIncludeSensitive() {
        return includeSensitive;
    }

    /**
     * @param includeSensitive the includeSensitive to set
     */
    public void setIncludeSensitive(boolean includeSensitive) {
        this.includeSensitive = includeSensitive;
    }

    /**
     * @param ipAddress the ipAddress to set
     */
    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    /**
     * @return the headerMap
     */
    @JsonIgnore //don't want to display this in the status...
    public Map<String, String> getHeaderMap() {
        return headerMap;
    }
    
    /**
     * @param headerMap the headerMap to set
     */
    public void setHeaderMap(Map<String, String> headerMap) {
        this.headerMap = headerMap;
    }

    /**
     * Encompasses the different types of downloads that can be performed.
     */ 
    public enum DownloadType{
        FACET,
        RECORDS_DB,
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
                .append(", downloadParams=").append(downloadParams)
                .append(", ipAddress=").append(ipAddress).append(", email=")
                .append(email).append(", requestParams=").append(requestParams)
                .append("]");
        return builder.toString();
    }
  
}
