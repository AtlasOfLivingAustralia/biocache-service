package org.ala.biocache.dto;

import java.util.Date;

/**
 * 
 * Stores the details of a download.  Will allow for monitoring of downloads
 * 
 * @author Natasha Carter
 *
 */
public class DownloadDetailsDTO {

    private DownloadType downloadType;
    private Date startDate;
    private Date lastUpdate;
    private long totalRecords=0;
    private long recordsDownloaded=0;
    private String downloadParams;
    private String ipAddress;
  
    
    public DownloadDetailsDTO(String params, String ipAddress, DownloadType type){
        this.downloadParams = params;
        this.ipAddress = ipAddress;
        this.downloadType = type;
        this.startDate = new Date();
        this.lastUpdate = new Date();
    }
    
    public String getLastUpdate(){
        return lastUpdate.toString();
    }
    
    public String getStartDate(){
        return startDate.toString();
    }
    
    public long getRecordsDownloaded(){
        return recordsDownloaded;
    }
    
    public String getDownloadParams(){
        return downloadParams;
    }
    
    public String getIpAddress(){
        return ipAddress;
    }
    
    public DownloadType getDownloadType(){
        return downloadType;
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
     * Encompasses the different types of downloads that can be performed.
     */ 
    public enum DownloadType{
        FACET,
        RECORDS_DB,
        RECORDS_INDEX
    }
  
}
