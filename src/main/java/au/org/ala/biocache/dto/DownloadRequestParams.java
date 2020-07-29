/* *************************************************************************
 *  Copyright (C) 2011 Atlas of Living Australia
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

import au.org.ala.biocache.service.DownloadService;
import au.org.ala.biocache.util.QueryFormatUtils;
import au.org.ala.biocache.validate.LogType;
import org.springframework.beans.InvalidPropertyException;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;

/**
 * Data Transfer Object to represent the request parameters required to download
 * the results of a search.
 *
 * @author "Natasha Carter <Natasha.Carter@csiro.au>"
 */
public class DownloadRequestParams extends SpatialSearchRequestParams {

    protected boolean emailNotify = true;
    protected String email = "";
    protected String reason = "";
    protected String file = "data";
    /** CSV list of fields that should be downloaded.  If el or cl will need to map to appropriate column name */
    protected String fields = "";
    /** CSV list of extra fields to be added to the download - useful if wish to make use of default list */
    protected String extra = "";
    /** the CSV list of issue types to include in the download, defaults to all. Also supports none. */
    protected String qa="all";
    /** The CSV separator to use */
    protected Character sep=',';
    /** The CSV escape character to use*/
    protected Character esc='"';
    /** The header is to use darwin core headers (from messages.properties) */
    protected Boolean dwcHeaders=false;
    /** Include all available misc fields. For Cassandra downloads only. */
    protected Boolean includeMisc = false;
    
    @NotNull @LogType(type="reason")//@Range(min=0, max=10)
    protected Integer reasonTypeId = null;    
    @LogType(type="source")
    protected Integer sourceTypeId = null;
    //The file type for the download file."shp" or "csv"
    @Pattern(regexp="(csv|shp|tsv)")
    protected String fileType="csv";

    /** URL to layersService to include intersections with layers that are not indexed */
    protected String layersServiceUrl = "";
    /** Override header names with a CSV with 'requested field','header' pairs */
    protected String customHeader = "";


    /**
     * Request to generate a DOI for the download or not. Default false
     */
    protected Boolean mintDoi=false;

    /**
     * What is the search in the UI that generates this occurrence download.
     */
    protected String searchUrl;

    /**
     * What is the DOI landing page that will be used to display individual DOIs
     */
    protected String doiDisplayUrl;

    /**
     * The name of the hub issuing the download request.
     * This will be used in e-mails, and zip content
     */
    protected String hubName;

    /**
     * If a DOI is to be minted containing download data, this allows the requesting application to attach
     * custom metadata to be stored with the DOI as application metadata.
     */
    Map<String, String> doiMetadata = new HashMap<String, String>();

    /**
     * Quality Filters information from the hub about the download
     */
    protected List<String> qualityFiltersInfo = new ArrayList<>();

    /**
     * Custom toString method to produce a String to be used as the request parameters
     * for the Biocache Service webservices
     *
     * @return request parameters string
     */
    @Override
    public String toString() {
        return addParams(super.toString(), false);
    }

    /**
     * Produce a URI encoded query string for use in java.util.URI, etc
     *
     * @return
     */
    public String getEncodedParams() {
        return addParams(super.getEncodedParams(), true);
    }

    protected String addParams(String paramString, Boolean encodeParams) {
        StringBuilder req = new StringBuilder(paramString);
        // since emailNotify default is "true", only add param if !emailNotify
        if (!emailNotify) {
            req.append("&emailNotify=false");
        }
        req.append("&email=").append(super.conditionalEncode(email, encodeParams));
        req.append("&reason=").append(super.conditionalEncode(reason, encodeParams));
        req.append("&file=").append(super.conditionalEncode(getFile(), encodeParams));
        req.append("&fields=").append(super.conditionalEncode(fields, encodeParams));
        req.append("&extra=").append(super.conditionalEncode(extra, encodeParams));
        if(reasonTypeId != null) {
            req.append("&reasonTypeId=").append(reasonTypeId);
        }
        if(sourceTypeId != null) {
            req.append("&sourceTypeId=").append(sourceTypeId);
        } 
        if(!"csv".equals(fileType)){
            req.append("&fileType=").append(super.conditionalEncode(fileType, encodeParams));
        }
        if(!"all".equals(qa)){
            req.append("&qa=").append(super.conditionalEncode(qa, encodeParams));
        }
        if (dwcHeaders) {
            req.append("&dwcHeaders=true");
        }
        if (includeMisc) {
            req.append("&includeMisc=true");
        }

        for (String qualityFilter : qualityFiltersInfo) {
            req.append("&qualityFiltersInfo=").append(super.conditionalEncode(qualityFilter, encodeParams));
        }
        
        return req.toString();
    }

    public boolean isEmailNotify() {
        return emailNotify;
    }

    public void setEmailNotify(boolean emailNotify) {
        this.emailNotify = emailNotify;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getFile() {
        // sanitiseFileName can be called multiple times on the same string without changing the string
        // ... but no requirement to use setFile, so sanitising on each call here also
        // Given these objects are created using reflection and other methods, best to do it this way
        return sanitiseFileName(file);
    }

    public void setFile(String file) {
        this.file = sanitiseFileName(file);
    }

    /**
     * Subset of valid characters to enable surety that it will work across filesystems.
     * 
     * @param nextFile The filename to sanitise
     * @return A sanitised version of the given filename that can be used to avoid filesystem inconsistencies
     */
    private static String sanitiseFileName(String nextFile) {
        return nextFile.replaceAll("[^a-zA-Z0-9-_\\.]", "_");
	}

	public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    /**
     * @return the fields
     */
    public String getFields() {
        return fields;
    }

    /**
     * @param fields the fields to set
     */
    public void setFields(String fields) {
        QueryFormatUtils.assertNoSensitiveValues(DownloadRequestParams.class, "fields", fields);
        this.fields = fields;
    }

    /**
     * @return the extra
     */
    public String getExtra() {
        return extra;
    }

    /**
     * @param extra the extra to set
     */
    public void setExtra(String extra) {
        QueryFormatUtils.assertNoSensitiveValues(DownloadRequestParams.class, "extra", extra);
        this.extra = extra;
    }

    /**
     * @return the reasonTypeId
     */
    public Integer getReasonTypeId() {
        return reasonTypeId;
    }

    /**
     * @param reasonTypeId the reasonTypeId to set
     */
    public void setReasonTypeId(Integer reasonTypeId) {
        this.reasonTypeId = reasonTypeId;
    }

    /**
     * @return the sourceId
     */
    public Integer getSourceTypeId() {
        return sourceTypeId;
    }

    /**
     * @param sourceTypeId the sourceId to set
     */
    public void setSourceTypeId(Integer sourceTypeId) {
        this.sourceTypeId = sourceTypeId;
    }

    /**
     * @return the fileType
     */
    public String getFileType() {
        return fileType;
    }

    /**
     * @param fileType the fileType to set
     */
    public void setFileType(String fileType) {
        if (!DownloadService.downloadShpEnabled && "shp".equalsIgnoreCase(fileType)) {
            throw new InvalidPropertyException(DownloadRequestParams.class, "fileType", "Shapefile downloads are disabled.");
        }
        this.fileType = fileType;
    }

    /**
     * @return the qa
     */
    public String getQa() {
        return qa;
    }

    /**
     * @param qa the qa to set
     */
    public void setQa(String qa) {
        this.qa = qa;
    }

    public Character getEsc() {
        return esc;
    }

    public void setEsc(Character esc) {
        this.esc = esc;
    }

    public Character getSep() {
        return sep;
    }

    public void setSep(Character sep) {
        this.sep = sep;
    }
    
    public Boolean getDwcHeaders() { 
        return dwcHeaders;
    }
    
    public void setDwcHeaders(Boolean dwcHeaders) {
        this.dwcHeaders = dwcHeaders;
    }

    public Boolean getIncludeMisc() {
        return includeMisc;
    }

    public void setIncludeMisc(Boolean includeMisc) {
        this.includeMisc = includeMisc;
    }

    public String getLayersServiceUrl() {
        return layersServiceUrl;
    }

    public void setLayersServiceUrl(String layersServiceUrl) {
        this.layersServiceUrl = layersServiceUrl;
    }

    public String getCustomHeader() {
        return customHeader;
    }

    public void setCustomHeader(String customHeader) {
        this.customHeader = customHeader;
    }

    public Boolean getMintDoi() {
        return mintDoi;
    }

    public void setMintDoi(Boolean mintDoi) {
        this.mintDoi = mintDoi;
    }

    public String getSearchUrl() {
        return searchUrl;
    }

    public void setSearchUrl(String searchUrl) {
        this.searchUrl = searchUrl;
    }

    public String getDoiDisplayUrl() {
        return doiDisplayUrl;
    }

    public void setDoiDisplayUrl(String doiDisplayUrl) {
        this.doiDisplayUrl = doiDisplayUrl;
    }

    public String getHubName() {
        return hubName;
    }

    public void setHubName(String hubName) {
        this.hubName = hubName;
    }

    public Map<String, String> getDoiMetadata() {
        return doiMetadata;
    }

    public void setDoiMetadata(Map<String, String> doiMetadata) {
        this.doiMetadata = doiMetadata;
    }

    public List<String> getQualityFiltersInfo() {
        return qualityFiltersInfo;
    }

    public void setQualityFiltersInfo(List<String> qualityFiltersInfo) {
        this.qualityFiltersInfo = qualityFiltersInfo;
    }

}
