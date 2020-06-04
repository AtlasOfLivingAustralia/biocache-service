/**************************************************************************
 *  Copyright (C) 2017 Atlas of Living Australia
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates parameters need it to mint a DOI from the offline download functionality
 */
public class DownloadDoiDTO {
    private String title;
    private String applicationUrl;
    private String query;
    private String fileUrl;
    private String requesterId;
    private List<String> authorisedRoles;
    private List<String> licence;
    private String requesterName;
    private long recordCount;
    private String requestTime;
    private String queryTitle;
    private List<QualityFilterDTO> qualityFilters = new ArrayList<>();

    Map<String, String> applicationMetadata;

    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    public String getRequestTime() {
        return requestTime;
    }

    public void setRequestTime(String requestTime) {
        this.requestTime = requestTime;
    }

    private List<Map<String, String>> datasetMetadata;

    public String getApplicationUrl() {
        return applicationUrl;
    }

    public void setApplicationUrl(String applicationUrl) {
        this.applicationUrl = applicationUrl;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getFileUrl() {
        return fileUrl;
    }

    public void setFileUrl(String fileUrl) {
        this.fileUrl = fileUrl;
    }

    public String getRequesterId() {
        return requesterId;
    }

    public void setRequesterId(String requesterId) {
        this.requesterId = requesterId;
    }

    public String getRequesterName() {
        return requesterName;
    }

    public void setRequesterName(String requesterName) {
        this.requesterName = requesterName;
    }

    public List<Map<String, String>> getDatasetMetadata() {
        return datasetMetadata;
    }

    public void setDatasetMetadata(List<Map<String, String>> datasetMetadata) {
        this.datasetMetadata = datasetMetadata;
    }

    public List<String> getLicence() {
        return licence;
    }

    public void setLicence(List<String> licence) {
        this.licence = licence;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getQueryTitle() {
        return queryTitle;
    }

    public void setQueryTitle(String queryTitle) {
        this.queryTitle = queryTitle;

    }

    public List<String> getAuthorisedRoles() {
        return authorisedRoles;
    }

    public void setAuthorisedRoles(List<String> authorisedRoles) {
        this.authorisedRoles = authorisedRoles;
    }

    public Map<String, String> getApplicationMetadata() {
        return applicationMetadata;
    }

    public void setApplicationMetadata(Map<String, String> applicationMetadata) {
        this.applicationMetadata = applicationMetadata;
    }

    public List<QualityFilterDTO> getQualityFilters() {
        return qualityFilters;
    }

    public void setQualityFilters(List<QualityFilterDTO> qualityFilters) {
        this.qualityFilters = qualityFilters;
    }
}
