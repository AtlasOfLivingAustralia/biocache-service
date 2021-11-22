package au.org.ala.biocache.dto;

import io.swagger.v3.oas.annotations.Parameter;

import javax.validation.constraints.Pattern;

public class DownloadRequestDTO extends SpatialSearchRequestDTO {

    @Parameter(name="emailNotify", description = "Send notification email.")
    boolean emailNotify = true;

    @Parameter(name="email", description = "The email address to sent the download email once complete.")
    String email = "";

    @Parameter(name="reason", description = "Reason for download.")
    String reason = "";

    @Parameter(name="file", description = "Download File name.")
    String file = "data";

    @Parameter(name="fields", description = "Fields to download.")
    String fields = "";

    @Parameter(name="extra", description = "CSV list of extra fields to be added to the download")
    String extra = "";

    @Parameter(name="qa", description = "the CSV list of issue types to include in the download, defaults to all. Also supports none")
    String qa = "all";

    @Parameter(name="sep", description = "Field delimiter")
    Character sep = ',';

    @Parameter(name="esc", description = "Field escape")
    Character esc = '"';

    @Parameter(name="dwcHeaders", description = "Use darwin core headers")
    Boolean dwcHeaders = false;

    @Parameter(name="includeMisc", description = "Include miscelleous properties")
    Boolean includeMisc = false;

    @Parameter(name="reasonTypeId", description = "Logger reason ID See https://logger.ala.org.au/service/logger/reasons")
    Integer reasonTypeId = null;

    @Parameter(name="sourceTypeId", description = "Source ID See https://logger.ala.org.au/service/logger/sources")
    Integer sourceTypeId = null;

    @Parameter(name="fileType", description = "File type. CSV or TSV")
    @Pattern(regexp="(csv|tsv)")
    String fileType = "csv";

    @Parameter(name="layersServiceUrl", description = "URL to layersService to include intersections with layers that are not indexed", hidden = true)
    String layersServiceUrl = "";

    @Parameter(name="customHeader", description = "Override header names with a CSV with 'requested field','header' pairs")
    String customHeader = "";

    @Parameter(name="mintDoi", description = "Request to generate a DOI for the download or not. Default false")
    Boolean mintDoi = false;

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

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public String getFields() {
        return fields;
    }

    public void setFields(String fields) {
        this.fields = fields;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }

    public String getQa() {
        return qa;
    }

    public void setQa(String qa) {
        this.qa = qa;
    }

    public Character getSep() {
        return sep;
    }

    public void setSep(Character sep) {
        this.sep = sep;
    }

    public Character getEsc() {
        return esc;
    }

    public void setEsc(Character esc) {
        this.esc = esc;
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

    public Integer getReasonTypeId() {
        return reasonTypeId;
    }

    public void setReasonTypeId(Integer reasonTypeId) {
        this.reasonTypeId = reasonTypeId;
    }

    public Integer getSourceTypeId() {
        return sourceTypeId;
    }

    public void setSourceTypeId(Integer sourceTypeId) {
        this.sourceTypeId = sourceTypeId;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
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
}
