package au.org.ala.biocache.dto;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.Pattern;

@Schema(name = "Download parameters")
@Getter
@Setter
public class DownloadRequestParams extends SpatialSearchRequestParams {

    @Parameter(name="email", description = "The email address to sent the download email once complete.")
    String email = "";

    @Parameter(name="reason", description = "Reason for download")
    String reason = "";

    @Parameter(name="file", description = "Download File name")
    String file = "data";

    @Parameter(name="fields", description = "Fields to download")
    String fields = "";

    @Parameter(name="extra", description = "CSV list of extra fields to be added to the download")
    String extra = "";

    @Parameter(name="qa", description = "the CSV list of issue types to include in the download, defaults to all. Also supports none")
    String qa = "all";

    @Parameter(name="sep", description = "Field delimiter", schema = @Schema(type = "string", allowableValues = {",", "\t"}))
    Character sep = ',';

    @Parameter(name="esc", description = "Field escape", schema = @Schema(type = "string", defaultValue = "\""))
    Character esc = '"';

    @Parameter(name="dwcHeaders", description = "Use darwin core headers",  schema = @Schema(type = "boolean", defaultValue = "false"))
    Boolean dwcHeaders = false;

    @Parameter(name="includeMisc", description = "Include miscellaneous properties",  schema = @Schema(type = "boolean", defaultValue = "false"))
    Boolean includeMisc = false;

    @Parameter(name="reasonTypeId", description = "Logger reason ID See https://logger.ala.org.au/service/logger/reasons",  schema = @Schema(type = "string", defaultValue = "10"))
    Integer reasonTypeId = null;

    @Parameter(name="sourceTypeId", description = "Source ID See https://logger.ala.org.au/service/logger/sources",  schema = @Schema(type = "string", defaultValue = "0"))
    Integer sourceTypeId = null;

    @Parameter(name="fileType", description = "File type. CSV or TSV", schema = @Schema(type = "string", allowableValues = {"csv", "tsv"}))
    @Pattern(regexp="(csv|tsv)")
    String fileType = "csv";

    @Parameter(name="layersServiceUrl", description = "URL to layersService to include intersections with layers that are not indexed", hidden = true)
    String layersServiceUrl = "";

    @Parameter(name="customHeader", description = "Override header names with a CSV with 'requested field','header' pairs")
    String customHeader = "";

    @Parameter(name="mintDoi", description = "Request to generate a DOI for the download or not. Default false")
    Boolean mintDoi = false;

    @Parameter(name="emailNotify", description = "Send notification email.")
    boolean emailNotify = true;
}
