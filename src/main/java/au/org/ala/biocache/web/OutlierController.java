package au.org.ala.biocache.web;

import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.RecordJackKnifeStats;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.annotation.JsonInclude;
import javax.inject.Inject;
import java.util.List;

@Controller
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OutlierController {

    @Inject
    protected SearchDAO searchDAO;

    /**
     * Checks to see if the supplied GUID represents an Australian species.
     * @return
     * @throws Exception
     */

    @Operation(summary = "Retrieves the environmental outlier details for a record", tags = "Outliers")
    @Tag(name = "Outliers", description = "Services for reporting on environmental or limital range status of occurrences")
    @RequestMapping(value={"/outlier/record/{recordUuid}", }, method = RequestMethod.GET)
    @ApiParam(value = "recordUuid", required = true)
    public @ResponseBody List<RecordJackKnifeStats> getOutlierForUUid(
             @PathVariable("recordUuid") String recordUuid) throws Exception {
        return searchDAO.getOutlierStatsFor(recordUuid);
    }

    /**
     * Checks to see if the supplied GUID represents an Australian species.
     * @return
     * @throws Exception
     */
    @Deprecated
    @Operation(summary = "Deprecated - use /outlier/record/{recordUuid}", tags = "Deprecated")
    @RequestMapping(value={"/outlier/record/{uuid}.json" }, method = RequestMethod.GET)
    @ApiParam(value = "uuid", required = true)
    public @ResponseBody List<RecordJackKnifeStats> getOutlierForUUidDeprecated(
            @PathVariable("uuid") String recordUuid) throws Exception {
        return searchDAO.getOutlierStatsFor(recordUuid);
    }
}
