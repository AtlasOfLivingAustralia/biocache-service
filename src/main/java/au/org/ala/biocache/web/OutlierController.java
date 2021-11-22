package au.org.ala.biocache.web;

import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.RecordJackKnifeStats;
import io.swagger.annotations.Api;
import io.swagger.v3.oas.annotations.Operation;
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
    @RequestMapping(value={"/outlier/record/{uuid}", "/outlier/record/{uuid}.json" }, method = RequestMethod.GET)
    public @ResponseBody List<RecordJackKnifeStats> getOutlierForUUid(@PathVariable("uuid") String recordUuid) throws Exception {
        return searchDAO.getOutlierStatsFor(recordUuid);
    }
}
