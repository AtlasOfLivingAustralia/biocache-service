package au.org.ala.biocache.web;

import au.org.ala.biocache.Store;
import au.org.ala.biocache.outliers.JackKnifeStats;
import au.org.ala.biocache.outliers.RecordJackKnifeStats;
import au.org.ala.biocache.util.SearchUtils;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

@Controller
public class OutlierController {
    @Inject
    private SearchUtils searchUtils;

    /**
     * Checks to see if the supplied GUID represents an Australian species.
     * @return
     * @throws Exception
     */
    @RequestMapping(value={"/outlierInfo/**" }, method = RequestMethod.GET)
    public @ResponseBody Map<String,JackKnifeStats> getJackKnifeStats(HttpServletRequest request) throws Exception {
        String guid = searchUtils.getGuidFromPath(request);

        return Store.getJackKnifeStatsFor(guid);
    }

    /**
     * Checks to see if the supplied GUID represents an Australian species.
     * @return
     * @throws Exception
     */
    @RequestMapping(value={"/outlier/record/{uuid}" }, method = RequestMethod.GET)
    public @ResponseBody List<RecordJackKnifeStats> getOutlierForUUid(@PathVariable("uuid") String recordUuid) throws Exception {
        return Store.getJackKnifeRecordDetailsFor(recordUuid);
    }
}
