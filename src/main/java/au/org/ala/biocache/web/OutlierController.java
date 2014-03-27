package au.org.ala.biocache.web;

import au.org.ala.biocache.Store;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import au.org.ala.biocache.outliers.*;

import java.util.List;
import java.util.Map;

@Controller
public class OutlierController {

    /**
     * Checks to see if the supplied GUID represents an Australian species.
     * @param guid
     * @return
     * @throws Exception
     */
    @RequestMapping(value={"/outlierInfo/{guid:.+}.json*","/outlierInfo/{guid:.+}*" })
    public @ResponseBody Map<String,JackKnifeStats> getJackKnifeStats(@PathVariable("guid") String guid) throws Exception {
        return Store.getJackKnifeStatsFor(guid);
    }

    /**
     * Checks to see if the supplied GUID represents an Australian species.
     * @return
     * @throws Exception
     */
    @RequestMapping(value={"/outlier/record/{uuid}" })
    public @ResponseBody List<RecordJackKnifeStats> getOutlierForUUid(@PathVariable("uuid") String recordUuid) throws Exception {
        return Store.getJackKnifeRecordDetailsFor(recordUuid);
    }
}
