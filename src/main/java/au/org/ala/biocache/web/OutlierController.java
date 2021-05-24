package au.org.ala.biocache.web;

import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.RecordJackKnifeStats;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.inject.Inject;
import java.util.List;

@Controller
public class OutlierController {

    @Inject
    protected SearchDAO searchDAO;

    /**
     * Checks to see if the supplied GUID represents an Australian species.
     * @return
     * @throws Exception
     */
    @RequestMapping(value={"/outlier/record/{uuid}" }, method = RequestMethod.GET)
    public @ResponseBody List<RecordJackKnifeStats> getOutlierForUUid(@PathVariable("uuid") String recordUuid) throws Exception {
        return searchDAO.getOutlierStatsFor(recordUuid);
    }
}
