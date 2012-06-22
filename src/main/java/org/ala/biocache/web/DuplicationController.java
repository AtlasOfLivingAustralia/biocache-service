package org.ala.biocache.web;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import au.org.ala.biocache.*;
import au.org.ala.util.DuplicateRecordDetails;

import java.util.Map;

@Controller
public class DuplicationController {

    /**
     * Retrieves the duplication information for the supplied guid.
     * 
     * Returns null when the record is not a the "representative" occurrence.
     * @param guid
     * @return
     * @throws Exception
     */
    @RequestMapping(value={"/duplicates/{guid:.+}.json*","/duplicates/{guid:.+}*" })
    public @ResponseBody DuplicateRecordDetails getJackKnifeStats(@PathVariable("guid") String guid) throws Exception {
        return Store.getDuplicateDetails(guid);
    }

}
