/**************************************************************************
 *  Copyright (C) 2013 Atlas of Living Australia
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
package au.org.ala.biocache.util;

import au.org.ala.biocache.Store;
import au.org.ala.biocache.model.FullRecord;
import au.org.ala.biocache.model.Location;
import au.org.ala.biocache.parser.ProcessedValue;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component("OccurrenceUtils")
public class OccurrenceUtils {

    /**
     * Retrieve occurrence record.
     *
     * @param uuid
     * @param includeSensitive
     * @return quality assertions
     */
    static public FullRecord[] getAllVersionsByUuid(String uuid, Boolean includeSensitive) {
        FullRecord [] occ = Store.getAllVersionsByUuid(uuid, includeSensitive);

        for (FullRecord fr : occ) {
            Location loc = fr.getLocation();
            if (loc != null && "null,null,null,null".equals(loc.getBbox())) {
                loc.setBbox(null);
            }
        }

        return occ;
    }

    public static Map getComparisonByUuid(String uuid) {
        Map<String, List<ProcessedValue>> map = Store.getComparisonByUuid(uuid);

        if (map != null) {
            for (String type : map.keySet()) {
                if ("Location".equals(type)) {
                    for (ProcessedValue value : map.get(type)) {
                        if ("bbox".equals(value.getName())) {
                            if ("null,null,null,null".equals(value.getProcessed())) {
                                map.get(type).remove(value);
                                break;
                            }
                        }
                    }
                }
            }
        }

        return map;
    }
}
