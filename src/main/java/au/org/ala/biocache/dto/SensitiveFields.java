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
package au.org.ala.biocache.dto;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SensitiveFields {

    private static final Map<String, String> SENSITIVE_FIELD_MAPPING;

    static {
        Map<String, String> map = new HashMap<>();
        map.put("longitude", "sensitive_decimalLongitude");
        map.put("decimalLongitude", "sensitive_decimalLongitude");
        map.put("latitude", "sensitive_decimalLatitude");
        map.put("decimalLatitude", "sensitive_decimalLatitude");

        map.put("locality", "sensitive_locality");

        map.put("footprint_wkt", "sensitive_footprintWKT");
        map.put("footprintWKT", "sensitive_footprintWKT");

        map.put("location_remarks", "sensitive_locationRemarks");
        map.put("locationRemarks", "sensitive_locationRemarks");

        map.put("verbatim_coordinates", "sensitive_verbatimCoordinates");
        map.put("verbatimCoordinates", "sensitive_verbatimCoordinates");

        map.put("verbatim_latitude", "sensitive_verbatimLatitude");
        map.put("verbatimLatitude", "sensitive_verbatimLatitude");

        map.put("verbatim_locality", "sensitive_verbatimLocality");
        map.put("verbatimLocality", "sensitive_verbatimLocality");

        map.put("verbatim_longitude", "sensitive_verbatimLongitude");
        map.put("verbatimLongitude", "sensitive_verbatimLongitude");

        map.put("day", "sensitive_day");

        map.put("occurrence_date", "sensitive_eventDate");
        map.put("eventDate", "sensitive_eventDate");

        map.put("event_id", "sensitive_eventID");
        map.put("eventID", "sensitive_eventID");

        map.put("event_time", "sensitive_eventTime");
        map.put("eventTime", "sensitive_eventTime");

        map.put("month", "sensitive_month");

        map.put("verbatim_event_date", "sensitive_verbatimEventDate");
        map.put("verbatimEventDate", "sensitive_verbatimEventDate");

        SENSITIVE_FIELD_MAPPING = Collections.unmodifiableMap(map);
    }

    public static boolean contains(String fieldName) {
        return SENSITIVE_FIELD_MAPPING.containsKey(fieldName);
    }

    public static String get(String fieldName) {
        return SENSITIVE_FIELD_MAPPING.get(fieldName);
    }
}
