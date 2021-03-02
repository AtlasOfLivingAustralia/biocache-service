/**
 * Copyright (C) 2011 Atlas of Living Australia
 * All Rights Reserved.
 * <p>
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 * <p>
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 */
package au.org.ala.biocache.dto;

import java.io.Serializable;
import java.util.Map;

/**
 * Holds the Occurrence information about a specific occurrence from SOLR.
 * <p>
 * This takes the place of biocache-store's FullRecord.
 */
public class SolrRecordDTO implements Serializable {
    Map<String, Object> fields;

    public Map<String, Object> legacyFormat() {

        //TODO:
//        private FullRecord raw;
//        private FullRecord processed;
////    private FullRecord consensus;
//        private Map<String,List<QualityAssertion>> systemAssertions;
//        private List<QualityAssertion> userAssertions;
//        private List<MediaDTO> sounds;
//        private List<MediaDTO> video;
//        private List<MediaDTO> images;
//        private String alaUserName;

        return null;
    }

}
