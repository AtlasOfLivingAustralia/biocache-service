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

import java.util.HashMap;
import java.util.Map;

/**
 * DTO representing a media object.
 *  
 * @author Dave Martin
 */
public class MediaDTO {

    protected String contentType;
    protected String filePath;
    protected Map<String,String> alternativeFormats = new HashMap<String,String>();

    public Map<String, String> getAlternativeFormats() {
        return alternativeFormats;
    }

    public void setAlternativeFormats(Map<String, String> alternativeFormats) {
        this.alternativeFormats = alternativeFormats;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}
