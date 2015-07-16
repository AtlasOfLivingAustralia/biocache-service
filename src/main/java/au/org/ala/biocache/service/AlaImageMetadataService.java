/*
 * Copyright (C) 2014 Atlas of Living Australia
 * All Rights Reserved.
 *
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 */
package au.org.ala.biocache.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * The ALA implementation of the image metadata service. Relies on the ala image service.
 */
@Component("imageMetadataService")
public class AlaImageMetadataService implements ImageMetadataService {

    /** Logger initialisation */
    private final static Logger logger = Logger.getLogger(AlaImageMetadataService.class);

    @Value("${media.store.url:http://images-dev.ala.org.au}")
    protected String imageServiceUrl;

    @Override
    public String getUrlFor(String imageId){
        if(StringUtils.isNotBlank(imageServiceUrl)){
            return imageServiceUrl + "/ws/image/" + imageId;
        } else {
            return null;
        }
    }

    @Override
    public Map<String, List<Map<String, Object>>> getImageMetadataForOccurrences(List<String> occurrenceIDs) throws Exception {

        if(StringUtils.isBlank(imageServiceUrl) || occurrenceIDs == null || occurrenceIDs.isEmpty()){
            return new HashMap<String, List<Map<String, Object>>>();
        }

        logger.debug("Retrieving the image metadata for " + occurrenceIDs.size() + " records");
        Map<String, Object> payload = new HashMap<String, Object>();
        payload.put("key", "occurrenceid");
        payload.put("values", occurrenceIDs);

        HttpClient httpClient = new DefaultHttpClient();
        HttpPost post = new HttpPost(imageServiceUrl + "/ws/findImagesByMetadata");

        ObjectMapper om = new ObjectMapper();

        post.setEntity(new StringEntity(om.writeValueAsString(payload), "UTF-8"));
        HttpResponse httpResponse = httpClient.execute(post);

        String jsonResponseString = EntityUtils.toString(httpResponse.getEntity());

        Map<String, Object> jsonResponse = om.readValue(jsonResponseString, Map.class);
        Map<String, List<Map<String, Object>>> imageMetadata = (Map<String, List<Map<String, Object>>>) jsonResponse.get("images");
        logger.debug("Obtained image metadata for " + imageMetadata.size() + " records");
        return imageMetadata;
    }
}
