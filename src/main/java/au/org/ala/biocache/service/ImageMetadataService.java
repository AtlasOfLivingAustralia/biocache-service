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

import java.util.List;
import java.util.Map;

/**
 * Provides the interface that services that support image metadata.  External sites should implement
 * their own version based on their metadata service
 */
public interface ImageMetadataService {

    /**
     * Returns the metadata for all images associated with the supplied list of occurrences
     * @param occurrenceIds The list of uuids for the occurrences whose metadata needs to be looked up
     * @return a map of occurrenceIds to imageMetadata
     * @throws Exception
     */
    Map<String, List<Map<String, Object>>> getImageMetadataForOccurrences(List<String> occurrenceIds) throws Exception;

    /**
     * Returns a URL to the image metadata.
     * @param imageId
     * @return a absolute URL to the image metadata.
     */
    String getUrlFor(String imageId);
}
