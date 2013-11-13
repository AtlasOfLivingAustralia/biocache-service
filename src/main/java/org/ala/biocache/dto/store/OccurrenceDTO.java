/**
 *  Copyright (C) 2011 Atlas of Living Australia
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
 */
package org.ala.biocache.dto.store;

import au.org.ala.biocache.FullRecord;
import au.org.ala.biocache.QualityAssertion;
import org.ala.biocache.dto.MediaDTO;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Holds the Occurrence information about a specific occurrence
 * from the biocache store
 *
 * @author "Natasha Carter <natasha.carter@csiro.au>"
 */
public class OccurrenceDTO implements Serializable {

	private static final long serialVersionUID = 2240957361572672142L;
	private FullRecord raw;
    private FullRecord processed;
    private FullRecord consensus;
    private Map<String,List<QualityAssertion>> systemAssertions;
    private List<QualityAssertion> userAssertions;
    private List<MediaDTO> sounds;
    private List<MediaDTO> video;
    private List<MediaDTO> images;
    private String alaUserName;

    public OccurrenceDTO() {}

    public OccurrenceDTO(FullRecord[] record){
        if(record != null){
            if(record.length >= 2){
                raw = record[0];
                processed = record[1];
            }
            if(record.length == 3){
                consensus = record[2];
            }
        }
    }

    public FullRecord getConsensus() {
        return consensus;
    }

    public void setConsensus(FullRecord consensus) {
        this.consensus = consensus;
    }

    public FullRecord getProcessed() {
        return processed;
    }

    public void setProcessed(FullRecord processed) {
        this.processed = processed;
    }

    public FullRecord getRaw() {
        return raw;
    }

    public void setRaw(FullRecord raw) {
        this.raw = raw;
    }

    public Map<String,List<QualityAssertion>> getSystemAssertions() {
        return systemAssertions;
    }

    public void setSystemAssertions(Map<String,List<QualityAssertion>> systemAssertions) {
        this.systemAssertions = systemAssertions;
    }

    public List<QualityAssertion> getUserAssertions() {
        return userAssertions;
    }

    public void setUserAssertions(List<QualityAssertion> userAssertions) {
        this.userAssertions = userAssertions;
    }

    public List<MediaDTO> getSounds() {
        return sounds;
    }

    public void setSounds(List<MediaDTO> sounds) {
        this.sounds = sounds;
    }

    public List<MediaDTO> getVideo() {
        return video;
    }

    public void setVideo(List<MediaDTO> video) {
        this.video = video;
    }

    public List<MediaDTO> getImages() {
        return images;
    }

    public void setImages(List<MediaDTO> images) {
        this.images = images;
    }

    /**
     * @return the alaUserName
     */
    public String getAlaUserName() {
        return alaUserName;
    }

    /**
     * @param alaUserName the alaUserName to set
     */
    public void setAlaUserName(String alaUserName) {
        this.alaUserName = alaUserName;
    }
}
