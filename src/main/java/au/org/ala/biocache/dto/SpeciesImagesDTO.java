/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
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

/**
 * Group result for a SOLR search
 *
 */
public class SpeciesImagesDTO {

    /** each unique lft in order */
    private long [] lft;
    /** total count and first image info */
    private SpeciesImageDTO [] speciesImage;

    /**
     * Constructor
     */
    public SpeciesImagesDTO(long[] lft, SpeciesImageDTO [] speciesImage) {
        this.lft = lft;
        this.speciesImage = speciesImage;
    }

    /**
     * Default constructor
     */
    public SpeciesImagesDTO() {}

    public long[] getLft() {
        return lft;
    }

    public void setLft(long[] lft) {
        this.lft = lft;
    }

    public SpeciesImageDTO[] getSpeciesImage() {
        return speciesImage;
    }

    public void setSpeciesImage(SpeciesImageDTO[] speciesImage) {
        this.speciesImage = speciesImage;
    }
}
