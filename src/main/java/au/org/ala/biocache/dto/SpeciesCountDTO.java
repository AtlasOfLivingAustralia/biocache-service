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

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Group result for a SOLR search
 */
@Schema(name = "SpeciesCount", description = "Species count with left values and occurrence counts")
public class SpeciesCountDTO {

    /**
     * each unique lft in order
     */
    private long[] lft;
    /**
     * counts for each lft
     */
    private long[] counts;
    /**
     * index version for this object
     */
    private long indexVersion;
    /**
     * age of this index
     */
    private long age = System.currentTimeMillis();


    /**
     * Constructor
     */
    public SpeciesCountDTO(long[] lft, long[] counts, long indexVersion) {
        this.lft = lft;
        this.counts = counts;
        this.indexVersion = indexVersion;
    }

    /**
     * Default constructor
     */
    public SpeciesCountDTO() {
    }

    public long[] getLft() {
        return lft;
    }

    public void setLft(long[] lft) {
        this.lft = lft;
    }

    public long[] getCounts() {
        return counts;
    }

    public void setCounts(long[] counts) {
        this.counts = counts;
    }

    public long getIndexVersion() {
        return indexVersion;
    }

    public void setIndexVersion(long indexVersion) {
        this.indexVersion = indexVersion;
    }

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }
}
