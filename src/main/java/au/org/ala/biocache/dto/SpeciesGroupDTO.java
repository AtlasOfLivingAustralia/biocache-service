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

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

/**
 * DTO for species group information including counts and indent levels
 * 
 * @author "Natasha Carter <natasha.carter@csiro.au>"
 */
@Schema(name = "SpeciesGroup", description = "Species Group")
public class SpeciesGroupDTO {

    private String name;
    private long count;
    private long speciesCount;
    private int level;

    private List<SpeciesGroupDTO> childGroups = null;

    public SpeciesGroupDTO(){}

    public SpeciesGroupDTO(String name){
        this.name = name;
    }

    public SpeciesGroupDTO(String name, long speciesCount, long count, int level){
        this.name = name;
        this.speciesCount = speciesCount;
        this.count = count;
        this.level = level;
    }

    public SpeciesGroupDTO(String name, List<SpeciesGroupDTO> childGroups){
        this.name = name;
        this.childGroups = childGroups;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "SpeciesGroupDTO[" + "name=" + name + ", count=" + count + ", level=" + level + ']';
    }

    /**
     * @return the speciesCount
     */
    public long getSpeciesCount() {
        return speciesCount;
    }

    /**
     * @param speciesCount the speciesCount to set
     */
    public void setSpeciesCount(long speciesCount) {
        this.speciesCount = speciesCount;
    }


    public List<SpeciesGroupDTO> getChildGroups() {
        return childGroups;
    }

    public void setChildGroups(List<SpeciesGroupDTO> childGroups) {
        this.childGroups = childGroups;
    }
}
