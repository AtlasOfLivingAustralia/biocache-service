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
package au.org.ala.names.model;

/**
 * When an error occurs during a recursive search the search does NOT stop.  The error is noted and a result is attempted
 * based on the higher level classification.  Each error that occurs will be reported in the MetricResultsDTO and will be one of these types.
 *
 *
 * For more discussion of error types see http://code.google.com/p/ala-portal/wiki/ALANames#Error_Types
 *
 * @author Natasha Carter
 */
public enum ErrorType {
    /**
     * An spp. marker was identified.
     */
    SPECIES_PLURAL("speciesPlural", "An spp. marker was identified."),
    /**
     * An indeterminate marker was detected and and exact match could not be found.
     */
    INDETERMINATE_SPECIES("indeterminateSpecies", "An indeterminate marker was detected and and exact match could not be found."),
    /**
     * A low confidence identification of the species was made using a ? indicator.
     */
    QUESTION_SPECIES("questionSpecies", "A low confidence identification of the species was made using a ? indicator."),
    /**
     * An aff. marker was detected in the original scientific name.
     */
    AFFINITY_SPECIES("affinitySpecies", "An aff. marker was detected in the original scientific name."),
    /**
     * A cf. marker was detected in the original scientific name
     */
    CONFER_SPECIES("conferSpecies", "A cf. marker was detected in the original scientific name"),
    /**
     * A homonym was detected.
     */
    HOMONYM("homonym", "A homonym was detected."),
    /**
     * A generic SearchResultException was detected.
     */
    GENERIC("genericError", "A generic SearchResultException was detected."),
    /**
     * The parent names has been detected as a synonym of the child - generally occurs when a species is split into 1 or more
     * subspecies
     */
    PARENT_CHILD_SYNONYM("parentChildSynonym", "The parent names has been detected as a synonym of the child"),
    /**
     * The species is excluded from the national species list.  Usually because it is not found in Australia
     */
    EXCLUDED("excludedSpecies", "The species is excluded from the national species list.  Usually because it is not found in Australia"),
    /**
     * There are 2 species names one is excluded and the other is not
     */
    ASSOCIATED_EXCLUDED("associatedNameExcluded", "There are 2 species names one is excluded and the other is not"),
    /**
     * The original scientific name has been misapplied to another concept in the past - indicates that the matched result
     * is accepted but there is also a misapplied synonym
     */
    MATCH_MISAPPLIED("matchedToMisappliedName", "The original scientific name has been misapplied to another concept in the past"),
    /**
     * The scientific name has been misapplied to a taxon concept in the past.  The matched concept does NOT exist as an accepted concept.
     */
    MISAPPLIED("misappliedName", "The scientific name has been misapplied to a taxon concept in the past.  The matched concept does NOT exist as an accepted concept."),
    /**
     * No issue was detected
     */
    NONE("noIssue", "No issue was detected");
    private String title;
    private String description;

    ErrorType(String title, String description) {

        this.title = title;
        this.description = description;
    }

    @Override
    public String toString() {
        return title;
    }
}
