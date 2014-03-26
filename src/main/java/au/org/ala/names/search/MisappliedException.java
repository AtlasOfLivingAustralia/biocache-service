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

package au.org.ala.names.search;

import au.org.ala.names.model.NameSearchResult;

/**
 * Indicates that the matched name has been misapplied in the past.
 * <p/>
 * If there is a "misaapliedResult" value it means that there is also an acceptedConcept
 * for the same name.
 *
 * @author Natasha Carter
 */
public class MisappliedException extends SearchResultException {
    private NameSearchResult matchedResult, misappliedResult;

    /**
     * Constructor to use of the first result is misapplied.
     *
     * @param match
     */
    public MisappliedException(NameSearchResult match) {
        super("The scientific name has been misapplied in the past");
        matchedResult = match;
        errorType = au.org.ala.names.model.ErrorType.MISAPPLIED;
    }

    /**
     * Constructor to use if the first result is accepted and second result is a misapplied synonym
     *
     * @param match
     * @param misapplied
     */
    public MisappliedException(NameSearchResult match, NameSearchResult misapplied) {
        this(match);
        misappliedResult = misapplied;
        errorType = au.org.ala.names.model.ErrorType.MATCH_MISAPPLIED;
    }

    public NameSearchResult getMatchedResult() {
        return matchedResult;
    }

    public void setMatchedResult(NameSearchResult matchedResult) {
        this.matchedResult = matchedResult;
    }

    public NameSearchResult getMisappliedResult() {
        return misappliedResult;
    }

    public void setMisappliedResult(NameSearchResult misappliedResult) {
        this.misappliedResult = misappliedResult;
    }

}
