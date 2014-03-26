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
 * An exception that is thrown when the match is to a name that has been excluded.
 * <p/>
 * In the BIE we would want to use the match anyway because all excluded names will have
 * a species page.
 * <p/>
 * In the biocache we would want to flag the record and match to the associated record?? OR maybe
 *
 * @author Natasha Carter
 */
public class ExcludedNameException extends SearchResultException {
    private NameSearchResult excludedName;
    private NameSearchResult nonExcludedName;

    public ExcludedNameException(String message, NameSearchResult excludedName) {
        super(message);
        this.excludedName = excludedName;
        errorType = au.org.ala.names.model.ErrorType.EXCLUDED;

    }

    public ExcludedNameException(String message, NameSearchResult nonExcludedName, NameSearchResult excludedName) {
        this(message, excludedName);
        this.nonExcludedName = nonExcludedName;
        errorType = au.org.ala.names.model.ErrorType.ASSOCIATED_EXCLUDED;
    }

    public NameSearchResult getExcludedName() {
        return excludedName;
    }

    public void setExcludedName(NameSearchResult excludedName) {
        this.excludedName = excludedName;
    }

    public NameSearchResult getNonExcludedName() {
        return nonExcludedName;
    }

    public void setNonExcludedName(NameSearchResult nonExcludedName) {
        this.nonExcludedName = nonExcludedName;
    }

}
