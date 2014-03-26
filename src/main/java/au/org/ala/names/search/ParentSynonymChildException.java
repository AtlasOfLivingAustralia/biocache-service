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
 * An exception that is thrown when the result represents a situation
 * where the parent is a synonym of the child concept.
 *
 * @author Natasha Carter
 */
public class ParentSynonymChildException extends SearchResultException {
    private NameSearchResult parentResult;
    private NameSearchResult childResult;

    public ParentSynonymChildException(NameSearchResult parentResult, NameSearchResult childResult) {
        super("The parent name is a synonym to the child name");
        this.parentResult = parentResult;
        this.childResult = childResult;
        errorType = au.org.ala.names.model.ErrorType.PARENT_CHILD_SYNONYM;

    }

    public NameSearchResult getChildResult() {
        return childResult;
    }

    public NameSearchResult getParentResult() {
        return parentResult;
    }

}
