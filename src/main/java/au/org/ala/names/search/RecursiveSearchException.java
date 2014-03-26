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

import au.org.ala.names.model.ErrorType;

import java.util.Set;

import org.gbif.ecat.voc.NameType;

/**
 * Represents an exception that occurs during a recursive match.  Where there
 * are one or  more issues with the supplied name that prevented a match.
 *
 * @author Natasha Carter
 */
public class RecursiveSearchException extends SearchResultException {
    private Set<ErrorType> errors;
    private NameType nameType;

    public RecursiveSearchException(Set<ErrorType> errors, NameType nameType) {
        super("There are one or more issues with the name to prevent a match");
        this.errors = errors;
        this.nameType = nameType;
    }

    public Set<ErrorType> getErrors() {
        return errors;
    }

    public NameType getNameType() {
        return nameType;
    }

}
