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
 * An Enum for all the fields that are indexed for the name matching
 *
 * @author Natasha Carter
 */
public enum NameIndexField {
    ID("id"),
    LSID("lsid"),
    ACCEPTED("accepted_lsid"),
    iS_SYNONYM("is_synonym"),//whether or not the record is a synonym
    GENUS_EX("genus_ex"), //genus sounds like expression - handles masculine and feminine too.
    SPECIES_EX("specific_ex"),// specific epithet sounds like expression
    INFRA_EX("infra_ex"),//infra specific epithet sounds like expression
    SPECIFIC("specific"),
    INFRA_SPECIFIC("infra"),
    NAME("name"),//canonical name
    RANK_ID("rank_id"),
    RANK("rank"),
    AUTHOR("author"),
    PHRASE("phrase"),//stores the values of a "phrase" name.  Some more intelligence will be needed when matching these
    VOUCHER("voucher"), //stores a voucher value minus the spaces and fullstops.
    ALA("ala"), //stores whether or not it is an ALA generated name
    SYNONYM_TYPE("syn_type"); //stores the type of synonym that it represents
    String name;

    NameIndexField(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }
}
