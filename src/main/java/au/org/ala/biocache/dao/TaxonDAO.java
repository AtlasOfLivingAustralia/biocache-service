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
package au.org.ala.biocache.dao;

import java.io.Writer;

/**
 * DAO for searching the biocache at a taxonomic level.
 */
public interface TaxonDAO {

	/**
	 * Writes a hierarchy to the writer that matches the supplied query.
	 * 
	 * @param metadataUrl
	 * @param query
	 * @param filterQueries
	 * @param writer
	 * @throws Exception
	 */
    void extractHierarchy(String metadataUrl, String query, String[] filterQueries, Writer writer) throws Exception;

	/**
	 * Writes a hierarchy to the writer that matches the supplied query.
	 * 
	 * @param metadataUrl
	 * @param query
	 * @param filterQueries
	 * @param writer
	 * @throws Exception
	 */    
    void extractBySpeciesGroups(String metadataUrl, String query, String[] filterQueries, Writer writer) throws Exception;
}
