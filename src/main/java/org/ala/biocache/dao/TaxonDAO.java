package org.ala.biocache.dao;

import java.io.OutputStream;
import java.io.Writer;

/**
 * DAO for searching the biocache at a taxonomic level.
 */
public interface TaxonDAO {

    public void extractHierarchy(String metadataUrl, String query, String[] filterQueries, Writer writer) throws Exception;

    public void extractBySpeciesGroups(String metadataUrl, String query, String[] filterQueries, Writer writer) throws Exception;
}
