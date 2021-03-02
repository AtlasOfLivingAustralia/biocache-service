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

import au.org.ala.biocache.dto.OccurrenceIndex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.Writer;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

@Component("taxonDao")
public class TaxonDAOImpl implements TaxonDAO {

    private static final Logger logger = Logger.getLogger(TaxonDAOImpl.class);

    /**
     * SOLR client instance
     */
    @Inject
    private IndexDAO indexDAO;

    public void extractBySpeciesGroups(String metadataUrl, String q, String[] fq, Writer writer) throws Exception{

        List<FacetField.Count> speciesGroups = extractFacet(q, fq, OccurrenceIndex.SPECIES_GROUP);
        for(FacetField.Count spg: speciesGroups){
            if (spg.getName() != null) {
                List<FacetField.Count> orders = extractFacet(q, (String[]) ArrayUtils.add(fq, OccurrenceIndex.SPECIES_GROUP + ":" + spg.getName()), OccurrenceIndex.ORDER);
                for (FacetField.Count o : orders) {
                    if (o.getName() != null) {
                        outputNestedMappableLayerStart(OccurrenceIndex.ORDER, o.getName(), writer);
                        List<FacetField.Count> families = extractFacet(q, (String[]) ArrayUtils.add(fq, OccurrenceIndex.ORDER + ":" + o.getName()), OccurrenceIndex.FAMILY);
                        for (FacetField.Count f : families) {
                            if (f.getName() != null) {
                                outputNestedMappableLayerStart(OccurrenceIndex.FAMILY, f.getName(), writer);
                                List<FacetField.Count> genera = extractFacet(q, (String[]) ArrayUtils.addAll(fq, new String[]{OccurrenceIndex.FAMILY + ":" + f.getName(), OccurrenceIndex.SPECIES_GROUP + ":" + spg.getName()}), OccurrenceIndex.GENUS);
                                for (FacetField.Count g : genera) {
                                    if (g.getName() != null) {
                                        outputNestedMappableLayerStart(OccurrenceIndex.GENUS, g.getName(), writer);
                                        List<FacetField.Count> species = extractFacet(q, (String[]) ArrayUtils.addAll(fq, new String[]{OccurrenceIndex.GENUS + ":" + g.getName(), OccurrenceIndex.SPECIES_GROUP + ":" + spg.getName(), OccurrenceIndex.FAMILY + ":" + f.getName()}), OccurrenceIndex.SPECIES);
                                        for (FacetField.Count s : species) {
                                            if (s.getName() != null) {
                                                outputLayer(metadataUrl, OccurrenceIndex.SPECIES, s.getName(), writer);
                                            }
                                        }
                                        outputNestedLayerEnd(writer);
                                    }
                                }
                                outputNestedLayerEnd(writer);
                            }
                        }
                        outputNestedLayerEnd(writer);
                    }
                }
                outputNestedLayerEnd(writer);
            }
        }
    }

    @Override
    public void extractHierarchy(String metadataUrl, String q, String[] fq, Writer writer) throws Exception {

        List<FacetField.Count> kingdoms = extractFacet(q, fq, OccurrenceIndex.KINGDOM);
        for(FacetField.Count k: kingdoms){
            if (k.getName() != null) {
                outputNestedLayerStart(k.getName(), writer);
                List<FacetField.Count> phyla = extractFacet(q, (String[]) ArrayUtils.add(fq, OccurrenceIndex.KINGDOM + ":" + k.getName()), OccurrenceIndex.PHYLUM);
                for (FacetField.Count p : phyla) {
                    if (p.getName() != null) {
                        outputNestedMappableLayerStart(OccurrenceIndex.PHYLUM, p.getName(), writer);
                        List<FacetField.Count> classes = extractFacet(q, (String[]) ArrayUtils.add(fq, OccurrenceIndex.PHYLUM + ":" + p.getName()), OccurrenceIndex.CLASS);
                        for (FacetField.Count c : classes) {
                            if (c.getName() != null) {
                                outputNestedMappableLayerStart(OccurrenceIndex.CLASS, c.getName(), writer);
                                List<FacetField.Count> orders = extractFacet(q, (String[]) ArrayUtils.add(fq, OccurrenceIndex.CLASS + ":" + c.getName()), OccurrenceIndex.ORDER);
                                for (FacetField.Count o : orders) {
                                    if (o.getName() != null) {
                                        outputNestedMappableLayerStart(OccurrenceIndex.ORDER, o.getName(), writer);
                                        List<FacetField.Count> families = extractFacet(q, (String[]) ArrayUtils.addAll(fq, new String[]{OccurrenceIndex.ORDER + ":" + o.getName(), OccurrenceIndex.KINGDOM + ":" + k.getName()}), OccurrenceIndex.FAMILY);
                                        for (FacetField.Count f : families) {
                                            if (f.getName() != null) {
                                                outputNestedMappableLayerStart(OccurrenceIndex.FAMILY, f.getName(), writer);
                                                List<FacetField.Count> genera = extractFacet(q, (String[]) ArrayUtils.addAll(fq, new String[]{OccurrenceIndex.FAMILY + ":" + f.getName(), OccurrenceIndex.KINGDOM + ":" + k.getName()}), OccurrenceIndex.GENUS);
                                                for (FacetField.Count g : genera) {
                                                    if (g.getName() != null) {
                                                        outputNestedMappableLayerStart(OccurrenceIndex.GENUS, g.getName(), writer);
                                                        List<FacetField.Count> species = extractFacet(q, (String[]) ArrayUtils.addAll(fq, new String[]{OccurrenceIndex.GENUS + ":" + g.getName(), OccurrenceIndex.KINGDOM + ":" + k.getName(), OccurrenceIndex.FAMILY + ":" + f.getName()}), OccurrenceIndex.SPECIES);
                                                        for (FacetField.Count s : species) {
                                                            if (s.getName() != null) {
                                                                outputLayer(metadataUrl, OccurrenceIndex.SPECIES, s.getName(), writer);
                                                            }
                                                        }
                                                        outputNestedLayerEnd(writer);
                                                    }
                                                }
                                                outputNestedLayerEnd(writer);
                                            }
                                        }
                                        outputNestedLayerEnd(writer);
                                    }
                                }
                                outputNestedLayerEnd(writer);
                            }
                        }
                        outputNestedLayerEnd(writer);
                    }
                }
                outputNestedLayerEnd(writer);
            }
        }
    }

    void outputNestedMappableLayerStart(String rank, String taxon, Writer out) throws Exception {
        out.write("<Layer queryable=\"1\"><Name>" + rank + ":" + taxon + "</Name><Title>" + taxon + "</Title>");
        out.flush();
    }

    void outputNestedLayerStart(String layerName, Writer out) throws Exception {
        out.write("<Layer><Name>"+layerName + "</Name><Title>"+layerName + "</Title>\n\t");
        out.flush();
    }

    void outputNestedLayerEnd(Writer out) throws Exception {
        out.write("</Layer>");
        out.flush();
    }

    void outputLayer(String metadataUrlRoot, String rank, String taxon, Writer out) throws Exception {
        String normalised = taxon.replaceFirst("\\([A-Za-z]*\\) ", "").replace(" ", "_").replace("&", "&amp;"); //remove the subgenus, replace spaces with underscores
        String normalisedTitle = taxon.replaceFirst("\\([A-Za-z]*\\) ", "").replace("&", "&amp;"); //remove the subgenus, replace spaces with underscores

        out.write("<Layer queryable=\"1\"><Name>" + rank + ":" + normalised + "</Name><Title>"+ rank + ":" + normalisedTitle + "</Title>"+
                "<MetadataURL type=\"TC211\">\n" +
                "<Format>text/html</Format>\n" +
                "<OnlineResource xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:type=\"simple\"" +
                " xlink:href=\""+metadataUrlRoot+"?q="+rank+":"+ URLEncoder.encode(taxon,"UTF-8") +"\"/>\n" +
                "</MetadataURL>"+
                "</Layer>");
        out.flush();
    }

    private List<FacetField.Count> extractFacet(String queryString, String[] filterQueries, String facetName) throws Exception {

        // TODO: PIPELINES: query mapping needs to be performed!!!

        SolrQuery query = new SolrQuery(queryString);
        query.setFacet(true);
        query.addFacetField(facetName);
        query.setRows(0);
        query.setFacetLimit(200000);
        query.setStart(0);
        query.setFacetMinCount(1);
        query.setFacetSort("index");
        //query.setFacet
        if(filterQueries != null){
            for(String fq: filterQueries) query.addFilterQuery(fq);
        }
        QueryResponse response = indexDAO.query(query);
        List<FacetField.Count> fc = response.getFacetField(facetName).getValues();
        if(fc == null){
            fc = new ArrayList<FacetField.Count>();
        }
        return fc;
    }
}
