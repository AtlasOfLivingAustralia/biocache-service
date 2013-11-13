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
package org.ala.biocache.dao;

import java.io.Writer;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.springframework.stereotype.Component;

import au.org.ala.biocache.IndexDAO;
import au.org.ala.biocache.SolrIndexDAO;

@Component("taxonDao")
public class TaxonDAOImpl implements TaxonDAO {

    private static final Logger logger = Logger.getLogger(TaxonDAOImpl.class);
    protected SolrServer server;

    /**
     * Initialise the SOLR server instance
     */
    public TaxonDAOImpl() {
        if (this.server == null) {
            try {
                //use the solr server that has been in the biocache-store...
                SolrIndexDAO dao = (SolrIndexDAO) au.org.ala.biocache.Config.getInstance(IndexDAO.class);
                dao.init();
                server = dao.solrServer();
            } catch (Exception ex) {
                logger.error("Error initialising embedded SOLR server: " + ex.getMessage(), ex);
            }
        }
    }

    public void extractBySpeciesGroups(String metadataUrl, String q, String[] fq, Writer writer) throws Exception{

        List<FacetField.Count> speciesGroups = extractFacet(q,fq, "species_group");
        for(FacetField.Count spg: speciesGroups){
            List<FacetField.Count> orders = extractFacet(q, (String[])ArrayUtils.add(fq, "species_group:"+spg.getName()), "order");
            for(FacetField.Count o: orders){
                outputNestedMappableLayerStart("order", o.getName(), writer);
                List<FacetField.Count> families = extractFacet(q, (String[])ArrayUtils.add(fq, "order:"+o.getName()), "family");
                for(FacetField.Count f: families){
                    outputNestedMappableLayerStart("family", f.getName(), writer);
                    List<FacetField.Count> genera = extractFacet(q, (String[])ArrayUtils.addAll(fq, new String[]{"family:"+f.getName(), "species_group:" + spg.getName()}), "genus");
                    for(FacetField.Count g: genera){
                        outputNestedMappableLayerStart("genus", g.getName(), writer);
                        List<FacetField.Count> species = extractFacet(q, (String[])ArrayUtils.addAll(fq, new String[]{"genus:"+g.getName(), "species_group:" + spg.getName(),"family:"+f.getName()}), "species");
                        for(FacetField.Count s: species){
                            outputLayer(metadataUrl, "species", s.getName(), writer);
                        }
                        outputNestedLayerEnd(writer);
                    }
                    outputNestedLayerEnd(writer);
                }
                outputNestedLayerEnd(writer);
            }
            outputNestedLayerEnd(writer);
        }
    }

    @Override
    public void extractHierarchy(String metadataUrl, String q, String[] fq, Writer writer) throws Exception {

        List<FacetField.Count> kingdoms = extractFacet(q,fq,"kingdom");
        for(FacetField.Count k: kingdoms){
            outputNestedLayerStart(k.getName(), writer);
            List<FacetField.Count> phyla = extractFacet(q, (String[]) ArrayUtils.add(fq, "kingdom:" + k.getName()), "phylum");
            for(FacetField.Count p: phyla){
                outputNestedMappableLayerStart("phylum", p.getName(), writer);
                List<FacetField.Count> classes = extractFacet(q, (String[]) ArrayUtils.add(fq, "phylum:" + p.getName()), "class");
                for(FacetField.Count c: classes){
                    outputNestedMappableLayerStart("class", c.getName(), writer);
                    List<FacetField.Count> orders = extractFacet(q, (String[])ArrayUtils.add(fq, "class:"+c.getName()), "order");
                    for(FacetField.Count o: orders){
                        outputNestedMappableLayerStart("order", o.getName(), writer);
                        List<FacetField.Count> families = extractFacet(q, (String[])ArrayUtils.addAll(fq, new String[]{"order:"+o.getName(), "kingdom:" + k.getName()}), "family");
                        for(FacetField.Count f: families){
                            outputNestedMappableLayerStart("family", f.getName(), writer);
                            List<FacetField.Count> genera = extractFacet(q, (String[])ArrayUtils.addAll(fq, new String[]{"family:"+f.getName(), "kingdom:" + k.getName()}), "genus");
                            for(FacetField.Count g: genera){
                                outputNestedMappableLayerStart("genus", g.getName(), writer);
                                List<FacetField.Count> species = extractFacet(q, (String[])ArrayUtils.addAll(fq, new String[]{"genus:"+g.getName(), "kingdom:" + k.getName(),"family:"+f.getName()}), "species");
                                for(FacetField.Count s: species){
                                    outputLayer(metadataUrl, "species", s.getName(), writer);
                                }
                                outputNestedLayerEnd(writer);
                            }
                            outputNestedLayerEnd(writer);
                        }
                        outputNestedLayerEnd(writer);
                    }
                    outputNestedLayerEnd(writer);
                }
                outputNestedLayerEnd(writer);
            }
            outputNestedLayerEnd(writer);
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
        QueryResponse response = server.query(query);
        List<FacetField.Count> fc = response.getFacetField(facetName).getValues();
        if(fc == null){
            fc = new ArrayList<FacetField.Count>();
        }
        return fc;
    }
}
