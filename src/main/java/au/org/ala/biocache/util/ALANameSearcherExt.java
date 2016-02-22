/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
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
package au.org.ala.biocache.util;

import au.org.ala.names.model.LinnaeanRankClassification;
import au.org.ala.names.model.NameSearchResult;
import au.org.ala.names.model.RankType;
import au.org.ala.names.search.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.solr.client.solrj.util.ClientUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Some additional methods that should be moved to ALANameSearcher.
 * 
 * Created by Adam Collins on 22/09/15.
 */
public class ALANameSearcherExt extends ALANameSearcher {


    private IndexSearcher identifierIdxSearcher = null;
    private IndexSearcher vernIdxSearcher = null;
    private Map<String, String> commonNames = new HashMap<String, String>();
    
    public ALANameSearcherExt(String path) throws IOException {
        super(path);
    }

    private IndexSearcher getIdentifierIdxSearcher() throws IOException {
        if (identifierIdxSearcher == null) {
            identifierIdxSearcher = getALANameSearcherSearcher("cbSearcher");
        }
        return identifierIdxSearcher;
    }

    private IndexSearcher getVernIdxSearcher() throws IOException {
        if (vernIdxSearcher == null) {
            vernIdxSearcher = getALANameSearcherSearcher("vernSearcher");
            
            //hack for finding common name from lsid
            int count = vernIdxSearcher.getIndexReader().numDocs();
            Map m = new HashMap<String, String>();
            for (int i=0;i<count;i++) {
                m.put(vernIdxSearcher.doc(i).get("lsid"), vernIdxSearcher.doc(i).get("common").toLowerCase());
            }
            commonNames = m;
        }
        return vernIdxSearcher;
    }
    
    /**
     * get IndexSearcher from nameIndex to avoid reloading index
     *
     * one of: cbSearcher, vernSercher, idSearcher, irmngSearcher
     *  
     * @param searcher
     * @return
     */
    private IndexSearcher getALANameSearcherSearcher(String searcher) {
        try {
            Field field = ALANameSearcher.class.getDeclaredField(searcher);
            field.setAccessible(true);
            IndexSearcher value = (IndexSearcher) field.get(this);
            field.setAccessible(false);
            
            return value;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void appendAutocompleteResults(Map<String, Map> output, TopDocs results, boolean includeSynonyms, boolean commonNameResults) throws IOException {
        ScoreDoc[] scoreDocs = results.scoreDocs;
        int scoreDocsCount = scoreDocs.length;
        for(int excludedResult = 0; excludedResult < scoreDocsCount; ++excludedResult) {
            ScoreDoc i = scoreDocs[excludedResult];
            Document src = commonNameResults ? getVernIdxSearcher().doc(i.doc) : getIdentifierIdxSearcher().doc(i.doc);
            NameSearchResult nsr = commonNameResults ?
                    searchForRecordByLsid(src.get("lsid"))
                    : new NameSearchResult(src, null);

            if (nsr == null || (nsr.getLeft() == null && !includeSynonyms)) continue;

            Map m = formatAutocompleteNsr(i.score, nsr);

            //use the matched common name
            if (commonNameResults) {
                m.put("commonname", src.get("common"));
                m.put("match", "commonName");
            } else {
                m.put("match", "scientificName");
            }

            while (includeSynonyms && nsr != null && m != null && nsr.getAcceptedLsid() != null) {
                if (output.containsKey(nsr.getAcceptedLsid())) {
                    List list = (List) output.get(nsr.getAcceptedLsid()).get("synonymMatch");
                    if (list == null) list = new ArrayList();
                    list.add(m);
                    output.get(nsr.getAcceptedLsid()).put("synonymMatch", list);
                    m = null;
                    nsr = null;
                } else {
                    nsr = searchForRecordByLsid(nsr.getAcceptedLsid());

                    if (nsr != null) {
                        List list = new ArrayList();
                        list.add(m);

                        m = formatAutocompleteNsr(i.score, nsr);
                        m.put("synonymMatch", list);
                    }
                }
            }

            if (((nsr != null && nsr.getAcceptedLsid() == null) || includeSynonyms) && m != null) {
                if (m.get("name").toString().equals("Acacia")) {
                    int aa = 4;
                }
                Map existing = output.get(m.get("lsid").toString());
                if (existing == null) {
                    output.put(m.get("lsid").toString(), m);
                } else {
                    //use best score
                    if ((Float) m.get("score") > (Float) existing.get("score")) {
                        output.put(m.get("lsid").toString(), m);
                    }
                }
            }
        }
    }
    
    private Query buildAutocompleteQuery(String field, String q, boolean allSearches) {
        //best match
        Query fq1 = new TermQuery(new Term(field,q));  //exact match
        fq1.setBoost(12f);

        //partial matches
        Query fq5 = new WildcardQuery(new Term(field,q + "*")); //begins with that begins with
        Query fq6 = new WildcardQuery(new Term(field,"* " + q + "*")); //contains word that begins with

        //any match
        Query fq7 = new WildcardQuery(new Term(field,"*" + q + "*")); //any match

        //join
        BooleanQuery o = new BooleanQuery();
        o.add(fq1, BooleanClause.Occur.SHOULD);

        o.add(fq5, BooleanClause.Occur.SHOULD);
        o.add(fq6, BooleanClause.Occur.SHOULD);

        o.add(fq7, BooleanClause.Occur.SHOULD);

        return o;
    }

    private String getPreferredGuid(String taxonConceptGuid) throws Exception {
        Query qGuid = new TermQuery(new Term("guid", taxonConceptGuid));
        Query qOtherGuid = new TermQuery(new Term("otherGuid", taxonConceptGuid));

        BooleanQuery fullQuery = new BooleanQuery(true);
        fullQuery.add(qGuid, BooleanClause.Occur.SHOULD);
        fullQuery.add(qOtherGuid, BooleanClause.Occur.SHOULD);

        TopDocs topDocs = getIdentifierIdxSearcher().search(fullQuery, 1);
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            Document doc = getIdentifierIdxSearcher().doc(scoreDoc.doc);
            return doc.get("guid");
        }
        return taxonConceptGuid;
    }

    private boolean isKingdom(String name) {
        try {
            LinnaeanRankClassification lc = new LinnaeanRankClassification(name, null);
            NameSearchResult nsr = searchForRecord(lc, false);
            return nsr != null && nsr.getRank() == RankType.KINGDOM;
        } catch (Exception e) {
            return false;
        }
    }

    private String[] extractComponents(String in) {
        String[] retArray = new String[2];
        int lastOpen = in.lastIndexOf("(");
        int lastClose = in.lastIndexOf(")");
        if (lastOpen < lastClose) {
            //check to see if the last brackets are a kingdom
            String potentialKingdom = in.substring(lastOpen + 1, lastClose);
            if (isKingdom(potentialKingdom)) {
                retArray[0] = in.substring(0, lastOpen);
                retArray[1] = potentialKingdom;
            } else {
                retArray[0] = in;
            }
        } else {
            retArray[0] = in;
            //kingdom is null
        }
        return retArray;

    }

    private String getLsidByNameAndKingdom(String parameter) {
        String lsid = null;
        String name = null;
        String kingdom = null;

        String[] parts = extractComponents(parameter);
        name = parts[0];
        name = name.replaceAll("_", " ");
        name = name.replaceAll("\\+", " ");
        kingdom = parts[1];
        if (kingdom != null) {
            LinnaeanRankClassification cl = new LinnaeanRankClassification(kingdom, null);
            cl.setScientificName(name);
            try {
                lsid = searchForLSID(cl.getScientificName(), cl, null);
            } catch (ExcludedNameException e) {
                if (e.getNonExcludedName() != null)
                    lsid = e.getNonExcludedName().getLsid();
                else
                    lsid = e.getExcludedName().getLsid();
            } catch (ParentSynonymChildException e) {
                //the child is the one we want
                lsid = e.getChildResult().getLsid();
            } catch (MisappliedException e) {
                if (e.getMisappliedResult() != null)
                    lsid = e.getMatchedResult().getLsid();
            } catch (SearchResultException e) {
            }
        }
        //check for a scientific name first - this will lookup in the name matching index.  This will produce the correct result in a majority of scientific name cases.
        if (lsid == null || lsid.length() < 1) {
            try {
                lsid = searchForLSID(name, true, true);
            } catch (ExcludedNameException e) {
                if (e.getNonExcludedName() != null)
                    lsid = e.getNonExcludedName().getLsid();
                else
                    lsid = e.getExcludedName().getLsid();
            } catch (ParentSynonymChildException e) {
                //the child is the one we want
                lsid = e.getChildResult().getLsid();
            } catch (MisappliedException e) {
                if (e.getMisappliedResult() != null)
                    lsid = e.getMatchedResult().getLsid();
            } catch (SearchResultException e) {
            }
        }

        if (lsid == null || lsid.length() < 1) {
            lsid = searchForLSIDCommonName(name);
        }

        if (lsid == null || lsid.length() < 1) {
            lsid = findLSIDByConcatName(name);
        }

        return lsid;
    }

    private String concatName(String name) {
        String patternA = "[^a-zA-Z]";
        /* replace multiple whitespaces between words with single blank */
        String patternB = "\\b\\s{2,}\\b";

        String cleanQuery = "";
        if (name != null) {
            cleanQuery = ClientUtils.escapeQueryChars(name);//.toLowerCase();
            cleanQuery = cleanQuery.toLowerCase();
            cleanQuery = cleanQuery.replaceAll(patternA, "");
            cleanQuery = cleanQuery.replaceAll(patternB, "");
            cleanQuery = cleanQuery.trim();
        }
        return cleanQuery;
    }

    private String findLSIDByConcatName(String name) {
        try {
            String concatName = concatName(name);

            Query query = new TermQuery(new Term("concat_name", concatName));

            TopDocs topDocs = getIdentifierIdxSearcher().search(query, 2);
            if (topDocs != null && topDocs.totalHits == 1) {
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    Document doc = getIdentifierIdxSearcher().doc(scoreDoc.doc);
                    return doc.get("guid");
                }
            }
        } catch (Exception e) {
            // do nothing
        }
        return null;

    }

    /**
     * from bie ws/guid/batch
     *
     * returned list of guid that is the same length as the input list
     *
     * @param taxaQueries a list of taxa queries
     * @return
     */
    public List<String> getGuidsForTaxa(List<String> taxaQueries) {
        List guids = new ArrayList<String>();
        for (int i = 0; i < taxaQueries.size(); i++) {
            String scientificName = taxaQueries.get(i);
            String lsid = getLsidByNameAndKingdom(scientificName);
            if (lsid != null && lsid.length() > 0) {
                String guid = null;
                try {
                    guid = getExtendedTaxonConceptByGuid(lsid, true, true);
                } catch (Exception e) {
                }
                guids.add(guid);
            }

            if (guids.size() < i + 1) guids.add(null);
        }
        return guids;
    }

    private String getExtendedTaxonConceptByGuid(String guid, boolean checkPreferred, boolean checkSynonym) throws Exception {

        //Because a concept can be accepted and a synonym we need to check if the original guid exists before checking preferred
        NameSearchResult nsr = searchForRecordByLsid(guid);
        boolean hasAccepted = nsr != null && nsr.getAcceptedLsid() == null;

        if (checkPreferred && !hasAccepted) {
            guid = getPreferredGuid(guid);
        }
        if (checkSynonym && !hasAccepted) {
            if (nsr != null && nsr.isSynonym()) {
                guid = nsr.getAcceptedLsid();
            }
        }

        return guid;
    }

    /**
     * Basic autocomplete. All matches are resolved to accepted LSID.
     *
     * @param q
     * @param max
     * @param includeSynonyms
     * @return
     */
    public List<Map> autocomplete(String q, int max, boolean includeSynonyms) {
        try {
            if(false) {
                return null;
            } else {
                Map<String, Map> output = new HashMap<String, Map>();

                //more queries for better scoring values
                String lq = q.toLowerCase();
                String uq = q.toUpperCase();

                //name search
                Query fq = buildAutocompleteQuery("name", lq, false);
                BooleanQuery b = new BooleanQuery();
                b.add(fq, BooleanClause.Occur.MUST);
                b.add(new WildcardQuery(new Term("left", "*")), includeSynonyms ? BooleanClause.Occur.SHOULD : BooleanClause.Occur.MUST);
                TopDocs results = getIdentifierIdxSearcher().search(b, max);
                appendAutocompleteResults(output, results, includeSynonyms, false);

                //format search term for the current common name index
                uq = concatName(uq).toUpperCase();

                //common name search
                fq = buildAutocompleteQuery("common", uq, true);
                results = getVernIdxSearcher().search(fq, max);
                appendAutocompleteResults(output, results, includeSynonyms, true);

                return new ArrayList(output.values());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private Map formatAutocompleteNsr(float score, NameSearchResult nsr) {
        Map m = new HashMap();
        m.put("score", score);
        m.put("lsid", nsr.getLsid());
        m.put("left", nsr.getLeft());
        m.put("right", nsr.getRight());
        m.put("rank", nsr.getRank());
        m.put("rankId", nsr.getRank() != null ? nsr.getRank().getId() : 10000);
        m.put("cl", nsr.getRankClassification());
        m.put("name", nsr.getRankClassification() != null ? nsr.getRankClassification().getScientificName() : null);
        m.put("acceptedLsid", nsr.getAcceptedLsid());
        m.put("commonname", commonNames.get(nsr.getLsid())); //nameIndex.getCommonNameForLSID(nsr.getLsid()));

        return m;
    }
}
