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

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.names.lucene.analyzer.LowerCaseKeywordAnalyzer;
import au.org.ala.names.model.*;
import au.org.ala.names.parser.PhraseNameParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.*;

import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.gbif.ecat.model.ParsedName;
import org.gbif.ecat.parser.NameParser;
import au.org.ala.names.util.TaxonNameSoundEx;

import java.util.Iterator;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.util.Version;
import org.gbif.dwc.record.DarwinCoreRecord;
import org.gbif.dwc.text.*;
import org.gbif.ecat.voc.NameType;

/**
 * Creates the Lucene index based on the names that are exported from
 * http://code.google.com/p/ala-portal/source/browse/trunk/ala-names-generator/src/main/resources/create-dumps.sql
 *
 * @author Natasha
 */
public class ALANameIndexer {

    //private String cbExportFile = "cb_name_usages.txt";
    //private String lexFile = "cb_lex_names.txt";
    //private String irmngFile = "irmng_classification.txt";
    private String colFile = "/data/bie-staging/ala-names/col_common_names.txt";
    private String extraALAConcepts = "/data/bie-staging/ala-names/ala-extra.txt";
    private String alaConcepts = "/data/bie-staging/ala-names/ala_accepted_concepts_dump.txt";
    private String alaSynonyms = "/data/bie-staging/ala-names/ala_synonyms_dump.txt";
    private String irmngDwcaDirectory = "/data/bie-staging/irmng/IRMNG_DWC_HOMONYMS";
    private String afdFile = "/data/bie-staging/anbg/AFD-common-names.csv";
    private String apniFile = "/data/bie-staging/anbg/APNI-common-names.csv";
    //private String taxonConeptName = "/data/bie-staging/anbg/taxonConcepts.txt";
    private Log log = LogFactory.getLog(ALANameIndexer.class);
    // protected ApplicationContext context;
    //private DataSource dataSource;
    // protected JdbcTemplate dTemplate;
    private IndexSearcher idSearcher;
    //the position in the line for each of the required values

    private final int POS_ID = 0;
    private final int POS_PARENT_ID = 1;
    private final int POS_LSID = 2;
    private final int POS_PARETN_LSID = 3;
    private final int POS_ACC_LSID = 4;
    private final int POS_NAME_LSID = 5;
    private final int POS_SCI_NAME = 6;
    private final int POS_GENUS_OR_HIGHER = 7;
    private final int POS_SP_EPITHET = 8;
    private final int POS_INFRA_EPITHET = 9;
    private final int POS_AUTHOR = 10;
    private final int POS_AUTHOR_YEAR = 11;
    private final int POS_RANK_ID = 12;
    private final int POS_RANK = 13;
    private final int POS_LFT = 14;
    private final int POS_RGT = 15;
    private final int POS_KID = 16;
    private final int POS_K = 17;
    private final int POS_PID = 18;
    private final int POS_P = 19;
    private final int POS_CID = 20;
    private final int POS_C = 21;
    private final int POS_OID = 22;
    private final int POS_O = 23;
    private final int POS_FID = 24;
    private final int POS_F = 25;
    private final int POS_GID = 26;
    private final int POS_G = 27;
    private final int POS_SID = 28;
    private final int POS_S = 29;
    private final int POS_SRC = 30;
    private final int POS_EXCLUDED = 36;

    private String indexDirectory;
    private IndexWriter cbIndexWriter;

    //Fields that are being indexed or stored in the lucene index
    public enum IndexField {

        NAME("name"),
        NAMES("names"),
        ID("id"),
        RANK("rank"),
        SEARCHABLE_NAME("searchcan"),
        LSID("lsid"),
        HOMONYM("homonym"),
        ACCEPTED("synonym"),
        LEFT("left"),
        RIGHT("right"),
        COMMON_NAME("common");

        String name;

        IndexField(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    };
    NameParser parser = new PhraseNameParser();
    Set<String> knownHomonyms = new HashSet<String>();
    Set<String> blacklist = new HashSet<String>();
    private TaxonNameSoundEx tnse;

    public void init() throws Exception {

        tnse = new TaxonNameSoundEx();
        // init the known homonyms
        LineIterator lines = new LineIterator(new BufferedReader(
                new InputStreamReader(
                        this.getClass().getClassLoader().getResource(
                                "au/org/ala/propertystore/known_homonyms.txt").openStream(), "ISO-8859-1")));
        LineIterator blines = new LineIterator(new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResource("blacklist.txt").openStream())));
        try {
            //load known homonyms
            while (lines.hasNext()) {
                String line = lines.nextLine().trim();
                knownHomonyms.add(line.toUpperCase());
            }
            //load the blacklist
            while (blines.hasNext()) {
                String line = blines.nextLine().trim();
                if (!line.startsWith("#") && StringUtils.isNotBlank(line))
                    blacklist.add(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lines.close();
            blines.close();
        }
    }

    /**
     * Creates the index from the specified checklist bank names usage export file into
     * the specified index directory.
     *
     * @param exportsDir   The directory that contains the exports that are necesary to generate the index.
     * @param indexDir     The directory in which the 2 indices will be created.
     * @param generateSciNames true when the scientific name index should be created
     * @param generateCommonNames true when the common name index should be generated
     * @throws Exception
     */
    public void createIndex(String exportsDir, String indexDir, boolean generateSciNames, boolean generateCommonNames) throws Exception {
        createIndex(exportsDir, indexDir, alaConcepts, alaSynonyms, irmngDwcaDirectory, generateSciNames, generateCommonNames);
    }

    /**
     * Creates the IRMNG homonym index based on the DWCA and species homonyms supplied from the NSL
     * @param exportsDir
     * @param indexDir
     * @throws Exception
     */
    public void createIrmngIndex(String exportsDir, String indexDir) throws Exception {
        Analyzer analyzer = new LowerCaseKeywordAnalyzer();
        IndexWriter irmngWriter = createIndexWriter(new File(indexDir + File.separator + "irmng"), analyzer, true);
        indexIrmngDwcA(irmngWriter, irmngDwcaDirectory);
        indexIRMNG(irmngWriter, exportsDir + File.separator + "ala-species-homonyms.txt", RankType.SPECIES);
        irmngWriter.forceMerge(1);
        irmngWriter.close();
    }

    public void createIndex(String exportsDir, String indexDir, String acceptedFile, String synonymFile, String irmngDwca, boolean generateSciNames, boolean generateCommonNames) throws Exception {

        Analyzer analyzer = new LowerCaseKeywordAnalyzer();
        //generate the extra id index
        createExtraIdIndex(indexDir + File.separator + "id", new File(exportsDir + File.separator + "identifiers.txt"));
        if (generateSciNames) {
            indexALA(createIndexWriter(new File(indexDir + File.separator + "cb"), analyzer, true), acceptedFile, synonymFile);//exportsDir + File.separator + "ala_accepted_concepts_dump.txt");//, exportsDir + File.separator + lexFile);
            //IRMNG index to aid in the resolving of homonyms
            IndexWriter irmngWriter = createIndexWriter(new File(indexDir + File.separator + "irmng"), analyzer, true);
            indexIrmngDwcA(irmngWriter, irmngDwca);

            indexIRMNG(irmngWriter, exportsDir + File.separator + "ala-species-homonyms.txt", RankType.SPECIES);
            irmngWriter.forceMerge(1);
            irmngWriter.close();
        }
        if (generateCommonNames) {
            //vernacular index to search for common names
            indexCommonNames(createIndexWriter(new File(indexDir + File.separator + "vernacular"), new KeywordAnalyzer(), true), exportsDir, indexDir);
        }
    }

    /**
     * Creates the temporary index that provides a lookup of checklist bank id to
     * GUID
     */
    private IndexSearcher createTmpGuidIndex(String cbExportFile) throws Exception {
        System.out.println("Starting to create the tmp guid index...");
        IndexWriter iw = createIndexWriter(new File("/data/tmp/guid"), new KeywordAnalyzer(), true);
        au.com.bytecode.opencsv.CSVReader cbreader = new au.com.bytecode.opencsv.CSVReader(new FileReader(cbExportFile), '\t', '"', '/', 1);
        for (String[] values = cbreader.readNext(); values != null; values = cbreader.readNext()) {
            Document doc = new Document();
            String id = values[POS_ID];
            String guid = values[POS_LSID];
            doc.add(new StringField("id", id, Store.YES));
            if (StringUtils.isEmpty(id))
                guid = id;

            doc.add(new StoredField("guid", guid));
            iw.addDocument(doc);
        }
        System.out.println("Finished writing the tmp guid index...");
        iw.commit();
        iw.forceMerge(1);
        iw.close();
        //As of lucene 4.0 all IndexReaders are read only
        return new IndexSearcher(DirectoryReader.open(FSDirectory.open(new File("/data/tmp/guid"))));
    }

    /**
     * Creates an index writer in the specified directory.  It will create/recreate
     * the target directory
     *
     * @param directory
     * @param analyzer
     * @return
     * @throws Exception
     */
    protected IndexWriter createIndexWriter(File directory, Analyzer analyzer, boolean replace) throws Exception {
        IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_34, analyzer);
        if (replace)
            conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        else
            conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        if (directory.exists() && replace) {
            FileUtils.forceDelete(directory);
        }
        FileUtils.forceMkdir(directory);
        IndexWriter iw = new IndexWriter(FSDirectory.open(directory), conf);
        return iw;
    }

    private String getValueFromIndex(IndexSearcher is, String searchField, String value, String retField) {
        TermQuery tq = new TermQuery(new Term(searchField, value));
        try {
            org.apache.lucene.search.TopDocs results = is.search(tq, 1);
            if (results.totalHits > 0)
                return is.doc(results.scoreDocs[0].doc).get(retField);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return value;
    }

    /**
     * Adds the extra ALA concepts from the legislated lists that are missing from the NSL.
     *
     * @param iw
     * @param file
     * @throws Exception
     */
    private void addExtraALAConcept(IndexWriter iw, String file) throws Exception {
        if (new File(file).exists()) {
            au.com.bytecode.opencsv.CSVReader reader = new au.com.bytecode.opencsv.CSVReader(new FileReader(file), ',', '"', '\\', 1);
            for (String[] values = reader.readNext(); values != null; values = reader.readNext()) {
                String lsid = values[0];
                String scientificName = values[1];
                String authority = values[2];
                Document doc = createALAIndexDocument(scientificName, "-1", lsid, authority, null);
                iw.addDocument(doc);
            }
        }
    }

    private void addALASyonyms(IndexWriter iw, String file) throws Exception {
        au.com.bytecode.opencsv.CSVReader reader = new au.com.bytecode.opencsv.CSVReader(new FileReader(file), '\t', '"', '\\', 1);
        for (String[] values = reader.readNext(); values != null; values = reader.readNext()) {

            String source = values[11];
            //give CoL synonyms a lower boost than NSL
            float boost = source.trim().equals("") || source.equalsIgnoreCase("CoL") ? 0.75f : 1.0f;
            Document doc = createALASynonymDocument(values[5], values[6], values[0], values[1], values[2], values[3], values[4], boost, values[9]);
            if (doc != null)
                iw.addDocument(doc);
        }
    }

    private void indexALA(IndexWriter iw, String file, String synonymFile) throws Exception {
        int records = 0;
        long time = System.currentTimeMillis();
        au.com.bytecode.opencsv.CSVReader reader = new au.com.bytecode.opencsv.CSVReader(new FileReader(file), '\t', '"', '\\', 1);
        for (String[] values = reader.readNext(); values != null; values = reader.readNext()) {

            String lsid = values[POS_LSID];
            String id = values[POS_ID];
            String rank = values[POS_RANK];
            int rankId = -1;
            try {
                rankId = Integer.parseInt(values[POS_RANK_ID]);
            } catch (Exception e) {
            }

            String acceptedValues = values[POS_ACC_LSID];
            float boost = 1.0f;
            //give the major ranks a larger boost
            if (rankId % 1000 == 0) {
                boost = 5.0f;
            }
            //give non-col concepts a higher boost
            String source = values[POS_SRC];
            if (!source.trim().equals("") && !source.equalsIgnoreCase("CoL")) {
                boost = boost * 2;
            }


            Document doc = createALAIndexDocument(values[POS_SCI_NAME], id, lsid, values[POS_RANK_ID],
                    values[POS_RANK], values[POS_K], values[POS_KID], values[POS_P],
                    values[POS_PID], values[POS_C], values[POS_CID],
                    values[POS_O], values[POS_OID], values[POS_F], values[POS_FID],
                    values[POS_G], values[POS_GID], values[POS_S], values[POS_SID],
                    values[POS_LFT], values[POS_RGT], acceptedValues,
                    values[POS_SP_EPITHET], values[POS_INFRA_EPITHET], values[POS_AUTHOR], boost);


            //add the excluded information if applicable
            if ("T".equals(values[POS_EXCLUDED]) || "Y".equals(values[POS_EXCLUDED])) {
                doc.add(new TextField(NameIndexField.SYNONYM_TYPE.toString(), SynonymType.EXCLUDES.getId().toString(), Store.YES));
            }
            if (doc != null) {
                iw.addDocument(doc);
                records++;
                if (records % 100000 == 0) {
                    log.info("Processed " + records + " in " + (System.currentTimeMillis() - time) + " msecs");
                }
            }
        }
        addExtraALAConcept(iw, extraALAConcepts);
        //add the synonyms
        addALASyonyms(iw, synonymFile);
        iw.commit();
        iw.forceMerge(1);
        iw.close();
        log.info("Lucene index created - processed a total of " + records + " records in " + (System.currentTimeMillis() - time) + " msecs ");
    }

    public void addAdditionalName(String lsid, String scientificName, String author, LinnaeanRankClassification cl) throws Exception {

        if (cbIndexWriter == null)
            cbIndexWriter = createIndexWriter(new File(indexDirectory + File.separator + "cb"), new LowerCaseKeywordAnalyzer(), false);
        Document doc = createALAIndexDocument(scientificName, "-1", lsid, author, cl);
        cbIndexWriter.addDocument(doc);

    }

    /**
     * Deletes the entry that has the supplied lsid. It will also delete all the synonyms associated with it
     * @param lsid
     * @throws Exception
     */
    public void deleteName(String lsid) throws Exception{
        if(cbIndexWriter == null){
            cbIndexWriter = createIndexWriter(new File(indexDirectory+ File.separator + "cb"), new LowerCaseKeywordAnalyzer(), false);
        }
        Term term = new Term("lsid", lsid);
        cbIndexWriter.deleteDocuments(new TermQuery(term));
        term = new Term("accepted_lsid", lsid);
        cbIndexWriter.deleteDocuments(new TermQuery(term));

    }
    public void commit() throws Exception{
        commit(false, false);
    }
    /**
     *
     * @param merge whether or not to merge the index
     * @param close whether or not to close the index
     * @throws Exception
     */
    public void commit(boolean close, boolean merge) throws Exception{
        if(cbIndexWriter !=  null){
            cbIndexWriter.commit();
            if(merge){
                cbIndexWriter.forceMerge(1);
            }
            if(close){
                cbIndexWriter.close();
            }
        }
    }

    /**
     * Indexes the IRMNG homonyms from the supplied DWCA direcory
     * @param iw The index writer to write the lucene docs to
     * @param archiveDirectory  The directory in which the IRMNG DWCA has been unzipped.
     * @throws Exception
     */
    protected void indexIrmngDwcA(IndexWriter iw, String archiveDirectory) throws Exception {
        log.info("Creating the IRMNG index from the DWCA " + archiveDirectory);
        //open the archive to extract the required information
        Archive archive = ArchiveFactory.openArchive(new File(archiveDirectory));
        Iterator<DarwinCoreRecord> it = archive.iteratorDwc();
        while (it.hasNext()) {
            Document doc = new Document();
            DarwinCoreRecord dwcr = it.next();
            String kingdom = dwcr.getKingdom();
            if (StringUtils.isNotEmpty(kingdom)) {
                doc.add(new TextField(RankType.KINGDOM.getRank(), kingdom, Store.YES));
            }
            String phylum = dwcr.getPhylum();
            if (StringUtils.isNotEmpty(phylum)) {
                doc.add(new TextField(RankType.PHYLUM.getRank(), phylum, Store.YES));
            }
            String classs = dwcr.getClasss();
            if (StringUtils.isNotEmpty(classs)) {
                doc.add(new TextField(RankType.CLASS.getRank(), classs, Store.YES));
            }
            String order = dwcr.getOrder();
            if (StringUtils.isNotEmpty(order)) {
                doc.add(new TextField(RankType.ORDER.getRank(), order, Store.YES));
            }
            String family = dwcr.getFamily();
            if (StringUtils.isNotEmpty(family)) {
                doc.add(new TextField(RankType.FAMILY.getRank(), family, Store.YES));
            }
            String genus = dwcr.getGenus();
            String calculatedRank = "genus";
            if (StringUtils.isNotEmpty(genus)) {
                doc.add(new TextField(RankType.GENUS.getRank(), genus, Store.YES));
                String specificEpithet = dwcr.getSpecificEpithet();
                if (StringUtils.isNotEmpty(specificEpithet)) {
                    calculatedRank = "species";
                    doc.add(new TextField(RankType.SPECIES.getRank(), genus + " " + specificEpithet, Store.YES));
                }
            }
            String rank = dwcr.getTaxonRank() != null ? dwcr.getTaxonRank() : calculatedRank;
            doc.add(new TextField(IndexField.RANK.toString(), rank, Store.YES));
            //now add the author - we don't do anything about this on homonym resolution yet
            //Add the author information
            String author = dwcr.getScientificNameAuthorship();
            if (StringUtils.isNotEmpty(author)) {
                //TODO think about whether we need to treat the author string with the taxamatch
                doc.add(new TextField(NameIndexField.AUTHOR.toString(), author, Store.YES));
            }
            //now add it to the index
            iw.addDocument(doc);

        }
    }

    /**
     * Indexes an IRMNG export for use in homonym resolution.
     *
     * @param iw
     * @param irmngExport
     * @throws Exception
     */
    void indexIRMNG(IndexWriter iw, String irmngExport, RankType rank) throws Exception {
        log.info("Creating IRMNG index ...");
        File file = new File(irmngExport);
        if (file.exists()) {
            CSVReader reader = new CSVReader(new FileReader(file), '\t', '"', '~');// CSVReader.build(file,"UTF-8", "\t", 0);
            int count = 0;
            String[] values = null;
            while ((values = reader.readNext()) != null) {
                Document doc = new Document();
                if (values != null && values.length >= 7) {
                    doc.add(new TextField(RankType.KINGDOM.getRank(), values[0], Store.YES));
                    doc.add(new TextField(RankType.PHYLUM.getRank(), values[1], Store.YES));
                    doc.add(new TextField(RankType.CLASS.getRank(), values[2], Store.YES));
                    doc.add(new TextField(RankType.ORDER.getRank(), values[3], Store.YES));
                    doc.add(new TextField(RankType.FAMILY.getRank(), values[4], Store.YES));
                    doc.add(new TextField(RankType.GENUS.getRank(), values[5], Store.YES));
                    if (rank == RankType.GENUS) {
                        doc.add(new TextField(IndexField.ID.toString(), values[6], Store.YES));
                        doc.add(new TextField(IndexField.ACCEPTED.toString(), values[8], Store.YES));
                        doc.add(new TextField(IndexField.HOMONYM.toString(), values[10], Store.YES));
                    } else if (rank == RankType.SPECIES) {
                        doc.add(new TextField(RankType.SPECIES.getRank(), values[6], Store.YES));
                    }
                    doc.add(new TextField(IndexField.RANK.toString(), rank.getRank(), Store.YES));
                    iw.addDocument(doc);
                    count++;
                }


            }
            iw.commit();

            log.info("Finished indexing " + count + " IRMNG " + rank + " taxa.");
        } else
            log.warn("Unable to create IRMNG index.  Can't locate " + irmngExport);
    }

    /**
     * Indexes common names from CoL and ANBG for use in the Common name search.
     *
     * @param iw  The index writer to write the common documents to
     * @param exportDir  The directory that contains the common name export files.
     * @param indexDir The directory in which to create the index.
     * @throws Exception
     */
    private void indexCommonNames(IndexWriter iw, String exportDir, String indexDir) throws Exception {
        log.info("Creating Common Names Index ...");

        //TODO think about adding additional sources for common names

        IndexSearcher currentNameSearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(new File(indexDir + File.separator + "cb"))));
        IndexSearcher extraSearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(new File(indexDir + File.separator + "id"))));

        addCoLCommonNames(iw, currentNameSearcher);
        addAnbgCommonNames(afdFile, iw, currentNameSearcher, extraSearcher, '\t');
        addAnbgCommonNames(apniFile, iw, currentNameSearcher, extraSearcher, ',');

        iw.commit();
        iw.forceMerge(1);
        iw.close();
    }

    /**
     * Adds the CoL common names to the common name index.
     * @param iw
     * @param currentSearcher
     * @throws Exception
     *
     */
    private void addCoLCommonNames(IndexWriter iw, IndexSearcher currentSearcher) throws Exception {
        File fileCol = new File(colFile);
        if (fileCol.exists()) {
            CSVReader reader = new CSVReader(new FileReader(fileCol), ',', '"', '~');
            int count = 0;
            String[] values = null;
            while ((values = reader.readNext()) != null) {
                if (values.length == 3) {
                    if (doesTaxonConceptExist(currentSearcher, values[2])) {
                        iw.addDocument(getCommonNameDocument(values[0], values[1], values[2], 1.0f));
                        count++;
                    } else {
                        System.out.println("Unable to locate LSID " + values[2] + " in current dump");
                    }
                }

            }
            log.info("Finished indexing " + count + " common names from " + fileCol);
        } else
            log.warn("Unable to index common names. Unable to locate : " + fileCol);
    }

    /**
     * Adds an ANBG CSV file of common names to the common name index.
     *
     * @param fileName The file name to add to the common name index
     * @param iw  The index writer to write the common documents to
     * @param currentSearcher The searcher to find a scientific name
     * @param idSearcher  The searcher to find an lsid
     * @param recordSep The record separator for the CSV file
     * @throws Exception
     */
    private void addAnbgCommonNames(String fileName, IndexWriter iw, IndexSearcher currentSearcher, IndexSearcher idSearcher, char recordSep) throws Exception {
        File namesFile = new File(fileName);
        Pattern p = Pattern.compile(",");
        if (namesFile.exists()) {
            CSVReader reader = new CSVReader(new FileReader(namesFile), recordSep, '"', '\\');//CSVReader.build(namesFile,"UTF-8","\t", '"' , 1);
            int count = 0;
            String[] values = reader.readNext();
            while ((values = reader.readNext()) != null) {
                if (values != null && values.length >= 4) {
                    //all ANBG records should have the highest boost as they are our authoritive source
                    //we only want to add an ANBG record if the taxon concept LSID exists in the taxonConcepts.txt export
                    if (doesTaxonConceptExist(currentSearcher, values[3]) || doesTaxonConceptExist(idSearcher, values[3])) {
                        //each common name could be a comma separated list
                        if (!values[2].contains(",") || values[2].toLowerCase().contains(" and ")) {
                            iw.addDocument(getCommonNameDocument(values[2], null, values[3], 2.0f));
                            count++;
                        } else {
                            //we need to process each common name in the list
                            String[] names = p.split(values[2]);
                            for (String name : names) {
                                iw.addDocument(getCommonNameDocument(name, null, values[3], 2.0f));
                                count++;
                            }
                        }
                    } else {
                        System.out.println("Unable to locate LSID " + values[3] + " in current dump");
                    }
                }

            }
            log.info("Finished indexing " + count + " common names from " + fileName);
        } else
            log.warn("Unable to index common names. Unable to locate : " + fileName);
    }

    /**
     * Creates a temporary index that will provide a lookup up of lsid to "real lsid".
     * <p/>
     * This deals with the following situations:
     * - common names that are sourced from CoL (LSIDs will be mapped to corresponding ANBG LSID)
     * - Multiple ANBG LSIDs exist for the same scientific name and more than 1 are mapped to the same common name.
     *
     * @param idFile
     * @throws Exception
     */
    private void createExtraIdIndex(String idxLocation, File idFile) throws Exception {
        CSVReader reader = new CSVReader(new FileReader(idFile), '\t', '"', '~');//CSVReader.build(idFile, "UTF-8", "\t", '"', 0);
        File indexDir = new File(idxLocation);
        IndexWriter iw = createIndexWriter(indexDir, new KeywordAnalyzer(), true);//new IndexWriter(FSDirectory.open(indexDir), new KeywordAnalyzer(), true, MaxFieldLength.UNLIMITED);
        String[] values = null;
        while ((values = reader.readNext()) != null) {

            if (values != null && values.length >= 3) {
                Document doc = new Document();
                //doc.add(new Field("lsid", values[2], Store.NO, Index.NOT_ANALYZED));
                doc.add(new StringField("lsid", values[2], Store.NO));
                //doc.add(new Field("reallsid", values[1], Store.YES, Index.NO));
                doc.add(new StoredField("reallsid", values[1]));
                iw.addDocument(doc);
            }
        }
        iw.commit();
        iw.forceMerge(1);
        iw.close();
        idSearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(indexDir)));
    }

    /**
     * Creates a temporary index that stores the taxon concept LSIDs that were
     * included in the last ANBG exports.
     *
     * @param tcFileName
     * @return
     * @throws Exception
     */
    private IndexSearcher createTmpIndex(String tcFileName) throws Exception {
        //creating the tmp index in the /tmp/taxonConcept directory
        CSVReader reader = new CSVReader(new FileReader(new File(tcFileName)), '\t', '"', '~');
        File indexDir = new File("/tmp/taxonConcept");
        IndexWriter iw = createIndexWriter(indexDir, new KeywordAnalyzer(), true);
        String[] values = null;
        while ((values = reader.readNext()) != null) {
            if (values != null && values.length > 1) {
                //just add the LSID to the index
                Document doc = new Document();

                doc.add(new StringField("lsid", values[0], Store.NO));
                iw.addDocument(doc);

            }
        }
        iw.commit();
        iw.forceMerge(1);
        iw.close();
        return new IndexSearcher(DirectoryReader.open(FSDirectory.open(indexDir)));
    }

    /**
     * Determines whether or not the supplied taxon lsid was included in the
     * latest ANBG exports.
     *
     * @param is
     * @param lsid
     * @return
     */
    private boolean doesTaxonConceptExist(IndexSearcher is, String lsid) {
        TermQuery query = new TermQuery(new Term("lsid", lsid));
        try {
            org.apache.lucene.search.TopDocs results = is.search(query, 1);
            return results.totalHits > 0;
        } catch (IOException e) {
            return false;
        }

    }

    /**
     * Uses the id index to find the accepted id for the supplied LSID.
     * <p/>
     * When no accepted id can be found the original LSID is returned.
     *
     * @param value
     * @return
     */
    private String getAcceptedLSID(String value) {
        TermQuery tq = new TermQuery(new Term("lsid", value));
        try {
            org.apache.lucene.search.TopDocs results = idSearcher.search(tq, 1);
            if (results.totalHits > 0)
                return idSearcher.doc(results.scoreDocs[0].doc).get("reallsid");
        } catch (IOException e) {
        }
        return value;
    }

    protected Document getCommonNameDocument(String cn, String sn, String lsid,float boost){
        return getCommonNameDocument(cn,sn,lsid,boost,true);
    }

    protected Document getCommonNameDocument(String cn, String sn, String lsid, float boost,boolean checkAccepted) {
        Document doc = new Document();
        //we are only interested in keeping all the alphanumerical values of the common name
        //when searching the same operations will need to be peformed on the search string
        TextField tf = new TextField(IndexField.COMMON_NAME.toString(), cn.toUpperCase().replaceAll("[^A-Z0-9ÏËÖÜÄÉÈČÁÀÆŒ]", ""), Store.YES);
        tf.setBoost(boost);

        doc.add(tf);
        if (sn != null) {

            doc.add(new TextField(IndexField.NAME.toString(), sn, Store.YES));
        }
        String newLsid = getAcceptedLSID(lsid);

        doc.add(new StoredField(IndexField.LSID.toString(), newLsid));

        return doc;
    }

    public Document createALAIndexDocument(String name, String id, String lsid, String author, LinnaeanRankClassification cl){
        return createALAIndexDocument(name,id, lsid, author,null,null, null, null,cl);
    }

    public Document createALAIndexDocument(String name, String id, String lsid, String author, String rank, String rankId, String left, String right, LinnaeanRankClassification cl){
        if(cl == null)
            cl = new LinnaeanRankClassification();
        return createALAIndexDocument(name, id, lsid, rankId, rank, cl.getKingdom(), cl.getKid(), cl.getPhylum()
                , cl.getPid(), cl.getKlass(), cl.getCid(), cl.getOrder(), cl.getOid(), cl.getFamily(),
                cl.getFid(), cl.getGenus(), cl.getGid(), cl.getSpecies(), cl.getSid(), left, right, null, null, null, author, 1.0f);
    }

    protected Document createALASynonymDocument(String scientificName, String author, String id, String lsid, String nameLsid, String acceptedLsid, String acceptedId, float boost, String synonymType) {
        lsid = StringUtils.isBlank(lsid) ? nameLsid : lsid;
        Document doc = createALAIndexDocument(scientificName, id, lsid, null, null,
                null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                acceptedLsid, null, null, author, boost);
        if (doc != null && synonymType != null) {
            try {
                doc.add(new TextField(NameIndexField.SYNONYM_TYPE.toString(), synonymType, Store.YES));
            } catch (Exception e) {
                System.out.println("Error on " + scientificName + " " + author + " " + id + ".  " + e.getMessage());
            }
        }
        return doc;
    }

    private boolean isBlacklisted(String scientificName) {
        return blacklist.contains(scientificName.trim());
    }

    private Document createALAIndexDocument(String name, String id, String lsid, String rank, String rankString,
                                            String kingdom, String kid, String phylum, String pid, String clazz, String cid, String order,
                                            String oid, String family, String fid, String genus, String gid,
                                            String species, String sid, String left, String right, String acceptedConcept, String specificEpithet,
                                            String infraspecificEpithet, String author,
                                            float boost) {
        //
        if (isBlacklisted(name)) {
            System.out.println(name + " has been blacklisted");
            return null;
        }
        Document doc = new Document();


        //Add the ids
        doc.add(new StringField(NameIndexField.ID.toString(), id, Store.YES));

        doc.add(new StringField(NameIndexField.LSID.toString(), lsid, Store.YES));
        if (lsid.startsWith("ALA")) {
            doc.add(new TextField(NameIndexField.ALA.toString(), "T", Store.NO));
        }

        //Add the scientific name information

        Field f = new TextField(NameIndexField.NAME.toString(), name, Store.YES);
        f.setBoost(boost);
        doc.add(f);


        //rank information
        if (StringUtils.isNotEmpty(rank)) {
            doc.add(new StringField(NameIndexField.RANK_ID.toString(), rank, Store.YES));
        }
        if (StringUtils.isNotEmpty(rankString)) {
            doc.add(new StringField(NameIndexField.RANK.toString(), rankString, Store.YES));
        }


        //handle the synonyms
        if (StringUtils.isNotEmpty(acceptedConcept)) {
            doc.add(new StringField(NameIndexField.ACCEPTED.toString(), acceptedConcept, Store.YES));
            doc.add(new TextField(NameIndexField.iS_SYNONYM.toString(), "T", Store.NO));
        } else {
            doc.add(new TextField(NameIndexField.iS_SYNONYM.toString(), "F", Store.NO));
        }

        //Add the classification information
        if (StringUtils.trimToNull(kingdom) != null) {
            doc.add(new TextField(RankType.KINGDOM.getRank(), kingdom, Store.YES));
            if (StringUtils.isNotBlank(kid)) {
                doc.add(new StoredField("kid", kid));
            }
        }
        if (StringUtils.trimToNull(phylum) != null) {
            doc.add(new TextField(RankType.PHYLUM.getRank(), phylum, Store.YES));
            if (StringUtils.isNotBlank(pid)) {
                doc.add(new StoredField("pid", pid));
            }
        }
        if (StringUtils.trimToNull(clazz) != null) {
            doc.add(new TextField(RankType.CLASS.getRank(), clazz, Store.YES));
            if (StringUtils.isNotBlank(cid)) {
                doc.add(new StoredField("cid", cid));
            }
        }
        if (StringUtils.trimToNull(order) != null) {
            doc.add(new TextField(RankType.ORDER.getRank(), order, Store.YES));
            if (StringUtils.isNotBlank(oid)) {
                doc.add(new StoredField("oid", oid));
            }
        }
        if (StringUtils.trimToNull(family) != null) {
            doc.add(new TextField(RankType.FAMILY.getRank(), family, Store.YES));
            if (StringUtils.isNotBlank(fid)) {
                doc.add(new StoredField("fid", fid));
            }
        }
        if (StringUtils.trimToNull(genus) != null) {
            doc.add(new TextField(RankType.GENUS.getRank(), genus, Store.YES));
            if (StringUtils.isNotBlank(gid)) {
                doc.add(new StoredField("gid", gid));
            }
        }
        if (StringUtils.trimToNull(species) != null) {
            doc.add(new TextField(RankType.SPECIES.getRank(), species, Store.YES));
            if (StringUtils.isNotBlank(sid)) {
                doc.add(new StoredField("sid", sid));
            }
        }
        if (StringUtils.trimToNull(left) != null) {
            doc.add(new StringField("left", left, Store.YES));
        }
        if (StringUtils.trimToNull(right) != null) {
            doc.add(new StringField("right", right, Store.YES));
        }


        //Add the author information
        if (StringUtils.isNotEmpty(author)) {
            //TODO think about whether we need to treat the author string with the taxamatch
            doc.add(new TextField(NameIndexField.AUTHOR.toString(), author, Store.YES));
        }


        //Generate the canonical
        //add the canonical form of the name
        try {
            ParsedName cn = parser.parse(name);
            //if(cn != null && !cn.hasProblem() && !cn.isIndetermined()){
            if (cn != null && cn.isParsableType() && !cn.isIndetermined()
                    && cn.getType() != NameType.informal && !"6500".equals(rank) && cn.getType() != NameType.doubtful)// a scientific name with some informal addition like "cf." or indetermined like Abies spec. ALSO prevent subgenus because they parse down to genus plus author
            {

                Field f2 = new TextField(NameIndexField.NAME.toString(), cn.canonicalName(), Store.YES);
                f2.setBoost(boost);
                doc.add(f2);
                if (specificEpithet == null && cn.isBinomial()) {
                    //check to see if we need to determine the epithets from the parse
                    genus = cn.getGenusOrAbove();
                    if (specificEpithet == null) specificEpithet = cn.getSpecificEpithet();
                    if (infraspecificEpithet == null) infraspecificEpithet = cn.getInfraSpecificEpithet();
                }
            }
            //check to see if the concept represents a phrase name
            if (cn instanceof ALAParsedName) {
                //set up the field type that is stored and Index.ANALYZED_NO_NORMS
                FieldType ft = new FieldType(TextField.TYPE_STORED);
                ft.setOmitNorms(true);
                ALAParsedName alapn = (ALAParsedName) cn;
                if ((!"sp.".equals(alapn.rank)) && alapn.specificEpithet != null) {
                    doc.add(new Field(NameIndexField.SPECIFIC.toString(), alapn.getSpecificEpithet(), ft));
                } else if ((!"sp.".equals(alapn.rank)) && alapn.specificEpithet == null) {
                    log.warn(lsid + " " + name + " has an empty specific for non sp. phrase");
                }
                if (StringUtils.trimToNull(alapn.getLocationPhraseDesciption()) != null) {
                    doc.add(new Field(NameIndexField.PHRASE.toString(), alapn.cleanPhrase, ft));
                }
                if (alapn.getPhraseVoucher() != null) {
                    doc.add(new Field(NameIndexField.VOUCHER.toString(), alapn.cleanVoucher, ft));
                }
                if (StringUtils.isBlank(genus) && StringUtils.isNotBlank(alapn.getGenusOrAbove())) {
                    //add the genus to the index as it is necessary to match on the phrase name.
                    doc.add(new TextField(RankType.GENUS.getRank(), alapn.getGenusOrAbove(), Store.YES));
                }

            }
        } catch (org.gbif.ecat.parser.UnparsableException e) {
            //check to see if the name is a virus in which case an extra name is added without the virus key word
            if (e.type == NameType.virus) {
                doc.add(new TextField(NameIndexField.NAME.toString(), ALANameSearcher.virusStopPattern.matcher(name).replaceAll(" "), Store.YES));
            }

        } catch (Exception e) {
            e.printStackTrace();
            //throw e;
        }


        //add the sound expressions for the name if required
        try {
            if (StringUtils.isNotBlank(genus)) {
                doc.add(new TextField(NameIndexField.GENUS_EX.toString(), TaxonNameSoundEx.treatWord(genus, "genus"), Store.YES));
            }
            if (StringUtils.isNotBlank(specificEpithet)) {
                doc.add(new TextField(NameIndexField.SPECIES_EX.toString(), TaxonNameSoundEx.treatWord(specificEpithet, "species"), Store.YES));
            } else if (StringUtils.isNotBlank(genus)) {
                doc.add(new TextField(NameIndexField.SPECIES_EX.toString(), "<null>", Store.YES));
            }
            if (StringUtils.isNotBlank(infraspecificEpithet)) {
                doc.add(new TextField(NameIndexField.INFRA_EX.toString(), TaxonNameSoundEx.treatWord(infraspecificEpithet, "species"), Store.YES));
            } else if (StringUtils.isNotBlank(specificEpithet)) {
                //make searching for an empty infraspecific soudex easier
                doc.add(new TextField(NameIndexField.INFRA_EX.toString(), "<null>", Store.YES));
            }
        } catch (Exception e) {
            log.warn(lsid + " " + name + " has issues creating a soundex: " + e.getMessage());
        }


        return doc;

    }

    private String getPhraseName(String specificEpithet, String infraspecificEpithet) {

        return null;
    }

    public String getIndexDirectory() {
        return indexDirectory;
    }

    public void setIndexDirectory(String indexDirectory) {
        this.indexDirectory = indexDirectory;
    }


    /**
     * Generates the Lucene index required for the name matching API.
     * eg
     * au.org.ala.names.search.ALANameIndexer "/data/exports" "/data/lucene/namematching"
     * Extra optional args that should appear after the directory names
     * -sn: Only create the indexes necessary for the scientific name lookups
     * -cn: Only create the indexes necessary for the common name lookups
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        ALANameIndexer indexer = new ALANameIndexer();
        indexer.init();
        for (String arg : args)
            System.out.println(arg);
        if (args.length >= 2) {
            boolean sn = true;
            boolean cn = true;
            if (args.length == 3) {
                sn = args[2].equals("-sn");
                cn = args[2].equals("-cn");
            }
            if (args.length == 5) {
                //the file names have been supplied to the generated.  Used in the case where we are not generating the ALA names index.
                indexer.createIndex(args[0], args[1], args[2], args[3], args[4], sn, cn);
            } else {
                indexer.createIndex(args[0], args[1], sn, cn);
            }
        } else {
            System.out.println("au.org.ala.names.search.ALANameIndexer <directory with export files> <directory in which to create indexes> [<accepted name file>] [<synonym name file>][-cn OR -sn]");

        }
    }
}
