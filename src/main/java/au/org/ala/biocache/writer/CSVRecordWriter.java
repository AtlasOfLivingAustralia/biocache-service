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
package au.org.ala.biocache.writer;

import au.com.bytecode.opencsv.CSVWriter;
import au.org.ala.biocache.stream.OptionalZipOutputStream;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 
 * A Writer that outputs a record in CSV format
 * 
 * @author Natasha Carter
 */
public class CSVRecordWriter implements RecordWriterError {
    private final static Logger logger = LoggerFactory.getLogger(CSVRecordWriter.class);

    private final OutputStream outputStream;
    private final char separatorChar;
    private final char quoteChar;
    private final char escapeChar;
    
    private final AtomicBoolean initialised = new AtomicBoolean(false);
    private final AtomicBoolean finalised = new AtomicBoolean(false);
    private final AtomicBoolean finalisedComplete = new AtomicBoolean(false);

    private final String[] header;

    private final List<Throwable> errors = new ArrayList<>();
    
    // Resources that are created during initialise because their creation sequence may include Exception's
    private CSVWriter csvWriter;
    
    public CSVRecordWriter(OutputStream out, String[] header){
        outputStream = out;
        separatorChar = ',';
        quoteChar = '"';
        escapeChar = CSVWriter.DEFAULT_ESCAPE_CHARACTER;
        this.header = header;
    }

    public CSVRecordWriter(OutputStream out, String[] header, char sep, char esc){
        outputStream = out;
        separatorChar = sep;
        quoteChar = '"';
        escapeChar = esc;
        this.header = header;
    }
    
    /**
     * Writes the supplied record to output stream  
     */
    @Override
    public void write(String[] record) {
        if (!initialised.get()) {
            throw new IllegalStateException("Must call initialise method before calling write.");
        }
        if (csvWriter == null) {
            throw new IllegalStateException("The initialise method did not create a CSVWriter instance.");
        }
        csvWriter.writeNext(record);

        //mark the end of line
        if (outputStream instanceof OptionalZipOutputStream) {
            try {
                // add record byte length, standard separator byte length and buffer (*2) for occasional record character encoding
                long length = record.length * "\",\"".getBytes(StandardCharsets.UTF_8).length * 2;
                for (String s : record) if (s != null) length += s.getBytes(StandardCharsets.UTF_8).length;
                if (((OptionalZipOutputStream) outputStream).isNewFile(this, length)) {
                    write(header);
                }
            } catch (Exception e) {
                errors.add(e);
            }
        }
    }

    @Override
    public boolean hasError() {
        CSVWriter toCheckCsvWriter = csvWriter;
        return (toCheckCsvWriter != null && toCheckCsvWriter.checkError()) || !errors.isEmpty();
    }

    @Override
    public List<Throwable> getErrors() {
        return errors;
    }

    @Override
    public void flush() {
        try {
            CSVWriter toFlushCsvWriter = csvWriter;
            if(toFlushCsvWriter != null) {
                toFlushCsvWriter.flush();
            }
        } catch(java.io.IOException e){
            logger.debug(e.getMessage(), e);
            errors.add(e);
        }
    }

    @Override
    public void initialise() {
        if (initialised.compareAndSet(false, true)) {
            csvWriter = new CSVWriter(new OutputStreamWriter(new CloseShieldOutputStream(outputStream), StandardCharsets.UTF_8), separatorChar, quoteChar, escapeChar);
            csvWriter.writeNext(header);
        }
    }
    
    @Override
    public void finalise() {
        if (finalised.compareAndSet(false, true)) {
            try {
                flush();
            } finally {
                try {
                    CSVWriter toCloseCsvWriter = csvWriter;
                    if(toCloseCsvWriter != null) {
                        toCloseCsvWriter.close();
                    }
                } catch (IOException e) {
                    errors.add(e);
                } finally {
                    finalisedComplete.set(true);
                }
            }
        }
    }

    @Override
    public boolean finalised() {
        return finalisedComplete.get();
    }

    @Override
    public void close() throws IOException {
        finalise();
    }

}
