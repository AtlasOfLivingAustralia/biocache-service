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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 
 * A Writer that outputs a record in CSV format
 * 
 * @author Natasha Carter
 */
public class CSVRecordWriter implements RecordWriterError {
    private final static Logger logger = LoggerFactory.getLogger(CSVRecordWriter.class);

    private CSVWriter csvWriter;
    private OutputStream outputStream;

    private final AtomicBoolean finalised = new AtomicBoolean(false);
    private final AtomicBoolean finalisedComplete = new AtomicBoolean(false);

    private String[] header;
    
    public CSVRecordWriter(OutputStream out, String[] header){
        outputStream = out;
        csvWriter = new CSVWriter(new OutputStreamWriter(out, Charset.forName("UTF-8")), ',', '"');  
        csvWriter.writeNext(header);
        this.header = header;
    }

    public CSVRecordWriter(OutputStream out, String[] header, char sep, char esc){
        outputStream = out;
        csvWriter = new CSVWriter(new OutputStreamWriter(out, Charset.forName("UTF-8")), sep, '"', esc);
        csvWriter.writeNext(header);
        this.header = header;
    }
    
    /**
     * Writes the supplied record to output stream  
     */
    @Override
    public void write(String[] record) {
       csvWriter.writeNext(record);

       //mark the end of line
       if (outputStream instanceof OptionalZipOutputStream) {
           try {
               long length = 0;
               for (String s : record) if (s != null) length += s.getBytes("UTF-8").length;
               if (((OptionalZipOutputStream) outputStream).isNewFile(csvWriter, length)) {
                   write(header);
               }
           } catch (Exception e) {
               //ignore
           }
       }
    }

    @Override
    public boolean hasError() {
        return csvWriter.checkError();
    }

    @Override
    public void flush() {
        try {
            csvWriter.flush();
        } catch(java.io.IOException e){
            logger.debug(e.getMessage(), e);
        }
    }

    @Override
    public void finalise() {
        if (finalised.compareAndSet(false, true)) {
            flush();
            finalisedComplete.set(true);
        }
    }

    @Override
    public boolean finalised() {
        return finalisedComplete.get();
    }
}
