package org.ala.biocache.writer;

import java.io.OutputStream;
import java.io.OutputStreamWriter;

import au.com.bytecode.opencsv.CSVWriter;
import au.org.ala.biocache.RecordWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * A Writer that outputs a record in CSV format
 * 
 * @author Natasha Carter
 */
public class CSVRecordWriter implements RecordWriter{
    private final static Logger logger = LoggerFactory.getLogger(CSVRecordWriter.class);

    private CSVWriter csvWriter;

    public CSVRecordWriter(OutputStream out, String[] header){
        csvWriter = new CSVWriter(new OutputStreamWriter(out), ',', '"');  
        csvWriter.writeNext(header);
    }
    
    /**
     * Writes the supplied record to output stream  
     */
    @Override
    public void write(String[] record) {
       csvWriter.writeNext(record);       
    }

    @Override
    public void finalise() {
        try {
            csvWriter.flush();            
        } catch(java.io.IOException e){
            logger.debug(e.getMessage(), e);
        }
    }
}
