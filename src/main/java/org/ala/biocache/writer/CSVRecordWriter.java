package org.ala.biocache.writer;

import java.io.OutputStream;
import java.io.OutputStreamWriter;

import au.com.bytecode.opencsv.CSVWriter;
import au.org.ala.biocache.RecordWriter;
/**
 * 
 * A Writer that outputs a record in CSV format
 * 
 * @author Natasha Carter
 */
public class CSVRecordWriter implements RecordWriter{
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
        try{
            csvWriter.flush();            
        }
        catch(java.io.IOException e){
            
        }
    }
}
