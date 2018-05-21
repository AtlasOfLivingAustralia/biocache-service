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

import au.org.ala.biocache.stream.OptionalZipOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
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
public class TSVRecordWriter implements RecordWriterError {
    private final static Logger logger = LoggerFactory.getLogger(TSVRecordWriter.class);

    private final OutputStream outputStream;
    private final String[] header;

    private final AtomicBoolean initialised = new AtomicBoolean(false);
    private final AtomicBoolean finalised = new AtomicBoolean(false);
    private final AtomicBoolean finalisedComplete = new AtomicBoolean(false);

    private final AtomicBoolean writerError = new AtomicBoolean(false);

    private final List<Throwable> errors = new ArrayList<>();
    
    public TSVRecordWriter(OutputStream out, String[] header){
        this.outputStream = out;
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
        StringBuilder line = new StringBuilder(256);

        //assume correct column count
        for (int i = 0; i < record.length; i++) {
            if (i > 0) line.append('\t');
            line.append(record[i].replace("\r","").replace("\n","").replace("\t",""));
        }

        line.append("\n");

        try {
            String str = line.toString();
            byte [] bytes = str.getBytes(StandardCharsets.UTF_8);
            outputStream.write(bytes);

            //mark the end of line
            if (outputStream instanceof OptionalZipOutputStream) {
                if (((OptionalZipOutputStream) outputStream).isNewFile(this, bytes.length)) {
                    write(header);
                }
            }
        } catch (java.io.IOException e) {
            logger.error("Found error writing to TSV file", e);
            errors.add(e);
            writerError.set(true);
        }
    }

	@Override
	public void initialise() {
		if (initialised.compareAndSet(false, true)) {
			write(header);
		}
	}

    @Override
    public void finalise() {
        if(finalised.compareAndSet(false, true)) {
            try {
                flush();
            } finally {
                finalisedComplete.set(true);
            }
        }
    }

    @Override
    public boolean finalised() {
        return finalisedComplete.get();
    }

    @Override
    public boolean hasError() {
        return writerError.get();
    }

    @Override
    public List<Throwable> getErrors() {
        return errors;
    }

    @Override
    public void flush() {
        try {
            outputStream.flush();
        } catch(java.io.IOException e) {
            errors.add(e);
            writerError.set(true);
        }
    }

    @Override
	public void close() throws IOException {
		finalise();
	}

}
