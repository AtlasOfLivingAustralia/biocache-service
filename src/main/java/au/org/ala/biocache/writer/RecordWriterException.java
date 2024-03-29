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

import au.org.ala.biocache.dto.DownloadDetailsDTO;
import au.org.ala.biocache.dto.DownloadRequestDTO;
import org.apache.log4j.Logger;

import java.io.IOException;

public class RecordWriterException extends IOException {
    private final static Logger logger = Logger.getLogger(RecordWriterException.class);

    public RecordWriterException() {
        super("RecordWriter Exception");
    }

    public RecordWriterException(String msg) {
        super("RecordWriter Exception: " + msg);
    }

    public RecordWriterException(Throwable cause) {
        super("RecordWriter Exception", cause);
    }

    public RecordWriterException(String msg, Throwable cause) {
        super("RecordWriter Exception: " + msg, cause);
    }
    
    public static RecordWriterException newRecordWriterException(DownloadDetailsDTO dd, DownloadRequestDTO downloadParams, boolean solr, RecordWriterError writer) {
        String msg = "";
        if (dd != null) {
            if (dd.getFileLocation() != null) {
                msg += "Offline request: " + dd.getFileLocation();
                logger.error("msg");
            } else {
                msg += "Online " + (solr?"SOLR":"Cassandra") + " download request: " + downloadParams.toString() + ", " + dd.getIpAddress();
            }
        } else if (downloadParams != null) {
            msg += "Online " + (solr?"SOLR":"Cassandra") + "  download request: " + downloadParams.toString();
        }
        return new RecordWriterException(msg, writer == null || writer.getErrors().isEmpty() ? null : writer.getErrors().get(0));
    }
}
