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

import au.org.ala.biocache.util.RecordWriter;

import java.io.Closeable;
import java.util.List;

/**
 * Method for catching RecordWriter errors
 */
public interface RecordWriterError extends RecordWriter, Closeable {

    /**
     * @return true when there is a write error
     */
    boolean hasError();

    /**
     * @return A list of the errors found or an empty list if there were no errors.
     */
    List<Throwable> getErrors();

    void flush();
}
