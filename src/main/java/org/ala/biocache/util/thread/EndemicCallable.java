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
package org.ala.biocache.util.thread;

import java.util.concurrent.Callable;
import java.util.List;

import org.ala.biocache.dao.SearchDAO;
import org.ala.biocache.dto.FieldResultDTO;
import org.ala.biocache.dto.SpatialSearchRequestParams;
import org.apache.log4j.Logger;

/**
 * The class essentially wraps the extraction of a set of endemic facets into 
 * a thread.  In order to support a return value from the thread we are making use of
 * the Callable interface.  Which will return a Future<List<FieldResultDTO>>.
 * 
 * @author "Natasha Carter <Natasha.Carter@csiro.au>"
 */
public class EndemicCallable implements Callable<List<FieldResultDTO>> {

    private static final Logger logger = Logger.getLogger(EndemicCallable.class);
    private SpatialSearchRequestParams srp;
    private int batch;
    private SearchDAO searchDAO;

    public EndemicCallable(SpatialSearchRequestParams requestParams, int batch, SearchDAO searchDAO){
        this.srp = requestParams;
        this.batch = batch;
        this.searchDAO = searchDAO;
    }
  
    @Override
    public List<FieldResultDTO> call() {
        try {
            logger.debug("Starting the endemic callable for " +batch);
            List<FieldResultDTO> results = searchDAO.getValuesForFacet(srp);
            logger.debug("Finished endemic callable for " + batch + " ("+ results.size()+ ")");
            return results;
        } catch(Exception e) {
            logger.error("Unable to get facets for the endemic call " , e);
            return null;
        }
    }
}
