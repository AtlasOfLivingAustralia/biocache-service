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
        try{
            logger.debug("Starting the endemic callable for " +batch);
            List<FieldResultDTO> results = searchDAO.getValuesForFacet(srp);
            logger.debug("Finished endemic callable for " + batch + " ("+ results.size()+ ")");
            return results;
        }
        catch(Exception e){
            logger.error("Unable to get facets for the endemic call " , e);
            return null;
        }
    }
}
