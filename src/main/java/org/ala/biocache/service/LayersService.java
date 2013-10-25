package org.ala.biocache.service;
/**
 * Provides access to the layers metadata information that could be of use elsewhere 
 * @author Natasha Carter (natasha.carter@csiro.au)
 *
 */
public interface LayersService {
    public java.util.Map<String,String> getLayerNameMap();
}
