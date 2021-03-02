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
package au.org.ala.biocache.service;

import java.io.Reader;

/**
 * Provides access to the layers metadata information that could be of use elsewhere.
 *
 * @author Natasha Carter (natasha.carter@csiro.au)
 */
public interface LayersService {

    /**
     * Retrieve a map of layers
     * @return
     */
    java.util.Map<String,String> getLayerNameMap();

    /**
     * Retrieve a layer name with the supplied code.
     * @param code
     * @return
     */
    String getName(String code);

    String findAnalysisLayerName(String analysisLayer, String layersServiceUrl);

    Integer getDistributionsCount(String lsid);

    Integer getChecklistsCount(String lsid);

    Integer getTracksCount(String lsid);

    void refreshCache();

    String getLayersServiceUrl();

    Reader sample(String[] analysisLayers, double[][] points, Object o);
}
