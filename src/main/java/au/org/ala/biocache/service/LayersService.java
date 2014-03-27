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
    public java.util.Map<String,String> getLayerNameMap();

    /**
     * Retrieve a layer name with the supplied code.
     * @param code
     * @return
     */
    public String getName(String code);
}
