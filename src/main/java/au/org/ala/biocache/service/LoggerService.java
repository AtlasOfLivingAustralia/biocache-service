/**************************************************************************
 *  Copyright (C) 2012 Atlas of Living Australia
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

import java.util.List;
import java.util.Map;

/**
 * Service for ALA logger
 *
 * NC 20130913: copied across from hubs-webapp so that we can cache log items on the server.
 *
 * @author Nick dos Remedios (nick.dosremedios@csiro.au)
 */
public interface LoggerService {
    public List<Map<String,Object>> getReasons();
    public List<Map<String,Object>> getSources();
    public List<Integer> getReasonIds();
    public List<Integer> getSourceIds();
}
