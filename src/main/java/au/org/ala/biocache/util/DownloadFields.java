/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
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
package au.org.ala.biocache.util;

import au.org.ala.biocache.dto.DownloadHeaders;
import au.org.ala.biocache.dto.DownloadRequestDTO;
import au.org.ala.biocache.dto.IndexFieldDTO;
import au.org.ala.biocache.service.LayersService;
import au.org.ala.biocache.service.ListsService;
import au.org.ala.biocache.service.RestartDataService;
import au.org.ala.biocache.util.solr.FieldMappingUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractMessageSource;

import java.util.*;

/**
 * Stores the download fields whose fieldNames can be overridden in
 * a properties file.  Sourced from layers-service and message.properties
 *
 * @author "Natasha Carter <Natasha.Carter@csiro.au>"
 */
public class DownloadFields {

    private final static Logger logger = LoggerFactory.getLogger(DownloadFields.class);

    private AbstractMessageSource messageSource;

    private LayersService layersService;

    private ListsService listsService;

    private Properties layerProperties = RestartDataService.get(this, "layerProperties", new TypeReference<Properties>() {
    }, Properties.class);

    private Map<String, IndexFieldDTO> indexFieldMaps;

    private Map<String, IndexFieldDTO> indexByDwcMaps;

    protected FieldMappingUtil fieldMappingUtil;

    public DownloadFields(FieldMappingUtil fieldMappingUtil, Set<IndexFieldDTO> indexFields, AbstractMessageSource messageSource, LayersService layersService, ListsService listsService) {

        this.fieldMappingUtil = fieldMappingUtil;

        this.messageSource = messageSource;

        this.layersService = layersService;

        this.listsService = listsService;

        update(indexFields);
    }

    private Long lastUpdate = 0L;
    private Thread updateThread = null;

    private synchronized void updateLayerNames() {
        //update hourly
        if (layerProperties == null || layerProperties.size() == 0 || System.currentTimeMillis() > lastUpdate + 3600 * 1000) {
            //close any running update threads
            if (updateThread != null && updateThread.isAlive()) {
                updateThread.interrupt();
            }
            lastUpdate = System.currentTimeMillis();
            updateThread =
                    new Thread() {
                        public void run() {
                            Properties newDownloadProperties = new Properties();
                            try {
                                if (StringUtils.isNotEmpty(layersService.getLayersServiceUrl())) {
                                    newDownloadProperties.putAll(layersService.getLayerNameMap());

                                    // something might have gone wrong if empty
                                    if (newDownloadProperties.size() > 0) {
                                        layerProperties = newDownloadProperties;
                                    }
                                }
                            } catch (Exception e) {
                                logger.error("failed to update layer names", e);
                            }
                        }
                    };
            if (layerProperties == null || layerProperties.size() == 0) {
                //wait
                updateThread.run();
            } else {
                //do not wait
                updateThread.start();
            }
        }
    }

    public void update(Set<IndexFieldDTO> indexedFields) {
        //initialise the properties
        try {
            Map<String, IndexFieldDTO> map = new TreeMap<String, IndexFieldDTO>();
            Map<String, IndexFieldDTO> mapByDwC = new TreeMap<String, IndexFieldDTO>();
            for (IndexFieldDTO field : indexedFields) {
                map.put(field.getName(), field);
                if (field.getDwcTerm() != null) {
                    mapByDwC.put(field.getDwcTerm(), field);
                }
            }
            indexFieldMaps = map;
            indexByDwcMaps = mapByDwC;

            updateLayerNames();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public DownloadHeaders newDownloadHeader(DownloadRequestDTO downloadParams) {
        updateLayerNames();

        boolean dwcHeaders = downloadParams.getDwcHeaders();
        String layersServiceUrl = downloadParams.getLayersServiceUrl();

        java.util.List<String> included = new java.util.LinkedList<String>();
        java.util.List<String> labels = new java.util.LinkedList<String>();
        java.util.List<String> originalName = new java.util.LinkedList<String>();
        java.util.List<String> analysisLabels = new java.util.LinkedList<String>();
        java.util.List<String> analysisIds = new java.util.LinkedList<String>();
        java.util.List<String> speciesListLabels = new java.util.LinkedList<String>();
        java.util.List<String> speciesListIds = new java.util.LinkedList<String>();

        String[] fieldNames = downloadParams.getFields().split(",");

        for (String fieldName : fieldNames) {
            // legacy name translation
            String solrName = fieldMappingUtil.translateFieldName(fieldName);

            // find in index/fields
            // find the field in the SOLR schema.
            IndexFieldDTO field = indexFieldMaps.get(solrName);
            if (field == null) {
                // find the field in the SOLR schema using the DwC name.
                field = indexByDwcMaps.get(solrName);
            }

            // only include fields when stored=true or docvalues=true
            // sensitive* fields will not be found and will always be stored=true or docvalues=true
            if ((field != null && field.isDocvalue()) || fieldName.startsWith("sensitive")) {
                // SOLR field name to use
                String name = field != null ? field.getName() : solrName;
                included.add(name);

                //attempt to get the headervalue from the properties
                //only dwcHeader lookup is permitted when dwcHeaders == true

                String v = dwcHeaders ? name : layerProperties.getProperty(name, messageSource.getMessage(name, null, name, Locale.getDefault()));
                String dwc = dwcHeaders ? messageSource.getMessage("dwc." + name, null, "", Locale.getDefault()) : null;
                String header = dwc != null && dwc.length() > 0 ? dwc : v;
                labels.add(header);

                // keep track of the original field name requested
                originalName.add(fieldName);
            } else if (field == null && layersServiceUrl != null && layersService.findAnalysisLayerName(solrName, layersServiceUrl) != null) {
                // indexName is a valid layer at layersServiceUrl
                analysisIds.add(solrName);
                analysisLabels.add(layersService.findAnalysisLayerName(solrName, layersServiceUrl));
            } else if (field == null && listsService.getKvp(solrName) != null) {
                // indexName is a valid species list at the listsService
                speciesListLabels.addAll(listsService.getKvpNames(solrName, listsService.getKvp(solrName)));
                speciesListIds.addAll(listsService.getKvpFields(solrName, listsService.getKvp(solrName)));
            }
        }
        // apply custom header
        // Each pair of CSV values in customHeader are: {requestedField,customHeader}
        String[] customHeader = downloadParams.getCustomHeader().split(",");
        for (int i = 0; i + 1 < customHeader.length; i += 2) {
            for (int j = 0; j < included.size(); j++) {
                if (customHeader[i].equals(included.get(j)) || customHeader[i].equals(originalName.get(j))) {
                    labels.set(j, customHeader[i + 1]);
                }
            }
            for (int j = 0; j < analysisIds.size(); j++) {
                if (customHeader[i].equals(analysisIds.get(j))) {
                    analysisLabels.set(j, customHeader[i + 1]);
                }
            }
        }

        return new DownloadHeaders(included.toArray(new String[0]),
                labels.toArray(new String[0]),
                analysisLabels.toArray(new String[0]),
                analysisIds.toArray(new String[0]),
                speciesListLabels.toArray(new String[0]),
                speciesListIds.toArray(new String[0]));
    }
}
