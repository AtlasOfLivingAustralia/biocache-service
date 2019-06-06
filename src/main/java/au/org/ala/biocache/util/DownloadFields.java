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

import au.org.ala.biocache.Config;
import au.org.ala.biocache.dto.IndexFieldDTO;
import au.org.ala.biocache.service.LayersService;
import au.org.ala.biocache.service.ListsService;
import au.org.ala.biocache.service.RestartDataService;
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
    
    private Properties layerProperties = RestartDataService.get(this, "layerProperties", new TypeReference<Properties>(){}, Properties.class);

    private Map<String, IndexFieldDTO> indexFieldMaps;

    private Map<String, IndexFieldDTO> indexByDwcMaps;

    public DownloadFields(Set<IndexFieldDTO> indexFields, AbstractMessageSource messageSource, LayersService layersService, ListsService listsService) {
        this.messageSource = messageSource;

        this.layersService = layersService;

        this.listsService = listsService;
        
        update(indexFields);
    }

    private Long lastUpdate = 0L;
    private Thread updateThread = null;
    private synchronized void updateLayerNames() {
        //update hourly
        if (layerProperties == null || layerProperties.size() == 0 || System.currentTimeMillis() > lastUpdate + 3600*1000) {
            //close any running update threads
            if (updateThread != null && updateThread.isAlive()) {
                updateThread.interrupt();
            }
            lastUpdate = System.currentTimeMillis();
            updateThread = new Thread() {
                public void run() {
                    Properties newDownloadProperties = new Properties();
                    try {
                        if(StringUtils.isNotEmpty(Config.layersServiceUrl())) {
                            Map<String, String> fields = new LayersStore(Config.layersServiceUrl()).getFieldIdsAndDisplayNames();
                            for (String fieldId : fields.keySet()) {
                                newDownloadProperties.put(fieldId, fields.get(fieldId));
                            }

                            //something might have gone wrong if empty
                            if (newDownloadProperties.size() > 0) {
                                layerProperties = newDownloadProperties;
                            }
                        }
                    } catch (Exception e) {
                        logger.error("failed to update layer names from url: " + Config.layersServiceUrl() + " " + e.getMessage(), e);
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
    
    /**
     * Gets the header for the file
     * @param values
     * @return
     */
    public String[] getHeader(String[] values, boolean useSuffix, boolean dwcHeaders){
        updateLayerNames();

        String[] header = new String[values.length];
        for(int i =0; i < values.length; i++){
            //attempt to get the headervalue from the properties
            //only dwcHeader lookup is permitted when dwcHeaders == true
            String v = dwcHeaders ? values[i] : layerProperties.getProperty(values[i], messageSource.getMessage(values[i], null, generateTitle(values[i], useSuffix), Locale.getDefault()));
            String dwc = dwcHeaders ? messageSource.getMessage("dwc." + values[i], null, "", Locale.getDefault()) : null;
            header[i] = dwc != null && dwc.length() > 0 ? dwc : v;
        }
        return header;
    }

    /**
     * Generates a default title for a field that does NOT have an i18n
     * @param value
     * @return
     */
    private String generateTitle(String value, boolean useSuffix){
        String suffix = "";
        if(value.endsWith("_p")){
            suffix = " - Processed";
            value = value.replaceAll("_p", "");
        }
        value = StringUtils.join(StringUtils.splitByCharacterTypeCamelCase(value), " ");
        if(useSuffix) {
            value += suffix;
        }
        return value;
    }

    /**
     * This is a backwards compatibility field mapping, for external services using old API and old index field names.
     *
     * @param fieldName
     * @return
     */
    public String cleanRequestFieldName(String fieldName){
        if(fieldName != null && (fieldName.endsWith(".p") || fieldName.endsWith("_p"))){
            return fieldName.substring(0, fieldName.length() - 2);
        }
        return fieldName;
    }

    /**
     * Returns the index fields that are used for the supplied fieldNames.
     *
     * @param fieldNames
     * @return
     */
    public List<String>[] getIndexFields(String[] fieldNames, boolean dwcHeaders, String layersServiceUrl){
        updateLayerNames();

        java.util.List<String> mappedNames = new java.util.LinkedList<String>();
        java.util.List<String> headers = new java.util.LinkedList<String>();
        java.util.List<String> unmappedNames = new java.util.LinkedList<String>();
        java.util.List<String> originalName = new java.util.LinkedList<String>();
        java.util.List<String> analysisHeaders = new java.util.LinkedList<String>();
        java.util.List<String> analysisLayers = new java.util.LinkedList<String>();
        java.util.List<String> listHeaders = new java.util.LinkedList<String>();
        java.util.List<String> listFields = new java.util.LinkedList<String>();

        for(String fieldName : fieldNames){

            String indexName = cleanRequestFieldName(fieldName);

            //now check to see if this index field is stored
            IndexFieldDTO field = indexFieldMaps.get(indexName);
            if (field == null){
                field = indexByDwcMaps.get(indexName);
            }

            if ((field != null && (field.isStored() || field.isDocvalue())) || fieldName.startsWith("sensitive")) {

                String fieldNameToUse = field != null ? field.getName() : fieldName;

                mappedNames.add(fieldNameToUse);
                //only dwcHeader lookup is permitted when dwcHeaders == true or it is a cl or el field
                String header = dwcHeaders && field != null && (field.isStored() || field.isDocvalue()) && !isSpatialField(field.getName()) ?
                        field.getName() :
                        layerProperties.getProperty(
                                fieldNameToUse,
                                messageSource.getMessage(fieldNameToUse, null, generateTitle(fieldNameToUse, true),
                                        Locale.getDefault())
                        );

                String dwcHeader = dwcHeaders ? messageSource.getMessage("dwc." + fieldNameToUse, null, "", Locale.getDefault()) : null;

                headers.add(dwcHeader != null && dwcHeader.length() > 0 ? dwcHeader : header);

                originalName.add(fieldNameToUse);

            } else if (field == null && layersService.findAnalysisLayerName(indexName, layersServiceUrl) != null) {
                // indexName is a valid layer at layersServiceUrl
                analysisLayers.add(indexName);
                analysisHeaders.add(layersService.findAnalysisLayerName(indexName, layersServiceUrl));
            } else if (field == null && listsService.getKvp(indexName) != null) {
                // indexName is a valid species list at the listsService
                listHeaders.addAll(listsService.getKvpNames(indexName, listsService.getKvp(indexName)));
                listFields.addAll(listsService.getKvpFields(indexName, listsService.getKvp(indexName)));
            } else {
                unmappedNames.add(indexName);
            }
        }
        return new List[]{mappedNames, unmappedNames, headers, originalName, analysisHeaders, analysisLayers, listHeaders, listFields};
    }

    private boolean isSpatialField(String name) {
        return name.matches("((cl)|(el))[0-9]+");
    }

    public void update(Set<IndexFieldDTO> indexedFields) {
        //initialise the properties
        try {
            Map<String, IndexFieldDTO> map = new TreeMap<String,IndexFieldDTO>();
            Map<String, IndexFieldDTO> mapByDwC = new TreeMap<String,IndexFieldDTO>();
            for(IndexFieldDTO field: indexedFields){
                map.put(field.getName(), field);
                if(field.getDwcTerm() != null) {
                    mapByDwC.put(field.getDwcTerm(), field);
                }
            }
            indexFieldMaps = map;
            indexByDwcMaps = mapByDwC;

            updateLayerNames();
        } catch(Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
