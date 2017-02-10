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
import au.org.ala.biocache.Store;
import au.org.ala.biocache.dto.IndexFieldDTO;
import au.org.ala.biocache.service.LayersService;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractMessageSource;

import java.util.*;

/**
 * Stores the download fields whose values can be overridden in
 * a properties file.  Sourced from layers-service and message.properties
 *
 * @author "Natasha Carter <Natasha.Carter@csiro.au>"
 */
public class DownloadFields {
	
    private final static Logger logger = LoggerFactory.getLogger(DownloadFields.class);

    private AbstractMessageSource messageSource;

    private LayersService layersService;
    
    private Properties layerProperties = new Properties();
    private Map<String,IndexFieldDTO> indexFieldMaps;

    public DownloadFields(Set<IndexFieldDTO> indexFields, AbstractMessageSource messageSource, LayersService layersService){
        this.messageSource = messageSource;

        this.layersService = layersService;
        
        //initialise the properties
        try {
            indexFieldMaps = new TreeMap<String,IndexFieldDTO>();
            for(IndexFieldDTO field: indexFields){
                indexFieldMaps.put(field.getName(), field);
            }

            updateLayerNames();
        } catch(Exception e) {
        	logger.error(e.getMessage(), e);
        }
    }

    private void updateLayerNames() {
        //avoid a delay here
        Thread t = new Thread() {
            public void run() {
                Properties newDownloadProperties = new Properties();

                try {
                    Map<String, String> fields = new LayersStore(Config.layersServiceUrl()).getFieldIdsAndDisplayNames();
                    for (String fieldId : fields.keySet()) {
                        newDownloadProperties.put(fieldId, fields.get(fieldId));
                    }

                    //something might have gone wrong if empty
                    if (newDownloadProperties.size() > 0) {
                        layerProperties = newDownloadProperties;
                    }
                } catch (Exception e) {
                    logger.error("failed to update layer names from url: " + Config.layersServiceUrl(), e);
                }
            }
        };

        if (layerProperties == null || layerProperties.size() == 0) {
            //wait
            t.run();
        } else {
            //do not wait 
            t.start();
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
        if(value.endsWith(".p")){
            suffix = " - Processed";
            value = value.replaceAll("\\.p", "");
        }
        value = StringUtils.join(StringUtils.splitByCharacterTypeCamelCase(value), " ");
        if(useSuffix) {
            value += suffix;
        }
        return value;
    }

    /**
     * Returns the index fields that are used for the supplied values.
     *
     * @param values
     * @return
     */
    public List<String>[] getIndexFields(String[] values, boolean dwcHeaders, String layersServiceUrl){
        updateLayerNames();

        java.util.List<String> mappedNames = new java.util.LinkedList<String>();
        java.util.List<String> headers = new java.util.LinkedList<String>();
        java.util.List<String> unmappedNames = new java.util.LinkedList<String>();
        java.util.List<String> originalName = new java.util.LinkedList<String>();
        java.util.List<String> analysisHeaders = new java.util.LinkedList<String>();
        java.util.List<String> analysisLayers = new java.util.LinkedList<String>();
        java.util.Map<String, String> storageFieldMap = Store.getStorageFieldMap();
        for(String value : values){
            //check to see if it is the the
            String indexName = storageFieldMap.containsKey(value) ? storageFieldMap.get(value) : value;
            //now check to see if this index field is stored
            IndexFieldDTO field = indexFieldMaps.get(indexName);
            if((field != null && field.isStored()) || value.startsWith("sensitive")) {
                mappedNames.add(indexName);
                //only dwcHeader lookup is permitted when dwcHeaders == true or it is a cl or el field
                String v = dwcHeaders && !isSpatialField(field.getName()) ? value : layerProperties.getProperty(value, messageSource.getMessage(value, null, generateTitle(value, true), Locale.getDefault()));
                String dwc = dwcHeaders ? messageSource.getMessage("dwc." + value, null, "", Locale.getDefault()) : null;
                headers.add(dwc != null && dwc.length() > 0 ? dwc : v);
                originalName.add(value);
            } else if (layersService.findAnalysisLayerName(value, layersServiceUrl) != null) {
                analysisLayers.add(value);
                analysisHeaders.add(layersService.findAnalysisLayerName(value, layersServiceUrl));
            } else {
                unmappedNames.add(indexName);
            }
        }
        return new List[]{mappedNames,unmappedNames,headers,originalName, analysisHeaders, analysisLayers};
    }

    private boolean isSpatialField(String name) {
        return name.matches("((cl)|(el))[0-9]+");
    }
}
