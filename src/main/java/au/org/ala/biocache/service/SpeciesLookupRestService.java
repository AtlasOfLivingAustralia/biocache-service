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

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of SpeciesLookupService.java that calls the bie-service application
 * via JSON REST web services.
 * 
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
@Component("speciesLookupService")
public class SpeciesLookupRestService implements SpeciesLookupService {
    
    private final static Logger logger = Logger.getLogger(SpeciesLookupRestService.class);
	
    @Inject
    @Qualifier("restTemplate")
    private RestOperations restTemplate; // NB MappingJacksonHttpMessageConverter() injected by Spring

    /** URI prefix for bie-service - may be overridden in properties file */
    @Value("${service.bie.ws.url:http://bie.ala.org.au/ws}")
    protected String bieUriPrefix;

    //NC 20131018: Allow service to be disabled via config (enabled by default)
    @Value("${service.bie.enabled:false}")
    protected Boolean enabled;

    @Inject
    private AbstractMessageSource messageSource; // use for i18n of the headers

    private String[] baseHeader;
    private String[] countBaseHeader;
    private String[] synonymHeader;
    private String[] countSynonymHeader;

    /**
     * @see SpeciesLookupService#getGuidForName(String)
     * 
     * @param name
     * @return guid
     */
    @Override
    public String getGuidForName(String name) {
        String guid = null;
        if(enabled){
            
            try {
                final String jsonUri = bieUriPrefix + "/guid/" + name;
                logger.info("Requesting: " + jsonUri);
                List<Object> jsonList = restTemplate.getForObject(jsonUri, List.class);
                
                if (!jsonList.isEmpty()) {
                    Map<String, String> jsonMap = (Map<String, String>) jsonList.get(0);
                    if (jsonMap.containsKey("acceptedIdentifier")) {
                        guid = jsonMap.get("acceptedIdentifier");
                    }
                }
            } catch (Exception ex) {
                logger.error("RestTemplate error: " + ex.getMessage(), ex);
            }
        }
        
        return guid;
    }

    /**
     * Lookup the accepted name for a GUID
     *
     * @return
     */
    @Override
    public String getAcceptedNameForGuid(String guid) {
        String acceptedName = "";
        if(enabled){
            try {
                final String jsonUri = bieUriPrefix + "/species/shortProfile/" + guid + ".json";
                logger.info("Requesting: " + jsonUri);
                Map<String, String> jsonMap = restTemplate.getForObject(jsonUri, Map.class);
    
                if (jsonMap.containsKey("scientificName")) {
                    acceptedName = jsonMap.get("scientificName");
                }
    
            } catch (Exception ex) {
                logger.error("RestTemplate error: " + ex.getMessage(), ex);
            }
        }

        return acceptedName;
    }
    
    
    /**
     *
     * @param guids
     * @return
     */
    @Override
    public List<String> getNamesForGuids(List<String> guids) {
        List<String> names = null;
        if(enabled){
            try {
                final String jsonUri = bieUriPrefix + "/species/namesFromGuids.json";
                String params = "?guid=" + StringUtils.join(guids, "&guid=");
                names = restTemplate.postForObject(jsonUri + params, null, List.class);
            } catch (Exception ex) {
                logger.error("Requested URI: " + bieUriPrefix + "/species/namesFromGuids.json");
                logger.error("With POST body: guid=" + StringUtils.join(guids, "&guid="));
                logger.error("RestTemplate error: " + ex.getMessage(), ex);
            }
        }
        
        return names;
    }

    @Override
    public List<Map<String, String>> getNameDetailsForGuids(List<String> guids) {
        List<Map<String,String>> results =null;
        if(enabled){
            final String url = bieUriPrefix + "/species/guids/bulklookup.json";
            try{
                //String jsonString="";
                Map searchDTOList = restTemplate.postForObject(url, guids, Map.class);
                //System.out.println(test);
                results = (List<Map<String,String>>)searchDTOList.get("searchDTOList");
            } catch (Exception ex) {
                logger.error("Requested URI: " + url);
                logger.error("With POST body: guid=" + StringUtils.join(guids, "&guid="));
                logger.error("RestTemplate error: " + ex.getMessage(), ex);
            }
        }
        return results;
    }

    @Override
    public Map<String, List<Map<String, String>>> getSynonymDetailsForGuids(
            List<String> guids) {
        Map<String,List<Map<String, String>>> results = null;
        if(enabled){
            final String url = bieUriPrefix + "/species/bulklookup/namesFromGuids.json";
            try{
                results = restTemplate.postForObject(url, guids, Map.class); 
            } catch(Exception ex){
                logger.error("Requested URI: " + url);
                logger.error("With POST body: guid=" + StringUtils.join(guids, "&guid="));
                logger.error("RestTemplate error: " + ex.getMessage(), ex);
            }
        }
        return results;
    }
    @Override
    public List<String[]> getSpeciesDetails(List<String> guids,List<Long> counts, boolean includeCounts, boolean includeSynonyms){
        List<String[]> details= new  java.util.ArrayList<String[]>(guids.size());
        List<Map<String,String>> values =getNameDetailsForGuids(guids);
        Map<String,List<Map<String, String>>> synonyms = includeSynonyms? getSynonymDetailsForGuids(guids):new HashMap<String,List<Map<String,String>>>();
        int size = includeSynonyms&&includeCounts?13:((includeCounts && !includeSynonyms)||(includeSynonyms && !includeCounts))?12:11;


        for(int i =0 ;i<guids.size();i++){
            int countIdx=11;
            String[] row = new String[size];
            //guid
            String guid = guids.get(i);
            row[0]=guid;
            if(values!= null && synonyms != null){
                Map<String,String> map = values.get(i);
                if(map!=null){
                    //scientific name
                    row[1]=map.get("nameComplete");
                    row[2]=map.get("author");
                    row[3]=map.get("rank");
                    row[4]=map.get("kingdom");
                    row[5]=map.get("phylum");
                    row[6]=map.get("classs");
                    row[7]=map.get("order");
                    row[8]=map.get("family");
                    row[9]=map.get("genus");
                    row[10]=map.get("commonNameSingle");
                }

                if(includeSynonyms){
                    //retrieve a list of the synonyms
                    List<Map<String,String>> names =synonyms.get(guid);
                    StringBuilder sb =new StringBuilder();
                    for(Map<String,String> n :names){
                        if(!guid.equals(n.get("guid"))){
                            if(sb.length()>0){
                                sb.append(",");
                            }
                            sb.append(n.get("name"));
                        }
                    }
                    row[11] =sb.toString();
                    countIdx=12;
                }
            }
            if(includeCounts){
                row[countIdx] = counts.get(i).toString();
            }
            details.add(row);
        }


        return details;
    }

    @Override
    public String[] getHeaderDetails(String field,boolean includeCounts, boolean includeSynonyms){
        if(baseHeader == null){
            //initialise all the headers
            initHeaders();
        }
        String[] startArray = baseHeader;
        if(includeCounts){
            if(includeSynonyms){
                startArray=countSynonymHeader;
            } else{
                startArray = countBaseHeader;
            }
        } else if(includeSynonyms){
            startArray = synonymHeader;
        }
        return (String[])ArrayUtils.add(startArray, 0, messageSource.getMessage("facet."+field, null, field,null));
    }

    /**
     * initialise the common header components that will be added to the the supplied header field.
     */
    private void initHeaders(){
        baseHeader = new String[]{messageSource.getMessage("species.name", null,"Species Name", null),
                messageSource.getMessage("species.author", null,"Scientific Name Author", null),
                messageSource.getMessage("species.rank", null,"Taxon Rank", null),
                messageSource.getMessage("species.kingdom", null,"Kingdom", null),
                messageSource.getMessage("species.phylum", null,"Phylum", null),
                messageSource.getMessage("species.class", null,"Class", null),
                messageSource.getMessage("species.order", null,"Order", null),
                messageSource.getMessage("species.family", null,"Family", null),
                messageSource.getMessage("species.genus", null,"Genus", null),
                messageSource.getMessage("species.common", null,"Vernacular Name", null)};
        countBaseHeader = (String[]) ArrayUtils.add(baseHeader,messageSource.getMessage("species.count", null,"Number of Records", null));
        synonymHeader = (String[]) ArrayUtils.add(baseHeader,messageSource.getMessage("species.synonyms", null,"Synonyms", null));
        countSynonymHeader = (String[]) ArrayUtils.add(synonymHeader,messageSource.getMessage("species.count", null,"Number of Records", null));
    }
}
