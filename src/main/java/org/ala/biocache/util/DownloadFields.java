
package org.ala.biocache.util;

import java.io.InputStream;
import java.util.*;
import au.org.ala.biocache.Store;

import org.ala.biocache.dto.IndexFieldDTO;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * Stores the download fields whose values can be overridden in
 * a properties file
 *
 * @author "Natasha Carter <Natasha.Carter@csiro.au>"
 */
public class DownloadFields {

    private String defaultFields = "uuid,dataResourceUid,catalogNumber,taxonConceptID.p,scientificName,vernacularName,scientificName.p," +
            "taxonRank.p,vernacularName.p,kingdom.p,phylum.p,classs.p,order.p,family.p,genus.p,species.p,subspecies.p," +
            "institutionCode,collectionCode,longitude.p,latitude.p,coordinatePrecision,country.p,ibra.p,imcra.p," +
            "stateProvince.p,lga.p,minimumElevationInMeters,maximumElevationInMeters,minimumDepthInMeters," +
            "maximumDepthInMeters,year.p,month.p,day.p,eventDate.p,eventTime.p,basisOfRecord,typeStatus.p,sex,preparations";

    private Properties downloadProperties;
    private Map<String,IndexFieldDTO>indexFieldMaps;

    public DownloadFields(Set<IndexFieldDTO> indexFields){
        //initialise the properties
        try{
            downloadProperties = new Properties();
            InputStream is = getClass().getResourceAsStream("/download.properties");
            downloadProperties.load(is);
            indexFieldMaps = new TreeMap<String,IndexFieldDTO>();
            for(IndexFieldDTO field: indexFields)
                indexFieldMaps.put(field.getName(), field);
        }
        catch(Exception e){
            e.printStackTrace();
        }
        if(downloadProperties.getProperty("fields") == null)
            downloadProperties.setProperty("fields", defaultFields);
    }

    /**
     * Get the name of the field that should be included in the download.
     * @return
     */
    public String getFields(){
        return downloadProperties.getProperty("fields");
    }

    /**
     * Gets the header for the file
     * @param values
     * @return
     */
    public String[] getHeader(String[] values, boolean useSuffix){
        String[] header = new String[values.length];
        for(int i =0 ;i<values.length;i++){
            //attempt to get the headervalue from the properties
            String v = downloadProperties.getProperty(values[i]);
            header[i] = v!=null ?v :generateTitle(values[i], useSuffix);
        }
        return header;
    }

    /**
     * Generates a default title for a field that does NOT have an i18n
     * @param v
     * @return
     */
    private String generateTitle(String v, boolean useSuffix){
        String value = v;
        String suffix ="";
        if(value.endsWith(".p")){
            suffix = " - Processed";
            v =v.replaceAll("\\.p", "");
        }
        value = StringUtils.join(StringUtils.splitByCharacterTypeCamelCase(v)," ");
        if(useSuffix)
            value += suffix;
        return value;
    }

    /**
     * returns the index fields that are used for the supplied values
     * @param values
     * @return
     */
    public List<String>[] getIndexFields(String[] values){
        java.util.List<String> mappedNames = new java.util.LinkedList<String>();
        java.util.List<String> headers = new java.util.LinkedList<String>();
        java.util.List<String> unmappedNames = new java.util.LinkedList<String>();
        java.util.Map<String,String> storageFieldMap = Store.getStorageFieldMap();
        for(String value : values){
            //check to see if it is the the
            String indexName = storageFieldMap.containsKey(value)?storageFieldMap.get(value):value;
            //now check to see if this index field is stored
            IndexFieldDTO field = indexFieldMaps.get(indexName);
            if((field != null && field.isStored()) || value.startsWith("sensitive")){
                mappedNames.add(indexName);
                headers.add(downloadProperties.getProperty(value, generateTitle(value,true)));
            }
            else
                unmappedNames.add(indexName);
        }
        return new List[]{mappedNames,unmappedNames,headers};
    }
}
