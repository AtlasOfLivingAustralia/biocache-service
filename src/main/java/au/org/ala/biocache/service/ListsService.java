/**************************************************************************
 * Copyright (C) 2013 Atlas of Living Australia
 * All Rights Reserved.
 * <p>
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 * <p>
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.biocache.service;

import au.org.ala.biocache.dto.Kvp;
import au.org.ala.biocache.service.ListsService.SpeciesListItemDTO.KvpDTO;
import au.org.ala.biocache.service.ListsService.SpeciesListSearchDTO.SpeciesListDTO;
import au.org.ala.biocache.util.SearchUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Service to find the Threatened and Invasive species lists for LSIDs.
 */
@Component("listsService")
public class ListsService {

    private static final Logger logger = Logger.getLogger(DownloadService.class);

    @Inject
    protected RestOperations restTemplate; // NB MappingJacksonHttpMessageConverter() injected by Spring

    @Inject
    protected SearchUtils searchUtils;

    @Value("${list.tool.lookup.enabled:true}")
    private Boolean enabled;

    @Value("${list.tool.url:https://lists.ala.org.au}")
    private String speciesListUrl;

    private Map<String, Map<String, Set<String>>> data = RestartDataService.get(this, "data", new TypeReference<HashMap<String, Map<String, Set<String>>>>(){}, HashMap.class);

    @PostConstruct
    private void init() {
    }

    private CountDownLatch wait = new CountDownLatch(1);

    public Map<String, Map<String, Set<String>>> getValues() {
        try {
            wait.await();
        } catch (InterruptedException e) {
        }
        return data;
    }

    @Scheduled(fixedDelay = 43200000)// schedule to run every 12 hours
    public void refreshCache() {
        if (data.size() > 0) {
            //data exists, no need to wait
            wait.countDown();
        }

        if (enabled && StringUtils.isNotBlank(speciesListUrl)) {

            new Thread() {
                @Override
                public void run() {
                    try {
                        HashMap map = new HashMap();

                        Map threatened = restTemplate.getForObject(new URI(speciesListUrl + "/ws/speciesList/?isThreatened=eq:true&isAuthoritative=eq:true"), Map.class);
                        Map invasive = restTemplate.getForObject(new URI(speciesListUrl + "/ws/speciesList/?isInvasive=eq:true&isAuthoritative=eq:true"), Map.class);

                        if ((threatened != null && threatened.size() > 0) ||
                                (invasive != null && invasive.size() > 0)) {
                            map.put("Conservation", getItemsMap(threatened));
                            map.put("Invasive", getItemsMap(invasive));

                            data = map;
                        }
                    } catch (Exception e) {
                        logger.error("failed to get species lists for threatened or invasive species", e);
                    }
                    wait.countDown();
                }
            }.start();
        } else {
            wait.countDown();
        }
    }

    private Map<String, Set<String>> getItemsMap(Map speciesLists) throws Exception {
        List ja = (List) speciesLists.get("lists");
        Map<String, Set<String>> map = new HashMap();
        for (int i = 0; i < ja.size(); i++) {
            String name = ((Map) ja.get(i)).get("listName").toString();
            String dr = ((Map) ja.get(i)).get("dataResourceUid").toString();
            List<String> items = getListItems(dr);
            for (String item : items) {
                Set<String> existing = map.get(item);
                if (existing == null) {
                    existing = new HashSet<String>();
                }
                existing.add(name);
                map.put(item, existing);
            }
        }

        return map;
    }

    @Cacheable("speciesListItems")
    public List<String> getListItems(String dataResourceUid) throws Exception {
        List<String> list = new ArrayList();

        boolean hasAnotherPage = true;
        int max = 400; // response size can be limited by the gateway
        int offset = 0;

        while (hasAnotherPage) {
            List<SpeciesListItemDTO> speciesListItems = restTemplate.getForObject(new URI(speciesListUrl + "/ws/speciesListItems/" + dataResourceUid + "?max=" + max + "&offset=" + offset), SpeciesListItemsDTO.class);

            offset += max;
            hasAnotherPage = speciesListItems.size() == max;

            for (SpeciesListItemDTO s : speciesListItems) {
                if (s.lsid != null) {
                    list.add(s.lsid);
                }
            }
        }

        return list;
    }

    /**
     * Get species list KVP data object.
     * <p>
     * This is used as input into other ListsService functions.
     *
     * @param dataResourceUid
     * @return species list KVP data for use in other ListsService functions.
     */
    @Cacheable("speciesKvp")
    public List<Kvp> getKvp(String dataResourceUid) {
        List<Kvp> list = new ArrayList();

        boolean hasAnotherPage = true;
        int max = 400;  // response size can be limited by api gateway
        int offset = 0;

        try {
            while (hasAnotherPage) {
                SpeciesListItemsDTO speciesListItems = restTemplate.getForObject(new URI(speciesListUrl + "/ws/speciesListItems/" + dataResourceUid + "?includeKVP=true&max=" + max + "&offset=" + offset), SpeciesListItemsDTO.class);

                offset += max;
                hasAnotherPage = speciesListItems.size() == max;

                for (SpeciesListItemDTO item : speciesListItems) {
                    if (item.lsid != null) {
                        // ignore species list item when there are no lft rgt values for the LSID
                        String fq = searchUtils.getTaxonSearch(item.lsid)[0];
                        if (fq.startsWith("lft:[")) {
                            List<String> keys = new ArrayList<>();
                            List<String> values = new ArrayList<>();

                            for (KvpDTO kvp : item.kvpValues) {
                                keys.add(kvp.key);
                                values.add(kvp.value);
                            }

                            long lft = Long.parseLong(fq.replaceAll("(.*\\[| TO.*)", ""));

                            long rgt = Long.parseLong(fq.replaceAll("(.*TO |\\].*)", ""));
                            Kvp kvp = new Kvp(lft, rgt, keys, values);

                            list.add(kvp);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("failed to get species list kvp for list: " + dataResourceUid, e);
        }

        if (list.size() > 0) {
            list.sort(Kvp.KvpComparator);
            return list;
        } else {
            return null;
        }
    }

    public List<String> getTypes() {
        try {
            wait.await();
        } catch (InterruptedException e) {
        }

        return new ArrayList<String>(data.keySet());
    }

    public Set<String> get(String type, String lsid) {
        try {
            wait.await();
        } catch (InterruptedException e) {
        }
        return data.get(type).get(lsid);
    }

    @Cacheable("speciesListItems")
    public SpeciesListDTO getListInfo(String dr) throws URISyntaxException {

        SpeciesListDTO speciesList = restTemplate.getForObject(new URI(speciesListUrl + "/ws/speciesList/" + dr), SpeciesListDTO.class);

        return speciesList;
    }

    /**
     * Get list of field names for a species list dr's additional fields that are stored as kvps.
     * <p>
     * The identifer is "dr name - key name".
     *
     * @param dr   species list data resource uid
     * @param kvps KVP data returned by getKvp(dr)
     * @return list of kvp field identifiers
     */
    public List<String> getKvpNames(String dr, List<Kvp> kvps) {

        List<String> names = new ArrayList();

        try {
            SpeciesListDTO listInfo = getListInfo(dr);

            if (kvps != null && kvps.size() > 0) {
                for (String key : kvps.get(0).keys) {
                    names.add(listInfo.listName + " - " + key);
                }
            }
        } catch (Exception e) {

        }

        return names;
    }

    /**
     * Get list of identifiers for a species list dr's additional fields that are stored as kvps.
     * <p>
     * The identifer is "dr.idx" where idx is n'th kvp value.
     *
     * @param dr   species list data resource uid
     * @param kvps KVP data returned by getKvp(dr)
     * @return list of kvp field identifiers
     */
    public List<String> getKvpFields(String dr, List<Kvp> kvps) {
        List<String> fields = new ArrayList();

        if (kvps != null && kvps.size() > 0) {
            int keyIdx = 0;
            for (String key : kvps.get(0).keys) {
                fields.add(dr + "." + keyIdx);
                keyIdx++;
            }
        }

        return fields;
    }

    /**
     * Get value for a lsid's lft rgt values from a species list dr's additional fields that are stored as kvps.
     *
     * @param idx    integer to specify which kvp value
     * @param kvps   KVP data returned by getKvp(dr)
     * @param lftrgt lsid's lftrgt values as Kvp
     * @return this lsid and n'th kvp value as String
     */
    public String getKvpValue(int idx, List<Kvp> kvps, Kvp lftrgt) {
        String value = "";

        if (kvps != null && kvps.size() > idx) {
            Kvp kvp = find(kvps, lftrgt);
            if (kvp != null) {
                value = kvp.values.get(idx);
            }
        }

        return value;
    }

    public Kvp find(List<Kvp> kvps, Kvp lftrgt) {
        int idx = Collections.binarySearch(kvps, lftrgt, Kvp.KvpComparator);
        if (idx >= 0) {
            return kvps.get(idx);
        } else {
            // reverse through kvps until a match is found
            idx = Math.min(idx * -1, kvps.size() -1);
            while (idx >= 0) {
                if (kvps.get(idx).contains(lftrgt)) {
                    return kvps.get(idx);
                }
                idx--;
            }

            return null;
        }
    }

    public static class SpeciesListSearchDTO {
        public int listCount;
        public String sort;
        public String order;
        public int max;
        public int offset;
        public List<SpeciesListDTO> lists = new ArrayList<>();

        public Optional<SpeciesListDTO> findSpeciesListByDataResourceId(@NotNull String drId) {
            return lists.stream().filter(dto -> drId.equalsIgnoreCase(dto.dataResourceUid)).findFirst();
        }

        public static class SpeciesListDTO {
            public String dataResourceUid;
            public String listName;
            public String listType;
            public Date dateCreated;
            public Date lastUpdated;
            public String username;
            public String fullName;
            public int itemCount;
            public String region;
            public String category;
            public String generalisation;
            public String authority;
            public String sdsType;
            public boolean isAuthoritative;
            public boolean isInvasive;
            public boolean isThreatened;
        }
    }

    public static class SpeciesListItemDTO {
        public long id;
        public String name;
        public String commonName;
        public String scientificName;
        public String lsid;
        public String dataResourceUid;
        public List<KvpDTO> kvpValues;

        public static class KvpDTO {
            public String key;
            public String value;
        }
    }

    public static class SpeciesListItemsDTO extends ArrayList<SpeciesListItemDTO> {

    }
}

