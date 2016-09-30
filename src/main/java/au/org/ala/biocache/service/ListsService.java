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

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.net.URI;
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

    @Value("${list.tool.lookup.enabled:true}")
    private Boolean enabled;

    @Value("${list.tool.url:http://lists.ala.org.au}")
    private String speciesListUrl;

    private Map<String, Map<String, Set<String>>> data = new HashMap();

    @PostConstruct
    private void init() {
        refreshCache();
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
        if (enabled) {
            try {
                Map threatened = restTemplate.getForObject(new URI(speciesListUrl + "/ws/speciesList/?isThreatened=eq:true&isAuthoritative=eq:true"), Map.class);
                Map invasive = restTemplate.getForObject(new URI(speciesListUrl + "/ws/speciesList/?isInvasive=eq:true&isAuthoritative=eq:true"), Map.class);

                data.put("Conservation", getItemsMap(threatened));
                data.put("Invasive", getItemsMap(invasive));

            } catch (Exception e) {
                logger.error("failed to get species lists for threatened or invasive species", e);
            }
        }
        wait.countDown();
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

    private List<String> getListItems(String dataResourceUid) throws Exception {
        List<String> list = new ArrayList();

        List speciesListItems = restTemplate.getForObject(new URI(speciesListUrl + "/ws/speciesListItems/" + dataResourceUid), List.class);

        for (Object s : speciesListItems) {
            Map m = (Map) s;

            if (m.containsKey("lsid") && m.get("lsid") != null) {
                list.add(m.get("lsid").toString());
            }
        }

        return list;
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
}
