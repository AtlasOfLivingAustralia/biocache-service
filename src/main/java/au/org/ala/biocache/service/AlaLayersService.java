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

import au.org.ala.biocache.dto.SpatialObjectDTO;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.zip.ZipInputStream;

/**
 * The ALA Spatial portal implementation for the layer service.
 * Metadata information will be cached from spatial webservices.
 *
 * @author Natasha Carter (natasha.carter@csiro.au)
 */
@Component("layersService")
public class AlaLayersService implements LayersService {

    private final static Logger logger = LoggerFactory.getLogger(AlaLayersService.class);

    private Map<String, String> idToNameMap = RestartDataService.get(this, "idToNameMap", new TypeReference<HashMap<String, String>>() {
    }, HashMap.class);
    private List<Map<String, Object>> layers = RestartDataService.get(this, "layers", new TypeReference<ArrayList<Map<String, Object>>>() {
    }, ArrayList.class);
    private Map<String, String> extraLayers = new HashMap<String, String>();

    //NC 20131018: Allow cache to be disabled via config (enabled by default)
    @Value("${caches.layers.enabled:true}")
    protected Boolean enabled = null;

    @Value("${spatial.layers.url:https://spatial.ala.org.au/ws/fields}")
    protected String spatialUrl;

    @Value("${layers.service.download.sample:true}")
    protected Boolean layersServiceAnalysisLayers;

    @Value("${layers.service.url:https://spatial.ala.org.au/ws}")
    protected String layersServiceUrl;

    protected Map<String, Integer> distributions = RestartDataService.get(this, "distributions", new TypeReference<HashMap<String, Integer>>() {
    }, HashMap.class);
    protected Map<String, Integer> checklists = RestartDataService.get(this, "checklists", new TypeReference<HashMap<String, Integer>>() {
    }, HashMap.class);
    protected Map<String, Integer> tracks = RestartDataService.get(this, "tracks", new TypeReference<HashMap<String, Integer>>() {
    }, HashMap.class);

    @Inject
    private RestOperations restTemplate; // NB MappingJacksonHttpMessageConverter() injected by Spring

    private CountDownLatch wait = new CountDownLatch(1);

    @Override
    public Map<String, String> getLayerNameMap() {
        try {
            wait.await();
        } catch (InterruptedException e) {
        }
        return idToNameMap;
    }

    @Scheduled(fixedDelay = 43200000)// schedule to run every 12 hours
    public void refreshCache() {
        init();
    }

    @PostConstruct
    public void init() {
        // add FormHttpMessageConverter
        ((RestTemplate) restTemplate).getMessageConverters().add(new FormHttpMessageConverter());

        if (layers.size() > 0) {
            //data exists, no need to wait
            wait.countDown();
        }

        //initialise the cache based on the values at https://spatial.ala.org.au/ws/fields
        if (enabled) {
            new Thread() {
                @Override
                public void run() {
                    try {
                        //create a tmp map
                        Map tmpMap = new HashMap<String, String>();
                        List list = restTemplate.getForObject(spatialUrl, List.class);
                        if (list != null && list.size() > 0) layers = list;
                        for (Map<String, Object> values : layers) {
                            if ((Boolean) values.get("enabled") && (Boolean) values.get("indb")) {
                                tmpMap.put((String) values.get("id"), (String) values.get("desc"));
                            }
                        }

                        if (tmpMap.size() > 0) idToNameMap = tmpMap;

                        tmpMap = initDistribution("distributions");
                        if (tmpMap.size() > 0) distributions = tmpMap;

                        tmpMap = initDistribution("checklists");
                        if (tmpMap.size() > 0) checklists = tmpMap;

                        tmpMap = initDistribution("tracks");
                        if (tmpMap.size() > 0) tracks = tmpMap;
                    } catch (Exception e) {
                        logger.error("failed to init distribution and checklists", e);
                    }

                    wait.countDown();
                }
            }.start();
        } else {
            wait.countDown();
        }
    }

    @Override
    public String getName(String code) {
        try {
            wait.await();
        } catch (InterruptedException e) {
        }
        return idToNameMap.get(code);
    }

    public String findAnalysisLayerName(String analysisLayer, String layersServiceUrl) {

        String url = this.layersServiceUrl;
        if (StringUtils.isNotEmpty(layersServiceUrl)) url = layersServiceUrl;

        if (!layersServiceAnalysisLayers) {
            return null;
        }

        if (extraLayers.containsKey(analysisLayer)) {
            return extraLayers.get(analysisLayer);
        }

        String found = null;
        if (StringUtils.isNotBlank(url)) {
            String intersectUrl = null;
            try {
                //get analysis layer display name
                intersectUrl = url + "/intersect/" + URLEncoder.encode(analysisLayer, "UTF-8") + "/1/1";
                List json = restTemplate.getForObject(intersectUrl, List.class);
                if (json != null && json.size() > 0) {
                    found = (String) ((Map) json.get(0)).get("layername");
                }

                extraLayers.put(analysisLayer, found);
            } catch (Exception ex) {
                logger.error("RestTemplate error for " + url + ": " + ex.getMessage(), ex);
            }
        }

        return found;
    }

    public Integer getDistributionsCount(String lsid) {
        try {
            wait.await();
        } catch (InterruptedException e) {
        }

        Integer count = distributions.get(lsid);

        return count != null ? count : 0;
    }

    public Integer getChecklistsCount(String lsid) {
        try {
            wait.await();
        } catch (InterruptedException e) {
        }

        Integer count = checklists.get(lsid);

        return count != null ? count : 0;
    }

    public Integer getTracksCount(String lsid) {
        try {
            wait.await();
        } catch (InterruptedException e) {
        }

        Integer count = tracks.get(lsid);

        return count != null ? count : 0;
    }

    private Map initDistribution(String type) {
        Map<String, Integer> map = new HashMap<String, Integer>();

        String url = layersServiceUrl + "/" + type;
        try {
            if (org.apache.commons.lang.StringUtils.isNotBlank(layersServiceUrl)) {
                //get distributions
                List json = restTemplate.getForObject(url, List.class);
                if (json != null) {
                    for (int i = 0; i < json.size(); i++) {
                        String s = (String) ((Map) json.get(i)).get("lsid");
                        if (StringUtils.isNotEmpty(s)) {
                            Integer count = map.get(s);
                            if (count == null) count = 0;
                            count = count + 1;
                            map.put(s, count);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            //ignore errors for tracks
            if (!"tracks".equals(type)) {
                logger.error("RestTemplate error for " + url, ex);
            } else {
                logger.warn("RestTemplate warning for " + url + ". This is not fatal", ex);
            }
        }

        return map;
    }

    public String getLayersServiceUrl() {
        return layersServiceUrl;
    }

    @Override
    public Reader sample(String url, String[] analysisLayers, double[][] points) {
        // These are batches of 1000, so it should not take long to finish. 1000ms delay between status checks
        long sleepTime = 1000;

        try {
            HttpHeaders requestHeaders = new HttpHeaders();
            requestHeaders.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

            HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(sampleBody(analysisLayers, points), requestHeaders);
            Map status = (Map) restTemplate.postForObject(url + "/intersect/batch", request, Map.class);

            while (status.containsKey("statusUrl")) {
                Thread.sleep(sleepTime);
                status = (Map) restTemplate.getForObject((String) status.get("statusUrl"), Map.class);
            }

            if (status.containsKey("downloadUrl")) {
                URLConnection connection = new URL((String) status.get("downloadUrl")).openConnection();
                ZipInputStream zis = new ZipInputStream(connection.getInputStream());
                zis.getNextEntry();
                return new InputStreamReader(zis);
            }
        } catch (Exception e) {
            logger.error("layer sampling failed: " + url + ", " + e.getMessage());
        }
        return null;
    }

    private MultiValueMap<String, String> sampleBody(String[] analysisLayers, double[][] points) {
        MultiValueMap<String, String> response = new LinkedMultiValueMap<>();
        StringBuilder sb = new StringBuilder();

        //sb.append("fids=");
        for (int i = 0; i < analysisLayers.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(analysisLayers[i]);
        }
        response.add("fids", sb.toString());

        //sb.append("&points=");
        sb = new StringBuilder();
        for (int i = 0; i < points.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(points[i][1]).append(",").append(points[i][0]);
        }
        response.add("points", sb.toString());

        //return sb.toString();
        return response;
    }

    @Cacheable("spatialObject")
    @Override
    public SpatialObjectDTO getObject(String spatialObjectId) {
        String url = layersServiceUrl + "/object/" + Integer.parseInt(spatialObjectId);
        return restTemplate.getForObject(url, SpatialObjectDTO.class);
    }

    @Cacheable("wkt")
    @Override
    public String getObjectWkt(String spatialObjectId) {
        String url = layersServiceUrl + "/shape/wkt/" + Integer.parseInt(spatialObjectId);

        try {
            URLConnection con = new URL(url).openConnection();
            return StreamUtils.copyToString(con.getInputStream(), Charset.defaultCharset());
        } catch (Exception e) {
            return null;
        }
    }
}
