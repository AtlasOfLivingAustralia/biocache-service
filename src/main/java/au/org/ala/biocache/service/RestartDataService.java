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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manage dynamic data that is loaded during startup.
 */
@Component("restartDataService")
public class RestartDataService {

    protected static final Logger logger = Logger.getLogger(RestartDataService.class);

    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private static Map<Object, List<String>> sources = new ConcurrentHashMap<Object, List<String>>();
    private static Map<String, Object> values = new ConcurrentHashMap<>();

    //dir is set by AppConfig
    public static String dir;

    @Value("${restart.data.enabled:true}")
    public Boolean enabled;

    private Thread loop;

    @PostConstruct
    public void init() {

        if (enabled) {
            new File(dir).mkdirs();

            jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            loop = new Thread() {
                @Override
                public void run() {
                    try {
                        while (true) {

                            for (Object rc : sources.keySet()) {
                                for (String field : sources.get(rc)) {
                                    try {
                                        Field f = rc.getClass().getDeclaredField(field);
                                        if (!f.isAccessible()) {
                                            f.setAccessible(true);
                                        }
                                        Object value = f.get(rc);

                                        String key = rc.getClass().toString() + field;
                                        if (value != values.get(key)) {
                                            saveToDisk(key, value);
                                            values.remove(key);
                                            values.put(key, value);
                                        }
                                    } catch (Exception e) {
                                        logger.error("error checking value: " + rc.getClass().toString() + field, e);
                                    }
                                }
                            }

                            sleep(10000);
                        }
                    } catch (InterruptedException e) {
                    }
                }
            };

            loop.setName("restart-data-service");
            loop.start();
        }
    }

    private static Object loadFromDisk(String key, TypeReference type) {
        String path = dir + File.separator + key;
        try {
            synchronized (jsonMapper) {
                //get value
                File file = new File(path);
                Object diskValue = null;
                if (file.exists()) {
                    jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                    jsonMapper.getTypeFactory().constructType(type);

                    try {
                        diskValue = jsonMapper.readValue(file, type);
                    } catch (Exception e) {}

                    if (diskValue == null) {
                        //try backup
                        file = new File(path + ".backup");
                        if (file.exists()) {
                            diskValue = jsonMapper.readValue(file, type);
                        }
                    }

                    return diskValue;
                }
            }
        } catch (Exception e) {
            logger.error("failed to read: " + path + " into type:" + (type != null ? type.toString() : "null"), e);
        }
        return null;
    }

    private void saveToDisk(String key, Object value) {
        String path = dir + File.separator + key;
        try {
            synchronized (jsonMapper) {
                File file = new File(path);
                if (file.exists()) {
                    File backup = new File(path + ".backup");
                    if (backup.exists()) backup.delete();
                    FileUtils.moveFile(file, backup);
                }
                jsonMapper.writeValue(file, value);
            }
            logger.debug("writing " + path + " to disk");
        } catch (Exception e) {
            logger.error("failed to save to disk: " + path, e);
        }
    }


    public static <T> T get(Object parent, String name, TypeReference typeRef, Class<T> defaultValue) {
        if (typeRef == null) {
            logger.error("defaultValue cannot be null: " + parent.toString() + " " + name);
        }
        if (sources.containsKey(parent)) {
            sources.get(parent).add(name);
        } else {
            List list = new ArrayList<String>();
            list.add(name);
            sources.put(parent, list);
        }

        String key = parent.getClass().toString() + name;

        T value = null;
        try {
            value = (T) loadFromDisk(key, typeRef);
            if (value == null) {
                value = defaultValue.newInstance();
            } else {
                logger.debug("reading " + parent.getClass().toString() + " " + name + " from disk cache");
            }
            values.put(key, value);
        } catch (Exception e) {
            logger.error("failed to instantiate: " + defaultValue != null ? defaultValue.toString() : "null", e);
        }

        return value;
    }
}
