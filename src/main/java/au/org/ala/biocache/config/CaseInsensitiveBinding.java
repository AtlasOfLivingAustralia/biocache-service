package au.org.ala.biocache.config;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.CachedIntrospectionResults;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedCaseInsensitiveMap;

import javax.annotation.PostConstruct;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Make DTOs used as webservice parameters case insensitive when binding.
 * <p>
 * This class is used instead of a filter + coding convention implementation.
 * <p>
 * Looks like this should be current from for the version in use (3.2.18.RELEASE) to at least spring-framework 5.1.5
 */
@Component
public class CaseInsensitiveBinding {

    private final static Logger logger = Logger.getLogger(CaseInsensitiveBinding.class);

    /**
     * Comma delimited list of classes to make case insensitive during webservice parameter binding.
     * <p>
     * Use full class path or one of au.org.ala.biocache.dto.*
     */
    @Value("${case.insensitive.dto.list:ImageParams,WmsBbox,WmsCapabilitiesFilter,WmsCoordinate,WmsDimensions,WmsQueryParams,WmsRequest,WmsSrs,WmsStyle,WmsTileParams,SpatialSearchRequestParams,DownloadRequestParams,SearchRequestParams}")
    private String classNames;

    @PostConstruct
    public void init() {
        for (String className : classNames.split(",")) {
            if (StringUtils.isEmpty(className)) {
                continue;
            }

            Class c = null;
            try {
                c = Class.forName(className);
            } catch (ClassNotFoundException e) {
                try {
                    // try au.org.ala.biocache.dto.*
                    c = Class.forName("au.org.ala.biocache.dto." + className);
                } catch (ClassNotFoundException ex) {
                    logger.error("Failed to find class with name '" + className + "' or 'au.org.ala.biocache.dto." + className + "' within config case.insensitive.dto.list.");
                    continue;
                }
            }

            try {
                // init CachedIntrospectionResults for this class
                Method forClass = CachedIntrospectionResults.class.getDeclaredMethod("forClass", Class.class);
                forClass.setAccessible(true);
                CachedIntrospectionResults cached = (CachedIntrospectionResults) forClass.invoke(null, c);
                forClass.setAccessible(false);

                // change LinkedHashMap to LinkedCaseInsensitiveMap
                Field propertyDescriptorCache = CachedIntrospectionResults.class.getDeclaredField("propertyDescriptorCache");
                propertyDescriptorCache.setAccessible(true);
                Map<String, PropertyDescriptor> caseInsensitiveMap = new LinkedCaseInsensitiveMap<>();
                caseInsensitiveMap.putAll((Map<String, PropertyDescriptor>) propertyDescriptorCache.get(cached));
                propertyDescriptorCache.set(cached, caseInsensitiveMap);
                propertyDescriptorCache.setAccessible(false);
            } catch (Exception e) {
                logger.error("Failed to make CachedIntrospectionResults.propertyDescriptorCache case insensitive for '" + c.getName() + "'.", e);
            }
        }
    }
}
