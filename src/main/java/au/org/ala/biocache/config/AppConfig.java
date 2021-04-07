package au.org.ala.biocache.config;

import au.org.ala.biocache.service.RestartDataService;
import au.org.ala.biocache.service.NameMatchSpeciesLookupService;
import au.org.ala.biocache.service.BieSpeciesLookupService;
import au.org.ala.biocache.service.SpeciesLookupService;
import au.org.ala.dataquality.api.QualityServiceRpcApi;
import au.org.ala.dataquality.client.ApiClient;
import au.org.ala.names.ws.client.ALANameUsageMatchServiceClient;
import au.org.ala.ws.ClientConfiguration;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.web.client.RestOperations;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URL;

/**
 * This class handles the switching between implementations of interfaces based on
 * external configuration.
 */
@Configuration
public class AppConfig {

    private final static Logger logger = Logger.getLogger(AppConfig.class);

    @Inject
    private AbstractMessageSource messageSource; // use for i18n of the headers

    @Value("${name.index.dir:/data/lucene/namematching}")
    protected String nameIndexLocation;

    @Inject
    @Qualifier("restTemplate")
    private RestOperations restTemplate; // NB MappingJacksonHttpMessageConverter() injected by Spring

    /** URI prefix for bie-service - may be overridden in properties file */
    @Value("${service.bie.ws.url:https://bie-ws.ala.org.au/ws}")
    protected String bieUriPrefix;

    //NC 20131018: Allow service to be disabled via config (enabled by default)
    @Value("${service.bie.enabled:false}")
    protected Boolean enabledBie;

    //Disable the default that autocomplete uses the local names index. For use when there are no local names index files.
    @Value("${service.autocomplete.local.enabled:true}")
    protected Boolean autocompleteLocalEnabled;

    // Configuration for facets
    @Value("${facet.config:/data/biocache/config/facets.json}")
    protected String facetConfig;
    @Value("${facets.max:4}")
    protected Integer facetsMax;
    @Value("${facet.default:true}")
    protected Boolean facetDefault;

    // Configuration for DataQualityApi
    @Value("${dataquality.baseUrl:https://dataquality.ala.org.au/}")
    protected String dataQualityBaseUrl;


    //Set RestartDataService.dir before classes using RestartDataService are instantiated.
    @Value("${restart.data.dir:/tmp}")
    public void setDatabase(String dir) {
        logger.debug("setting RestartDataService.dir: " + dir);
        RestartDataService.dir = dir;
    }

    @Value("${namesearch.url:http://localhost:9179}")
    String nameSearchUrl = "http://localhost:9179";

    @Value("${namesearch.timeout:30}")
    Integer nameSearchTimeout = 30;

    @Value("${namesearch.cache.size:50}")
    Integer nameSearchCacheSize = 50;


    public @Bean(name = "nameUsageMatchService")
    ALANameUsageMatchServiceClient nameUsageMatchService() throws IOException {

        ClientConfiguration clientConfiguration =
                ClientConfiguration.builder()
                        .baseUrl(new URL(nameSearchUrl))
                        .timeOut(nameSearchTimeout * 1000) // Geocode service connection time-out
                        .cacheSize(nameSearchCacheSize * 1024 * 1024)
                        .build();

        return new ALANameUsageMatchServiceClient(clientConfiguration);
    }

    protected SpeciesLookupService getBieSpeciesLookupService() {
        logger.info("Initialising BIE rest-based species lookup services.");
        BieSpeciesLookupService service = new BieSpeciesLookupService();
        service.setBieUriPrefix(bieUriPrefix);
        service.setEnabled(enabledBie);
        service.setRestTemplate(restTemplate);
        service.setMessageSource(messageSource);
        return service;
    }

    protected SpeciesLookupService getNameMatchSpeciesLookupService() {
        logger.info("Initialising name match species lookup services.");
        NameMatchSpeciesLookupService service = new NameMatchSpeciesLookupService();
        service.setMessageSource(messageSource);
        return service;
    }

    public @Bean(name = "speciesLookupService")
    SpeciesLookupService speciesLookupServiceBean() {
        logger.info("Initialising species lookup services.");
        if (enabledBie){
            return getBieSpeciesLookupService();
        } else {
            return getNameMatchSpeciesLookupService();
        }
    }

    public @Bean(name = "speciesLookupIndexService")
    SpeciesLookupService speciesLookupIndexService() {
        logger.info("Initialising species lookup services.");
        try {
            if (autocompleteLocalEnabled) {
                return getNameMatchSpeciesLookupService();
            }
        } catch (Exception e) {
            logger.error("Failed to initialise local species lookup service for use with the species autocomplete ws. Attempting to use BIE instead.");
        }
        if (enabledBie) {
            return getBieSpeciesLookupService();
        } else {
            return null;
        }
    }

    @Bean("dataQualityApiClient")
    public ApiClient dataQualityApiClient() {
        ApiClient apiClient = new ApiClient();
        apiClient.getAdapterBuilder().baseUrl(dataQualityBaseUrl);
        return apiClient;
    }

    @Bean
    public QualityServiceRpcApi dataQualityApi(@Qualifier("dataQualityApiClient") ApiClient dataQualityApiClient) {
        return dataQualityApiClient.createService(QualityServiceRpcApi.class);
    }
}
